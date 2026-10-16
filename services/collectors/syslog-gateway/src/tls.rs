use crate::{
    config::{Config, TlsSettings},
    error::{Error, Result},
    relp, tcp, Intake,
};
use arc_swap::ArcSwap;
use contracts::netmon::v1::SyslogTransport;
use rustls::{pki_types::CertificateDer, server::WebPkiClientVerifier, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Cursor, Read},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::Semaphore, task::JoinSet};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

const MAX_FILE: u64 = 1024 * 1024;
struct Active {
    config: Arc<ServerConfig>,
    digest: [u8; 32],
}

/// An atomic snapshot is retained by every new handshake. Established sessions
/// retain their own rustls configuration when a validated replacement is swapped.
pub struct ReloadingTls {
    settings: TlsSettings,
    active: ArcSwap<Active>,
}

#[derive(Clone, Copy)]
pub enum Application {
    Syslog,
    Relp,
}

fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    let file = File::open(path).map_err(|_| Error::Tls("file unreadable"))?;
    if !file.metadata().map_err(|_| Error::Tls("file metadata unreadable"))?.is_file() {
        return Err(Error::Tls("regular files required"));
    }
    file.take(MAX_FILE + 1).read_to_end(&mut bytes).map_err(|_| Error::Tls("file unreadable"))?;
    if bytes.len() as u64 > MAX_FILE {
        return Err(Error::Tls("file exceeds 1 MiB limit"));
    }
    Ok(bytes)
}

fn certificates(bytes: &[u8], ca: bool) -> Result<Vec<CertificateDer<'static>>> {
    let certs = rustls_pemfile::certs(&mut Cursor::new(bytes))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Tls("invalid certificate PEM"))?;
    if certs.is_empty() || certs.len() > 128 {
        return Err(Error::Tls("empty or oversized certificate chain"));
    }
    for cert in &certs {
        let (remaining, parsed) =
            x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|_| Error::Tls("invalid X.509 certificate"))?;
        if !remaining.is_empty()
            || !parsed.validity().is_valid()
            || parsed.validity().not_before >= parsed.validity().not_after
        {
            return Err(Error::Tls("certificate expired, not yet valid or malformed"));
        }
        if ca && !parsed.is_ca() {
            return Err(Error::Tls("client trust bundle contains a non-CA certificate"));
        }
    }
    Ok(certs)
}

fn load(settings: &TlsSettings) -> Result<Active> {
    let cert = read(&settings.cert_file)?;
    let key = read(&settings.key_file)?;
    let ca = settings.client_ca_file.as_ref().map(|path| read(path)).transpose()?;
    let mut hash = Sha256::new();
    for bytes in [cert.as_slice(), key.as_slice(), ca.as_ref().map_or(&[][..], |ca| ca.as_slice())] {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    let certs = certificates(&cert, false)?;
    let private_key = rustls_pemfile::private_key(&mut Cursor::new(key.as_slice()))
        .map_err(|_| Error::Tls("invalid private key PEM"))?
        .ok_or(Error::Tls("private key missing"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Tls("protocol configuration invalid"))?;
    let builder = if let Some(ca) = ca {
        let mut roots = RootCertStore::empty();
        for cert in certificates(&ca, true)? {
            roots.add(cert).map_err(|_| Error::Tls("invalid client CA"))?;
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|_| Error::Tls("invalid client verifier"))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let config = builder
        .with_single_cert(certs, private_key)
        .map_err(|_| Error::Tls("invalid or mismatched certificate/private key"))?;
    Ok(Active { config: Arc::new(config), digest: hash.finalize().into() })
}

impl ReloadingTls {
    pub fn new(settings: TlsSettings) -> Result<Self> {
        let active = ArcSwap::from_pointee(load(&settings)?);
        Ok(Self { settings, active })
    }
    pub fn snapshot(&self) -> Arc<ServerConfig> {
        self.active.load().config.clone()
    }
    pub fn reload(&self) -> Result<bool> {
        let replacement = load(&self.settings)?;
        if self.active.load().digest == replacement.digest {
            return Ok(false);
        }
        self.active.store(Arc::new(replacement));
        Ok(true)
    }
    pub async fn watch(self: Arc<Self>, stop: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(self.settings.reload_interval);
        interval.tick().await;
        loop {
            tokio::select! { _ = stop.cancelled() => return Ok(()), _ = interval.tick() => {} }
            let manager = self.clone();
            match tokio::task::spawn_blocking(move || manager.reload()).await {
                Ok(Ok(true)) => tracing::info!(status = "tls_reloaded", "validated TLS configuration activated"),
                Ok(Ok(false)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, status = "tls_reload_rejected", "previous TLS configuration retained")
                }
                Err(_) => return Err(Error::Task),
            }
        }
    }
}

pub async fn listen(
    listener: TcpListener,
    cfg: Arc<Config>,
    tls: Arc<ReloadingTls>,
    intake: Intake,
    stop: CancellationToken,
    application: Application,
) -> Result<()> {
    let slots = Arc::new(Semaphore::new(cfg.max_connections));
    let mut connections = JoinSet::new();
    let failed = Arc::new(AtomicU64::new(0));
    let handshake_timeout = cfg.tls.as_ref().ok_or(Error::Tls("listener settings missing"))?.handshake_timeout;
    let transport = match application {
        Application::Syslog => SyslogTransport::Tls,
        Application::Relp => SyslogTransport::RelpTls,
    };
    loop {
        let permit = tokio::select! { _ = stop.cancelled() => break, permit = slots.clone().acquire_owned() => permit.map_err(|_| Error::Task)?, };
        let (socket, peer) =
            tokio::select! { _ = stop.cancelled() => break, accepted = listener.accept() => accepted?, };
        let (cfg, intake, stop, failed) = (cfg.clone(), intake.clone(), stop.clone(), failed.clone());
        let acceptor = TlsAcceptor::from(tls.snapshot());
        connections.spawn(async move {
            let _permit = permit;
            let accepted = tokio::select! { _ = stop.cancelled() => return, result = tokio::time::timeout(handshake_timeout, acceptor.accept(socket)) => result, };
            let mut stream = match accepted {
                Ok(Ok(stream)) => stream,
                _ => { let count = failed.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                    if count.is_power_of_two() { tracing::warn!(transport = transport as i32, source_ip = %peer.ip(), status = "tls_handshake_failed", failed_total = count, "TLS handshake rejected or timed out"); } return; }
            };
            tracing::debug!(transport = transport as i32, source_ip = %peer.ip(), site = %cfg.site, status = "tls_handshake_ok", "TLS session established");
            let _result = match application {
                Application::Syslog => tcp::session(&mut stream, peer, cfg.clone(), intake, stop, transport).await,
                Application::Relp => relp::session(&mut stream, peer, cfg.clone(), intake, stop, transport).await,
            };
            let _shutdown = tokio::time::timeout(cfg.idle_timeout, stream.shutdown()).await;
        });
        while let Some(result) = connections.try_join_next() {
            result.map_err(|_| Error::Task)?;
        }
    }
    drop(intake);
    while let Some(result) = connections.join_next().await {
        result.map_err(|_| Error::Task)?;
    }
    Ok(())
}
