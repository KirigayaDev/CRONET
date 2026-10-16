use crate::error::Failure;
use contracts::netmon::v1::raw_cli_result::Status;
use russh::{client, keys::PublicKeyOrCertificate};
use ssh_key::PublicKey;
use std::{
    collections::HashMap,
    io::Read,
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Clone, Default)]
pub struct KnownHosts(HashMap<SocketAddr, Vec<PublicKey>>);
impl KnownHosts {
    /// Deliberately strict subset: literal IPs, optional bracketed port, comma lists.
    /// No TOFU, DNS, glob/hash entries, CA markers or host certificates.
    pub fn parse(text: &str) -> Result<Self, &'static str> {
        if text.len() > 1024 * 1024 {
            return Err("known_hosts exceeds 1 MiB");
        }
        let mut hosts = Self::default();
        let mut count = 0;
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
            let mut fields = line.split_whitespace();
            let names = fields.next().ok_or("invalid known_hosts")?;
            let algorithm = fields.next().ok_or("invalid known_hosts")?;
            let encoded = fields.next().ok_or("invalid known_hosts")?;
            let key =
                PublicKey::from_openssh(&format!("{algorithm} {encoded}")).map_err(|_| "invalid host public key")?;
            for name in names.split(',') {
                count += 1;
                if count > 4096 {
                    return Err("too many known_hosts entries");
                }
                let target = if name.starts_with('[') {
                    let (ip, port) =
                        name.strip_prefix('[').and_then(|s| s.rsplit_once("]:")).ok_or("invalid bracketed host")?;
                    SocketAddr::new(
                        ip.parse::<IpAddr>().map_err(|_| "known_hosts requires literal IPs")?,
                        port.parse::<u16>().map_err(|_| "invalid host port")?,
                    )
                } else {
                    SocketAddr::new(name.parse::<IpAddr>().map_err(|_| "known_hosts requires literal IPs")?, 22)
                };
                if target.port() == 0 {
                    return Err("invalid host port");
                }
                hosts.0.entry(target).or_default().push(key.clone());
            }
        }
        if hosts.0.is_empty() {
            return Err("known_hosts must contain a trusted key");
        }
        Ok(hosts)
    }
    pub fn load(path: &Path) -> Result<Self, &'static str> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| "cannot open CLI_KNOWN_HOSTS_FILE")?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read known_hosts")?;
        Self::parse(std::str::from_utf8(&bytes).map_err(|_| "known_hosts must be UTF-8")?)
    }
}

pub struct HostVerifier {
    hosts: Arc<KnownHosts>,
    target: SocketAddr,
    rejected: Arc<AtomicBool>,
}
impl client::Handler for HostVerifier {
    type Error = russh::Error;
    async fn check_server_key(&mut self, offered: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        let accepted = match offered {
            PublicKeyOrCertificate::PublicKey { key, .. } => {
                self.hosts.0.get(&self.target).is_some_and(|keys| keys.iter().any(|k| k.key_data() == key.key_data()))
            }
            PublicKeyOrCertificate::Certificate(_) => false,
        };
        if !accepted {
            self.rejected.store(true, Ordering::Relaxed);
        }
        Ok(accepted)
    }
}

/// A cloned OS socket lets cancellation close russh's background task immediately.
/// Dropping a russh Handle alone does not join/abort that task.
pub struct SocketGuard(std::net::TcpStream);
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
    }
}

pub async fn connect(
    target: SocketAddr,
    hosts: Arc<KnownHosts>,
    limit: Duration,
) -> Result<(client::Handle<HostVerifier>, SocketGuard), Failure> {
    let rejected = Arc::new(AtomicBool::new(false));
    let handler = HostVerifier { hosts, target, rejected: rejected.clone() };
    tokio::time::timeout(limit, async move {
        let socket = tokio::net::TcpStream::connect(target)
            .await
            .map_err(|_| Failure::new(Status::ConnectionError, "SSH connection failed"))?;
        let socket = socket.into_std().map_err(|_| Failure::new(Status::ConnectionError, "SSH socket setup failed"))?;
        let guard = SocketGuard(
            socket.try_clone().map_err(|_| Failure::new(Status::ConnectionError, "SSH socket setup failed"))?,
        );
        let socket = tokio::net::TcpStream::from_std(socket)
            .map_err(|_| Failure::new(Status::ConnectionError, "SSH socket setup failed"))?;
        let cfg = client::Config {
            window_size: 64 * 1024,
            maximum_packet_size: 32 * 1024,
            channel_buffer_size: 4,
            ..Default::default()
        };
        let handle = client::connect_stream(Arc::new(cfg), socket, handler).await.map_err(|_| {
            if rejected.load(Ordering::Relaxed) {
                Failure::new(Status::HostKeyFail, "server host key is not trusted")
            } else {
                Failure::new(Status::ConnectionError, "SSH handshake failed")
            }
        })?;
        Ok((handle, guard))
    })
    .await
    .map_err(|_| Failure::timeout())?
}
