mod handler;

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bus::{Bus, BusConfig, StreamLimits};
use contracts::{netmon::v1::RawTrap, subjects};
use creds::{CachedCredStore, CredStore, Credential, EnvCredStore, VaultAuth, VaultConfig, VaultCredStore};
use handler::{DropReason, Filter, Outcome, RateLimiter, MAX_DATAGRAM};
use ipnet::IpNet;
use secrecy::{ExposeSecret, SecretSlice};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, RwLock},
};
use tracing_subscriber::EnvFilter;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn build_cred_store() -> Result<Arc<dyn CredStore>, String> {
    let ttl = Duration::from_secs(60);
    match env("NETMON_CRED_BACKEND").as_deref().unwrap_or("vault") {
        "env" => Ok(Arc::new(CachedCredStore::new(EnvCredStore, ttl, 100))),
        "vault" => {
            let auth = match (env("VAULT_ROLE_ID"), env("VAULT_SECRET_ID"), env("VAULT_TOKEN")) {
                (Some(role_id), Some(secret_id), _) => VaultAuth::AppRole {
                    mount: env("VAULT_APPROLE_MOUNT").unwrap_or_else(|| "approle".into()),
                    role_id,
                    secret_id: secret_id.into(),
                },
                (_, _, Some(t)) => VaultAuth::Token(t.into()),
                _ => return Err("нужны VAULT_ROLE_ID+VAULT_SECRET_ID или VAULT_TOKEN".into()),
            };
            let ca_pem = match env("VAULT_CACERT") {
                Some(p) => Some(std::fs::read(p).map_err(|e| format!("VAULT_CACERT: {e}"))?),
                None => None,
            };
            let store = VaultCredStore::new(VaultConfig {
                addr: env("VAULT_ADDR").ok_or("VAULT_ADDR обязателен")?,
                auth,
                namespace: env("VAULT_NAMESPACE"),
                ca_pem,
                request_timeout: Duration::from_secs(5),
            })
            .map_err(|e| e.to_string())?;
            Ok(Arc::new(CachedCredStore::new(store, ttl, 100)))
        }
        other => Err(format!("NETMON_CRED_BACKEND={other}")),
    }
}

/// community берутся из секретов (ссылки через запятую) и перечитываются раз в минуту:
/// ротация community не требует перезапуска.
async fn load_communities(store: &dyn CredStore, refs: &[String]) -> Result<Vec<SecretSlice<u8>>, String> {
    let mut out = Vec::new();
    for r in refs {
        match &*store.get(r).await.map_err(|e| format!("{r}: {e}"))? {
            Credential::V2c { community } => out.push(SecretSlice::from(community.expose_secret().as_bytes().to_vec())),
            Credential::V3(_) | Credential::Basic { .. } => return Err(format!("{r}: ожидалась community v2c")),
        }
    }
    Ok(out)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    if let Err(e) = run().await {
        tracing::error!(error = %e, "trap-receiver остановлен с ошибкой");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let site = env("NETMON_SITE").ok_or("NETMON_SITE обязателен")?;
    if !site.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err("NETMON_SITE: недопустимые символы".into());
    }
    let collector_id = env("NETMON_COLLECTOR_ID").or_else(|| env("HOSTNAME")).unwrap_or_else(|| "trap-receiver".into());
    // 162 - привилегированный порт: в проде лучше слушать 1162 и пробросить 162 снаружи
    let listen: SocketAddr = env("NETMON_TRAP_LISTEN")
        .unwrap_or_else(|| "0.0.0.0:1162".into())
        .parse()
        .map_err(|_| "NETMON_TRAP_LISTEN: ожидается IP:порт")?;
    let allowed: Vec<IpNet> = env("NETMON_ALLOWED_SOURCES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().map_err(|_| format!("NETMON_ALLOWED_SOURCES: {s:?}")))
        .collect::<Result<_, _>>()?;
    if allowed.is_empty() {
        tracing::warn!("NETMON_ALLOWED_SOURCES пуст: трапы принимаются с любых адресов");
    }
    let rate: u32 =
        env("NETMON_TRAP_RATE_PER_SOURCE").map_or(Ok(200), |v| v.parse()).map_err(|_| "NETMON_TRAP_RATE_PER_SOURCE")?;
    let refs: Vec<String> = env("NETMON_TRAP_COMMUNITY_REFS")
        .ok_or("NETMON_TRAP_COMMUNITY_REFS обязателен")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let creds = build_cred_store()?;

    let filter = Arc::new(RwLock::new(Filter {
        allowed_sources: allowed,
        communities: load_communities(&*creds, &refs).await?,
        collector_id: collector_id.clone(),
    }));
    {
        let (creds, refs, filter) = (creds.clone(), refs.clone(), filter.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                match load_communities(&*creds, &refs).await {
                    Ok(c) => filter.write().await.communities = c,
                    Err(e) => tracing::warn!(error = %e, "не удалось обновить community, остаются прежние"),
                }
            }
        });
    }

    let bus = Bus::connect(&BusConfig {
        url: env("NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".into()),
        creds_file: env("NATS_CREDS_FILE"),
        name: collector_id.clone(),
    })
    .await
    .map_err(|e| e.to_string())?;
    if env("NETMON_BOOTSTRAP_STREAMS").as_deref() == Some("true") {
        let limits = StreamLimits::from_profile(&env("NETMON_STREAM_PROFILE").unwrap_or_else(|| "prod".into()))
            .ok_or("NETMON_STREAM_PROFILE")?;
        bus.ensure_streams(1, &limits).await.map_err(|e| e.to_string())?;
    }

    // сокет через socket2: нужен большой буфер приёма, трапы приходят пачками
    let domain = if listen.is_ipv4() { socket2::Domain::IPV4 } else { socket2::Domain::IPV6 };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, None).map_err(|e| e.to_string())?;
    sock.set_recv_buffer_size(4 * 1024 * 1024).map_err(|e| e.to_string())?;
    sock.set_nonblocking(true).map_err(|e| e.to_string())?;
    sock.bind(&listen.into()).map_err(|e| format!("bind {listen}: {e}"))?;
    let sock = Arc::new(UdpSocket::from_std(sock.into()).map_err(|e| e.to_string())?);

    // между приёмом и публикацией - ограниченная очередь: при перегрузе считаем потери,
    // а не растим память
    let (tx, mut rx) = mpsc::channel::<Box<RawTrap>>(10_000);
    let subject = subjects::trap(&site);
    let publisher = {
        let bus = bus.clone();
        tokio::spawn(async move {
            while let Some(trap) = rx.recv().await {
                if let Err(e) = bus.publish_proto(subject.clone(), &trap.trap_id, &trap).await {
                    tracing::error!(error = %e, trap_id = %trap.trap_id, "трап не опубликован");
                }
            }
        })
    };

    tracing::info!(%listen, site = %site, "trap-receiver запущен");
    let mut limiter = RateLimiter::new(rate);
    let mut buf = vec![0u8; MAX_DATAGRAM + 1];
    let (mut accepted, mut dropped, mut overflow) = (0u64, 0u64, 0u64);
    let mut stats = tokio::time::interval(Duration::from_secs(60));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = stats.tick() => {
                tracing::info!(accepted, dropped, queue_overflow = overflow, "статистика за время работы");
            }
            r = sock.recv_from(&mut buf) => {
                let (n, src) = match r { Ok(v) => v, Err(e) => { tracing::warn!(error = %e, "recv_from"); continue; } };
                let outcome = handler::process(&buf[..n], src, SystemTime::now(), &*filter.read().await, &mut limiter);
                match outcome {
                    Outcome::Drop(reason) => {
                        dropped += 1;
                        // не логируем каждый пакет: под флудом лог стал бы вектором DoS
                        if !matches!(reason, DropReason::RateLimited | DropReason::NotAllowedSource) {
                            tracing::debug!(%src, ?reason, "трап отброшен");
                        }
                    }
                    Outcome::Accept { trap, reply } => {
                        if let Some(r) = reply { let _ = sock.send_to(&r, src).await; }
                        match tx.try_send(trap) {
                            Ok(()) => accepted += 1,
                            Err(_) => { overflow += 1; }
                        }
                    }
                }
            }
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(10), publisher).await;
    Ok(())
}
