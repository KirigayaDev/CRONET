mod manager;
mod path;
mod session;

use std::{sync::Arc, time::Duration};

use bus::{Bus, BusConfig, StreamLimits};
use contracts::{netmon::v1::RawGnmiUpdate, subjects};
use creds::{CachedCredStore, CredStore, EnvCredStore, VaultAuth, VaultConfig, VaultCredStore};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

struct NatsSink {
    bus: Bus,
    subject: String,
}

#[async_trait::async_trait]
impl session::Sink for NatsSink {
    async fn publish(&self, u: RawGnmiUpdate) -> Result<(), String> {
        let id = format!("{}:{}:{}", u.device_id, u.session_id, u.sequence);
        self.bus.publish_proto(self.subject.clone(), &id, &u).await.map_err(|e| e.to_string())
    }
}

fn build_cred_store() -> Result<Arc<dyn CredStore>, String> {
    let ttl = Duration::from_secs(300);
    match env("NETMON_CRED_BACKEND").as_deref().unwrap_or("vault") {
        "env" => Ok(Arc::new(CachedCredStore::new(EnvCredStore, ttl, 10_000))),
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
            Ok(Arc::new(CachedCredStore::new(store, ttl, 10_000)))
        }
        other => Err(format!("NETMON_CRED_BACKEND={other}")),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    if let Err(e) = run().await {
        tracing::error!(error = %e, "gnmi-telemetry остановлен с ошибкой");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let site = env("NETMON_SITE").ok_or("NETMON_SITE обязателен")?;
    if !site.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err("NETMON_SITE: недопустимые символы".into());
    }
    let collector_id = env("NETMON_COLLECTOR_ID")
        .or_else(|| env("HOSTNAME"))
        .unwrap_or_else(|| format!("gnmi-{}", std::process::id()));
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
    let ca_pem = match env("NETMON_GNMI_CA_FILE") {
        Some(p) => Some(std::fs::read(&p).map_err(|e| format!("NETMON_GNMI_CA_FILE: {e}"))?),
        None => None,
    };
    let secs = |name: &str, d: u64| -> Result<Duration, String> {
        Ok(Duration::from_secs(
            env(name).map_or(Ok(d), |v| v.parse()).map_err(|_| format!("{name}: некорректное значение"))?,
        ))
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop_tx.send(true);
    });

    tracing::info!(site = %site, collector = %collector_id, "gnmi-telemetry запущен");
    manager::Manager {
        sink: Arc::new(NatsSink { bus: bus.clone(), subject: subjects::gnmi(&site) }),
        bus,
        creds: build_cred_store()?,
        settings: manager::Settings {
            site,
            collector_id,
            max_devices: env("NETMON_GNMI_MAX_DEVICES")
                .map_or(Ok(500), |v| v.parse())
                .map_err(|_| "NETMON_GNMI_MAX_DEVICES")?,
            lease_ttl: secs("NETMON_GNMI_LEASE_TTL_SECS", 30)?,
            reconcile_every: secs("NETMON_GNMI_RECONCILE_SECS", 5)?,
            ca_pem,
            backoff_min: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
        },
    }
    .run(stop_rx)
    .await
}
