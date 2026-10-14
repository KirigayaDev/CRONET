mod config;
mod poll;
mod transport;
mod worker;

use std::{sync::Arc, time::Duration};

use bus::{Bus, BusConfig};
use creds::{CachedCredStore, CredStore, EnvCredStore, VaultAuth, VaultConfig, VaultCredStore};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn build_cred_store(ttl: Duration) -> Result<Arc<dyn CredStore>, String> {
    match env("NETMON_CRED_BACKEND").as_deref().unwrap_or("vault") {
        "env" => {
            tracing::warn!("используется EnvCredStore: только для разработки");
            Ok(Arc::new(CachedCredStore::new(EnvCredStore, ttl, 10_000)))
        }
        "vault" => {
            let addr = env("VAULT_ADDR").ok_or("VAULT_ADDR обязателен")?;
            let auth = match (env("VAULT_ROLE_ID"), env("VAULT_SECRET_ID"), env("VAULT_TOKEN")) {
                (Some(role_id), Some(secret_id), _) => VaultAuth::AppRole {
                    mount: env("VAULT_APPROLE_MOUNT").unwrap_or_else(|| "approle".into()),
                    role_id,
                    secret_id: secret_id.into(),
                },
                (_, _, Some(token)) => VaultAuth::Token(token.into()),
                _ => return Err("нужны VAULT_ROLE_ID+VAULT_SECRET_ID (AppRole) или VAULT_TOKEN".into()),
            };
            let ca_pem = match env("VAULT_CACERT") {
                Some(path) => Some(std::fs::read(&path).map_err(|e| format!("VAULT_CACERT: {e}"))?),
                None => None,
            };
            let store = VaultCredStore::new(VaultConfig {
                addr,
                auth,
                namespace: env("VAULT_NAMESPACE"),
                ca_pem,
                request_timeout: Duration::from_secs(5),
            })
            .map_err(|e| e.to_string())?;
            Ok(Arc::new(CachedCredStore::new(store, ttl, 10_000)))
        }
        other => Err(format!("NETMON_CRED_BACKEND={other}: ожидается vault или env")),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    if let Err(e) = run().await {
        tracing::error!(error = %e, "snmp-poller остановлен с ошибкой");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = config::Config::from_env()?;
    let creds = build_cred_store(cfg.cred_ttl)?;

    let bus = Bus::connect(&BusConfig {
        url: cfg.nats_url.clone(),
        creds_file: cfg.nats_creds.clone(),
        name: cfg.collector_id.clone(),
    })
    .await
    .map_err(|e| e.to_string())?;
    if cfg.bootstrap_streams {
        bus.ensure_streams(cfg.stream_replicas, &cfg.stream_limits).await.map_err(|e| e.to_string())?;
    }

    let policy = Arc::new(poll::Policy {
        collector_id: cfg.collector_id.clone(),
        default_timeout: cfg.default_timeout,
        default_max_repetitions: cfg.default_max_repetitions,
        max_varbinds: cfg.max_varbinds_per_task,
        max_task_time: cfg.max_task_time,
        allowed_targets: cfg.allowed_targets.clone(),
        allow_legacy_crypto: cfg.allow_legacy_crypto,
    });

    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop_tx.send(true);
    });

    tracing::info!(site = %cfg.site, collector = %cfg.collector_id, concurrency = cfg.max_concurrency, "snmp-poller запущен");
    worker::Worker {
        bus,
        site: cfg.site,
        policy,
        creds,
        transport: Arc::new(transport::Snmp2Transport),
        max_concurrency: cfg.max_concurrency,
    }
    .run(stop_rx)
    .await
    .map_err(|e| e.to_string())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = term.recv() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
