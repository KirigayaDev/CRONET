use bus::{Bus, BusConfig};
use cli_collector::{
    config::{env, Config},
    worker::Worker,
};
use creds::{CachedCredStore, CredStore, EnvCredStore, VaultAuth, VaultConfig, VaultCredStore};
use std::{sync::Arc, time::Duration};
use tracing_subscriber::EnvFilter;

fn cred_store() -> Result<Arc<dyn CredStore>, &'static str> {
    let store: Arc<dyn CredStore> = match env("NETMON_CRED_BACKEND").as_deref().unwrap_or("vault") {
        "env" => {
            tracing::warn!("EnvCredStore is for development only");
            Arc::new(CachedCredStore::new(EnvCredStore, Duration::from_secs(60), 10_000))
        }
        "vault" => {
            let auth = match (env("VAULT_ROLE_ID"), env("VAULT_SECRET_ID"), env("VAULT_TOKEN")) {
                (Some(role_id), Some(secret_id), _) => VaultAuth::AppRole {
                    mount: env("VAULT_APPROLE_MOUNT").unwrap_or_else(|| "approle".into()),
                    role_id,
                    secret_id: secret_id.into(),
                },
                (_, _, Some(token)) => VaultAuth::Token(token.into()),
                _ => return Err("Vault AppRole credentials or token required"),
            };
            let ca_pem = env("VAULT_CACERT").map(std::fs::read).transpose().map_err(|_| "cannot read Vault CA")?;
            let vault = VaultCredStore::new(VaultConfig {
                addr: env("VAULT_ADDR").ok_or("VAULT_ADDR required")?,
                auth,
                namespace: env("VAULT_NAMESPACE"),
                ca_pem,
                request_timeout: Duration::from_secs(5),
            })
            .map_err(|_| "invalid Vault configuration")?;
            Arc::new(CachedCredStore::new(vault, Duration::from_secs(60), 10_000))
        }
        _ => return Err("invalid credential backend"),
    };
    Ok(store)
}
#[tokio::main]
async fn main() {
    // Dependency TRACE/DEBUG logs may contain wire data. Keep them disabled even if RUST_LOG is broad.
    let filter = EnvFilter::new("off,cli_collector=info");
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
    if let Err(error) = run().await {
        tracing::error!(error, "cli-collector stopped");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), &'static str> {
    let cfg = Config::from_env()?;
    let creds = cred_store()?;
    let bus = Bus::connect(&BusConfig {
        url: cfg.nats_url,
        creds_file: cfg.nats_creds,
        name: cfg.policy.collector_id.clone(),
    })
    .await
    .map_err(|_| "NATS connection failed")?;
    if cfg.bootstrap {
        bus.ensure_streams(cfg.replicas, &cfg.limits).await.map_err(|_| "stream bootstrap failed")?;
    }
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown().await;
        let _ = tx.send(true);
    });
    Worker {
        bus,
        site: cfg.site,
        policy: cfg.policy,
        creds,
        concurrency: cfg.concurrency,
        shutdown_grace: cfg.shutdown_grace,
    }
    .run(rx)
    .await
}
async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
