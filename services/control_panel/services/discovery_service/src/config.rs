use crate::{
    error::{Error, Result},
    scan::ScanPlan,
};
use clap::{Parser, Subcommand};
use creds::{CachedCredStore, CredStore, EnvCredStore, VaultAuth, VaultConfig, VaultCredStore};
use secrecy::SecretString;
use std::{sync::Arc, time::Duration};

#[derive(Parser)]
#[command(name = "discovery-service", about = "Bounded one-shot SNMP discovery")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    Scan(ScanArgs),
}
#[derive(clap::Args)]
pub struct ScanArgs {
    #[arg(long)]
    pub cidr: String,
    #[arg(long)]
    pub credential_ref: String,
    #[arg(long, value_enum)]
    pub snmp_version: SnmpVersion,
    #[arg(long, default_value_t = 64)]
    pub concurrency: usize,
    #[arg(long, default_value_t = 2000)]
    pub timeout_ms: u64,
    #[arg(long)]
    pub icmp: bool,
    #[arg(long, default_value_t = 4096)]
    pub max_hosts: u64,
    #[arg(long, default_value_t = 161)]
    pub snmp_port: u16,
    /// Include per-host records as JSON, rather than only the summary.
    #[arg(long)]
    pub json_results: bool,
}
#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum SnmpVersion {
    V2c,
    V3,
}
impl ScanArgs {
    pub fn plan(&self) -> Result<ScanPlan> {
        ScanPlan::new(
            &self.cidr,
            self.max_hosts,
            self.concurrency,
            Duration::from_millis(self.timeout_ms),
            self.icmp,
            self.snmp_port,
        )
    }
}
pub struct RuntimeConfig {
    pub cmdb_endpoint: String,
    pub token: SecretString,
    pub cmdb_timeout: Duration,
    pub allow_legacy_crypto: bool,
}
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}
impl RuntimeConfig {
    pub fn from_env() -> Result<Self> {
        let token = env("CMDB_BEARER_TOKEN").ok_or(Error::CmdbAuth)?;
        let endpoint = env("CMDB_GRPC_URL").unwrap_or_else(|| "http://127.0.0.1:50051".into());
        let allow_legacy_crypto =
            env("NETMON_ALLOW_LEGACY_CRYPTO").unwrap_or_else(|| "false".into()).parse().map_err(|_| Error::Config)?;
        Ok(Self {
            cmdb_endpoint: endpoint,
            token: token.into(),
            cmdb_timeout: Duration::from_secs(5),
            allow_legacy_crypto,
        })
    }
}
pub fn credential_store() -> Result<Arc<dyn CredStore>> {
    let ttl = Duration::from_secs(300);
    match env("NETMON_CRED_BACKEND").as_deref().unwrap_or("vault") {
        "env" => {
            tracing::warn!("Env credential backend is for development/tests only");
            Ok(Arc::new(CachedCredStore::new(EnvCredStore, ttl, 1)))
        }
        "vault" => {
            let addr = env("VAULT_ADDR").ok_or(Error::Credential)?;
            let auth = match (env("VAULT_ROLE_ID"), env("VAULT_SECRET_ID"), env("VAULT_TOKEN")) {
                (Some(role_id), Some(secret_id), _) => VaultAuth::AppRole {
                    mount: env("VAULT_APPROLE_MOUNT").unwrap_or_else(|| "approle".into()),
                    role_id,
                    secret_id: secret_id.into(),
                },
                (_, _, Some(token)) => VaultAuth::Token(token.into()),
                _ => return Err(Error::Credential),
            };
            let ca_pem = env("VAULT_CACERT").map(std::fs::read).transpose().map_err(|_| Error::Credential)?;
            let store = VaultCredStore::new(VaultConfig {
                addr,
                auth,
                namespace: env("VAULT_NAMESPACE"),
                ca_pem,
                request_timeout: Duration::from_secs(5),
            })
            .map_err(|_| Error::Credential)?;
            Ok(Arc::new(CachedCredStore::new(store, ttl, 1)))
        }
        _ => Err(Error::Credential),
    }
}
