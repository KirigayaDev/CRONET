use crate::error::{Error, Result};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};

pub const MAX_ENTRIES: usize = 100_000;
pub const DEDUP_WINDOW: Duration = Duration::from_secs(120);
fn lateness() -> Duration {
    Duration::from_secs(30)
}
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Version {
    V2c,
    V3,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub id: String,
    pub enabled: bool,
    pub site: String,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub jitter: Duration,
    pub snmp_version: Version,
    pub credential_ref: String,
    #[serde(default)]
    pub get_oids: Vec<String>,
    #[serde(default)]
    pub walk_roots: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub task_deadline: Duration,
    #[serde(default = "lateness", with = "humantime_serde")]
    pub max_lateness: Duration,
    pub max_repetitions: u32,
    pub port: u16,
}
pub fn safe_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}
impl Schedule {
    pub fn validate(&self) -> Result<()> {
        if !safe_name(&self.id)
            || !safe_name(&self.site)
            || self.interval.as_millis() == 0
            || self.interval > Duration::from_secs(86400)
            || self.jitter >= self.interval
            || self.timeout.as_millis() == 0
            || self.timeout > Duration::from_secs(30)
            || self.task_deadline.as_millis() == 0
            || self.task_deadline > Duration::from_secs(3600)
            || self.max_lateness > Duration::from_secs(60)
            || self.port == 0
            || !(1..=100).contains(&self.max_repetitions)
            || self.credential_ref.trim().is_empty()
            || self.credential_ref.len() > 2048
            || self.get_oids.len() > 100
            || self.walk_roots.len() > 20
            || self.get_oids.len() + self.walk_roots.len() == 0
        {
            return Err(Error::Config);
        }
        // Slots are millisecond based; reject sub-millisecond precision rather than truncate differently.
        for value in [self.interval, self.jitter, self.timeout, self.task_deadline, self.max_lateness] {
            if value.subsec_nanos() % 1_000_000 != 0 {
                return Err(Error::Config);
            }
        }
        for oid in self.get_oids.iter().chain(&self.walk_roots) {
            snmp_types::parse_oid(oid).map_err(|_| Error::Config)?;
        }
        Ok(())
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub schedules: Vec<Schedule>,
}
impl Definition {
    pub fn validate(&self) -> Result<()> {
        if self.schedules.len() > 1024 {
            return Err(Error::Config);
        }
        let mut seen = HashSet::new();
        for schedule in &self.schedules {
            schedule.validate()?;
            if !seen.insert(&schedule.id) {
                return Err(Error::Config);
            }
        }
        Ok(())
    }
    pub fn fired_ttl(&self) -> Duration {
        self.schedules
            .iter()
            .map(|s| s.interval.saturating_mul(2))
            .max()
            .unwrap_or_default()
            .max(Duration::from_secs(3600))
    }
}
fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    if std::fs::metadata(path).map_err(|_| Error::Config)?.len() > max {
        return Err(Error::Config);
    }
    let bytes = std::fs::read(path).map_err(|_| Error::Config)?;
    if bytes.len() as u64 > max {
        return Err(Error::Config);
    }
    Ok(bytes)
}
pub fn load_definition(path: Option<&Path>, json: Option<&str>) -> Result<Definition> {
    let bytes = if let Some(path) = path {
        read_bounded(path, 1_048_576)?
    } else {
        let json = json.ok_or(Error::Config)?;
        if json.len() > 1_048_576 {
            return Err(Error::Config);
        }
        json.as_bytes().to_vec()
    };
    let definition: Definition = serde_json::from_slice(&bytes).map_err(|_| Error::Config)?;
    definition.validate()?;
    Ok(definition)
}
pub enum TokenSource {
    File(PathBuf),
    Environment(SecretString),
}
impl TokenSource {
    pub fn select(file: Option<PathBuf>, environment: Option<SecretString>) -> Result<Self> {
        if let Some(path) = file {
            Ok(Self::File(path))
        } else {
            Ok(Self::Environment(environment.ok_or(Error::Config)?))
        }
    }
    pub fn load(&self) -> Result<SecretString> {
        let value = match self {
            Self::File(path) => String::from_utf8(read_bounded(path, 16384)?).map_err(|_| Error::Config)?,
            Self::Environment(value) => value.expose_secret().to_owned(),
        };
        let value = value.trim();
        if value.is_empty() || value.len() > 16384 {
            return Err(Error::Config);
        }
        Ok(value.to_owned().into())
    }
}
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}
fn duration(name: &str, default: Duration) -> Result<Duration> {
    env(name).map(|s| humantime::parse_duration(&s).map_err(|_| Error::Config)).unwrap_or(Ok(default))
}
fn parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    env(name).map(|s| s.parse().map_err(|_| Error::Config)).unwrap_or(Ok(default))
}
pub struct Config {
    pub definition: Definition,
    pub instance_id: String,
    pub namespace: String,
    pub nats_url: String,
    pub nats_creds: Option<String>,
    pub cmdb_url: String,
    pub token: TokenSource,
    pub resync: Duration,
    pub heartbeat: Duration,
    pub member_ttl: Duration,
    pub claim_ttl: Duration,
    pub replicas: usize,
    pub bootstrap: bool,
    pub stream_limits: bus::StreamLimits,
}
impl Config {
    pub fn from_env() -> Result<Self> {
        let path = env("SCHEDULER_CONFIG_PATH").map(PathBuf::from);
        let definition = load_definition(path.as_deref(), env("SCHEDULER_CONFIG_JSON").as_deref())?;
        let heartbeat = duration("SCHEDULER_HEARTBEAT_INTERVAL", Duration::from_secs(5))?;
        let member_ttl = duration("SCHEDULER_MEMBER_TTL", Duration::from_secs(15))?;
        let claim_ttl = duration("SCHEDULER_CLAIM_TTL", Duration::from_secs(20))?;
        let resync = duration("SCHEDULER_RESYNC_INTERVAL", Duration::from_secs(60))?;
        let instance_id = env("SCHEDULER_INSTANCE_ID").unwrap_or_else(|| {
            format!(
                "{}-{}-{}",
                env("HOSTNAME").unwrap_or_else(|| "scheduler".into()),
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            )
        });
        let namespace = env("SCHEDULER_NAMESPACE").unwrap_or_else(|| "SCHEDULER".into());
        let replicas = parse("SCHEDULER_STREAM_REPLICAS", 1usize)?;
        if !safe_name(&instance_id)
            || !safe_name(&namespace)
            || namespace.len() > 64
            || !(1..=5).contains(&replicas)
            || heartbeat < Duration::from_secs(1)
            || heartbeat > Duration::from_secs(30)
            || member_ttl < heartbeat * 3
            || member_ttl > Duration::from_secs(120)
            || !(Duration::from_secs(12)..=Duration::from_secs(30)).contains(&claim_ttl)
            || !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&resync)
        {
            return Err(Error::Config);
        }
        Ok(Self {
            definition,
            instance_id,
            namespace,
            heartbeat,
            member_ttl,
            claim_ttl,
            resync,
            replicas,
            nats_url: env("NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".into()),
            nats_creds: env("NATS_CREDS_FILE"),
            cmdb_url: env("CMDB_GRPC_URL").unwrap_or_else(|| "http://127.0.0.1:50051".into()),
            token: TokenSource::select(
                env("CMDB_BEARER_TOKEN_FILE").map(PathBuf::from),
                env("CMDB_BEARER_TOKEN").map(Into::into),
            )?,
            bootstrap: parse("SCHEDULER_BOOTSTRAP_STREAMS", false)?,
            stream_limits: bus::StreamLimits::from_profile(
                &env("SCHEDULER_STREAM_PROFILE").unwrap_or_else(|| "prod".into()),
            )
            .ok_or(Error::Config)?,
        })
    }
}
