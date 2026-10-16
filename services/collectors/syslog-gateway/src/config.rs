use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use crate::error::{Error, Result};

pub const MIN_SPOOL_BYTES: u64 = 16 * 1024 * 1024;
const KEYS: &[&str] = &[
    "SYSLOG_UDP_ADDR",
    "SYSLOG_TCP_ADDR",
    "SYSLOG_GATEWAY_ID",
    "SYSLOG_MAX_FRAME_BYTES",
    "SYSLOG_MAX_CONNECTIONS",
    "SYSLOG_IDLE_SECS",
    "SYSLOG_CHANNEL_CAPACITY",
    "SYSLOG_SPOOL_PATH",
    "SYSLOG_SPOOL_MAX_BYTES",
    "SYSLOG_REPLAY_BATCH",
    "SYSLOG_SHUTDOWN_SECS",
    "SYSLOG_UDP_RCVBUF_BYTES",
];

#[derive(Clone)]
pub struct Config {
    pub udp_addr: SocketAddr,
    pub tcp_addr: SocketAddr,
    pub site: String,
    pub gateway_id: String,
    pub max_frame: usize,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub channel_capacity: usize,
    pub spool_path: PathBuf,
    pub spool_max_bytes: u64,
    pub replay_batch: usize,
    pub shutdown_timeout: Duration,
    pub udp_rcvbuf: usize,
    pub nats_url: String,
    pub nats_creds: Option<String>,
    pub bootstrap_streams: bool,
    pub stream_replicas: usize,
    pub stream_limits: bus::StreamLimits,
}

fn invalid(name: &str) -> Error {
    Error::Config(format!("{name}: invalid value"))
}

fn parse<T: FromStr>(env: &BTreeMap<String, String>, name: &str, default: T) -> Result<T> {
    env.get(name).map_or(Ok(default), |value| value.parse().map_err(|_| invalid(name)))
}

fn text(env: &BTreeMap<String, String>, name: &str, default: &str) -> String {
    env.get(name).cloned().unwrap_or_else(|| default.into())
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(env: BTreeMap<String, String>) -> Result<Self> {
        for name in env.keys() {
            if name.starts_with("SYSLOG_") && !KEYS.contains(&name.as_str()) {
                return Err(invalid(name));
            }
        }
        let site = text(&env, "NETMON_SITE", "");
        if site.is_empty()
            || site.len() > 128
            || !site.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(invalid("NETMON_SITE"));
        }
        let fallback =
            format!("{}-{}", text(&env, "HOSTNAME", &text(&env, "COMPUTERNAME", "syslog")), std::process::id());
        let gateway_id = text(&env, "SYSLOG_GATEWAY_ID", &fallback);
        if gateway_id.is_empty() || gateway_id.len() > 256 || gateway_id.chars().any(char::is_control) {
            return Err(invalid("SYSLOG_GATEWAY_ID"));
        }
        let nats_url = text(&env, "NATS_URL", "nats://127.0.0.1:4222");
        let addr = nats_url.parse::<async_nats::ServerAddr>().map_err(|_| invalid("NATS_URL"))?;
        if !matches!(addr.scheme(), "nats" | "tls") || addr.host().is_empty() {
            return Err(invalid("NATS_URL"));
        }
        let max_frame = parse(&env, "SYSLOG_MAX_FRAME_BYTES", 65536usize)?;
        let max_connections = parse(&env, "SYSLOG_MAX_CONNECTIONS", 1024usize)?;
        let channel_capacity = parse(&env, "SYSLOG_CHANNEL_CAPACITY", 1024usize)?;
        let replay_batch = parse(&env, "SYSLOG_REPLAY_BATCH", 100usize)?;
        let spool_max_bytes = parse(&env, "SYSLOG_SPOOL_MAX_BYTES", 1024 * 1024 * 1024u64)?;
        let idle = parse(&env, "SYSLOG_IDLE_SECS", 120u64)?;
        let shutdown = parse(&env, "SYSLOG_SHUTDOWN_SECS", 30u64)?;
        let udp_rcvbuf = parse(&env, "SYSLOG_UDP_RCVBUF_BYTES", 4 * 1024 * 1024usize)?;
        for (name, valid) in [
            ("SYSLOG_MAX_FRAME_BYTES", (1..=65536).contains(&max_frame)),
            ("SYSLOG_MAX_CONNECTIONS", (1..=65536).contains(&max_connections)),
            ("SYSLOG_CHANNEL_CAPACITY", (1..=65536).contains(&channel_capacity)),
            ("SYSLOG_REPLAY_BATCH", (1..=1000).contains(&replay_batch)),
            ("SYSLOG_SPOOL_MAX_BYTES", (MIN_SPOOL_BYTES..=1024 * 1024 * 1024 * 1024).contains(&spool_max_bytes)),
            ("SYSLOG_IDLE_SECS", (1..=86400).contains(&idle)),
            ("SYSLOG_SHUTDOWN_SECS", (1..=300).contains(&shutdown)),
            ("SYSLOG_UDP_RCVBUF_BYTES", (65536..=64 * 1024 * 1024).contains(&udp_rcvbuf)),
        ] {
            if !valid {
                return Err(invalid(name));
            }
        }
        let spool_path = PathBuf::from(text(&env, "SYSLOG_SPOOL_PATH", "var/syslog/spool.redb"));
        if spool_path.as_os_str().is_empty() || spool_path.file_name().is_none() {
            return Err(invalid("SYSLOG_SPOOL_PATH"));
        }
        let nats_creds = env.get("NATS_CREDS_FILE").cloned();
        if let Some(path) = &nats_creds {
            if path.is_empty() || !std::path::Path::new(path).is_file() {
                return Err(invalid("NATS_CREDS_FILE"));
            }
        }
        let stream_replicas = parse(&env, "NETMON_STREAM_REPLICAS", 1usize)?;
        if !(1..=5).contains(&stream_replicas) {
            return Err(invalid("NETMON_STREAM_REPLICAS"));
        }
        let stream_limits = bus::StreamLimits::from_profile(&text(&env, "NETMON_STREAM_PROFILE", "dev"))
            .ok_or_else(|| invalid("NETMON_STREAM_PROFILE"))?;
        Ok(Self {
            udp_addr: parse(
                &env,
                "SYSLOG_UDP_ADDR",
                "0.0.0.0:5514".parse::<SocketAddr>().map_err(|_| invalid("SYSLOG_UDP_ADDR"))?,
            )?,
            tcp_addr: parse(
                &env,
                "SYSLOG_TCP_ADDR",
                "0.0.0.0:5514".parse::<SocketAddr>().map_err(|_| invalid("SYSLOG_TCP_ADDR"))?,
            )?,
            site,
            gateway_id,
            max_frame,
            max_connections,
            channel_capacity,
            replay_batch,
            spool_path,
            spool_max_bytes,
            idle_timeout: Duration::from_secs(idle),
            shutdown_timeout: Duration::from_secs(shutdown),
            udp_rcvbuf,
            nats_url,
            nats_creds,
            stream_replicas,
            stream_limits,
            bootstrap_streams: parse(&env, "NETMON_BOOTSTRAP_STREAMS", true)?,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn defaults_and_validation() {
        let base = BTreeMap::from([("NETMON_SITE".into(), "a-Z_09".into())]);
        let cfg = Config::from_map(base.clone()).unwrap();
        assert_eq!(cfg.max_frame, 65536);
        assert_eq!(cfg.udp_addr.port(), 5514);
        for (key, value) in [
            ("NETMON_SITE", ""),
            ("NETMON_SITE", "a.b"),
            ("NETMON_SITE", "Москва"),
            ("SYSLOG_UDP_ADDR", "bad"),
            ("SYSLOG_TCP_ADDR", "localhost:5514"),
            ("SYSLOG_MAX_FRAME_BYTES", "0"),
            ("SYSLOG_MAX_FRAME_BYTES", "65537"),
            ("SYSLOG_MAX_CONNECTIONS", "0"),
            ("SYSLOG_CHANNEL_CAPACITY", "0"),
            ("SYSLOG_REPLAY_BATCH", "1001"),
            ("SYSLOG_REPLAY_BATCH", "0"),
            ("SYSLOG_SPOOL_MAX_BYTES", "0"),
            ("SYSLOG_SPOOL_MAX_BYTES", "garbage"),
            ("SYSLOG_IDLE_SECS", "0"),
            ("SYSLOG_SHUTDOWN_SECS", "0"),
            ("SYSLOG_SPOOL_PATH", ""),
            ("SYSLOG_GATEWAY_ID", ""),
            ("NATS_URL", "http://example.com"),
            ("NATS_URL", "nats://["),
            ("NETMON_STREAM_PROFILE", "other"),
            ("NETMON_BOOTSTRAP_STREAMS", "yes"),
            ("SYSLOG_TYPO", "1"),
            ("NATS_CREDS_FILE", ""),
        ] {
            let mut values = base.clone();
            values.insert(key.into(), value.into());
            assert!(Config::from_map(values).is_err(), "accepted {key}");
        }
    }

    #[test]
    fn config_errors_do_not_echo_credentials() {
        let env = BTreeMap::from([
            ("NETMON_SITE".into(), "test".into()),
            ("NATS_URL".into(), "http://secret:password@localhost".into()),
        ]);
        let error = Config::from_map(env).err().unwrap().to_string();
        assert!(!error.contains("secret"));
        assert!(!error.contains("password"));
    }
}
