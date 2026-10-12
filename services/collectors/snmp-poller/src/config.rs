use std::{env, time::Duration};

use ipnet::IpNet;

pub struct Config {
    pub nats_url: String,
    pub nats_creds: Option<String>,
    pub site: String,
    pub collector_id: String,
    pub max_concurrency: usize,
    pub default_timeout: Duration,
    pub default_max_repetitions: u32,
    pub max_varbinds_per_task: usize,
    /// Потолок на общее время одной задачи (все запросы вместе).
    pub max_task_time: Duration,
    /// Разрешённые сети для опроса. Пусто = ограничений нет.
    pub allowed_targets: Vec<IpNet>,
    pub allow_legacy_crypto: bool,
    pub cred_ttl: Duration,
    pub bootstrap_streams: bool,
    pub stream_replicas: usize,
    pub stream_limits: bus::StreamLimits,
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    match var(name) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| format!("{name}: некорректное значение")),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let site = var("NETMON_SITE").ok_or("NETMON_SITE обязателен (например site-a)")?;
        if !site.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err("NETMON_SITE: допустимы буквы, цифры, '-' и '_' (иначе сломается subject NATS)".into());
        }
        let mut allowed = Vec::new();
        if let Some(list) = var("NETMON_ALLOWED_TARGETS") {
            for item in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                allowed.push(
                    item.parse::<IpNet>().map_err(|_| format!("NETMON_ALLOWED_TARGETS: некорректная сеть {item:?}"))?,
                );
            }
        }
        let max_concurrency: usize = parse("NETMON_MAX_CONCURRENCY", 200)?;
        if max_concurrency == 0 {
            return Err("NETMON_MAX_CONCURRENCY должен быть больше нуля".into());
        }
        Ok(Self {
            nats_url: var("NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".into()),
            nats_creds: var("NATS_CREDS_FILE"),
            collector_id: var("NETMON_COLLECTOR_ID")
                .or_else(|| var("HOSTNAME"))
                .unwrap_or_else(|| format!("snmp-poller-{}", std::process::id())),
            site,
            max_concurrency,
            default_timeout: Duration::from_millis(parse("NETMON_DEFAULT_TIMEOUT_MS", 5_000)?),
            default_max_repetitions: parse("NETMON_DEFAULT_MAX_REPETITIONS", 20)?,
            max_varbinds_per_task: parse("NETMON_MAX_VARBINDS", 20_000)?,
            max_task_time: Duration::from_secs(parse("NETMON_MAX_TASK_SECS", 30)?),
            allowed_targets: allowed,
            allow_legacy_crypto: parse("NETMON_ALLOW_LEGACY_CRYPTO", false)?,
            cred_ttl: Duration::from_secs(parse("NETMON_CRED_TTL_SECS", 300)?),
            bootstrap_streams: parse("NETMON_BOOTSTRAP_STREAMS", false)?,
            stream_replicas: parse("NETMON_STREAM_REPLICAS", 1)?,
            stream_limits: {
                let profile = var("NETMON_STREAM_PROFILE").unwrap_or_else(|| "prod".into());
                bus::StreamLimits::from_profile(&profile).ok_or("NETMON_STREAM_PROFILE: ожидается prod или dev")?
            },
        })
    }
}
