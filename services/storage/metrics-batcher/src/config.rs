use std::time::Duration;

use crate::error::Error;

#[derive(Clone)]
pub struct Config {
    pub nats_url: String,
    pub nats_creds: Option<String>,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub site: String,
    pub instance: String,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub concurrency: usize,
    pub flush_interval: Duration,
    pub insert_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub retry_min: Duration,
    pub retry_max: Duration,
    pub bootstrap: bool,
    pub stream_replicas: usize,
    pub stream_limits: bus::StreamLimits,
}

pub fn site_token(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

impl Config {
    pub fn from_env() -> Result<Self, Error> {
        Self::load(|key| std::env::var(key).ok())
    }

    pub fn load(get: impl Fn(&str) -> Option<String>) -> Result<Self, Error> {
        let var = |key| get(key).filter(|v| !v.is_empty());
        let required = |key| var(key).ok_or_else(|| Error::Config(format!("{key} is required")));
        let number = |key, default, low, high| -> Result<usize, Error> {
            let value = var(key)
                .map(|v| v.parse::<usize>())
                .transpose()
                .map_err(|_| Error::Config(format!("{key}: expected integer")))?
                .unwrap_or(default);
            if !(low..=high).contains(&value) {
                return Err(Error::Config(format!("{key}: expected {low}..={high}")));
            }
            Ok(value)
        };
        let site = required("NETMON_SITE")?;
        if site != "*" && !site_token(&site) {
            return Err(Error::Config("NETMON_SITE: expected a site token or *".into()));
        }
        let clickhouse_url = required("CLICKHOUSE_URL")?;
        let endpoint =
            url::Url::parse(&clickhouse_url).map_err(|_| Error::Config("CLICKHOUSE_URL: invalid URL".into()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
        {
            return Err(Error::Config("CLICKHOUSE_URL: expected HTTP(S) origin; supply credentials separately".into()));
        }
        let database = var("CLICKHOUSE_DATABASE").unwrap_or_else(|| "cronet".into());
        if database != "cronet" {
            return Err(Error::Config("CLICKHOUSE_DATABASE must be cronet (C1 schema)".into()));
        }
        let max_rows = number("METRICS_BATCH_MAX_ROWS", 1_000, 1, 10_000)?;
        let max_bytes = number("METRICS_BATCH_MAX_BYTES", 4 * 1024 * 1024, 64 * 1024, 16 * 1024 * 1024)?;
        let concurrency = number("METRICS_MAX_CONCURRENCY", 1, 1, 16)?;
        if concurrency * max_bytes > 128 * 1024 * 1024 || concurrency * max_rows > 64_000 {
            return Err(Error::Config("combined worker batch limits exceed 128 MiB / 64000 rows".into()));
        }
        let millis = |key, default, low, high| number(key, default, low, high).map(|n| Duration::from_millis(n as u64));
        let retry_min = millis("METRICS_RETRY_MIN_MS", 100, 10, 30_000)?;
        let retry_max = millis("METRICS_RETRY_MAX_MS", 5_000, 10, 30_000)?;
        if retry_min > retry_max {
            return Err(Error::Config("retry minimum exceeds maximum".into()));
        }
        let instance = var("NETMON_INSTANCE_ID").unwrap_or_else(|| format!("metrics-batcher-{}", std::process::id()));
        if !site_token(&instance) {
            return Err(Error::Config("NETMON_INSTANCE_ID: invalid token".into()));
        }
        let bootstrap = var("NETMON_BOOTSTRAP_STREAMS")
            .map(|v| v.parse::<bool>())
            .transpose()
            .map_err(|_| Error::Config("NETMON_BOOTSTRAP_STREAMS: expected boolean".into()))?
            .unwrap_or(false);
        let profile = var("NETMON_STREAM_PROFILE").unwrap_or_else(|| "prod".into());
        Ok(Self {
            nats_url: var("NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".into()),
            nats_creds: var("NATS_CREDS_FILE"),
            clickhouse_url,
            clickhouse_database: database,
            clickhouse_user: required("CLICKHOUSE_USER")?,
            clickhouse_password: get("CLICKHOUSE_PASSWORD")
                .ok_or_else(|| Error::Config("CLICKHOUSE_PASSWORD is required".into()))?,
            site,
            instance,
            max_rows,
            max_bytes,
            concurrency,
            flush_interval: millis("METRICS_FLUSH_INTERVAL_MS", 750, 10, 60_000)?,
            insert_timeout: millis("METRICS_INSERT_TIMEOUT_MS", 10_000, 100, 60_000)?,
            shutdown_timeout: millis("METRICS_SHUTDOWN_TIMEOUT_MS", 15_000, 100, 120_000)?,
            retry_min,
            retry_max,
            bootstrap,
            stream_replicas: number("NETMON_STREAM_REPLICAS", 1, 1, 5)?,
            stream_limits: bus::StreamLimits::from_profile(&profile)
                .ok_or_else(|| Error::Config("NETMON_STREAM_PROFILE: expected dev or prod".into()))?,
        })
    }

    pub fn filter(&self) -> String {
        if self.site == "*" {
            contracts::subjects::METRICS_FILTER.into()
        } else {
            contracts::subjects::metric(&self.site)
        }
    }

    pub fn durable(&self) -> String {
        if self.site == "*" {
            "metrics-batcher-all".into()
        } else {
            format!("metrics-batcher-site-{}", self.site)
        }
    }
}
