use crate::{
    session::{bounded_regex, Policy},
    transport::KnownHosts,
};
use bus::StreamLimits;
use std::{path::Path, sync::Arc, time::Duration};

pub struct Config {
    pub site: String,
    pub nats_url: String,
    pub nats_creds: Option<String>,
    pub policy: Arc<Policy>,
    pub concurrency: usize,
    pub shutdown_grace: Duration,
    pub bootstrap: bool,
    pub replicas: usize,
    pub limits: StreamLimits,
}
pub fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}
fn number(name: &str, default: usize, min: usize, max: usize) -> Result<usize, &'static str> {
    let n = match env(name) {
        Some(s) => s.parse().map_err(|_| "invalid numeric CLI configuration")?,
        None => default,
    };
    if n < min || n > max {
        return Err("CLI configuration outside permitted range");
    }
    Ok(n)
}
fn duration(name: &str, default: usize, max: usize) -> Result<Duration, &'static str> {
    Ok(Duration::from_millis(number(name, default, 10, max)? as u64))
}
impl Config {
    pub fn from_env() -> Result<Self, &'static str> {
        let site = env("NETMON_SITE").ok_or("NETMON_SITE is required")?;
        if site.len() > 64 || !site.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)) {
            return Err("invalid NETMON_SITE");
        }
        let collector_id = env("NETMON_COLLECTOR_ID")
            .or_else(|| env("HOSTNAME"))
            .unwrap_or_else(|| format!("cli-{}", std::process::id()));
        if collector_id.len() > 128 || collector_id.chars().any(char::is_control) {
            return Err("invalid collector id");
        }
        let hosts = Arc::new(KnownHosts::load(Path::new(
            &env("CLI_KNOWN_HOSTS_FILE").ok_or("CLI_KNOWN_HOSTS_FILE is required")?,
        ))?);
        // Vendor-specific diagnostic command policy must be explicitly supplied.
        let allowed = bounded_regex(&env("CLI_ALLOWED_COMMAND_REGEX").ok_or("CLI_ALLOWED_COMMAND_REGEX is required")?)
            .map_err(|_| "invalid CLI_ALLOWED_COMMAND_REGEX")?;
        if allowed.is_match("") {
            return Err("allow policy must not match empty commands");
        }
        let prompt = env("CLI_PROMPT_REGEX").unwrap_or_else(|| "(?m)[#>][ \\t]*$".into());
        let compiled = bounded_regex(&prompt).map_err(|_| "invalid CLI_PROMPT_REGEX")?;
        if compiled.is_match("") {
            return Err("prompt must not match empty output");
        }
        let task_timeout = duration("CLI_TASK_TIMEOUT_MS", 60_000, 600_000)?;
        let policy = Arc::new(Policy {
            collector_id,
            hosts,
            allowed,
            prompt,
            connect_timeout: duration("CLI_CONNECT_TIMEOUT_MS", 5000, 60_000)?,
            auth_timeout: duration("CLI_AUTH_TIMEOUT_MS", 5000, 60_000)?,
            command_timeout: duration("CLI_COMMAND_TIMEOUT_MS", 10_000, 120_000)?,
            task_timeout,
            max_command_bytes: number("CLI_MAX_COMMAND_BYTES", 1024 * 1024, 1, 1024 * 1024)?,
            max_total_bytes: number("CLI_MAX_TOTAL_BYTES", 4 * 1024 * 1024, 1, 4 * 1024 * 1024)?,
        });
        let bootstrap = match env("NETMON_BOOTSTRAP_STREAMS").as_deref().unwrap_or("false") {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => return Err("invalid NETMON_BOOTSTRAP_STREAMS"),
        };
        Ok(Self {
            site,
            nats_url: env("NATS_URL").unwrap_or_else(|| "nats://127.0.0.1:4222".into()),
            nats_creds: env("NATS_CREDS"),
            policy,
            concurrency: number("CLI_MAX_CONCURRENCY", 16, 1, 256)?,
            shutdown_grace: duration("CLI_SHUTDOWN_GRACE_MS", 10_000, 600_000)?,
            bootstrap,
            replicas: number("NETMON_STREAM_REPLICAS", 1, 1, 5)?,
            limits: StreamLimits::from_profile(&env("NETMON_STREAM_PROFILE").unwrap_or_else(|| "prod".into()))
                .ok_or("invalid stream profile")?,
        })
    }
}
