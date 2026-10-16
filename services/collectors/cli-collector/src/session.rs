use crate::{
    error::Failure,
    output::{normalize, Normalizer},
    transport::{self, KnownHosts},
};
use contracts::netmon::v1::{raw_cli_result::Status, CliTask, CommandOutput, RawCliResult};
use creds::{CredStore, Credential};
use encoding_rs::{Encoding, UTF_8, WINDOWS_1251};
use regex::{Regex, RegexBuilder};
use russh::{client::Msg, Channel, ChannelMsg};
use secrecy::ExposeSecret;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct Policy {
    pub collector_id: String,
    pub hosts: Arc<KnownHosts>,
    pub allowed: Regex,
    pub prompt: String,
    pub connect_timeout: Duration,
    pub auth_timeout: Duration,
    pub command_timeout: Duration,
    pub task_timeout: Duration,
    pub max_command_bytes: usize,
    pub max_total_bytes: usize,
}
pub struct Validated {
    target: SocketAddr,
    prompt: Regex,
    encoding: &'static Encoding,
    command_timeout: Duration,
    task_timeout: Duration,
}
pub fn bounded_regex(pattern: &str) -> Result<Regex, Failure> {
    if pattern.is_empty() || pattern.len() > 512 {
        return Err(Failure::bad());
    }
    RegexBuilder::new(pattern).size_limit(256 * 1024).dfa_size_limit(256 * 1024).build().map_err(|_| Failure::bad())
}
impl Policy {
    pub fn validate(&self, t: &CliTask) -> Result<Validated, Failure> {
        if !identifier(&t.task_id)
            || !identifier(&t.device_id)
            || t.credential_ref.is_empty()
            || t.credential_ref.len() > 512
            || t.credential_ref.chars().any(char::is_control)
        {
            return Err(Failure::bad());
        }
        let target: SocketAddr = t.target.parse().map_err(|_| Failure::bad())?;
        if target.port() == 0 || target.ip().is_unspecified() || target.ip().is_multicast() {
            return Err(Failure::bad());
        }
        let deadline = t.deadline.as_ref().ok_or_else(Failure::bad)?;
        if !(0..1_000_000_000).contains(&deadline.nanos) || deadline.seconds < 0 || deadline.seconds > 253_402_300_799 {
            return Err(Failure::bad());
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| Failure::bad())?;
        let deadline = Duration::new(deadline.seconds as u64, deadline.nanos as u32);
        let remaining = deadline
            .checked_sub(now)
            .filter(|d| !d.is_zero())
            .ok_or_else(|| Failure::new(Status::Expired, "task deadline has passed"))?;
        if t.commands.is_empty() || t.commands.len() > 32 {
            return Err(Failure::bad());
        }
        for command in &t.commands {
            if command.is_empty()
                || command.len() > 512
                || command.trim() != command
                || command.chars().any(|c| c.is_control() || ";|&<>`$\\".contains(c))
            {
                return Err(Failure::bad());
            }
            if !self.allowed.find(command).is_some_and(|m| m.start() == 0 && m.end() == command.len()) {
                return Err(Failure::bad());
            }
        }
        let encoding = match t.encoding.as_deref().unwrap_or("utf-8").to_ascii_lowercase().as_str() {
            "utf-8" => UTF_8,
            "windows-1251" => WINDOWS_1251,
            _ => return Err(Failure::bad()),
        };
        let prompt = bounded_regex(if t.prompt_regex.is_empty() { &self.prompt } else { &t.prompt_regex })?;
        if prompt.is_match("") {
            return Err(Failure::bad());
        }
        let command_timeout =
            if t.timeout_ms == 0 { self.command_timeout } else { Duration::from_millis(t.timeout_ms.into()) };
        if command_timeout < Duration::from_millis(10) || command_timeout > self.command_timeout {
            return Err(Failure::bad());
        }
        Ok(Validated { target, prompt, encoding, command_timeout, task_timeout: self.task_timeout.min(remaining) })
    }
}
pub fn identifier(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
}
pub fn timestamp() -> prost_types::Timestamp {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    prost_types::Timestamp { seconds: d.as_secs() as i64, nanos: d.subsec_nanos() as i32 }
}
fn millis(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u64::MAX as u128) as u64
}

pub async fn collect(task: &CliTask, policy: &Policy, store: &dyn CredStore) -> RawCliResult {
    let start = Instant::now();
    let mut result = RawCliResult {
        task_id: task.task_id.clone(),
        device_id: task.device_id.clone(),
        collector_id: policy.collector_id.clone(),
        attempt: task.attempt,
        ..Default::default()
    };
    let outcome = match policy.validate(task) {
        Err(e) => Err(e),
        Ok(valid) => {
            let outcome = tokio::time::timeout(valid.task_timeout, execute(task, policy, &valid, store, &mut result))
                .await
                .unwrap_or_else(|_| Err(Failure::timeout()));
            // Also normalize partial raw bytes when the outer task timer cancels a read.
            for output in &mut result.outputs {
                if output.normalized_output.is_none() {
                    output.normalized_output = Some(normalize(&output.raw_output, valid.encoding));
                }
            }
            outcome
        }
    };
    if let Err(e) = outcome {
        result.status = e.status.into();
        result.error_detail = e.detail.into();
    }
    result.truncated |= result.outputs.iter().any(|o| o.truncated);
    result.collected_at = Some(timestamp());
    result.duration_ms = millis(start);
    result
}
async fn execute(
    task: &CliTask,
    policy: &Policy,
    v: &Validated,
    store: &dyn CredStore,
    result: &mut RawCliResult,
) -> Result<(), Failure> {
    let credential = store
        .get(&task.credential_ref)
        .await
        .map_err(|_| Failure::new(Status::CredError, "credential lookup failed"))?;
    let Credential::Basic { username, password } = credential.as_ref() else {
        return Err(Failure::new(Status::CredError, "CLI requires Basic credentials"));
    };
    let (mut handle, _socket_guard) =
        transport::connect(v.target, policy.hosts.clone(), policy.connect_timeout).await?;
    let outcome = async {
        let auth =
            tokio::time::timeout(policy.auth_timeout, handle.authenticate_password(username, password.expose_secret()))
                .await
                .map_err(|_| Failure::timeout())?
                .map_err(|_| Failure::new(Status::AuthFail, "SSH authentication failed"))?;
        if !auth.success() {
            return Err(Failure::new(Status::AuthFail, "SSH authentication failed"));
        }
        let mut channel = tokio::time::timeout(v.command_timeout, async {
            let mut ch = handle.channel_open_session().await.map_err(|_| Failure::command())?;
            ch.request_pty(true, "vt100", 200, 24, 0, 0, &[]).await.map_err(|_| Failure::command())?;
            wait_success(&mut ch).await?;
            ch.request_shell(true).await.map_err(|_| Failure::command())?;
            wait_success(&mut ch).await?;
            Ok::<_, Failure>(ch)
        })
        .await
        .map_err(|_| Failure::timeout())??;
        let mut total = 0;
        let mut initial = CommandOutput::default();
        let mut normalizer = Normalizer::new(v.encoding, policy.max_command_bytes);
        let greeting = read_prompt(&mut channel, &mut initial, &mut total, policy, v, &mut normalizer).await;
        result.truncated = initial.truncated;
        greeting?;
        // Initial greeting counts against task byte budget but is not a command result.
        for command in &task.commands {
            let start = Instant::now();
            result.outputs.push(CommandOutput { command: command.clone(), ..Default::default() });
            let Some(output) = result.outputs.last_mut() else {
                return Err(Failure::command());
            };
            let outcome = tokio::time::timeout(v.command_timeout, async {
                channel.data_bytes(format!("{command}\n")).await.map_err(|_| Failure::command())?;
                read_prompt(&mut channel, output, &mut total, policy, v, &mut normalizer).await
            })
            .await
            .map_err(|_| Failure::timeout())
            .and_then(|r| r);
            output.duration_ms = millis(start);
            if output.normalized_output.is_none() {
                output.normalized_output = Some(normalize(&output.raw_output, v.encoding));
            }
            outcome?;
        }
        let _ = channel.close().await;
        Ok(())
    }
    .await;
    let _ =
        tokio::time::timeout(Duration::from_millis(500), handle.disconnect(russh::Disconnect::ByApplication, "", ""))
            .await;
    outcome
}
async fn wait_success(ch: &mut Channel<Msg>) -> Result<(), Failure> {
    // Request replies must not be ignored: unsupported PTY/shell is COMMAND_ERROR.
    loop {
        match ch.wait().await {
            Some(ChannelMsg::Success) => return Ok(()),
            Some(ChannelMsg::WindowAdjusted { .. }) => {}
            _ => return Err(Failure::command()),
        }
    }
}
async fn read_prompt(
    ch: &mut Channel<Msg>,
    out: &mut CommandOutput,
    total: &mut usize,
    policy: &Policy,
    v: &Validated,
    normalizer: &mut Normalizer,
) -> Result<(), Failure> {
    let outcome = tokio::time::timeout(v.command_timeout, async {
        loop {
            match ch.wait().await {
                Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => {
                    let remaining = policy
                        .max_command_bytes
                        .saturating_sub(out.raw_output.len())
                        .min(policy.max_total_bytes.saturating_sub(*total));
                    let n = data.len().min(remaining);
                    out.raw_output.extend_from_slice(&data[..n]);
                    *total += n;
                    normalizer.feed(&data[..n], false);
                    if n < data.len() {
                        out.truncated = true;
                        return Err(Failure::new(Status::CommandError, "CLI output exceeded byte limit"));
                    }
                    let tail = normalizer.tail();
                    if v.prompt.find_iter(&tail).any(|m| m.end() == tail.len()) {
                        return Ok(());
                    }
                }
                Some(ChannelMsg::WindowAdjusted { .. }) => {}
                _ => return Err(Failure::command()),
            }
        }
    })
    .await
    .map_err(|_| Failure::timeout())
    .and_then(|r| r);
    if outcome.is_err() {
        normalizer.feed(&[], true);
    }
    out.normalized_output = Some(normalizer.take_text());
    outcome
}
