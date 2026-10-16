use crate::session::{collect, identifier, Policy};
use async_nats::jetstream::{AckKind, Message};
use bus::Bus;
use contracts::{netmon::v1::CliTask, subjects};
use creds::CredStore;
use futures::StreamExt;
use prost::Message as _;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};

pub const MAX_TASK_BYTES: usize = 64 * 1024;
pub const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
pub struct Worker {
    pub bus: Bus,
    pub site: String,
    pub policy: Arc<Policy>,
    pub creds: Arc<dyn CredStore>,
    pub concurrency: usize,
    pub shutdown_grace: Duration,
}
impl Worker {
    pub async fn run(self, mut stop: watch::Receiver<bool>) -> Result<(), &'static str> {
        if self.concurrency == 0 || self.concurrency > 256 {
            return Err("invalid CLI concurrency");
        }
        // Bus uses retry_on_initial_connect, so INFO may not exist at connect() return.
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            ready = tokio::time::timeout(Duration::from_secs(10), self.bus.client.flush()) => {
                if !matches!(ready, Ok(Ok(()))) { return Err("NATS connection not ready"); }
            }
        }
        // Worst case: raw + 4-byte normalized characters + protobuf/command overhead.
        if self.bus.client.server_info().max_payload
            < self.policy.max_total_bytes.saturating_mul(5).saturating_add(MAX_TASK_BYTES)
        {
            return Err("NATS max_payload too small: increase it or reduce CLI_MAX_TOTAL_BYTES");
        }
        let consumer = self
            .bus
            .pull_consumer(
                subjects::TASKS_CLI_STREAM,
                &format!("cli-collector-{}", self.site),
                subjects::cli_task(&self.site),
                self.policy.task_timeout + Duration::from_secs(30),
                3,
                512,
            )
            .await
            .map_err(|_| "cannot create CLI consumer")?;
        let mut tasks = JoinSet::new();
        tracing::info!("CLI worker ready");
        loop {
            while let Some(joined) = tasks.try_join_next() {
                if joined.is_err() {
                    tracing::error!("CLI task aborted; input remains unacknowledged");
                }
            }
            if *stop.borrow() {
                break;
            }
            if tasks.len() >= self.concurrency {
                tokio::select! { biased;
                    _ = stop.changed() => break,
                    joined = tasks.join_next() => {
                        if joined.is_some_and(|j| j.is_err()) { tracing::error!("CLI task aborted; input remains unacknowledged"); }
                    }
                }
                continue;
            }
            // Only shutdown cancels an outstanding pull. Joining another task must
            // not drop its reply subscription and strand a delivered task until ack_wait.
            tokio::select! { biased;
                _ = stop.changed() => break,
                message = async {
                    let mut batch = consumer.fetch().max_messages(1).expires(Duration::from_secs(1)).messages().await.map_err(|_| "CLI pull failed")?;
                    batch.next().await.transpose().map_err(|_| "CLI delivery failed")
                } => {
                    match message {
                        Ok(Some(msg)) => { let bus = self.bus.clone(); let site = self.site.clone(); let policy = self.policy.clone(); let creds = self.creds.clone();
                            tasks.spawn(async move { process(&bus, &site, &policy, creds.as_ref(), msg).await; });
                        },
                        Ok(None) => {},
                        Err(_) => { tracing::warn!("CLI pull failed; reconnecting"); tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} } },
                    }
                }
            }
        }
        drop(consumer);
        if tokio::time::timeout(self.shutdown_grace, async { while tasks.join_next().await.is_some() {} })
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        tracing::info!("CLI worker stopped");
        Ok(())
    }
}
async fn process(bus: &Bus, site: &str, policy: &Policy, creds: &dyn CredStore, message: Message) {
    if message.payload.len() > MAX_TASK_BYTES {
        let _ = message.ack_with(AckKind::Term).await;
        tracing::warn!("oversized CLI envelope terminated");
        return;
    }
    let task = match CliTask::decode(message.payload.as_ref()) {
        Ok(t) => t,
        Err(_) => {
            let _ = message.ack_with(AckKind::Term).await;
            tracing::warn!("invalid CLI protobuf terminated");
            return;
        }
    };
    let result = collect(&task, policy, creds).await;
    // publish_proto resolves only AFTER the JetStream result acknowledgement.
    let id = if identifier(&task.task_id) {
        format!("{}:{}", task.task_id, task.attempt)
    } else {
        // A BAD_TASK identity may contain CR/LF or exceed the NATS header limit.
        // Use input stream sequence for a safe, redelivery-stable fallback.
        let sequence = message.info().map(|i| i.stream_sequence).unwrap_or_default();
        format!("invalid-cli:{sequence}:{}", task.attempt)
    };
    let confirmed = matches!(
        tokio::time::timeout(PUBLISH_TIMEOUT, bus.publish_proto(subjects::cli_result(site), &id, &result)).await,
        Ok(Ok(()))
    );
    if confirmed {
        if message.double_ack().await.is_err() {
            tracing::warn!("CLI input acknowledgement failed; redelivery is possible");
        }
    } else {
        let _ = message.ack_with(AckKind::Nak(Some(Duration::from_secs(1)))).await;
        tracing::warn!("CLI result unconfirmed; task scheduled for redelivery");
    }
}
