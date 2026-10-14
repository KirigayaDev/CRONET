use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_nats::jetstream::{message::AckKind, Message};
use bus::Bus;
use contracts::{netmon::v1::PollTask, subjects};
use creds::CredStore;
use futures::StreamExt;
use prost::Message as _;
use tokio::sync::{watch, Semaphore};

use crate::{poll, transport::SnmpTransport};

pub struct Worker {
    pub bus: Bus,
    pub site: String,
    pub policy: Arc<poll::Policy>,
    pub creds: Arc<dyn CredStore>,
    pub transport: Arc<dyn SnmpTransport>,
    pub max_concurrency: usize,
}

/// Максимальный размер входящей задачи. Больше - заведомо мусор.
const MAX_TASK_BYTES: usize = 256 * 1024;

impl Worker {
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), bus::BusError> {
        let ack_wait = Duration::from_secs(60);
        let consumer = self
            .bus
            .pull_consumer(
                subjects::TASKS_STREAM,
                &format!("snmp-poller-{}", self.site),
                subjects::snmp_task(&self.site),
                ack_wait,
                3,
                i64::try_from(self.max_concurrency * 2).unwrap_or(400),
            )
            .await?;

        let mut messages = consumer
            .stream()
            .max_messages_per_batch(self.max_concurrency.clamp(1, 100))
            .messages()
            .await
            .map_err(|e| bus::BusError::JetStream(e.to_string()))?;

        let sem = Arc::new(Semaphore::new(self.max_concurrency));
        let this = Arc::new(self);

        loop {
            let permit = tokio::select! {
                p = sem.clone().acquire_owned() => match p { Ok(p) => p, Err(_) => break },
                _ = shutdown.changed() => break,
            };
            let msg = tokio::select! {
                m = messages.next() => m,
                _ = shutdown.changed() => break,
            };
            let Some(msg) = msg else { break };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "ошибка чтения из JetStream");
                    continue;
                }
            };
            let this = this.clone();
            tokio::spawn(async move {
                this.handle(msg).await;
                drop(permit);
            });
        }

        tracing::info!("остановка: ждём завершения задач в работе");
        let all = u32::try_from(this.max_concurrency).unwrap_or(u32::MAX);
        let _ = tokio::time::timeout(Duration::from_secs(35), sem.acquire_many(all)).await;
        Ok(())
    }

    async fn handle(&self, msg: Message) {
        if msg.payload.len() > MAX_TASK_BYTES {
            tracing::warn!(bytes = msg.payload.len(), "слишком большая задача, отбрасываем");
            let _ = msg.ack_with(AckKind::Term).await;
            return;
        }
        let task = match PollTask::decode(msg.payload.as_ref()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "не удалось декодировать PollTask, Term");
                let _ = msg.ack_with(AckKind::Term).await;
                return;
            }
        };

        let result = poll::execute(&task, SystemTime::now(), &self.policy, &*self.creds, &*self.transport).await;
        tracing::info!(
            task_id = %task.task_id,
            device = %task.device_id,
            status = result.status,
            varbinds = result.varbinds.len(),
            duration_ms = result.duration_ms,
            "опрос завершён"
        );

        let msg_id = format!("{}:{}", task.task_id, task.attempt);
        match self.bus.publish_proto(subjects::snmp_result(&self.site), &msg_id, &result).await {
            Ok(()) => {
                if let Err(e) = msg.double_ack().await {
                    // результат уже опубликован; при повторной доставке дубликат отсеется по Msg-Id
                    tracing::warn!(error = %e, "ack задачи не подтверждён");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, task_id = %task.task_id, "результат не опубликован, возвращаем задачу в очередь");
                let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(5)))).await;
            }
        }
    }
}
