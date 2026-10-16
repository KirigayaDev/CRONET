//! Тонкая обвязка над NATS JetStream для коллекторов.
//!
//! * `ensure_streams` создаёт стримы с политиками из архитектуры
//!   (задачи - WorkQueue, сырые данные - Limits с ограничением по возрасту и размеру);
//! * `publish_proto` публикует protobuf с `Nats-Msg-Id` и ждёт подтверждения
//!   JetStream (иначе нельзя безопасно делать ack задачи);
//! * `pull_consumer` создаёт durable pull-консьюмер для воркеров poller'ов.

use std::time::Duration;

use async_nats::{
    header::NATS_MESSAGE_ID,
    jetstream::{
        self,
        consumer::{pull, AckPolicy, DeliverPolicy},
        stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType},
        Context,
    },
    HeaderMap,
};
use contracts::subjects;
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("подключение к NATS: {0}")]
    Connect(String),
    #[error("JetStream: {0}")]
    JetStream(String),
    #[error("публикация не подтверждена: {0}")]
    Publish(String),
}

fn js_err(e: impl std::fmt::Display) -> BusError {
    BusError::JetStream(e.to_string())
}

/// Размеры стримов в байтах. Профиль `prod` рассчитан на сервер с большим диском,
/// `dev` - на ноутбук и CI (JetStream не создаст стрим, если места на диске меньше лимита).
#[derive(Debug, Clone, Copy)]
pub struct StreamLimits {
    pub tasks: i64,
    pub raw_snmp: i64,
    pub raw_trap: i64,
    pub raw_gnmi: i64,
    pub metrics: i64,
    pub logs: i64,
}

impl StreamLimits {
    const MB: i64 = 1024 * 1024;

    pub fn prod() -> Self {
        Self {
            tasks: 512 * Self::MB,
            raw_snmp: 4096 * Self::MB,
            raw_trap: 2048 * Self::MB,
            raw_gnmi: 8192 * Self::MB,
            metrics: 2048 * Self::MB,
            logs: 4096 * Self::MB,
        }
    }

    pub fn dev() -> Self {
        Self {
            tasks: 32 * Self::MB,
            raw_snmp: 128 * Self::MB,
            raw_trap: 64 * Self::MB,
            raw_gnmi: 128 * Self::MB,
            metrics: 128 * Self::MB,
            logs: 128 * Self::MB,
        }
    }

    pub fn from_profile(name: &str) -> Option<Self> {
        match name {
            "prod" => Some(Self::prod()),
            "dev" => Some(Self::dev()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Bus {
    pub client: async_nats::Client,
    pub js: Context,
}

pub struct BusConfig {
    pub url: String,
    /// Файл учётных данных NATS (.creds), необязательно.
    pub creds_file: Option<String>,
    pub name: String,
}

impl Bus {
    pub async fn connect(cfg: &BusConfig) -> Result<Self, BusError> {
        let mut opts = async_nats::ConnectOptions::new()
            .name(&cfg.name)
            // клиент сам переподключается; пока шины нет, публикации ждут в
            // ограниченном буфере клиента и затем возвращают ошибку
            .retry_on_initial_connect()
            .max_reconnects(None);
        if let Some(path) = &cfg.creds_file {
            opts = opts.credentials_file(path).await.map_err(|e| BusError::Connect(e.to_string()))?;
        }
        let client = opts.connect(&cfg.url).await.map_err(|e| BusError::Connect(e.to_string()))?;
        let js = jetstream::new(client.clone());
        Ok(Self { client, js })
    }

    /// Идемпотентно создаёт стримы и KV-бакеты проекта.
    /// `replicas` = 3 в кластере, 1 для локальной разработки.
    pub async fn ensure_streams(&self, replicas: usize, limits: &StreamLimits) -> Result<(), BusError> {
        // Задачи: WorkQueue - сообщение живёт, пока его не подтвердили.
        self.js
            .get_or_create_stream(StreamConfig {
                name: subjects::TASKS_STREAM.into(),
                subjects: vec![subjects::TASKS_FILTER.into()],
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                num_replicas: replicas,
                // устаревшие задачи бессмысленны: через час удаляются в любом случае
                max_age: Duration::from_secs(3600),
                max_bytes: limits.tasks,
                discard: DiscardPolicy::Old,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .map_err(js_err)?;

        for (name, filter, max_age_h, max_bytes) in [
            (subjects::RAW_SNMP_STREAM, subjects::RAW_SNMP_FILTER, 24u64, limits.raw_snmp),
            // авария, потерянная в core NATS, хуже потерянной точки метрики: трапы тоже в JetStream
            (subjects::RAW_TRAP_STREAM, subjects::RAW_TRAP_FILTER, 72, limits.raw_trap),
            (subjects::RAW_GNMI_STREAM, subjects::RAW_GNMI_FILTER, 24, limits.raw_gnmi),
        ] {
            self.js
                .get_or_create_stream(StreamConfig {
                    name: name.into(),
                    subjects: vec![filter.into()],
                    retention: RetentionPolicy::Limits,
                    storage: StorageType::File,
                    num_replicas: replicas,
                    max_age: Duration::from_secs(max_age_h * 3600),
                    max_bytes,
                    // при переполнении теряем самое старое, а не блокируем коллекторы
                    discard: DiscardPolicy::Old,
                    duplicate_window: Duration::from_secs(120),
                    ..Default::default()
                })
                .await
                .map_err(js_err)?;
        }
        // Normalized metrics must survive a storage outage. Reject new publishes
        // at the disk cap instead of silently evicting unacknowledged telemetry.
        self.js
            .get_or_create_stream(StreamConfig {
                name: subjects::METRICS_STREAM.into(),
                subjects: vec![subjects::METRICS_FILTER.into()],
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                num_replicas: replicas,
                max_bytes: limits.metrics,
                discard: DiscardPolicy::New,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .map_err(js_err)?;
        // Same outage protection for normalized logs; raw stream policies above stay unchanged.
        self.js
            .get_or_create_stream(StreamConfig {
                name: subjects::LOGS_STREAM.into(),
                subjects: vec![subjects::LOGS_FILTER.into()],
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                num_replicas: replicas,
                max_bytes: limits.logs,
                discard: DiscardPolicy::New,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .map_err(js_err)?;
        Ok(())
    }

    /// KV-бакет (создаётся при отсутствии). `max_age` > 0 даёт TTL записей:
    /// так устроены lease'ы, которые исчезают, если владелец перестал их продлевать.
    pub async fn kv(&self, bucket: &str, max_age: Duration, replicas: usize) -> Result<jetstream::kv::Store, BusError> {
        if let Ok(s) = self.js.get_key_value(bucket).await {
            return Ok(s);
        }
        self.js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket.to_owned(),
                history: 1,
                max_age,
                num_replicas: replicas,
                ..Default::default()
            })
            .await
            .map_err(js_err)
    }

    /// Публикация с дедупликацией. Возвращается после подтверждения JetStream.
    pub async fn publish_proto<M: Message>(&self, subject: String, msg_id: &str, msg: &M) -> Result<(), BusError> {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, msg_id);
        let ack = self
            .js
            .publish_with_headers(subject, headers, msg.encode_to_vec().into())
            .await
            .map_err(|e| BusError::Publish(e.to_string()))?;
        ack.await.map_err(|e| BusError::Publish(e.to_string()))?;
        Ok(())
    }

    /// Durable pull-консьюмер: все инстансы с одним `durable` делят нагрузку.
    pub async fn pull_consumer(
        &self,
        stream: &str,
        durable: &str,
        filter_subject: String,
        ack_wait: Duration,
        max_deliver: i64,
        max_ack_pending: i64,
    ) -> Result<jetstream::consumer::Consumer<pull::Config>, BusError> {
        let stream = self.js.get_stream(stream).await.map_err(js_err)?;
        stream
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.into()),
                    filter_subject,
                    ack_policy: AckPolicy::Explicit,
                    deliver_policy: DeliverPolicy::All,
                    ack_wait,
                    max_deliver,
                    max_ack_pending,
                    ..Default::default()
                },
            )
            .await
            .map_err(js_err)
    }
}
