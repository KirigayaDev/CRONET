use crate::{
    batch::{Batch, Receipt},
    clickhouse::{ClickHouseSink, Sink},
    config::Config,
    error::Error,
    log,
};
use async_nats::jetstream::{
    consumer::{pull, Consumer},
    message::{AckKind, Acker},
};
use bus::{Bus, BusConfig};
use contracts::subjects;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};

pub struct NatsReceipt(pub Acker);

impl Receipt for NatsReceipt {
    async fn confirm(&self) -> bool {
        matches!(tokio::time::timeout(Duration::from_secs(2), self.0.double_ack()).await, Ok(Ok(())))
    }
    async fn progress(&self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.0.ack_with(AckKind::Progress)).await;
    }
    async fn terminate(&self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.0.ack_with(AckKind::Term)).await;
    }
}

/// A permanent failure sends Term, never a successful storage ACK. A lost Term
/// can redeliver and be validated again; no invalid payload is inserted.
pub async fn decode_or_term(payload: &[u8], subject: &str, receipt: &impl Receipt) -> Option<log::LogRow> {
    match log::decode(payload, subject) {
        Ok(row) => Some(row),
        Err(reason) => {
            tracing::warn!(reason = %reason, subject, "invalid LogEvent rejected");
            receipt.terminate().await;
            None
        }
    }
}

pub async fn flush<R: Receipt>(
    batch: &mut Batch<R>,
    sink: &impl Sink,
    cfg: &Config,
    stop: &mut watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut delay = cfg.retry_min;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.tick().await;
    loop {
        match batch.commit(sink).await {
            Ok(()) => return Ok(()),
            Err(error) if *stop.borrow() => return Err(error),
            Err(_) => tracing::warn!(
                rows = batch.len(),
                retry_ms = delay.as_millis(),
                "ClickHouse unavailable; retaining unacknowledged batch"
            ),
        }
        let wait = tokio::time::sleep(delay);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                _ = &mut wait => break,
                _ = stop.changed() => break,
                _ = heartbeat.tick() => batch.progress().await,
            }
        }
        delay = (delay * 2).min(cfg.retry_max);
    }
}

async fn consume(
    consumer: Consumer<pull::Config>,
    sink: Arc<impl Sink + 'static>,
    cfg: Arc<Config>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut batch = Batch::new(cfg.max_rows, cfg.max_bytes, cfg.flush_interval);
    let mut pull_backoff = cfg.retry_min;
    while !*stop.borrow() {
        if batch.ready() {
            flush(&mut batch, &*sink, &cfg, &mut stop).await?;
            continue;
        }
        // One finite request at a time: no hidden prefetch queue or unseen
        // deliveries expiring during an arbitrarily long ClickHouse outage.
        let request = consumer.fetch().max_messages(1).expires(batch.wait().max(Duration::from_millis(1)));
        let messages = tokio::select! {
            result = request.messages() => result,
            _ = stop.changed() => break,
        };
        let message = match messages {
            Ok(mut messages) => tokio::select! {
                value = messages.next() => value.transpose().map_err(|_| ()),
                _ = stop.changed() => break,
            },
            Err(_) => Err(()),
        };
        let message = match message {
            Ok(Some(message)) => {
                pull_backoff = cfg.retry_min;
                message
            }
            Ok(None) => continue,
            Err(()) => {
                tracing::warn!("JetStream pull unavailable; reconnecting with backoff");
                if !batch.is_empty() {
                    flush(&mut batch, &*sink, &cfg, &mut stop).await?;
                }
                tokio::select! { _ = tokio::time::sleep(pull_backoff) => {}, _ = stop.changed() => break }
                pull_backoff = (pull_backoff * 2).min(cfg.retry_max);
                continue;
            }
        };
        let (message, acker) = message.split();
        let receipt = NatsReceipt(acker);
        let Some(row) = decode_or_term(&message.payload, message.subject.as_str(), &receipt).await else { continue };
        if !batch.fits(&row) {
            // Leave this delivery on JetStream rather than exceeding the byte
            // cap or retaining an extra row while an earlier batch retries.
            let _ = tokio::time::timeout(Duration::from_secs(2), receipt.0.ack_with(AckKind::Nak(Some(cfg.retry_min))))
                .await;
            drop(message);
            drop(row);
            flush(&mut batch, &*sink, &cfg, &mut stop).await?;
        } else {
            // Keep only the ACK handle; release protobuf bytes and headers once
            // validated so unknown protobuf fields cannot inflate batch memory.
            drop(message);
            batch.push(row, receipt)?;
        }
    }
    flush(&mut batch, &*sink, &cfg, &mut stop).await
}

pub async fn run(cfg: Config, mut stop: watch::Receiver<bool>) -> Result<(), Error> {
    if *stop.borrow() {
        return Ok(());
    }
    let bus_config =
        BusConfig { url: cfg.nats_url.clone(), creds_file: cfg.nats_creds.clone(), name: cfg.instance.clone() };
    let bus = tokio::select! {
        result = Bus::connect(&bus_config) => result.map_err(|_| Error::Bus)?,
        _ = stop.changed() => return Ok(()),
    };
    let consumer = loop {
        let setup = async {
            if cfg.bootstrap {
                bus.ensure_streams(cfg.stream_replicas, &cfg.stream_limits).await?;
            }
            bus.pull_consumer(
                subjects::LOGS_STREAM,
                &cfg.durable(),
                cfg.filter(),
                Duration::from_secs(120),
                -1,
                (cfg.max_rows * cfg.concurrency) as i64,
            )
            .await
        };
        let result = tokio::select! { value = setup => value, _ = stop.changed() => return Ok(()) };
        match result {
            Ok(consumer) => break consumer,
            Err(_) => {
                tracing::warn!("LOGS durable unavailable; retrying initialization");
                tokio::select! { _ = tokio::time::sleep(cfg.retry_max) => {}, _ = stop.changed() => return Ok(()) }
            }
        }
    };
    let sink = Arc::new(ClickHouseSink::new(&cfg));
    let cfg = Arc::new(cfg);
    let (worker_stop, receiver) = watch::channel(false);
    let mut workers = JoinSet::new();
    for _ in 0..cfg.concurrency {
        workers.spawn(consume(consumer.clone(), sink.clone(), cfg.clone(), receiver.clone()));
    }
    tracing::info!(instance = %cfg.instance, durable = %cfg.durable(), concurrency = cfg.concurrency, "logs-batcher ready");
    let mut failure = false;
    tokio::select! {
        _ = stop.changed() => {},
        _ = workers.join_next() => failure = true,
    }
    let _ = worker_stop.send(true);
    let drain = async {
        while let Some(result) = workers.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                failure = true;
            }
        }
    };
    if tokio::time::timeout(cfg.shutdown_timeout, drain).await.is_err() {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        return Err(Error::Shutdown);
    }
    if failure {
        Err(Error::Worker)
    } else {
        Ok(())
    }
}
