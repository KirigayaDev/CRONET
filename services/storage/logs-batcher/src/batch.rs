use crate::{clickhouse::Sink, error::Error, log::LogRow};
use futures::future::join_all;
use std::{future::Future, time::Duration};
use tokio::time::Instant;

pub trait Receipt: Send + Sync {
    fn confirm(&self) -> impl Future<Output = bool> + Send;
    fn progress(&self) -> impl Future<Output = ()> + Send;
    fn terminate(&self) -> impl Future<Output = ()> + Send;
}

pub struct Batch<R> {
    rows: Vec<LogRow>,
    receipts: Vec<R>,
    bytes: usize,
    started: Option<Instant>,
    max_rows: usize,
    max_bytes: usize,
    interval: Duration,
}

impl<R: Receipt> Batch<R> {
    pub fn new(max_rows: usize, max_bytes: usize, interval: Duration) -> Self {
        Self {
            rows: Vec::with_capacity(max_rows),
            receipts: Vec::with_capacity(max_rows),
            bytes: 0,
            started: None,
            max_rows,
            max_bytes,
            interval,
        }
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn fits(&self, row: &LogRow) -> bool {
        self.len() < self.max_rows && self.bytes + row.encoded_bytes() <= self.max_bytes
    }
    pub fn push(&mut self, row: LogRow, receipt: R) -> Result<(), Error> {
        if !self.fits(&row) {
            return Err(Error::Worker);
        }
        self.started.get_or_insert_with(Instant::now);
        self.bytes += row.encoded_bytes();
        self.rows.push(row);
        self.receipts.push(receipt);
        Ok(())
    }
    pub fn ready(&self) -> bool {
        self.len() >= self.max_rows
            || self.bytes >= self.max_bytes
            || self.started.is_some_and(|start| start.elapsed() >= self.interval)
    }
    pub fn wait(&self) -> Duration {
        self.started.map(|start| self.interval.saturating_sub(start.elapsed())).unwrap_or(self.interval)
    }
    pub async fn progress(&self) {
        let send = async {
            for chunk in self.receipts.chunks(32) {
                join_all(chunk.iter().map(|r| r.progress())).await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), send).await;
    }
    pub async fn commit(&mut self, sink: &impl Sink) -> Result<(), Error> {
        if self.is_empty() {
            return Ok(());
        }
        {
            let write = sink.write(&self.rows);
            tokio::pin!(write);
            let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    result = &mut write => { result?; break; },
                    _ = heartbeat.tick() => self.progress().await,
                }
            }
        }
        // Never retry the INSERT just because an ACK response was lost. The
        // unconfirmed delivery is allowed to redeliver with the same event_id.
        let acknowledgements = async {
            let mut unconfirmed = 0;
            for chunk in self.receipts.chunks(32) {
                unconfirmed += join_all(chunk.iter().map(|r| r.confirm())).await.iter().filter(|ok| !**ok).count();
            }
            unconfirmed
        };
        let unconfirmed = match tokio::time::timeout(Duration::from_secs(5), acknowledgements).await {
            Ok(unconfirmed) => unconfirmed,
            Err(_) => self.len(),
        };
        tracing::info!(rows = self.len(), bytes = self.bytes, unconfirmed_acks = unconfirmed, "log batch committed");
        self.rows.clear();
        self.receipts.clear();
        self.bytes = 0;
        self.started = None;
        Ok(())
    }
}
