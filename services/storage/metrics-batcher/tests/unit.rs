#![allow(clippy::unwrap_used, clippy::expect_used)]
use contracts::netmon::v1::MetricPoint;
use metrics_batcher::{
    batch::{Batch, Receipt},
    clickhouse::Sink,
    config::Config,
    error::Error,
    metric::{self, MetricRow},
    worker,
};
use prost::Message;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::watch;

fn point() -> MetricPoint {
    MetricPoint {
        event_id: "01800000-1234-7000-8000-000000000001".into(),
        event_time: Some(prost_types::Timestamp { seconds: 1_792_152_000, nanos: 123_456_789 }),
        device_id: "01800000-1234-7000-8000-000000000002".into(),
        interface_id: None,
        metric: "interface.rx_bytes".into(),
        value: 12.5,
        collector_id: "test".into(),
        labels: [("site".into(), "test".into())].into(),
        site: "test".into(),
    }
}
fn row() -> MetricRow {
    metric::decode(&point().encode_to_vec(), "metrics.test").unwrap()
}
fn config_with(extra: &[(&str, &str)]) -> Result<Config, Error> {
    Config::load(|key| {
        extra.iter().find(|(k, _)| *k == key).map(|(_, v)| v.to_string()).or_else(|| match key {
            "CLICKHOUSE_URL" => Some("http://localhost:8123".into()),
            "CLICKHOUSE_USER" => Some("test".into()),
            "CLICKHOUSE_PASSWORD" => Some("synthetic".into()),
            "NETMON_SITE" => Some("test".into()),
            _ => None,
        })
    })
}

#[test]
fn config_defaults_and_routes() {
    let config = config_with(&[]).unwrap();
    assert_eq!(config.max_rows, 1_000);
    assert_eq!(config.max_bytes, 4 * 1024 * 1024);
    assert_eq!(config.flush_interval, Duration::from_millis(750));
    assert_eq!(config.filter(), "metrics.test");
    assert_eq!(config.durable(), "metrics-batcher-site-test");
    let wildcard = config_with(&[("NETMON_SITE", "*")]).unwrap();
    assert_eq!(wildcard.filter(), "metrics.>");
    assert_eq!(wildcard.durable(), "metrics-batcher-all");
}

#[test]
fn config_rejects_invalid_and_unbounded_values_without_echoing_secrets() {
    for pair in [
        ("METRICS_BATCH_MAX_ROWS", "0"),
        ("METRICS_BATCH_MAX_BYTES", "1"),
        ("METRICS_MAX_CONCURRENCY", "17"),
        ("NETMON_SITE", "a.b"),
        ("CLICKHOUSE_DATABASE", "default"),
        ("CLICKHOUSE_URL", "http://user:do-not-echo@localhost:8123"),
        ("METRICS_FLUSH_INTERVAL_MS", "0"),
        ("METRICS_BATCH_MAX_ROWS", "invalid"),
        ("NETMON_BOOTSTRAP_STREAMS", "maybe"),
    ] {
        let error = config_with(&[pair]).err().unwrap();
        assert!(!error.to_string().contains("do-not-echo"));
    }
    assert!(config_with(&[("METRICS_MAX_CONCURRENCY", "16"), ("METRICS_BATCH_MAX_BYTES", "16777216")]).is_err());
    assert!(config_with(&[("METRICS_RETRY_MIN_MS", "5000"), ("METRICS_RETRY_MAX_MS", "100")]).is_err());
}

#[test]
fn normalized_contract_round_trip_and_uuid_mapping() {
    let point = point();
    assert_eq!(MetricPoint::decode(point.encode_to_vec().as_slice()).unwrap(), point);
    let row = row();
    assert_eq!(row.event_time, 1_792_152_000_123);
    assert_eq!(row.event_id.to_string(), point.event_id);
    assert!(row.interface_id.is_none());
    assert_eq!(row.value, 12.5);
    let mut point = point;
    point.interface_id = Some(point.device_id.clone());
    assert_eq!(metric::decode(&point.encode_to_vec(), "metrics.test").unwrap().interface_id, Some(row.device_id));
    assert_eq!(contracts::subjects::metric("test"), "metrics.test");
}

#[test]
fn permanent_invalid_payloads_are_rejected() {
    assert!(metric::decode(&[0xff], "metrics.test").is_err());
    assert!(metric::decode(&vec![0; metric::MAX_PAYLOAD_BYTES + 1], "metrics.test").is_err());
    for change in 0..12 {
        let mut point = point();
        match change {
            0 => point.event_id = "bad".into(),
            1 => point.event_id = uuid::Uuid::nil().to_string(),
            2 => point.device_id = "bad".into(),
            3 => point.interface_id = Some("".into()),
            4 => point.metric = " ".into(),
            5 => point.event_time = None,
            6 => point.event_time.as_mut().unwrap().nanos = -1,
            7 => point.event_time.as_mut().unwrap().seconds = i64::MAX,
            8 => point.value = f64::NAN,
            9 => point.collector_id.clear(),
            10 => point.site = "wrong.site".into(),
            _ => {
                point.labels.insert("".into(), "value".into());
            }
        }
        assert!(metric::decode(&point.encode_to_vec(), "metrics.test").is_err(), "case {change}");
    }
    assert!(metric::decode(&point().encode_to_vec(), "metrics.other").is_err());
}

#[test]
fn label_count_key_value_and_total_limits() {
    for case in 0..4 {
        let mut point = point();
        point.labels = match case {
            0 => (0..65).map(|i| (format!("k{i}"), "v".into())).collect(),
            1 => [("k".repeat(129), "v".into())].into(),
            2 => [("k".into(), "v".repeat(1025))].into(),
            _ => (0..20).map(|i| (format!("k{i}"), "v".repeat(1000))).collect(),
        };
        assert!(metric::decode(&point.encode_to_vec(), "metrics.test").is_err());
    }
}

#[derive(Clone, Default)]
struct Counts {
    ack: Arc<AtomicUsize>,
    progress: Arc<AtomicUsize>,
}
impl Receipt for Counts {
    async fn confirm(&self) -> bool {
        self.ack.fetch_add(1, Ordering::SeqCst);
        true
    }
    async fn progress(&self) {
        self.progress.fetch_add(1, Ordering::SeqCst);
    }
}
struct FakeSink {
    outcomes: Mutex<VecDeque<bool>>,
    writes: AtomicUsize,
    receipts: Counts,
    delay: Duration,
}
impl FakeSink {
    fn new(outcomes: &[bool], receipts: Counts) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.iter().copied().collect()),
            writes: AtomicUsize::new(0),
            receipts,
            delay: Duration::ZERO,
        }
    }
}
impl Sink for FakeSink {
    async fn write(&self, rows: &[MetricRow]) -> Result<(), Error> {
        assert!(!rows.is_empty());
        assert_eq!(self.receipts.ack.load(Ordering::SeqCst), 0, "ACK before commit");
        tokio::time::sleep(self.delay).await;
        self.writes.fetch_add(1, Ordering::SeqCst);
        if self.outcomes.lock().unwrap().pop_front().unwrap_or(true) {
            Ok(())
        } else {
            Err(Error::Write)
        }
    }
}

#[tokio::test]
async fn flush_by_row_count_and_bytes() {
    let mut batch = Batch::new(2, 4096, Duration::from_secs(60));
    assert!(!batch.ready());
    batch.push(row(), Counts::default()).unwrap();
    assert!(!batch.ready());
    batch.push(row(), Counts::default()).unwrap();
    assert!(batch.ready());
    assert!(!batch.fits(&row()));
    let bytes = row().encoded_bytes();
    let mut batch = Batch::new(100, bytes * 2, Duration::from_secs(60));
    batch.push(row(), Counts::default()).unwrap();
    assert!(!batch.ready());
    batch.push(row(), Counts::default()).unwrap();
    assert!(batch.ready());
    assert_eq!(batch.bytes(), bytes * 2);
    assert!(!batch.fits(&row()));
    assert!(batch.push(row(), Counts::default()).is_err());
}

#[tokio::test(start_paused = true)]
async fn flush_timer_starts_with_first_row() {
    let mut batch = Batch::new(1000, 4096, Duration::from_millis(750));
    tokio::time::advance(Duration::from_secs(100)).await;
    assert!(!batch.ready());
    batch.push(row(), Counts::default()).unwrap();
    tokio::time::advance(Duration::from_millis(749)).await;
    assert!(!batch.ready());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(batch.ready());
}

#[tokio::test]
async fn failure_retains_unacked_batch_then_success_confirms() {
    let receipt = Counts::default();
    let sink = FakeSink::new(&[false, true], receipt.clone());
    let mut batch = Batch::new(100, 4096, Duration::from_secs(1));
    batch.push(row(), receipt.clone()).unwrap();
    assert!(batch.commit(&sink).await.is_err());
    assert_eq!(batch.len(), 1);
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 0);
    batch.commit(&sink).await.unwrap();
    assert!(batch.is_empty());
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_ack_confirmation_does_not_repeat_committed_insert() {
    struct NoAck(Counts);
    impl Receipt for NoAck {
        async fn confirm(&self) -> bool {
            self.0.ack.fetch_add(1, Ordering::SeqCst);
            false
        }
        async fn progress(&self) {}
    }
    let receipt = Counts::default();
    let sink = FakeSink::new(&[true], receipt.clone());
    let mut batch = Batch::new(100, 4096, Duration::from_secs(1));
    batch.push(row(), NoAck(receipt.clone())).unwrap();
    batch.commit(&sink).await.unwrap();
    batch.commit(&sink).await.unwrap();
    assert_eq!(sink.writes.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 1);
    assert!(batch.is_empty());
}

#[tokio::test(start_paused = true)]
async fn bounded_retry_backoff_keeps_delivery_alive() {
    let receipt = Counts::default();
    let sink = FakeSink::new(&[false, false, false, false, true], receipt.clone());
    let mut config = config_with(&[]).unwrap();
    config.retry_min = Duration::from_secs(5);
    config.retry_max = config.retry_min;
    let (_sender, mut stop) = watch::channel(false);
    let mut batch = Batch::new(100, 4096, Duration::from_secs(1));
    batch.push(row(), receipt.clone()).unwrap();
    worker::flush(&mut batch, &sink, &config, &mut stop).await.unwrap();
    assert_eq!(sink.writes.load(Ordering::SeqCst), 5);
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 1);
    assert!(receipt.progress.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn graceful_final_flush_success_and_failure() {
    for success in [true, false] {
        let receipt = Counts::default();
        let sink = FakeSink::new(&[success], receipt.clone());
        let (_sender, mut stop) = watch::channel(true);
        let mut batch = Batch::new(100, 4096, Duration::from_secs(60));
        batch.push(row(), receipt.clone()).unwrap();
        assert_eq!(worker::flush(&mut batch, &sink, &config_with(&[]).unwrap(), &mut stop).await.is_ok(), success);
        assert_eq!(receipt.ack.load(Ordering::SeqCst), usize::from(success));
        assert_eq!(batch.is_empty(), success);
    }
}

#[tokio::test(start_paused = true)]
async fn slow_insert_renews_ack_deadline_without_acknowledging() {
    let receipt = Counts::default();
    let mut sink = FakeSink::new(&[true], receipt.clone());
    sink.delay = Duration::from_secs(121);
    let mut batch = Batch::new(100, 4096, Duration::from_secs(1));
    batch.push(row(), receipt.clone()).unwrap();
    batch.commit(&sink).await.unwrap();
    assert!(receipt.progress.load(Ordering::SeqCst) >= 11);
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 1);
}
