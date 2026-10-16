#![allow(clippy::unwrap_used, clippy::expect_used)]
use contracts::netmon::v1::LogEvent;
use logs_batcher::{
    batch::{Batch, Receipt},
    clickhouse::Sink,
    config::Config,
    error::Error,
    log::{self, LogRow},
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

fn event() -> LogEvent {
    LogEvent {
        event_id: "01800000-1234-7000-8000-000000000001".into(),
        event_time: Some(prost_types::Timestamp { seconds: 1_792_152_000, nanos: 123_456_789 }),
        device_id: None,
        source_ip: "192.0.2.10".into(),
        hostname: "router-1".into(),
        facility: 23,
        severity: 7,
        app_name: "syslog".into(),
        message: vec![0, 255, 128, b'\n'],
        collector_id: "test".into(),
        attributes: [("site".into(), "test".into())].into(),
        site: "test".into(),
    }
}
fn row() -> LogRow {
    log::decode(&event().encode_to_vec(), "logs.test").unwrap()
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
fn config_defaults_routes_and_bounds() {
    let cfg = config_with(&[]).unwrap();
    assert_eq!((cfg.max_rows, cfg.max_bytes), (500, 8 * 1024 * 1024));
    assert_eq!(cfg.flush_interval, Duration::from_millis(750));
    assert_eq!(cfg.filter(), "logs.test");
    assert_eq!(cfg.durable(), "logs-batcher-site-test");
    let cfg = config_with(&[("NETMON_SITE", "*")]).unwrap();
    assert_eq!(cfg.filter(), "logs.>");
    assert_eq!(cfg.durable(), "logs-batcher-all");
    for pair in [
        ("LOGS_BATCH_MAX_ROWS", "0"),
        ("LOGS_BATCH_MAX_BYTES", "262144"),
        ("LOGS_MAX_CONCURRENCY", "17"),
        ("NETMON_SITE", "a.b"),
        ("CLICKHOUSE_DATABASE", "default"),
        ("CLICKHOUSE_URL", "http://user:secret@localhost:8123"),
        ("LOGS_FLUSH_INTERVAL_MS", "0"),
        ("LOGS_BATCH_MAX_ROWS", "invalid"),
        ("NETMON_BOOTSTRAP_STREAMS", "maybe"),
    ] {
        assert!(!config_with(&[pair]).err().unwrap().to_string().contains("secret"));
    }
    assert!(config_with(&[("LOGS_MAX_CONCURRENCY", "16"), ("LOGS_BATCH_MAX_BYTES", "16777216")]).is_err());
    assert!(config_with(&[("LOGS_RETRY_MIN_MS", "5000"), ("LOGS_RETRY_MAX_MS", "100")]).is_err());
}

#[test]
fn valid_log_preserves_uuid_bytes_and_maps_ip_families_and_nullable_device() {
    let mut event = event();
    assert_eq!(LogEvent::decode(event.encode_to_vec().as_slice()).unwrap(), event);
    let row = row();
    assert_eq!(row.event_time, 1_792_152_000_123);
    assert_eq!(row.event_id.to_string(), event.event_id);
    assert_eq!(row.message, event.message);
    assert_eq!(row.source_ip, "::ffff:192.0.2.10".parse::<std::net::Ipv6Addr>().unwrap());
    assert_eq!((row.facility, row.severity), (23, 7));
    assert!(row.device_id.is_none());
    event.device_id = Some("00000000-0000-0000-0000-000000000123".into());
    event.source_ip = "2001:db8::10".into();
    let row = log::decode(&event.encode_to_vec(), "logs.test").unwrap();
    assert_eq!(row.source_ip, "2001:db8::10".parse::<std::net::Ipv6Addr>().unwrap());
    assert_eq!(row.device_id.unwrap().to_string(), event.device_id.unwrap());
    assert_eq!(contracts::subjects::log("test"), "logs.test");
}

#[test]
fn permanent_invalid_uuid_ip_codes_metadata_and_timestamp() {
    for case in 0..14 {
        let mut event = event();
        match case {
            0 => event.event_id = "bad".into(),
            1 => event.event_id = uuid::Uuid::nil().to_string(),
            2 => event.device_id = Some("bad".into()),
            3 => event.device_id = Some("".into()),
            4 => event.source_ip = "999.1.2.3".into(),
            5 => event.facility = 24,
            6 => event.severity = 8,
            7 => event.event_time = None,
            8 => event.event_time.as_mut().unwrap().nanos = -1,
            9 => event.event_time.as_mut().unwrap().seconds = i64::MAX,
            10 => event.collector_id.clear(),
            11 => event.site = "wrong.site".into(),
            12 => event.hostname = "x".repeat(257),
            _ => event.app_name = "a\nb".into(),
        }
        assert!(log::decode(&event.encode_to_vec(), "logs.test").is_err(), "case {case}");
    }
    assert!(log::decode(&event().encode_to_vec(), "logs.other").is_err());
}

#[test]
fn bounded_envelope_message_and_attributes() {
    assert!(log::decode(&vec![0; log::MAX_PAYLOAD_BYTES + 1], "logs.test").is_err());
    let mut large = event();
    large.message = vec![255; log::MAX_MESSAGE_BYTES];
    assert!(log::decode(&large.encode_to_vec(), "logs.test").is_ok());
    large.message.push(0);
    assert!(log::decode(&large.encode_to_vec(), "logs.test").is_err());
    for case in 0..5 {
        let mut event = event();
        event.attributes = match case {
            0 => (0..65).map(|i| (format!("k{i}"), "v".into())).collect(),
            1 => [("k".repeat(129), "v".into())].into(),
            2 => [("k".into(), "v".repeat(4097))].into(),
            3 => [("".into(), "v".into())].into(),
            _ => (0..10).map(|i| (format!("k{i}"), "v".repeat(4000))).collect(),
        };
        assert!(log::decode(&event.encode_to_vec(), "logs.test").is_err());
    }
    // Every accepted row fits the minimum configured batch; no permanent NAK loop.
    let mut large = event();
    large.message = vec![0; log::MAX_MESSAGE_BYTES];
    let row = log::decode(&large.encode_to_vec(), "logs.test").unwrap();
    assert!(row.encoded_bytes() < 256 * 1024);
}

#[tokio::test]
async fn invalid_delivery_is_terminated_without_storage_ack() {
    let receipt = Counts::default();
    assert!(worker::decode_or_term(&[255], "logs.test", &receipt).await.is_none());
    assert_eq!(receipt.term.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.ack.load(Ordering::SeqCst), 0);
    assert!(worker::decode_or_term(&event().encode_to_vec(), "logs.test", &receipt).await.is_some());
    assert_eq!(receipt.term.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Default)]
struct Counts {
    ack: Arc<AtomicUsize>,
    progress: Arc<AtomicUsize>,
    term: Arc<AtomicUsize>,
}
impl Receipt for Counts {
    async fn terminate(&self) {
        self.term.fetch_add(1, Ordering::SeqCst);
    }
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
    async fn write(&self, rows: &[LogRow]) -> Result<(), Error> {
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
        async fn terminate(&self) {}
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
