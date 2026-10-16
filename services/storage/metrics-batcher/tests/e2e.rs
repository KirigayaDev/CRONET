#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Real processes and servers only. Opt in with METRICS_E2E=1; see tests/compose.yml.
use async_nats::jetstream::{
    consumer::{pull, Consumer},
    stream::{RetentionPolicy, StorageType},
};
use bus::{Bus, BusConfig, StreamLimits};
use contracts::{netmon::v1::MetricPoint, subjects};
use futures::{stream, StreamExt, TryStreamExt};
use prost::Message;
use std::{
    error::Error,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{watch, Semaphore},
    task::JoinHandle,
};
use uuid::Uuid;

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Service {
    child: Child,
}
impl Service {
    fn start(name: &str, site: &str, interval: u64, concurrency: usize, ch_url: &str, nats_url: &str) -> Result<Self> {
        let child = Command::new(env!("CARGO_BIN_EXE_metrics-batcher"))
            .env("NATS_URL", nats_url)
            .env_remove("NATS_CREDS_FILE")
            .env("CLICKHOUSE_URL", ch_url)
            .env("CLICKHOUSE_DATABASE", "cronet")
            .env("CLICKHOUSE_USER", std::env::var("CLICKHOUSE_USER")?)
            .env("CLICKHOUSE_PASSWORD", std::env::var("CLICKHOUSE_PASSWORD")?)
            .env("NETMON_SITE", site)
            .env("NETMON_INSTANCE_ID", name)
            .env("NETMON_BOOTSTRAP_STREAMS", "false")
            .env("METRICS_BATCH_MAX_ROWS", "1000")
            .env("METRICS_BATCH_MAX_BYTES", "4194304")
            .env("METRICS_FLUSH_INTERVAL_MS", interval.to_string())
            .env("METRICS_MAX_CONCURRENCY", concurrency.to_string())
            .env("METRICS_RETRY_MIN_MS", "100")
            .env("METRICS_RETRY_MAX_MS", "500")
            .env("METRICS_INSERT_TIMEOUT_MS", "1500")
            .env("METRICS_SHUTDOWN_TIMEOUT_MS", "5000")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self { child })
    }
    async fn stop(mut self) -> Result {
        let pid = self.child.id().ok_or("service already exited")?;
        assert!(Command::new("kill").args(["-TERM", &pid.to_string()]).status().await?.success());
        let status = tokio::time::timeout(Duration::from_secs(8), self.child.wait()).await??;
        assert!(status.success(), "service shutdown: {status}");
        Ok(())
    }
}

// Test-only network fault injection, without Docker socket access or server mocks.
// Every enabled connection is relayed byte-for-byte to the real target server.
struct Gate {
    url: String,
    enabled: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Gate {
    async fn new(target: &str, protocol: &str, enabled: bool) -> Result<Self> {
        let target = url::Url::parse(target)?;
        let address = format!("{}:{}", target.host_str().ok_or("gate target")?, target.port().unwrap_or(8123));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("{protocol}://{}", listener.local_addr()?);
        let (enabled, receiver) = watch::channel(enabled);
        let slots = Arc::new(Semaphore::new(32));
        let task = tokio::spawn(async move {
            loop {
                let Ok(permit) = slots.clone().acquire_owned().await else { break };
                let Ok((mut incoming, _)) = listener.accept().await else { break };
                let mut status = receiver.clone();
                if !*status.borrow_and_update() {
                    continue;
                }
                let address = address.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let Ok(Ok(mut outgoing)) =
                        tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address)).await
                    else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing) => {},
                        _ = status.changed() => {},
                    }
                });
            }
        });
        Ok(Self { url, enabled, task })
    }
    fn set(&self, value: bool) {
        self.enabled.send_replace(value);
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn point(site: &str, metric: &str, seed: u128, index: usize) -> MetricPoint {
    MetricPoint {
        event_id: Uuid::from_u128(seed * 1_000_000 + index as u128 + 1).to_string(),
        event_time: Some(prost_types::Timestamp {
            seconds: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            nanos: (index % 1000) as i32 * 1_000_000,
        }),
        device_id: Uuid::from_u128(123).to_string(),
        interface_id: (index % 2 == 0).then(|| Uuid::from_u128(456).to_string()),
        metric: metric.into(),
        value: index as f64 + 0.5,
        collector_id: "test-producer".into(),
        labels: [("fixture".into(), "c2".into())].into(),
        site: site.into(),
    }
}

async fn publish(bus: &Bus, site: &str, metric: &str, count: usize, seed: u128) -> Result {
    stream::iter(0..count)
        .map(|index| async move {
            let point = point(site, metric, seed, index);
            bus.publish_proto(subjects::metric(site), &point.event_id, &point).await
        })
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn rows(client: &clickhouse::Client, metric: &str) -> Result<(u64, u64)> {
    Ok(client
        .query("SELECT count(), uniqExact(event_id) FROM cronet.metrics WHERE metric = ?")
        .bind(metric)
        .fetch_one()
        .await?)
}

async fn wait_rows(client: &clickhouse::Client, metric: &str, expected: u64) -> Result {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let actual = rows(client, metric).await?;
            assert!(actual.0 <= expected, "duplicate rows for {metric}: {actual:?}");
            if actual.0 == expected {
                assert_eq!(actual.1, expected);
                return Ok::<(), Box<dyn Error + Send + Sync>>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    Ok(())
}

async fn consumer_state(bus: &Bus, site: &str) -> Result<(u64, u64, u64)> {
    let stream = bus.js.get_stream(subjects::METRICS_STREAM).await?;
    let mut consumer: Consumer<pull::Config> = stream.get_consumer(&format!("metrics-batcher-site-{site}")).await?;
    let info = consumer.info().await?;
    assert_eq!(info.config.max_ack_pending, if site == "normal" { 2000 } else { 1000 });
    Ok((info.num_pending, info.num_ack_pending as u64, info.ack_floor.consumer_sequence))
}

async fn wait_pending(bus: &Bus, site: &str, pending: u64) -> Result {
    let mut last = String::new();
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match consumer_state(bus, site).await {
                Ok((queued, in_flight, floor)) => {
                    last = format!("queued={queued}, in_flight={in_flight}, ack_floor={floor}");
                    if queued == 0 && in_flight == pending {
                        return;
                    }
                }
                Err(error) => last = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if outcome.is_err() {
        return Err(format!("consumer {site}: expected in_flight={pending}; last state: {last}").into());
    }
    Ok(())
}

async fn insert_stats(client: &clickhouse::Client, instance: &str) -> Result<(u64, u64)> {
    client.query("SYSTEM FLUSH LOGS").execute().await?;
    Ok(client.query("SELECT count(), toUInt64(sum(written_rows)) FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = ?")
        .bind(format!("metrics-batcher:{instance}")).fetch_one().await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn real_metrics_pipeline() -> Result {
    if std::env::var("METRICS_E2E").as_deref() != Ok("1") {
        eprintln!("METRICS_E2E not set: real-server test skipped");
        return Ok(());
    }
    let ch_url = std::env::var("CLICKHOUSE_URL")?;
    let nats_url = std::env::var("NATS_URL")?;
    let client = clickhouse::Client::default()
        .with_url(&ch_url)
        .with_user(std::env::var("CLICKHOUSE_USER")?)
        .with_password(std::env::var("CLICKHOUSE_PASSWORD")?);
    assert_eq!(client.query("SELECT version()").fetch_one::<String>().await?, "25.8.33.6");
    assert_eq!(
        client.query("SELECT count() FROM system.databases WHERE name = 'cronet'").fetch_one::<u64>().await?,
        0,
        "requires isolated empty server"
    );
    let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../clickhouse/migrations");
    assert!(
        Command::new("dbmate")
            .args(["--migrations-dir", &migrations.to_string_lossy(), "--no-dump-schema", "up", "--strict"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?
            .success(),
        "migrations failed"
    );
    let bus = Bus::connect(&BusConfig { url: nats_url.clone(), creds_file: None, name: "c2-e2e".into() }).await?;
    bus.ensure_streams(1, &StreamLimits::dev()).await?;
    let stream = bus.js.get_stream(subjects::METRICS_STREAM).await?;
    assert_eq!(stream.cached_info().config.retention, RetentionPolicy::WorkQueue);
    assert_eq!(stream.cached_info().config.storage, StorageType::File);

    publish(&bus, "normal", "test.2500", 2500, 1).await?;
    let normal = Service::start("normal", "normal", 750, 2, &ch_url, &nats_url)?;
    wait_rows(&client, "test.2500", 2500).await?;
    wait_pending(&bus, "normal", 0).await?;
    let stats = insert_stats(&client, "normal").await?;
    assert!(stats.0 >= 3);
    assert_eq!(stats.1, 2500);
    assert_eq!(client.query("SELECT count() FROM cronet.metrics WHERE metric = 'test.2500' AND (collector_id != 'test-producer' OR device_id != toUUID('00000000-0000-0000-0000-00000000007b') OR labels['fixture'] != 'c2')").fetch_one::<u64>().await?, 0);
    eprintln!("E2E 2500 messages: exact rows and event IDs, {} batch inserts, concurrency=2", stats.0);

    let first = point("normal", "test.poison", 2, 0);
    let last = point("normal", "test.poison", 2, 1);
    bus.publish_proto(subjects::metric("normal"), &first.event_id, &first).await?;
    bus.js.publish(subjects::metric("normal"), vec![0xff].into()).await?.await?;
    let mut invalid = first.clone();
    invalid.event_id = "invalid-uuid".into();
    bus.js.publish(subjects::metric("normal"), invalid.encode_to_vec().into()).await?.await?;
    bus.publish_proto(subjects::metric("normal"), &last.event_id, &last).await?;
    wait_rows(&client, "test.poison", 2).await?;
    wait_pending(&bus, "normal", 0).await?;
    eprintln!("E2E malformed protobuf + invalid UUID: rejected, following valid messages committed");
    let low = Instant::now();
    publish(&bus, "normal", "test.low", 1, 3).await?;
    wait_rows(&client, "test.low", 1).await?;
    wait_pending(&bus, "normal", 0).await?;
    eprintln!("E2E low traffic: 1 row flushed in {:?}", low.elapsed());
    normal.stop().await?;

    let shutdown = Service::start("shutdown", "shutdown", 10_000, 1, &ch_url, &nats_url)?;
    publish(&bus, "shutdown", "test.shutdown", 1, 4).await?;
    wait_pending(&bus, "shutdown", 1).await?;
    assert_eq!(rows(&client, "test.shutdown").await?.0, 0);
    shutdown.stop().await?;
    wait_rows(&client, "test.shutdown", 1).await?;
    wait_pending(&bus, "shutdown", 0).await?;
    eprintln!("E2E SIGTERM: partial batch flushed and ACK confirmed");

    let gate = Gate::new(&ch_url, "http", false).await?;
    let outage = Service::start("outage", "outage", 200, 1, &gate.url, &nats_url)?;
    publish(&bus, "outage", "test.recovery", 100, 5).await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok((queued, in_flight, floor)) = consumer_state(&bus, "outage").await {
                if queued + in_flight == 100 && in_flight > 0 && floor == 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let state = consumer_state(&bus, "outage").await?;
    assert_eq!(state.0 + state.1, 100);
    assert!(state.1 > 0);
    assert_eq!(state.2, 0);
    assert_eq!(rows(&client, "test.recovery").await?.0, 0);
    gate.set(true);
    wait_rows(&client, "test.recovery", 100).await?;
    wait_pending(&bus, "outage", 0).await?;
    outage.stop().await?;
    eprintln!("E2E ClickHouse outage: 100 unacked while unavailable; 100 unique rows after recovery");

    let nats_gate = Gate::new(&nats_url, "nats", true).await?;
    let mut reconnect = Service::start("reconnect", "reconnect", 200, 1, &ch_url, &nats_gate.url)?;
    publish(&bus, "reconnect", "test.nats-initial", 1, 6).await?;
    wait_rows(&client, "test.nats-initial", 1).await?;
    wait_pending(&bus, "reconnect", 0).await?;
    nats_gate.set(false);
    publish(&bus, "reconnect", "test.nats-recovery", 10, 7).await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(reconnect.child.try_wait()?.is_none());
    assert_eq!(rows(&client, "test.nats-recovery").await?.0, 0);
    nats_gate.set(true);
    wait_rows(&client, "test.nats-recovery", 10).await?;
    wait_pending(&bus, "reconnect", 0).await?;
    reconnect.stop().await?;
    eprintln!("E2E NATS outage: process remains alive and resumes durable consumption");

    let a = Service::start("replica-a", "multi", 750, 1, &ch_url, &nats_url)?;
    let b = Service::start("replica-b", "multi", 750, 1, &ch_url, &nats_url)?;
    wait_pending(&bus, "multi", 0).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish(&bus, "multi", "test.multi", 4000, 8).await?;
    wait_rows(&client, "test.multi", 4000).await?;
    wait_pending(&bus, "multi", 0).await?;
    a.stop().await?;
    b.stop().await?;
    let a = insert_stats(&client, "replica-a").await?;
    let b = insert_stats(&client, "replica-b").await?;
    assert!(a.1 > 0 && b.1 > 0);
    assert_eq!(a.1 + b.1, 4000);
    eprintln!("E2E two replicas: {} + {} = 4000 rows, no duplicate event IDs", a.1, b.1);

    let performance = Service::start("performance", "performance", 750, 1, &ch_url, &nats_url)?;
    let timer = Instant::now();
    publish(&bus, "performance", "test.performance", 10_000, 9).await?;
    wait_rows(&client, "test.performance", 10_000).await?;
    wait_pending(&bus, "performance", 0).await?;
    let elapsed = timer.elapsed();
    performance.stop().await?;
    let stats = insert_stats(&client, "performance").await?;
    assert!(stats.0 >= 10);
    assert_eq!(stats.1, 10_000);
    eprintln!(
        "C2 SANITY: 10000 metrics, {} batches, {:.0} rows/sec, elapsed {elapsed:?}",
        stats.0,
        10_000.0 / elapsed.as_secs_f64()
    );
    assert_eq!(client.query("SELECT count() FROM cronet.metrics").fetch_one::<u64>().await?, 16_615);
    eprintln!("C2 E2E passed: 8 scenarios, 16615 persisted metrics, 2 rejected messages");
    Ok(())
}
