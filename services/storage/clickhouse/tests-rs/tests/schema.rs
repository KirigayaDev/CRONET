use std::{error::Error, net::Ipv6Addr, path::Path, process::Command, time::Instant};

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const ROWS: usize = 10_000;
const BATCH_SIZE: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
struct Metric {
    event_time: i64,
    ingested_at: i64,
    #[serde(with = "clickhouse::serde::uuid")]
    device_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid::option")]
    interface_id: Option<Uuid>,
    metric: String,
    value: f64,
    collector_id: String,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
struct Log {
    event_time: i64,
    ingested_at: i64,
    #[serde(with = "clickhouse::serde::uuid::option")]
    device_id: Option<Uuid>,
    source_ip: Ipv6Addr,
    hostname: String,
    facility: u8,
    severity: u8,
    app_name: String,
    message: String,
    collector_id: String,
    attributes: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Row, Deserialize)]
struct Table {
    name: String,
    engine: String,
    partition_key: String,
    sorting_key: String,
    create_table_query: String,
    uuid: String,
}

fn migrations(directory: &Path, success: bool) -> Result {
    let output = Command::new("dbmate")
        .args(["--migrations-dir", &directory.to_string_lossy(), "--no-dump-schema", "up", "--strict"])
        .output()?;
    if output.status.success() != success {
        let mut diagnostic =
            format!("{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        for key in ["DATABASE_URL", "CLICKHOUSE_PASSWORD"] {
            if let Ok(secret) = std::env::var(key) {
                if !secret.is_empty() {
                    diagnostic = diagnostic.replace(&secret, "[redacted]");
                }
            }
        }
        return Err(format!("dbmate exit status: {}; {diagnostic}", output.status).into());
    }
    Ok(())
}

async fn tables(client: &Client) -> Result<Vec<Table>> {
    Ok(client
        .query(
            "SELECT name, engine, partition_key, sorting_key, create_table_query, toString(uuid) AS uuid \
             FROM system.tables WHERE database = 'cronet' ORDER BY name",
        )
        .fetch_all()
        .await?)
}

async fn columns(client: &Client, table: &str) -> Result<Vec<(String, String)>> {
    Ok(client
        .query("SELECT name, type FROM system.columns WHERE database = 'cronet' AND table = ? ORDER BY position")
        .bind(table)
        .fetch_all()
        .await?)
}

async fn check_schema(client: &Client) -> Result {
    assert_eq!(
        client.query("SELECT engine FROM system.databases WHERE name = 'cronet'").fetch_one::<String>().await?,
        "Atomic"
    );
    let metadata = tables(client).await?;
    assert_eq!(metadata.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["logs", "metrics", "schema_migrations"]);
    for table in metadata.iter().filter(|t| t.name != "schema_migrations") {
        assert_eq!(table.engine, "MergeTree");
        assert_eq!(table.partition_key, "toYYYYMMDD(event_time)");
        assert!(table.create_table_query.contains("TTL toDateTime(event_time) + toIntervalDay(30)"));
        let expected_sort =
            if table.name == "metrics" { "device_id, metric, event_time" } else { "device_id, event_time" };
        assert_eq!(table.sorting_key, expected_sort);
    }
    let common = [("event_time", "DateTime64(3, 'UTC')"), ("ingested_at", "DateTime64(3, 'UTC')")];
    for (table, remaining) in [
        (
            "metrics",
            vec![
                ("device_id", "UUID"),
                ("interface_id", "Nullable(UUID)"),
                ("metric", "LowCardinality(String)"),
                ("value", "Float64"),
                ("collector_id", "LowCardinality(String)"),
                ("labels", "Map(String, String)"),
            ],
        ),
        (
            "logs",
            vec![
                ("device_id", "Nullable(UUID)"),
                ("source_ip", "IPv6"),
                ("hostname", "LowCardinality(String)"),
                ("facility", "UInt8"),
                ("severity", "UInt8"),
                ("app_name", "LowCardinality(String)"),
                ("message", "String"),
                ("collector_id", "LowCardinality(String)"),
                ("attributes", "Map(String, String)"),
            ],
        ),
    ] {
        let expected: Vec<_> = common.iter().copied().chain(remaining).map(|(n, t)| (n.into(), t.into())).collect();
        assert_eq!(columns(client, table).await?, expected);
        let default = client
            .query("SELECT default_kind, default_expression FROM system.columns WHERE database = 'cronet' AND table = ? AND name = 'ingested_at'")
            .bind(table).fetch_one::<(String, String)>().await?;
        assert_eq!(default, ("DEFAULT".into(), "now64(3, 'UTC')".into()));
    }
    let versions = client
        .query("SELECT version FROM cronet.schema_migrations FINAL WHERE applied ORDER BY version")
        .fetch_all::<String>()
        .await?;
    assert_eq!(versions, ["20261016000100", "20261016000200"]);
    Ok(())
}

async fn batches_and_queries(client: &Client) -> Result {
    let start = client.query("SELECT toUnixTimestamp64Milli(now64(3)) - 14400000").fetch_one::<i64>().await?;
    let mut metrics = Vec::with_capacity(ROWS);
    let mut logs = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let device_id = Uuid::from_u128((i % 16 + 1) as u128);
        let event_time = start + i as i64 * 1_000 + 123;
        metrics.push(Metric {
            event_time,
            ingested_at: event_time + 500,
            device_id,
            interface_id: (i % 3 != 0).then(|| Uuid::from_u128((i % 32 + 100) as u128)),
            metric: if i / 16 % 2 == 0 { "interface.rx_bytes" } else { "interface.tx_bytes" }.into(),
            value: i as f64 * 1.25,
            collector_id: format!("collector-{}", i % 3),
            labels: vec![("site".into(), "test".into()), ("unit".into(), "bytes".into())],
        });
        logs.push(Log {
            event_time,
            ingested_at: event_time + 500,
            device_id: (i % 4 != 0).then_some(device_id),
            source_ip: if i % 2 == 0 { "::ffff:192.0.2.10" } else { "2001:db8::10" }.parse()?,
            hostname: format!("router-{}", i % 7),
            facility: (i % 24) as u8,
            severity: (i / 16 % 8) as u8,
            app_name: "syslog".into(),
            message: format!("Synthetic event {i}: интерфейс изменён"),
            collector_id: "collector-test".into(),
            attributes: vec![("vendor".into(), "synthetic".into()), ("raw_format".into(), "test".into())],
        });
    }
    let timer = Instant::now();
    for batch in metrics.chunks(BATCH_SIZE) {
        let mut insert = client.insert("cronet.metrics")?;
        for row in batch {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    let metrics_elapsed = timer.elapsed();
    let timer = Instant::now();
    for batch in logs.chunks(BATCH_SIZE) {
        let mut insert = client.insert("cronet.logs")?;
        for row in batch {
            insert.write(row).await?;
        }
        insert.end().await?;
    }
    eprintln!("RowBinary batches: {ROWS} metrics in {metrics_elapsed:?}; {ROWS} logs in {:?}", timer.elapsed());
    // Read back every field, including milliseconds, maps, both IP families, and nullable UUIDs.
    assert_eq!(
        client.query("SELECT ?fields FROM cronet.metrics ORDER BY event_time").fetch_all::<Metric>().await?,
        metrics
    );
    assert_eq!(client.query("SELECT ?fields FROM cronet.logs ORDER BY event_time").fetch_all::<Log>().await?, logs);

    let device = Uuid::from_u128(2);
    let lower = start + 2_017_123;
    let upper = start + 8_001_123;
    let expected_metrics: Vec<_> = metrics
        .iter()
        .filter(|r| {
            r.device_id == device && r.metric == "interface.rx_bytes" && r.event_time >= lower && r.event_time < upper
        })
        .cloned()
        .collect();
    assert!(!expected_metrics.is_empty());
    let selected = client
        .query("SELECT ?fields FROM cronet.metrics WHERE device_id = toUUID(?) AND metric = ? AND event_time >= fromUnixTimestamp64Milli(?) AND event_time < fromUnixTimestamp64Milli(?) ORDER BY event_time")
        .bind(device.to_string()).bind("interface.rx_bytes").bind(lower).bind(upper)
        .fetch_all::<Metric>().await?;
    assert_eq!(selected, expected_metrics);
    assert_eq!(selected.first().map(|r| r.event_time), Some(lower));
    assert!(selected.iter().all(|r| r.event_time < upper));

    let expected_logs: Vec<_> = logs
        .iter()
        .filter(|r| r.device_id == Some(device) && r.severity <= 3 && r.event_time >= lower && r.event_time < upper)
        .cloned()
        .collect();
    assert!(!expected_logs.is_empty());
    let selected = client
        .query("SELECT ?fields FROM cronet.logs WHERE device_id = toUUID(?) AND severity <= 3 AND event_time >= fromUnixTimestamp64Milli(?) AND event_time < fromUnixTimestamp64Milli(?) ORDER BY event_time")
        .bind(device.to_string()).bind(lower).bind(upper).fetch_all::<Log>().await?;
    assert_eq!(selected, expected_logs);
    let plan = client
        .query("EXPLAIN indexes=1 SELECT count() FROM cronet.logs WHERE device_id = toUUID(?) AND event_time >= fromUnixTimestamp64Milli(?)")
        .bind(device.to_string()).bind(lower).fetch_all::<String>().await?.join("\n");
    let primary_key_plan = plan.split("PrimaryKey").nth(1).ok_or("Expected primary key index in the log query plan")?;
    assert!(primary_key_plan.contains("device_id"), "Device filter must use the primary key: {plan}");
    for (facility, severity, constraint) in [(24, 0, "facility_range"), (0, 8, "severity_range")] {
        let error = client
            .query("INSERT INTO cronet.logs (event_time, facility, severity) VALUES (now64(3), ?, ?)")
            .bind(facility)
            .bind(severity)
            .execute()
            .await
            .expect_err("invalid syslog code must fail");
        assert!(error.to_string().contains(constraint), "Unexpected error: {error}");
    }
    for ip in ["::ffff:192.0.2.10", "2001:db8::10"] {
        let expected: Vec<_> =
            logs.iter().filter(|r| r.hostname == "router-1" && r.source_ip.to_string() == ip).cloned().collect();
        assert!(!expected.is_empty());
        let selected = client.query("SELECT ?fields FROM cronet.logs WHERE hostname = 'router-1' AND source_ip = toIPv6(?) ORDER BY event_time")
            .bind(ip).fetch_all::<Log>().await?;
        assert_eq!(selected, expected);
    }
    let unmatched = client
        .query("SELECT ?fields FROM cronet.logs WHERE device_id IS NULL ORDER BY event_time")
        .fetch_all::<Log>()
        .await?;
    assert_eq!(unmatched.len(), 2_500);
    assert_eq!(unmatched, logs.iter().filter(|r| r.device_id.is_none()).cloned().collect::<Vec<_>>());
    // A plain IPv4 literal is converted to the same IPv4-mapped representation.
    assert_eq!(
        client
            .query("SELECT count() FROM cronet.logs WHERE source_ip = toIPv6('192.0.2.10')")
            .fetch_one::<u64>()
            .await?,
        5_000
    );
    eprintln!("Queries and full round trips passed (UUID/NULL, UTC milliseconds, IPv4/IPv6, maps, UTF-8)");
    Ok(())
}

async fn failed_migration_recovery(client: &Client) -> Result {
    let directory = std::env::temp_dir().join(format!("cronet-migration-test-{}", std::process::id()));
    std::fs::create_dir(&directory)?;
    let migration = directory.join("20990101000100_retry.sql");
    std::fs::write(&migration, "-- migrate:up transaction:false\nINVALID SQL;\n-- migrate:down\nSELECT 1;\n")?;
    migrations(&directory, false)?;
    assert_eq!(
        client
            .query("SELECT count() FROM cronet.schema_migrations WHERE version = '20990101000100'")
            .fetch_one::<u64>()
            .await?,
        0
    );
    // Simulate DDL committed before dbmate could record its version (e.g. lost connection).
    client.query("CREATE TABLE cronet.retry_fixture (id UInt8) ENGINE = MergeTree ORDER BY id").execute().await?;
    std::fs::write(&migration, "-- migrate:up transaction:false\nCREATE TABLE IF NOT EXISTS cronet.retry_fixture (id UInt8) ENGINE = MergeTree ORDER BY id;\n-- migrate:down transaction:false\nSELECT throwIf(1, 'Forward-only');\n")?;
    migrations(&directory, true)?;
    migrations(&directory, true)?;
    assert_eq!(
        client
            .query("SELECT count() FROM cronet.schema_migrations WHERE version = '20990101000100'")
            .fetch_one::<u64>()
            .await?,
        1
    );
    std::fs::remove_file(migration)?;
    std::fs::remove_dir(directory)?;
    eprintln!("Failed migration remains pending; replay recovers an already committed DDL statement");
    Ok(())
}

#[tokio::test]
async fn real_clickhouse_schema() -> Result {
    // No default connection: accidental use against a developer/production DB must fail.
    let client = Client::default()
        .with_url(std::env::var("CLICKHOUSE_URL")?)
        .with_user(std::env::var("CLICKHOUSE_USER")?)
        .with_password(std::env::var("CLICKHOUSE_PASSWORD")?)
        .with_option("max_execution_time", "30");
    let version = client.query("SELECT version()").fetch_one::<String>().await?;
    assert!(version.starts_with("25.8."), "Required ClickHouse 25.8; got {version}");
    assert_eq!(
        client.query("SELECT count() FROM system.databases WHERE name = 'cronet'").fetch_one::<u64>().await?,
        0,
        "Requires a fresh isolated server"
    );
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
    migrations(&directory, true)?;
    check_schema(&client).await?;
    let before = tables(&client).await?;
    batches_and_queries(&client).await?;
    migrations(&directory, true)?;
    check_schema(&client).await?;
    assert_eq!(tables(&client).await?, before, "Replay must preserve table UUIDs and DDL");
    assert_eq!(client.query("SELECT count() FROM cronet.schema_migrations").fetch_one::<u64>().await?, 2);
    for table in ["metrics", "logs"] {
        assert_eq!(client.query(&format!("SELECT count() FROM cronet.{table}")).fetch_one::<u64>().await?, ROWS as u64);
    }
    let rollback = Command::new("dbmate")
        .args(["--migrations-dir", &directory.to_string_lossy(), "--no-dump-schema", "rollback"])
        .output()?;
    assert!(!rollback.status.success(), "Destructive rollback must be refused");
    check_schema(&client).await?;
    assert_eq!(tables(&client).await?, before);
    failed_migration_recovery(&client).await?;
    eprintln!("ClickHouse {version}: schema, TTL metadata, idempotency, forward-only safety, recovery passed");
    Ok(())
}
