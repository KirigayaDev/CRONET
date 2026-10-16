#![allow(clippy::unwrap_used)]
use async_nats::jetstream::stream::{RetentionPolicy, StorageType};
use bus::{Bus, BusConfig, StreamLimits};
use std::time::Duration;

#[tokio::test]
async fn additive_cli_streams_preserve_existing_stream_policies() {
    if std::env::var("NETMON_E2E").is_err() {
        return;
    }
    let bus = Bus::connect(&BusConfig {
        url: "nats://127.0.0.1:4222".into(),
        creds_file: None,
        name: "cli-stream-test".into(),
    })
    .await
    .unwrap();
    let limits = StreamLimits::dev();
    bus.ensure_streams(1, &limits).await.unwrap();
    bus.ensure_streams(1, &limits).await.unwrap();
    for (name, subject, retention, hours, bytes) in [
        ("TASKS_SNMP", "tasks.snmp.>", RetentionPolicy::WorkQueue, 1, limits.tasks),
        ("TASKS_CLI", "tasks.cli.>", RetentionPolicy::WorkQueue, 1, limits.tasks_cli),
        ("RAW_SNMP", "raw.snmp.>", RetentionPolicy::Limits, 24, limits.raw_snmp),
        ("RAW_TRAP", "raw.trap.>", RetentionPolicy::Limits, 72, limits.raw_trap),
        ("RAW_GNMI", "raw.gnmi.>", RetentionPolicy::Limits, 24, limits.raw_gnmi),
        ("RAW_CLI", "raw.cli.>", RetentionPolicy::Limits, 24, limits.raw_cli),
    ] {
        let info = bus.js.get_stream(name).await.unwrap().info().await.unwrap().clone();
        assert_eq!(info.config.subjects, [subject]);
        assert_eq!(info.config.retention, retention);
        assert_eq!(info.config.storage, StorageType::File);
        assert_eq!(info.config.max_age, Duration::from_secs(hours * 3600));
        assert_eq!(info.config.max_bytes, bytes);
        assert_eq!(info.config.duplicate_window, Duration::from_secs(120));
    }
}
