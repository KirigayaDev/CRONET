#![allow(clippy::unwrap_used)]

use async_nats::jetstream::stream::{RetentionPolicy, StorageType};
use bus::{Bus, BusConfig, StreamLimits};
use contracts::subjects;
use std::time::Duration;

#[test]
fn syslog_limits_are_additive() {
    let mb = 1024 * 1024;
    let dev = StreamLimits::dev();
    assert_eq!((dev.tasks, dev.raw_snmp, dev.raw_trap, dev.raw_gnmi), (32 * mb, 128 * mb, 64 * mb, 128 * mb));
    assert_eq!(dev.raw_syslog, 128 * mb);
    let prod = StreamLimits::prod();
    assert_eq!((prod.tasks, prod.raw_snmp, prod.raw_trap, prod.raw_gnmi), (512 * mb, 4096 * mb, 2048 * mb, 8192 * mb));
    assert_eq!(prod.raw_syslog, 8192 * mb);
}

#[tokio::test]
async fn syslog_stream_policy() {
    if std::env::var_os("NETMON_E2E").is_none() {
        return;
    }
    let bus = Bus::connect(&BusConfig {
        url: "nats://127.0.0.1:4222".into(),
        creds_file: None,
        name: "syslog-stream-test".into(),
    })
    .await
    .unwrap();
    bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    // Repeating bootstrap must preserve all existing stream policies.
    bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    for (name, filter, hours, bytes, retention) in [
        (subjects::TASKS_STREAM, subjects::TASKS_FILTER, 1, StreamLimits::dev().tasks, RetentionPolicy::WorkQueue),
        (
            subjects::RAW_SNMP_STREAM,
            subjects::RAW_SNMP_FILTER,
            24,
            StreamLimits::dev().raw_snmp,
            RetentionPolicy::Limits,
        ),
        (
            subjects::RAW_TRAP_STREAM,
            subjects::RAW_TRAP_FILTER,
            72,
            StreamLimits::dev().raw_trap,
            RetentionPolicy::Limits,
        ),
        (
            subjects::RAW_GNMI_STREAM,
            subjects::RAW_GNMI_FILTER,
            24,
            StreamLimits::dev().raw_gnmi,
            RetentionPolicy::Limits,
        ),
        (
            subjects::RAW_SYSLOG_STREAM,
            subjects::RAW_SYSLOG_FILTER,
            72,
            StreamLimits::dev().raw_syslog,
            RetentionPolicy::Limits,
        ),
    ] {
        let mut stream = bus.js.get_stream(name).await.unwrap();
        let config = &stream.info().await.unwrap().config;
        assert_eq!(config.subjects, [filter]);
        assert_eq!(config.retention, retention);
        assert_eq!(config.storage, StorageType::File);
        assert_eq!(config.max_age, Duration::from_secs(hours * 3600));
        assert_eq!(config.max_bytes, bytes);
        assert_eq!(config.duplicate_window, Duration::from_secs(120));
    }
}
