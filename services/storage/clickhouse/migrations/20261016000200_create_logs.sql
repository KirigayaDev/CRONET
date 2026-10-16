-- migrate:up transaction:false
CREATE TABLE IF NOT EXISTS cronet.logs
(
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    device_id Nullable(UUID),
    source_ip IPv6,
    hostname LowCardinality(String),
    facility UInt8,
    severity UInt8,
    app_name LowCardinality(String),
    message String,
    collector_id LowCardinality(String),
    attributes Map(String, String),
    CONSTRAINT facility_range CHECK facility <= 23,
    CONSTRAINT severity_range CHECK severity <= 7
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (device_id, event_time)
-- Shared retention policy: 30 days from event_time (see README).
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE
SETTINGS allow_nullable_key = 1;

-- migrate:down transaction:false
SELECT throwIf(1, 'Forward-only migration: restore a backup or add a new reviewed migration');
