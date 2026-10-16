-- migrate:up transaction:false
CREATE TABLE IF NOT EXISTS cronet.metrics
(
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    device_id UUID,
    interface_id Nullable(UUID),
    metric LowCardinality(String),
    value Float64,
    collector_id LowCardinality(String),
    labels Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (device_id, metric, event_time)
-- Shared retention policy: 30 days from event_time (see README).
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE;

-- migrate:down transaction:false
SELECT throwIf(1, 'Forward-only migration: restore a backup or add a new reviewed migration');
