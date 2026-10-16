-- migrate:up transaction:false
-- Zero marks legacy C1 rows. C2 always supplies a validated, nonzero event UUID.
-- Preserve the existing device/metric/time sorting key and MergeTree engine.
ALTER TABLE cronet.metrics
ADD COLUMN IF NOT EXISTS event_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000');

-- migrate:down transaction:false
SELECT throwIf(1, 'Forward-only migration: dropping event IDs would lose telemetry identity');
