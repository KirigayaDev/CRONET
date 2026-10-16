-- migrate:up transaction:false
ALTER TABLE cronet.logs
ADD COLUMN IF NOT EXISTS event_id UUID DEFAULT toUUID('00000000-0000-0000-0000-000000000000');

-- migrate:down transaction:false
SELECT throwIf(1, 'Forward-only migration: dropping log event IDs would lose producer identity');
