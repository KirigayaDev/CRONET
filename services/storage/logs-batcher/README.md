# Logs batcher (C3)

Consumes normalized `netmon.v1.LogEvent` from JetStream `LOGS` (`logs.<site>`),
then inserts bounded RowBinary batches into `cronet.logs`. C4 will normalize
raw syslog and generate producer IDs, preferably UUID v7. This service validates
and preserves every non-nil producer UUID; it never generates a replacement.
No raw syslog parsing, template matching, hashing, or alerting is performed.

## Contract and limits

`log.proto` defines event_id, event_time, optional device_id, source_ip, hostname,
facility, severity, app_name, **bytes message**, collector_id, attributes and site.
The message stays arbitrary bytes, including NUL/non-UTF-8, through
`#[serde(with = "serde_bytes")]` into ClickHouse String. This mapping is supported
by the [ClickHouse Rust client's type documentation](https://docs.rs/crate/clickhouse/0.13.3/source/README.md).
There is no lossy decoding. IPv4 is mapped to `::ffff:a.b.c.d`; IPv6 is preserved.
Absent device_id becomes SQL NULL. A present empty/invalid UUID is rejected.
Facility must be 0..23 and severity 0..7. Timestamps must fit ClickHouse's
1900..2299 DateTime64 range, with valid nanos, truncated to UTC milliseconds.
`ingested_at` uses the server's `now64(3, 'UTC')` default.

The protobuf envelope is limited to **256 KiB before decoding**, message bytes
to 240 KiB, attributes to 64 entries / 128-byte nonempty keys / 4096-byte values /
32 KiB total key+value bytes. Keys cannot contain controls; values may contain
arbitrary UTF-8. Hostname (256 bytes) and app_name (128) may be empty but cannot
contain controls. Collector ID (128) must be nonblank without controls. Site
must match the subject and be one ASCII alphanumeric/underscore/hyphen token,
at most 64 bytes. Empty messages are valid. Invalid events send **Term** and
log only a static reason and subject, never message/attributes/credentials.
Limits are checked after bounded prost decoding; unknown fields are discarded.

## Delivery, batching and shutdown

Defaults: **500 rows / 8 MiB uncompressed RowBinary / 750 ms from the first row**,
whichever is reached first. If the next row would exceed the byte cap, it is
NAKed before flushing the current batch. The minimum byte cap is 1 MiB, so an
accepted event always fits and cannot occupy more than a quarter of that cap.
Each worker holds one bounded batch; it retains only ACK handles after decoding.
One finite one-message pull at a time deliberately avoids hidden prefetched
deliveries during outages; this favors predictable bounds over peak throughput.
The C2 delivery pattern is adapted here without changing the metrics service.

The synchronous INSERT (`async_insert=0`) must complete before double ACK.
Failures retain the batch unacknowledged and retry with bounded exponential
backoff; Progress heartbeats renew delivery deadlines during slow inserts and
retries. A lost INSERT response or process failure can create duplicates.
A lost ACK confirmation does not itself retry an already confirmed INSERT.
This is **at-least-once**, not exactly-once: event_id is stored for downstream
deduplication, with no in-process hash cache or automatic MergeTree replacement.
Migration `20261016000400` is forward-only; old rows/writers get zero UUID,
while C3 accepts only nonzero IDs. Sorting, partitions, TTL, C1/C2 SQL stay intact.

SIGINT/SIGTERM stops pulling and attempts a final flush, bounded by the shutdown
deadline. Failed/timed-out writes remain unacknowledged for later redelivery.
JetStream uses WorkQueue/File, DiscardNew, no age eviction, unlimited delivery
attempts, 120-second ACK wait and `max_ack_pending = rows * concurrency`.
Replicas with the same site/config share one durable `logs-batcher-site-<site>`.
Wildcard site `*` uses `logs-batcher-all` / `logs.>`; wildcard and per-site
consumers cannot overlap on a WorkQueue stream. All replicas must use identical
consumer settings. Dev/prod LOGS disk caps are 128 MiB / 4096 MiB; at capacity
publishers receive an error and must retry. Bootstrap is opt-in and adds LOGS
without changing existing streams; it does not migrate an existing stream config.

## Configuration

Required: `NETMON_SITE`, `CLICKHOUSE_URL` (HTTP(S) origin), `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD` (may be empty). Credentials are separate from the URL.
Apply all migrations before starting. `CLICKHOUSE_DATABASE` defaults to and must
remain `cronet`. Optional `NATS_URL` defaults to `nats://127.0.0.1:4222`;
`NATS_CREDS_FILE` supplies the credentials file.

| Variable | Default | Range |
| --- | --- | --- |
| LOGS_BATCH_MAX_ROWS | 500 | 1..10000 |
| LOGS_BATCH_MAX_BYTES | 8388608 | 1..16 MiB |
| LOGS_FLUSH_INTERVAL_MS | 750 | 10..60000 |
| LOGS_MAX_CONCURRENCY | 1 | 1..16 |
| LOGS_INSERT_TIMEOUT_MS | 10000 | 100..60000 |
| LOGS_SHUTDOWN_TIMEOUT_MS | 15000 | 100..120000 |
| LOGS_RETRY_MIN_MS / LOGS_RETRY_MAX_MS | 100 / 5000 | 10..30000, min <= max |

Combined configured batches cannot exceed 128 MiB / 64000 rows per process
(decoded field/container, driver and NATS buffers have additional bounded overhead).
`NETMON_INSTANCE_ID` defaults to `logs-batcher-<pid>`; set a unique token for
diagnostics. `NETMON_BOOTSTRAP_STREAMS=false`, `NETMON_STREAM_PROFILE=prod`
(`dev` supported), `NETMON_STREAM_REPLICAS=1` (1..5), `RUST_LOG=info`.
Logs contain batch counts/bytes, retries and instance metadata only.

## Build and verification

From the repository root:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
docker build -f services/storage/logs-batcher/Dockerfile -t cronet-logs-batcher .
docker compose -p cronet-c3-local -f services/storage/logs-batcher/tests/compose.yml build integration
docker compose -p cronet-c3-local -f services/storage/logs-batcher/tests/compose.yml up -d --wait nats clickhouse
docker compose -p cronet-c3-local -f services/storage/logs-batcher/tests/compose.yml run --rm integration
docker compose -p cronet-c3-local -f services/storage/logs-batcher/tests/compose.yml down -v --remove-orphans
```

Use a fresh, uniquely named stack per run. The opt-in `LOGS_E2E=1` test launches
the real binary, NATS 2.11 and ClickHouse **25.8.33.6**, applies C1+C2+C3 SQL,
round-trips all fields of 2500 logs (2 KiB binary messages), checks Term advisories,
single-row timer, SIGTERM, ClickHouse/NATS outages, two replicas/4000 events,
and 10000 logs with informational duration/rows-per-second/batch count.
Network fault injection relays real server traffic, without Docker socket access.
C1 schema and C2 metrics tests run separately against fresh stacks.
The separate C3 CI job runs the same integration commands and builds the image.
The distroless production stage contains only runtime and binary, UID/GID 65532;
the build-context allowlist excludes tests, fixtures, reports and credentials.
