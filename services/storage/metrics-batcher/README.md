# metrics-batcher (C2)

Stores **normalized `MetricPoint` protobufs** from NATS JetStream in
`cronet.metrics`. Raw collector messages, OID parsing, logs and alerts belong to
other services. Apply the [ClickHouse migrations](../clickhouse/README.md) first.

## Delivery and batching

Publish to `subjects::metric(site)` (`metrics.<site>`) and reuse `event_id` as
`Nats-Msg-Id` when retrying publication. `site` must match the subject. The
`METRICS` stream uses `metrics.>`, WorkQueue retention and File storage. Its disk
cap is 2 GiB (`prod`) or 128 MiB (`dev`); DiscardNew rejects new publishes when
full, so producers must handle negative publish acknowledgements. Unacknowledged
metrics have no age limit. Existing task/raw streams retain their policies.

Replicas share `metrics-batcher-site-<site>` (or `metrics-batcher-all` for `*`).
Deploy either a wildcard group or disjoint site groups: WorkQueue consumers may
not have overlapping filters. Use the same batch/concurrency settings within a
group. Each process has a bounded number of workers, each holding one batch.
Rows flush at the first row/byte limit or elapsed interval from the first row.
The byte limit counts exact uncompressed RowBinary bytes; a row that would
overflow the current batch is NAKed with delay while that batch flushes.

Workers make one finite, one-message pull request at a time. This avoids a
prefetch queue containing deliveries whose ACK deadline cannot be extended
during an outage. `max_ack_pending = max_rows * concurrency` bounds the shared
durable's in-flight messages. Protobuf bytes are released after validation;
batches retain normalized rows and compact ACK handles. Native client buffers
and encoding buffers add overhead beyond the configured row bytes.

One `Client::insert` / RowBinary request writes each batch; `end()` must succeed
before any message is ACKed. Synchronous inserts are explicit (`async_insert=0`).
On failure the batch stays in memory and is retried with bounded exponential
backoff. Progress ACKs extend the 120-second delivery deadline during long waits;
they do not acknowledge completion. There is no finite delivery-attempt limit
that would discard telemetry during a prolonged outage. Pull errors back off;
the existing Bus client reconnects to NATS.

Successful writes use confirmed (`double_ack`) acknowledgements, at most 32 at
once and within a 5-second overall ACK deadline. An ACK failure never triggers
an immediate repeat of an already successful INSERT. The unconfirmed message
may redeliver later. **Delivery is at least once**: a lost ClickHouse response,
crash between insert and ACK, or lost ACK confirmation can produce duplicates.
`event_id` preserves identity for downstream inspection/deduplication; MergeTree
does not enforce uniqueness. Migration `20261016000300` adds this UUID column
without changing the engine or device/metric/time key. Legacy C1 rows/writers
receive the zero UUID; C2 requires and supplies a nonzero event UUID.

Invalid protobuf, UUID, routing, timestamp, metric/collector name, non-finite
value or labels is permanently rejected with `Term`. Logs contain reason and
subject metadata, never the payload, labels or credentials. Limits: 64 KiB wire
payload; 64 labels; key 128 bytes, value 1,024 bytes, combined labels 16 KiB;
metric 256 bytes, collector ID 128 bytes. Names/keys cannot be blank or contain
control characters. Timestamps must fit DateTime64 (1900–2299); submillisecond
precision is truncated. `ingested_at` uses the ClickHouse default; `site` is
used for routing and is not added to the storage table.

SIGINT/SIGTERM stops pulls and flushes the current batch. Successful final writes
are ACKed; failed/aborted writes stay unacknowledged. The shutdown timeout covers
all workers, including inserts, retries and ACKs. A pending cancelled pull can
redeliver after its ACK deadline. The process exits unsuccessfully if final
flush fails or the shutdown deadline is reached.

## Configuration

| Environment variable | Default / range |
| --- | --- |
| `NATS_URL`, `NATS_CREDS_FILE` | `nats://127.0.0.1:4222`; optional credentials file |
| `CLICKHOUSE_URL` | Required HTTP(S) origin; credentials supplied separately |
| `CLICKHOUSE_DATABASE` | `cronet` (C1 schema) |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Required; supply password through secret/environment |
| `NETMON_SITE` | Required single site token or `*` |
| `METRICS_BATCH_MAX_ROWS` | 1,000; 1–10,000 per worker |
| `METRICS_BATCH_MAX_BYTES` | 4 MiB; 64 KiB–16 MiB per worker |
| `METRICS_FLUSH_INTERVAL_MS` | 750; 10–60,000 |
| `METRICS_MAX_CONCURRENCY` | 1; 1–16 workers |
| `METRICS_INSERT_TIMEOUT_MS` | 10,000; 100–60,000, whole INSERT |
| `METRICS_RETRY_MIN_MS`, `METRICS_RETRY_MAX_MS` | 100 / 5,000; 10–30,000, min ≤ max |
| `METRICS_SHUTDOWN_TIMEOUT_MS` | 15,000; 100–120,000 |
| `NETMON_INSTANCE_ID` | Process ID-based name; optional token for logs/query diagnostics |
| `NETMON_BOOTSTRAP_STREAMS` | `false`; opt in to Bus stream creation |
| `NETMON_STREAM_PROFILE`, `NETMON_STREAM_REPLICAS` | `prod` / 1; profiles `prod`, `dev`; replicas 1–5 |

Combined configured batch limits may not exceed 128 MiB or 64,000 rows per
process. Credentials/configuration are never printed. `NETMON_INSTANCE_ID` is
included in ClickHouse `log_comment` so operators can inspect batch counts and
written rows in `system.query_log` without adding a storage column.

## Run and verify

From the repository root, with the required environment set:

```sh
cargo run --locked -p metrics-batcher
docker build -f services/storage/metrics-batcher/Dockerfile -t cronet-metrics-batcher .
docker run --rm --env-file /secure/path/metrics-batcher.env cronet-metrics-batcher
cargo test --locked -p metrics-batcher
```

The multi-stage production image contains the binary and distroless runtime/CA
libraries, runs as UID/GID 65532 and includes no source, tests, fixtures, reports
or credentials. Shared Compose is unchanged. Migrations run separately.

The opt-in real E2E test starts actual batcher processes against NATS JetStream
and `clickhouse/clickhouse-server:25.8.33.6-distroless`. It verifies 2,500 metrics,
timer flush for one metric, two rejected messages between valid points, SIGTERM
final flush, ClickHouse outage without premature ACK and recovery, NATS reconnect,
two replicas sharing one durable, and 10,000 metrics with measured rows/sec and
batch counts. Fault gates relay real protocol traffic; no server or writer mocks
are used. Timing measurements are informational, not CI thresholds.

```sh
docker compose -p cronet-c2-local -f services/storage/metrics-batcher/tests/compose.yml build integration
docker compose -p cronet-c2-local -f services/storage/metrics-batcher/tests/compose.yml up -d --wait nats clickhouse
docker compose -p cronet-c2-local -f services/storage/metrics-batcher/tests/compose.yml run --rm integration
# Always clean up; the next E2E run requires an empty server.
docker compose -p cronet-c2-local -f services/storage/metrics-batcher/tests/compose.yml down -v --remove-orphans
```

CI has a separate C2 integration job and always cleans up its stack. Ordinary
workspace tests skip the real-server test unless `METRICS_E2E=1`.
