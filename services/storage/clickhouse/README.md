# ClickHouse storage (C1)

Versioned DDL for the `cronet` database on `clickhouse/clickhouse-server:25.8-distroless`.
This directory has no resident service. C2/C3 writers, C4 parsing, C5 templating,
and CMDB dictionaries are separate tasks.

## Migrations

Use **[dbmate 2.36.0](https://github.com/amacneil/dbmate/tree/v2.36.0)**, which has a native ClickHouse driver and can
create the database on `up`. PostgreSQL's existing Alembic migrations are unrelated.
There is no custom migration framework. From the repository root, with
`DATABASE_URL` supplied securely by the environment:

```sh
# URL format: clickhouse://<user>:<url-encoded-password>@<host>:9000/cronet
dbmate --migrations-dir services/storage/clickhouse/migrations --no-dump-schema up --strict
```

Alternatively, build the one-shot migration image (only SQL is copied):

```sh
docker build -t cronet-clickhouse-migrations services/storage/clickhouse
docker run --rm -e DATABASE_URL cronet-clickhouse-migrations
```

The target database must be `cronet`; never use `default` as the storage schema.
Use a dedicated migration account with CREATE DATABASE/TABLE privileges; batchers
need only their table's INSERT permission (readers SELECT). Keep credentials out
of SQL, command lines, and Git. Use a secure connection for remote deployments.

Migrations `20261016000100` (metrics) and `20261016000200` (logs) each contain
one idempotent DDL statement with `transaction:false`: ClickHouse DDL is not
transactional. `schema_migrations` records completed versions. Repeating `up`
does nothing; `IF NOT EXISTS` permits recovery if DDL succeeded before its version
was recorded. It does not validate a manually modified existing table. Applied
files are immutable; make changes in a new version, and inspect metadata before
retrying a failed deployment. Run one migration process at a time: dbmate's
ClickHouse driver does not provide a migration lock or checksum validation.
Automatic `down` deliberately fails to protect stored telemetry; use a reviewed
forward migration or restore a backup. `--strict` rejects out-of-order versions.

## Tables and retention

Both tables use `MergeTree`, daily `toYYYYMMDD(event_time)` partitions, UTC
`DateTime64(3)` timestamps, and `ingested_at DEFAULT now64(3, 'UTC')`.

| Table | Columns after the two timestamps | Sorting key / main query |
| --- | --- | --- |
| `metrics` | `device_id UUID`, `interface_id Nullable(UUID)`, `metric LowCardinality(String)`, `value Float64`, `collector_id LowCardinality(String)`, `labels Map(String,String)` | `(device_id, metric, event_time)`; one device + metric + half-open time range |
| `logs` | `device_id Nullable(UUID)`, `source_ip IPv6`, `hostname LowCardinality(String)`, `facility UInt8`, `severity UInt8`, `app_name LowCardinality(String)`, `message String`, `collector_id LowCardinality(String)`, `attributes Map(String,String)` | `(device_id, event_time)`; device + time range, then severity/host/source filters |

**Shared retention policy: 30 days from event time for both tables.** Each DDL
encodes `TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE`. To change this
policy, add one new migration per table with the same interval; do not edit the
applied DDL. Ingestion time does not extend retention for late arrivals. TTL
deletion happens asynchronously during merges, so it is not a precise deletion
deadline. Apply a time predicate when a query needs a strict retention window.
No storage tiers, materialized views, or secondary/skipping indexes are required.
Source-only log searches scan the selected daily partitions; add measured
optimizations later if needed.

`source_ip` stores IPv6 and IPv4-mapped IPv6 (`toIPv6('192.0.2.10')` in SQL,
`Ipv4Addr::to_ipv6_mapped()` in Rust). A log without a CMDB match keeps a real
NULL `device_id`; `allow_nullable_key=1` permits direct device/time sorting and
primary-key filtering without a sentinel expression. Syslog facility is
0–23 and severity 0–7, enforced by constraints. Preserve unparsed vendor fields
in `attributes`; names/units and parser output can go in `labels` without adding
speculative columns. Raw collector protobufs remain raw contracts: C4 maps them
to storage rows after parsing.

C2/C3 should use explicit column lists and `clickhouse::Client::insert` or
`Inserter` with RowBinary, accumulate rows, then await `end`/`commit`. UUIDs use
`clickhouse::serde::uuid` (and `uuid::option`); `DateTime64(3)` can use signed Unix
milliseconds; maps can use `Vec<(String, String)>`. See the test row types for a
working example. These MergeTree tables append rows; retry deduplication belongs
to the future writer contract, and is not provided by C1.

## Real-server integration tests

The standalone `tests-rs` crate is outside the production Rust workspace. The
test image includes dbmate; the production migration image includes only SQL.
Compose was chosen after checking the [testcontainers-modules ClickHouse module](https://github.com/testcontainers/testcontainers-rs-modules-community/blob/main/src/clickhouse/mod.rs): its
module defaults to a different server tag, while Compose already fits this repo
and can run dbmate and Rust against the exact required image without another
Rust container-management dependency.

From the repository root (use a unique `-p` name for each concurrent run):

```sh
docker compose -p cronet-c1-local -f services/storage/clickhouse/tests/compose.yml build integration
docker compose -p cronet-c1-local -f services/storage/clickhouse/tests/compose.yml up -d --wait clickhouse
docker compose -p cronet-c1-local -f services/storage/clickhouse/tests/compose.yml run --rm integration
# Always clean up, including after failures; the next run needs an empty server.
docker compose -p cronet-c1-local -f services/storage/clickhouse/tests/compose.yml down -v --remove-orphans
```

The runner requires an empty `cronet` database namespace, applies migrations,
checks all columns/types, engines, partition/sorting keys and production TTL
metadata, writes **10,000 metrics + 10,000 logs in 1,000-row batches**, reads every
field back, and checks device/metric/time, severity, hostname/source, NULL UUID,
IPv4 and IPv6 queries. It then verifies replay preserves DDL, table UUIDs,
migration history and row counts, refuses destructive rollback, and exercises
failed-migration recovery with a test-only fixture. No test shortens production
TTL. CI runs the same Compose commands and always removes its isolated stack.
