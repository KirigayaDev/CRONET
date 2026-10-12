# CMDB Rust core

Standalone Cargo package on `feature/d1-cmdb-api`, based on schema commit
`cc7c21e73464c439c5a104ae292562a596d2939e`. All files for this stage live here.
The existing CMDB Alembic revision is `8b6e2f4a9c10`; this package never creates
tables, changes schema, reads .env files, or runs migrations itself.

## Structure

```text
cmdb_service/
  Cargo.toml
  Cargo.lock
  README.md
  src/
    lib.rs
    main.rs
    config.rs
    db.rs
    error.rs
    models.rs
    repository.rs
    service.rs
  tests/
    core.rs
    compose.core-test.yml
    Dockerfile.rust
```

`lib.rs` exposes the same core to future HTTP/gRPC adapters and integration tests.
The binary loads process configuration, creates a PgPool, checks `SELECT 1`,
prints readiness, closes the pool and exits. No network server is started.

## Dependencies

| Dependency | Purpose |
| --- | --- |
| tokio | Async runtime and bounded retry delay. |
| sqlx 0.8.6 | PostgreSQL pool, parameterized runtime queries, row/type derives; Tokio/Rustls TLS. Default database/migration features are disabled. |
| uuid | Native UUID identity and serde; v4 generates independent test identities. |
| chrono | TIMESTAMPTZ represented as DateTime<Utc>. |
| serde | Typed entity/input serialization, including three-state PATCH. |
| thiserror | Transport-independent domain errors with retained driver sources. |
| ipnetwork | Native INET values, retaining IPv4/IPv6 host addresses and prefix lengths. |
| mac_address | Native six-byte MACADDR values and parsing. |
| serde_json (dev only) | PATCH/lifecycle serialization tests. |

No Axum, tonic, JWT, Redis, CSV, NATS, discovery or scheduler dependencies are
added. The committed Cargo.lock captures resolved versions for reproducible builds.

## Layers and operations

- `config`: reads DATABASE_URL from process environment, validates PostgreSQL
  URL syntax and reports missing/invalid configuration without printing the URL.
  Host, database, username and password are never hardcoded in production code.
- `db`: creates PgPool (10 connections, 10-second acquisition timeout) and offers
  a connection healthcheck. The existing database_migrations service owns schema.
- `models`: six canonical entities with UUID, DateTime<Utc>, typed nullable
  fields, INET and MACADDR. Device carries only rack_id for location and no IP/MAC.
  Separate Create*/Update* inputs do not expose entity IDs or timestamps.
- `repository`: explicit SQL operations accepting `&mut PgConnection`. A caller
  can pass a pool connection or dereference a `&mut Transaction<Postgres>`;
  repository never begins or commits a transaction. All user values are bound
  parameters; names are not interpolated into SQL.
- `service`: validates obvious names/positive dimensions, handles NotFound,
  composes repository calls, and owns convenience-operation transaction boundaries.
  It contains no SQL. PostgreSQL remains authoritative for uniqueness, FK,
  placement completeness, bounds, rack resize, occupancy and native type validity.

| Entity | Operations |
| --- | --- |
| Site | create, get, list, update, delete, logical-name lookup |
| Room | create, get, list/all or site filter, update/reparent, delete, parent-scoped logical-name lookup |
| Rack | create, get, list/all or room filter, update/reparent/resize, delete, parent-scoped logical-name lookup |
| Device | create, get, list with optional rack_id/hostname/serial/lifecycle filters, update, decommission |
| Interface | create, get, list by device, update/reparent, delete |
| IP address | create, get, list by interface, update/reparent, delete |

Device filters combine with AND; hostname/serial comparisons are exact, bound
text comparisons. Lists use deterministic ordering. Pagination is deferred.
No hard-delete device operation is added: this stage asks for decommissioning.
Decommission only changes lifecycle_status and retains physical placement.
Release occupancy explicitly with rack_id/start_unit both cleared in one update.

Location-name lookups apply the database's expression to both name and input:

```sql
lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g')))
```

The display name is retained; no normalized-name column is created.
Field overrides have a minimal typed read helper, `is_field_overridden`, for
entity_type/entity_id/field_name, including a caller-transaction variant. Discovery
and a field-lock write workflow are deferred.

## PATCH semantics

Every update field is `Patch<T>`. Serde distinguishes:

- omitted field: `Unchanged` (serialized by omission);
- present value: `Set(value)`;
- explicit JSON null: `Clear`.

Thus a nullable description/hostname/placement/MAC can be cleared without
confusing omission with NULL. Required fields reject Clear in service validation
and remain protected by PostgreSQL NOT NULL. SQL CASE expressions preserve
unchanged fields. Patch can also be constructed directly by Rust callers.
LifecycleStatus is Active/Maintenance/Decommissioned, serialized and bound as
active/maintenance/decommissioned. SQLx maps it onto TEXT, not a PostgreSQL ENUM.

## Transactions and retry

Every mutation has a `Service::*_in(&mut Transaction<Postgres>, ...)` variant.
It does not create a nested transaction. A business operation can compose entity
mutations, override reads and a future audit append before a single commit:

```rust,no_run
# async fn example(pool: sqlx::PgPool, input: cmdb_service::models::CreateSite) -> cmdb_service::Result<()> {
let mut tx = pool.begin().await?;
let site = cmdb_service::Service::create_site_in(&mut tx, &input).await?;
// Future audit repository append uses this same tx and site.id.
tx.commit().await?;
# Ok(())
# }
```

Repository calls can also reuse the same transaction through its connection.
The service's convenience methods run their database-only operation through
`retryable_transaction`. This public helper is opt-in for composed operations:
its closure must be safe to replay and must not send external messages, mutate
external state, or publish results before commit.

Default retry budget is 3 total attempts (2 retries), with 25/50 ms delays.
Custom policies allow 1..=5 attempts and base delay <= 1 second. Only SQLSTATE
40001/40P01 from the operation or commit causes full transaction replay. Failed
operation transactions are explicitly rolled back before sleeping. Validation,
uniqueness/FK/exclusion conflicts, connection failures, and unknown commit outcomes
are never retried. Caller-owned `*_in` operations propagate errors; the owner must
roll back or retry the entire business transaction.

## Domain errors

| Condition | Domain error |
| --- | --- |
| Missing entity on get/update/delete | NotFound with entity type and UUID |
| Obvious invalid input | Validation |
| 23505 uniqueness, 23503 FK, 23P01 exclusion | Conflict with SQLSTATE and constraint name |
| 23514 check, 23502 NOT NULL, 22P02 type syntax, 22003 numeric range | Validation |
| 40001 serialization, 40P01 deadlock | Retryable; propagated after budget exhaustion |
| Other driver errors | Database, with source preserved and sanitized Display |

There are no HTTP status codes. Future transport adapters must map these errors
to their own protocol. Startup prints Display rather than credential-bearing
driver details. Full audit generation/authorization are future work.

## Test environment and quality commands

Native stable Rust can run the package. On the current Windows host rustc fails
even at `-vV` with exit code `0xc0e90002`, so this stage uses the isolated Linux
stable Rust runner below. No global toolchain or shared configuration is changed.

Run from this directory with a fresh unique Compose project name:

```sh
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml up -d --build rust
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml exec -T rust cargo fmt --check
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml exec -T rust cargo check --locked
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml exec -T rust cargo clippy --locked --all-targets -- -D warnings
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml exec -T rust cargo test --locked
docker compose -p cronet-cmdb-core-01a12211 -f tests/compose.core-test.yml down --volumes
```

The new test Compose copies the existing isolation approach without changing
the existing Compose: PostgreSQL 18 in tmpfs, no exposed ports, no user data
bind mounts, and a build of the existing Alembic runner. Migration completion
is required before tests. Rust source is mounted only from this new package;
registry/build caches are disposable project volumes outside Git. Trust auth
and the test URL are confined to this test network.

Unit tests need no DB. Integration tests intentionally fail if
CMDB_TEST_DATABASE_URL is missing, if its database is not
cronet_cmdb_test_schema, or if the database doesn't have the expected Alembic head.
No tests are silently skipped. All fixtures use independent UUID identities and
the disposable database is removed after checks.

Validation on 2026-10-12 used stable rustc 1.99.0 (Linux) and PostgreSQL 18:
`cargo fmt --check`, `cargo check --locked`, and
`cargo clippy --locked --all-targets -- -D warnings` passed.
`cargo test --locked` passed 5 unit tests and 12 integration tests, with no ignored
tests. The integration suite completed in 0.11 seconds after compilation.
The existing Alembic chain upgraded the initially empty test DB to
`8b6e2f4a9c10` before the suite. No production database was used.

Integration coverage includes startup/missing config, PgPool, Site CRUD/PATCH,
Room/Rack relations and filters, normalized lookup/conflicts, parameterized names,
validation/SQLSTATE mapping, FK vs NotFound, Device filters/placement/decommission,
all-state occupancy and explicit detach, Interface/MAC, IPv4/IPv6 prefixes and IP
CRUD, shared service/repository transactions, entity+audit rollback, override
reads, retry success/budget/rollback, and nonretryable conflicts.

## Next stage and shared code

Core is ready for transport adapters, permission checks, transactional audit
generation, CSV validation/confirmation and discovery lock enforcement in later
stages. Add pagination and decide hostname search policy with API requirements.

No shared-code modification is required for this core stage. Deployment of a
future long-running API will require a separately authorized addition to
control_panel/docker-compose.yml and public-key/DB/revocation configuration.
No shared Compose, auth_service, migration, Python model, script or CI file is
modified here.

Sources: [SQLx transaction interface](https://docs.rs/sqlx/0.8.6/sqlx/struct.Transaction.html),
[SQLx PostgreSQL MACADDR encoding](https://raw.githubusercontent.com/launchbadge/sqlx/v0.8.6/sqlx-postgres/src/types/mac_address.rs).
