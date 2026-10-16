# CMDB service: transactional audit, overrides and device CSV import

Root workspace member on `feature/d1-cmdb-api`. Stage 4 adds transactional
audit, manual field ownership and staged CSV device import to the existing
HTTP/Axum and gRPC/tonic APIs. Both transports share one Service and Authenticator.

The Alembic head is `d5c1a2b3e001` (D5), following D4 `d4c1a2b3e001` and D1 `8b6e2f4a9c10`. This package never creates tables,
changes schema, reads .env files, or runs migrations itself. Apply the existing
Alembic chain before startup. The existing audit/override/import tables are
reused without a new migration. Stage 5 integrates the canonical layout, shared
contracts, Docker/Compose and isolated CI. Discovery, Scheduler and service-account
identities remain separate work.

## Physical inventory (D4)

CMDB accepts normalized ENTITY-MIB components; it does not collect SNMP data.
Apply the forward Alembic migration before starting the binary. The migration
adds `inventory_snapshots` and `inventory_components`; existing D1 migrations
and tables are unchanged. The component primary key `(snapshot_id,
ent_physical_index)` also provides the required snapshot lookup index.

Authenticated HTTP routes under `/api/v1/cmdb`:

| Method | Path | Access |
| --- | --- | --- |
| POST | `/devices/{device_id}/inventory` | admin / superadmin |
| GET | `/devices/{device_id}/inventory` | authenticated reader |
| GET | `/devices/{device_id}/inventory/diff?previous_id=UUID&current_id=UUID` | authenticated reader |
| GET | `/devices/{device_id}/inventory/export?format=csv` (or `xlsx`) | authenticated reader |

Additive gRPC methods are `SubmitInventorySnapshot`, `GetLatestInventory`
(`IdRequest.id` is the device UUID), and `CompareInventorySnapshots`.
POST accepts `collected_at` (RFC3339, years 0001..9999) and `components`.
Each component contains `ent_physical_index`, `class`, optional `contained_in`,
`parent_rel_pos`, `name`, `description`, `vendor_type`, `serial`, `model`,
`hardware_rev`, `firmware_rev`, `software_rev`. Unknown fields are rejected.

Limits: 10,000 components; positive unique indices; nonnegative containment
(zero means root); positions >= -1 (-1 means unknown); no self-containment.
Class is nonblank, trimmed, ASCII lowercase, at most 64 bytes. Optional strings
are trimmed, blank becomes null, case is preserved; maximum 255 characters
(description: 1024); NUL is rejected. Sparse/partial containment is accepted.
Both transports allow bounded inventory messages up to 192 MiB.

Canonical components are sorted by index and serialized in fixed field order
before BLAKE3. Collection time is excluded. A device row lock serializes
latest-content comparison and the single UNNEST insert transaction, including
metadata-only audit. Identical latest content returns the original snapshot
and `unchanged=true`, with no new audit event. Latest means greatest
`collected_at`, then `created_at` and UUID; late observations remain history
and do not replace newer observations. No global historical hash deduplication
is applied. An empty snapshot records removal of all components.

Diff first matches unique `(class, serial)` identities, so a move/slot swap is
CHANGED. Remaining components match unique `(contained_in, parent_rel_pos,
class)` slots when the parent is positive and the position known; otherwise
`(index, class)` is used. Both nonblank serials differing in a matched slot
mean REPLACED. Ambiguous slots never imply replacement; compatible identical
indices can still match, otherwise components remain ADDED/REMOVED. Changes
include old/new values, changed field names, and a unified text diff (`similar`).
Both snapshot IDs must belong to the requested device.

CSV and XLSX are generated in memory; CPU work runs outside async workers.
CSV quotes multiline fields and prefixes spreadsheet formula-like text with
an apostrophe. XLSX writes explicit string cells, preserving serial zeros.
No export, key, fixture, test, or report is added to the production image.

Dependency note: published `rust_xlsxwriter 0.99.1` requires ZIP 8.x, whose
available versions are currently yanked. `Cargo.lock` retains ZIP 8.6.0 and its
checksum from that published crate's own lockfile; use `--locked` builds.
ZIP 9 is used only to independently read/validate XLSX exports in tests.
Existing locked dependency versions have not been upgraded.

## Structure

```text
cmdb_service/
  Cargo.toml
  README.md
  rustfmt.toml
  Dockerfile
  Dockerfile.dockerignore
  examples/devices-template.csv
  src/
    lib.rs
    main.rs
    config.rs
    db.rs
    error.rs
    models.rs
    repository.rs
    service.rs
    workflow.rs
    imports.rs
    repository/workflow.rs
    auth.rs
    api/
      mod.rs
      dto.rs
      http.rs
      grpc.rs
      workflow.rs
  tests/
    core.rs
    adapters.rs
    support/workflow_tests.rs
    compose.core-test.yml
    Dockerfile.rust
```

api/dto.rs keeps HTTP/OpenAPI schemas and input conversions separate from
domain models. api/mod.rs owns shared state and concurrent server lifecycle.
The shared contracts crate generates typed protobuf clients and servers in
Cargo's output directory, outside Git, and api/grpc.rs re-exports its CMDB module.

## Dependencies

Resolved versions are committed in the repository root Cargo.lock.

| Dependency | Purpose |
| --- | --- |
| axum 0.8 | HTTP routing, middleware, extraction and bounded multipart upload. |
| csv 1.4 | Device CSV parsing, with an additional strict quote/header/size guard. |
| tonic / tonic-prost 0.14 | gRPC transport and protobuf codec. |
| prost / prost-types 0.14 | Typed messages, standard Timestamp/NullValue. |
| contracts (workspace) | Shared generated CMDB clients and servers. |
| tokio-stream 0.1 | Serve tonic from a bound TCP listener. |
| jsonwebtoken 10 | RS256 verification with rust_crypto and use_pem. |
| rsa 0.9 | Validate actual RSA public key and minimum 2048-bit modulus at startup. |
| redis 0.32 | Async connection manager, blacklist EXISTS, readiness PING and Rustls. |
| utoipa 5 | OpenAPI routes, DTO schemas, parameters and Bearer security. |
| tower-http 0.6 | HTTP tracing with sanitized route templates. |
| tracing / tracing-subscriber | Startup, request and readiness events. |
| serde_json | JSON/OpenAPI serialization and tests; promoted from dev dependency. |
| tower / http-body-util (dev) | Exercise HTTP router and response bodies. |
| reqwest / tempfile (dev) | Real HTTP startup probes and temporary test RSA keys. |

Existing tokio, sqlx 0.8.6, uuid, chrono, serde, thiserror, ipnetwork and
mac_address remain the core dependencies. Tokio adds net/signal/sync features.
SQLx enables PostgreSQL, native types and JSONB. No NATS, SNMP, Discovery or
Scheduler dependency is added.

Shared generation uses protoc-bin-vendored and requires no system protoc.
The isolated test Dockerfile pins Rust 1.89 and installs OpenSSL for test keys.

## Configuration and startup

Configuration comes only from process environment:

| Variable | Requirement / default |
| --- | --- |
| DATABASE_URL | Required PostgreSQL URL; never logged. |
| REDIS_URL | Required Redis URL; never logged. |
| JWT_PUBLIC_KEY_PATH | Required readable RSA public PEM file. |
| CMDB_HTTP_ADDR | `0.0.0.0:8081`. |
| CMDB_GRPC_ADDR | `0.0.0.0:50051`. |
| JWT_ISSUER | `Re:simple_videohosting_auth`. |
| JWT_AUDIENCE | `Re:simple_videohosting_auth`. |

Credentials have no production defaults. Invalid URLs/addresses and empty
issuer/audience fail with sanitized errors. Config deliberately has no Debug.
The DB-only Config::from_database_url helper remains for core tests; the binary
uses Config::from_env, requiring the complete API configuration.

The binary loads config/public-key bytes, opens and checks PgPool, connects
Redis, validates RSA, creates shared Service/Authenticator, binds both listeners,
and serves HTTP and gRPC concurrently. Missing, malformed or private PEM fails
startup before either API is served. Production CMDB reads only a public key.

SIGTERM and Ctrl-C signal both servers with up to five seconds for graceful shutdown.
Unexpected exit of either server stops its peer and returns failure. The
PostgreSQL pool closes on return. Startup logs contain bind addresses only.

## Shared request flow and core

```text
HTTP JSON/path/query or gRPC typed request
  -> shared Authenticator
  -> Principal and authorization
  -> transport input conversion
  -> shared Service
  -> Repository with bound SQL
  -> PostgreSQL
```

Responses convert domain entities to HTTP DTOs or protobuf messages. Handlers
contain no entity SQL, transactions or duplicated business rules. Auth's user
lookup also lives in Repository. Readiness additionally checks the required schema.

The core retains UUID, DateTime<Utc>, native INET/MACADDR, separate Create*/Update*
inputs and transport-independent errors. PostgreSQL remains authoritative for
uniqueness, FK, placement completeness, bounds, rack resize and occupancy.
Name lookups use the existing database expression:

```sql
lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g')))
```

Device uses rack_id for location; interfaces own MACs and IPs belong to
interfaces. Decommission changes lifecycle only and retains occupancy. Clear
rack_id and start_unit together to release placement. No physical DELETE device
endpoint is added.

Every mutation retains its caller-transaction Service::*_in variant. Convenience
methods own transactions and use retryable_transaction: three total attempts by
default, 25/50 ms delays, only for SQLSTATE 40001/40P01. A composed closure must
be safe to replay without external side effects. Uniqueness/FK/exclusion errors,
connection failures and unknown commit outcomes are not retried. Caller-owned
transactions propagate errors to their owner. The field-override read helper
remains; user mutations now append audit and ownership markers atomically.
Caller-owned *_in helpers remain available for internal composed operations;
HTTP/gRPC always supply a MutationContext to the common audited wrapper.

## Authentication and authorization

HTTP accepts exactly one `Authorization: Bearer <access JWT>` header; gRPC
accepts exactly one `authorization: Bearer <access JWT>` metadata value.
Query parameters and cookies are not token sources. CMDB does not log in users,
issue/refresh JWTs, store passwords or provide a bypass.

The shared verifier checks:

1. RS256 signature only, using validated SPKI (PUBLIC KEY) or PKCS#1
   (RSA PUBLIC KEY) PEM.
2. Exact configured issuer and audience.
3. All required claims: iss, sub, aud, exp, nbf, iat, jti, token_version,
   token_type. sub/jti/token_version must be UUIDv4, as in the existing schema.
4. Finite, nonnegative Unix timestamps: exp > now; nbf and iat <= now.
   No clock leeway is applied.
5. token_type exactly access; refresh and csrf tokens are rejected.
6. Redis `EXISTS blacklisted_jwt_token:{jti}`; a present key rejects the token.
7. A current users row for sub, with token_version_uuid equal to the JWT claim.
8. Current is_admin/is_super_admin flags from that users row.

The Python schema serializes datetime.timestamp() as fractional Unix numbers.
Claims therefore use f64 and explicit time checks after signature verification,
without integer truncation. Read-only inspection found that the current Python
decoder explicitly configures audience but does not pass an expected issuer to
PyJWT. CMDB explicitly verifies issuer as required; auth_service is unchanged.

Blacklist and user/version checks run on every request. Redis connection/
response operations and PostgreSQL auth lookup have bounded three-second
timeouts. Infrastructure failures become AuthUnavailable, never an accepted
principal or a misleading invalid-token result. Invalid tokens are Unauthorized;
valid users without write privileges are Forbidden.

| Current database flags | Reads | Writes |
| --- | --- | --- |
| Neither admin nor super admin | Allowed | Forbidden |
| is_admin | Allowed | Allowed |
| is_super_admin | Allowed | Allowed |

HTTP POST/PATCH/DELETE, including decommission, and gRPC mutations require
either flag. No role table or claim-based privilege is invented. Principal
contains user_id and both flags and is retained in request extensions/handlers
for transactional audit actor propagation. Canonical mutations use this actor
in their entity/audit/override transaction.

There is no separate machine-to-machine/service-account identity model yet.
gRPC accepts existing **user access JWTs** only. D2/D3 integration must decide
that model separately; no static password or fake admin token is provided.

## HTTP routes

All paths below are relative to `/api/v1/cmdb` and require authentication.
These are the original 30 business operations; twelve workflow operations below
bring the protected HTTP total to 42.

| Path | Methods | List filters |
| --- | --- | --- |
| /sites | GET, POST | limit, offset |
| /sites/{id} | GET, PATCH, DELETE | |
| /rooms | GET, POST | site_id, limit, offset |
| /rooms/{id} | GET, PATCH, DELETE | |
| /racks | GET, POST | room_id, limit, offset |
| /racks/{id} | GET, PATCH, DELETE | |
| /devices | GET, POST | rack_id, hostname, serial, lifecycle_status, limit, offset |
| /devices/{id} | GET, PATCH | |
| /devices/{id}/decommission | POST | |
| /devices/{device_id}/interfaces | GET | limit, offset |
| /interfaces | POST | |
| /interfaces/{id} | GET, PATCH, DELETE | |
| /interfaces/{interface_id}/ip-addresses | GET | limit, offset |
| /ip-addresses | POST | |
| /ip-addresses/{id} | GET, PATCH, DELETE | |

Public routes: GET /health/live, GET /health/ready, GET /openapi.json.
Axum's automatic HEAD support follows GET routes with the same auth policy.
Creates return 201; reads/updates/decommission 200; deletes 204. Lists return
arrays. MACADDR and INET travel as strings parsed into native domain types;
IPv4/IPv6 prefix lengths are preserved.

### Pagination and PATCH

All six lists default to limit=50, offset=0. Valid limit is 1..=200; offset must
be nonnegative. Both transports reject invalid values. Service/Repository also
validate Pagination. Parameterized SQL LIMIT/OFFSET applies pagination with
existing deterministic ordering, never loading the table and truncating a Vec.
Existing convenience list methods now use the bounded default page.

Device filters combine with AND; hostname/serial are exact text comparisons.
Rooms filter by site_id, racks by room_id, interfaces by device_id, IPs by
interface_id.

HTTP PATCH retains three states: omitted = Unchanged, value = Set(value),
null = Clear. Required fields reject Clear; nullable fields may be cleared.
Lifecycle is active/maintenance/decommissioned. DTO annotations describe
nullability without coupling core models to Axum/OpenAPI.

### OpenAPI and errors

utoipa generates /openapi.json with all 45 operations, request/response schemas,
UUID parameters, filters, pagination bounds/defaults, Bearer JWT security and
main error responses. Public routes have no security requirement. Tests parse
serialized JSON back into utoipa's OpenApi and inspect routes/security/schemas.

Errors use stable JSON `{ "code": "...", "message": "..." }` with sanitized
messages, including extractor failures, unknown routes and unsupported methods.

| Condition | HTTP | gRPC |
| --- | --- | --- |
| Validation / malformed input / invalid import rows | 400 | INVALID_ARGUMENT |
| Finalized batch or unaccepted import warnings | 409 | FAILED_PRECONDITION |
| Missing/invalid access JWT | 401 + WWW-Authenticate: Bearer | UNAUTHENTICATED |
| Authenticated user cannot write | 403 | PERMISSION_DENIED |
| Missing entity | 404 | NOT_FOUND |
| Uniqueness conflict, 23505 | 409 | ALREADY_EXISTS |
| FK/exclusion conflict, 23503/23P01 | 409 | FAILED_PRECONDITION |
| Retry budget exhausted | 503 | UNAVAILABLE |
| Auth dependency unavailable | 503 | UNAVAILABLE |
| Unexpected database/internal error | 500 | INTERNAL |

gRPC UNAVAILABLE after bounded transaction retries is consistent with HTTP 503:
the transaction could not complete. Transports add no retry loop. Client errors
exclude driver details, SQL, URLs, tokens, key bodies and stack traces.

## gRPC

contracts/proto/cronet/cmdb/v1/cmdb.proto in the repository root defines package
cronet.cmdb.v1, service Cmdb, and twelve RPCs:

| Area | RPCs |
| --- | --- |
| Device | GetDevice, ListDevices, CreateDevice, UpdateDevice, DecommissionDevice |
| Interface | ListInterfaces, CreateInterface, UpdateInterface |
| IP | ListIpAddresses, CreateIpAddress, UpdateIpAddress |
| Location | GetRack |

UUID strings are validated at the boundary. Timestamps use standard
google.protobuf.Timestamp. Nullable PATCH uses StringPatch/Int32Patch with a
oneof value or NullValue clear: absent message means Unchanged, empty/malformed
message is rejected. Required update scalars use proto3 optional presence.
No entity is hidden in JSON/map payload. CreateDevice defaults unspecified
lifecycle to Active; invalid explicit update/filter enum values are rejected.

Every RPC uses the same Authenticator, policy and Service as HTTP. Tests use
the generated client against real TCP listeners and verify gRPC changes via HTTP.

## Health and logging

/health/live returns 200/ok while HTTP runs, without dependency checks.
/health/ready checks the ten required PostgreSQL D1 tables and Redis PING: 200/ok if available,
503/unavailable otherwise. A valid key is guaranteed at startup and is not
reloaded for each probe. Dependency failures log only a fixed warning.

Tracing records startup addresses, HTTP method/matched route templates and gRPC
request types. Spans exclude raw query strings/path IDs. Headers, cookies, JWTs,
payloads, URLs with credentials and key bodies are not logged. A scoped info
filter for cmdb_service/tower-http prevents environment-enabled dependency debug
logging. No permissive CORS layer is added.

## Transactional audit and request IDs

Every canonical mutation from HTTP or gRPC scopes the shared Service with:
actor_type, actor_id, source, request_id and optional import_batch_id. Current
actors are users from Principal. Sources are http, grpc and csv_import. The
context also has types for future service/discovery actors, without implementing
Discovery or M2M authentication now.

A valid single X-Request-ID UUID is accepted by HTTP; otherwise UUIDv4 is
created. Every HTTP response includes X-Request-ID, including errors. gRPC
accepts a valid single x-request-id metadata UUID or generates UUIDv4. Context
is created once per business request and remains the same across transaction
retries. No JWT enters Service, audit snapshots or ownership markers.

The shared mutation wrapper locks an existing entity before update/delete,
reads its canonical snapshot, runs the existing entity operation, appends audit,
and upserts applicable manual markers in one transaction. Delete removes the
entity's markers in that transaction. Failure in any part rolls back all parts.
The bounded retry helper replays the entire transaction for 40001/40P01.

Actions: create, update, delete, decommission, field_override_set,
field_override_clear. Create has NULL before and the created entity after;
update/decommission have both snapshots; delete has before and NULL after.
Explicit marker set/clear snapshots describe the marker, while entity_type/id
identify its canonical target. Auto markers accompany the canonical event rather
than producing a second event for every field. JSONB contains canonical data,
never auth/user records or credentials. detected_at and changed_at use database
clock_timestamp() for these known user changes. Audit remains append-only.

Audit reads accept entity_type/entity_id/actor_id/request_id/import_batch_id,
use SQL limit/offset (50/0, maximum 200) and detected_at DESC, id DESC.
There are no audit mutation endpoints.

## Manual field overrides

The exact allowlist is:

| Entity | Fields |
| --- | --- |
| devices | hostname, device_type, vendor, model, serial |
| interfaces | if_index, name, description, interface_type, mac_address |
| ip_addresses | address |
| sites / rooms / racks | None |

Manual creation locks populated optional technical fields and required technical
name/address fields. Absent optional values receive no marker. PATCH Set or
Clear marks an explicitly touched technical field, even when its value is NULL
or unchanged. Placement, lifecycle, parent IDs and is_management never auto-lock.

Markers store no value. Upsert retains one marker per entity/field, refreshes
locked_at and sets locked_by to the latest user. Explicit set/clear requires
admin/super_admin, a supported EntityType, a strict allowed field and a locked
existing canonical entity. No dynamic field-name SQL is constructed. Marker
set/clear also appends audit in its transaction. Canonical delete cleans markers
and records one canonical delete event.

## Device CSV staging

Use examples/devices-template.csv. Replace its example location names with
existing site/room/rack names. CSV is UTF-8 (optional BOM), comma separated,
with normal quoting/doubled quotes and quoted newlines. Headers may be reordered
but must contain exactly these eleven unique names:

site, room, rack, start_unit, height_u, hostname, device_type, vendor, model,
serial, lifecycle_status.

Maximum file size is 5 MiB and maximum row count is 10,000. Multipart payload
allows an additional 64 KiB envelope; file chunks are independently bounded.
Exactly one file part is accepted. Missing/duplicate/unknown headers, invalid
UTF-8, malformed row lengths/quotes, NUL characters and exceeded limits reject
the upload before any batch is committed. XLSX is unsupported.

Upload -> parse -> staging batch/rows -> initial validation -> preview -> typed
row edit -> revalidation -> explicit confirm. Staging performs no canonical
inventory writes and does not create sites/rooms/racks.

raw_data retains the exact logical CSV strings, including whitespace.
working_data trims optional strings, defaults empty lifecycle to active and
retains decimal text for dimensions so invalid source values remain editable.
The edit DTO accepts typed integer/lifecycle values, omitted fields, values and
explicit nulls; unknown fields such as raw_data are rejected. Technical clears
retain internal intent for ownership at confirm; that bookkeeping is excluded
from response DTOs and OpenAPI. Every edit revalidates the whole batch so an
intra-batch overlap cannot leave a neighboring row falsely valid.

Placed devices require a complete existing site -> room -> rack hierarchy plus
positive 32-bit start_unit/height_u. Names resolve using the existing PostgreSQL
case/whitespace normalization, scoped to their parents. Unplaced devices leave
location/start_unit empty and may retain a positive height_u. Lifecycle is
active/maintenance/decommissioned. Bounds and current occupancy are checked
independently of lifecycle. A sorted interval sweep detects overlaps between
otherwise valid rows of the same batch in O(n log n).

Errors and warnings are arrays of {field, code, message}. Shape/lifecycle,
positive dimensions, incomplete/missing hierarchy, out-of-bounds units,
canonical occupancy and batch overlaps are errors. A nonempty hostname/serial
already present in canonical inventory is a warning, because neither column
is globally unique in the schema.

Rows become valid/invalid after validation and imported after commit. A batch
is validated when all rows are valid, pending otherwise, confirmed after
successful commit or cancelled after explicit cancel. Finalized batches cannot
edit/revalidate/confirm/cancel again. Read endpoints allow any authenticated
user; upload/edit/revalidate/confirm/cancel require admin/super_admin.

## Atomic import confirmation

Confirm locks the batch FOR UPDATE and all staging rows FOR UPDATE, checks state,
then revalidates current working data. It locks resolved racks in UUID order
using the same rows protected by existing placement triggers, and revalidates
again after waiting for those locks. This catches a competing placement/resize
committing while confirm waits. Invalid rows reject confirmation; current
warnings require accept_warnings=true.

In the same transaction, all devices are created, each receives an audit event
with source=csv_import/action=create/current user/current request/import_batch_id,
and populated or explicitly cleared manual technical fields receive markers.
Then all rows become imported and batch becomes confirmed with confirmed_at.
A failure at device N rolls back devices 1..N-1, audit, overrides and state.
A second concurrent confirm waits for the batch lock and then sees finalized
state. Existing database constraints remain the final race/concurrency guard.

Invalid revalidation or a late constraint conflict returns 400/409 after the
canonical transaction rolls back. A separate staging-only revalidation refreshes
preview errors/warnings when the batch is still editable. Its failure never
hides the original canonical failure; cancellation/confirmation by another
request may prevent that optional refresh. Revalidate can always refresh an
editable batch explicitly. Unaccepted warnings return 409 without writes.

## Workflow HTTP endpoints

All paths below are prefixed with /api/v1/cmdb and use the existing auth policy.

| Method | Path | Behavior |
| --- | --- | --- |
| GET | /audit-events | Filtered/paginated audit |
| GET | /audit-events/{id} | Audit event |
| GET | /field-overrides | Filtered/paginated markers |
| POST | /field-overrides | Audited idempotent marker set |
| DELETE | /field-overrides/{entity_type}/{entity_id}/{field_name} | Audited clear |
| POST | /imports/devices | multipart/form-data file upload |
| GET | /imports/{batch_id} | Batch summary/counts |
| GET | /imports/{batch_id}/rows | Paginated preview |
| PATCH | /imports/{batch_id}/rows/{row_id} | Typed edit and revalidation |
| POST | /imports/{batch_id}/revalidate | Revalidate all rows |
| POST | /imports/{batch_id}/confirm | {"accept_warnings": true/false} |
| POST | /imports/{batch_id}/cancel | Finalize without inventory writes |

gRPC retains its twelve existing typed RPCs and wire contract. Its seven
mutations gain audit/auto overrides through the same Service; no CSV, override
management or audit-read RPC is added. OpenAPI includes all new DTOs and multipart.

## Isolated tests and quality commands

Native rustc on this Windows host fails even at -vV with exit code 0xc0e90002.
Validation uses stable Rust 1.99.0 in an isolated Linux Docker runner.
No host toolchain or shared configuration is changed.

Run from this directory with a fresh unique Compose project name:

```sh
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml up -d --build rust
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml exec -T rust cargo fmt --all --check
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml exec -T rust cargo check --locked -p cmdb-service --all-targets
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml exec -T rust cargo clippy --locked -p cmdb-service --all-targets -- -D warnings
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml exec -T rust cargo test --locked -p cmdb-service
docker compose -p cronet-cmdb-stage4-01a12211 -f tests/compose.core-test.yml down --volumes
```

This package's own Compose uses PostgreSQL 18 and Redis 8.2.2 in tmpfs, without
published ports or user DB bind mounts. It builds the existing Alembic runner
and waits for migration completion. The repository root is mounted read only.
Cargo registry/target caches are disposable project volumes outside Git.
Test DB trust authentication is confined to this isolated network.

Tests fail if CMDB_TEST_DATABASE_URL is missing, uses another database than
cronet_cmdb_test_schema, or has the wrong Alembic head. Adapter tests require
CMDB_TEST_REDIS_URL exactly redis://redis:6379. Fixtures generate UUIDs and
temporary RSA keys inside the test container. No actual project key,
production DB/Redis or secret-bearing .env is read. No tests are silently
skipped. Remove project/volumes to discard keys, fixtures and build caches.

Stage 4 validation passed:

- cargo fmt --all --check;
- cargo check --locked;
- cargo clippy --locked --all-targets -- -D warnings;
- cargo test --locked: 5 unit + 12 previous core integration + 32 adapter/auth/
  workflow integration = 49 tests, zero ignored. All 31 previous Rust tests
  remain; 18 meaningful workflow tests were added;
- unchanged Alembic suite: 27 tests, including upgrade to head, downgrade to
  72ca3097da29, repeated upgrade to 8b6e2f4a9c10 and users preservation;
- protobuf generation, real HTTP/gRPC startup and OpenAPI generation/parsing.

Adapter coverage includes JWT signature/algorithm/required claims/timestamps/
issuer/audience/type, blacklist and live token_version, invalid/private keys,
DB/Redis outages, normal/admin/super-admin flags, protection of all 30 operations,
HTTP entity flows, all-six-entity SQL pagination, PATCH, MAC/IPv4/IPv6, occupancy,
sanitized errors, all twelve real RPCs and cross-transport consistency.
The binary startup test checks both listeners and missing-key failure.

Previous core tests retain transactions/rollback, override reads, native types,
logical uniqueness, placement, all-lifecycle occupancy and bounded retries.
The startup expectation now requires API configuration, and Site listing
explicitly requests a page instead of assuming unbounded lists.

Workflow tests additionally cover actor/request IDs, before/after/delete,
audit/override failure rollback, a real 40001 retry after audit insertion,
override allowlists/idempotency/cleanup, all CSV validation and warning cases,
raw-data immutability, typed edits and explicit NULL intent, file/row limits,
atomic rollback at device N, concurrent confirm, and competing rack placement
committing while confirm is blocked. OpenAPI is validated component by component.

Existing migration regression commands are in
`../database_migrations/tests/README.cmdb.md`. That suite and its Compose run
unchanged in a separate disposable project.

## Stage 5 integration and shared code

The canonical service and D1 migration live under `services/control_panel`.
Only the 33 owned D1 files moved; legacy team files remain in `control_panel`.
The root workspace includes this package and keeps its existing MSRV metadata
at 1.85; only CMDB declares and tests Rust 1.89. Root Cargo.lock retains all
existing package versions and adds CMDB dependencies. The identical CMDB proto
lives in `contracts/proto/cronet/cmdb/v1/cmdb.proto`; existing shared contracts
and collectors retain their APIs.

The suite now has 52 Rust tests: 5 unit, 12 core and 35 adapter/workflow tests.
Added coverage exercises schema-aware readiness, graceful SIGTERM/SIGINT and a
shared-contract client writing inventory subsequently read over HTTP.
The unchanged migration suite has 27 tests and one head, `8b6e2f4a9c10`.

See `../../README.md` for the opt-in `cmdb` Compose profile, environment/public-key
setup, root build context, production runtime and isolated CI. Existing Compose
services are preserved except for the explicitly approved auth port and
ClickHouse distroless healthcheck corrections. There is no root MSRV change,
collector refactor, service-account bypass or legacy removal.
