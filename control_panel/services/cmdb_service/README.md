# CMDB service: core, HTTP, gRPC and authentication

Standalone Cargo package on `feature/d1-cmdb-api`. Stage 3 adds long-running
HTTP/Axum and gRPC/tonic adapters to the existing core. Both transports share
one Service and one Authenticator.

The existing Alembic head is `8b6e2f4a9c10`. This package never creates tables,
changes schema, reads .env files, or runs migrations itself. Apply the existing
Alembic chain before startup. Transactional audit generation, CSV workflow,
Discovery, Scheduler and service-account identities are later work.

## Structure

```text
cmdb_service/
  Cargo.toml
  Cargo.lock
  README.md
  build.rs
  proto/cmdb.proto
  src/
    lib.rs
    main.rs
    config.rs
    db.rs
    error.rs
    models.rs
    repository.rs
    service.rs
    auth.rs
    api/
      mod.rs
      dto.rs
      http.rs
      grpc.rs
  tests/
    core.rs
    adapters.rs
    compose.core-test.yml
    Dockerfile.rust
```

api/dto.rs keeps HTTP/OpenAPI schemas and input conversions separate from
domain models. api/mod.rs owns shared state and concurrent server lifecycle.
build.rs generates typed protobuf clients and servers in Cargo's output
directory, outside Git.

## Dependencies

Resolved versions are committed in Cargo.lock.

| Dependency | Purpose |
| --- | --- |
| axum 0.8 | HTTP routing, middleware and extraction. |
| tonic / tonic-prost 0.14 | gRPC transport and protobuf codec. |
| prost / prost-types 0.14 | Typed messages, standard Timestamp/NullValue. |
| tonic-prost-build 0.14 (build) | Generate clients and servers from cmdb.proto. |
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
SQLx enables only PostgreSQL and required native types. No CSV, NATS, SNMP,
Discovery or Scheduler dependency is added.

Building requires protoc and standard protobuf includes. The isolated test
Dockerfile installs protobuf-compiler, libprotobuf-dev and OpenSSL without
changing the host toolchain or any shared Dockerfile.

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

Ctrl-C signals both servers with up to five seconds for graceful shutdown.
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
lookup also lives in Repository. The existing db SELECT 1 health helper remains.

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
remains; field-lock write workflow and audit generation are deferred.

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
for future transactional audit actor propagation. This stage writes no partial
audit.

There is no separate machine-to-machine/service-account identity model yet.
gRPC accepts existing **user access JWTs** only. D2/D3 integration must decide
that model separately; no static password or fake admin token is provided.

## HTTP routes

All paths below are relative to `/api/v1/cmdb` and require authentication.
Each listed method is an operation: 30 business operations total.

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

utoipa generates /openapi.json with all 33 operations, request/response schemas,
UUID parameters, filters, pagination bounds/defaults, Bearer JWT security and
main error responses. Public routes have no security requirement. Tests parse
serialized JSON back into utoipa's OpenApi and inspect routes/security/schemas.

Errors use stable JSON `{ "code": "...", "message": "..." }` with sanitized
messages, including extractor failures, unknown routes and unsupported methods.

| Condition | HTTP | gRPC |
| --- | --- | --- |
| Validation / malformed input | 400 | INVALID_ARGUMENT |
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

proto/cmdb.proto defines package cronet.cmdb.v1, service Cmdb, and twelve RPCs:

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
/health/ready checks PostgreSQL SELECT 1 and Redis PING: 200/ok if available,
503/unavailable otherwise. A valid key is guaranteed at startup and is not
reloaded for each probe. Dependency failures log only a fixed warning.

Tracing records startup addresses, HTTP method/matched route templates and gRPC
request types. Spans exclude raw query strings/path IDs. Headers, cookies, JWTs,
payloads, URLs with credentials and key bodies are not logged. A scoped info
filter for cmdb_service/tower-http prevents environment-enabled dependency debug
logging. No permissive CORS layer is added.

## Isolated tests and quality commands

Native rustc on this Windows host fails even at -vV with exit code 0xc0e90002.
Validation uses stable Rust 1.99.0 in an isolated Linux Docker runner.
No host toolchain or shared configuration is changed.

Run from this directory with a fresh unique Compose project name:

```sh
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml up -d --build rust
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml exec -T rust cargo fmt --check
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml exec -T rust cargo check --locked
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml exec -T rust cargo clippy --locked --all-targets -- -D warnings
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml exec -T rust cargo test --locked
docker compose -p cronet-cmdb-api-01a12211 -f tests/compose.core-test.yml down --volumes
```

This package's own Compose uses PostgreSQL 18 and Redis 8.2.2 in tmpfs, without
published ports or user DB bind mounts. It builds the existing Alembic runner
and waits for migration completion. Only this package's source is mounted.
Cargo registry/target caches are disposable project volumes outside Git.
Test DB trust authentication is confined to this isolated network.

Tests fail if CMDB_TEST_DATABASE_URL is missing, uses another database than
cronet_cmdb_test_schema, or has the wrong Alembic head. Adapter tests require
CMDB_TEST_REDIS_URL exactly redis://redis:6379. Fixtures generate UUIDs and
temporary RSA keys inside the test container. No actual project key,
production DB/Redis or secret-bearing .env is read. No tests are silently
skipped. Remove project/volumes to discard keys, fixtures and build caches.

Stage 3 validation passed:

- cargo fmt --check;
- cargo check --locked;
- cargo clippy --locked --all-targets -- -D warnings;
- cargo test --locked: 5 unit + 12 previous core integration + 14 adapter/auth
  integration = 31 tests, zero ignored;
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

Existing migration regression commands are in
`../database_migrations/tests/README.cmdb.md`. That suite and its Compose run
unchanged in a separate disposable project.

## Following stages and shared code

Stage 4 remains transactional audit (Principal.user_id in the entity mutation
transaction), CSV validation/confirmation and import_batches/import_rows workflow.
Discovery/field-lock enforcement and D2/D3 machine identities remain later
decisions. This stage introduces no partial audit, CSV endpoint, UI, gateway,
NATS, Scheduler, Discovery or service account.

No shared-code change is required to build, run or test this stage independently.
Common deployment integration is a separate change: runtime image/service entry
and public-key/DB/Redis config are not added here. auth_service, existing Alembic
files/models, common Compose, scripts and CI are unchanged by implementation.
The only shared-file difference from the previous feature commit is from the
authorized conflict-free origin/main merge.
