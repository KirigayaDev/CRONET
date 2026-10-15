## Создание RSA ключей для подписи JWT токенов
```bash
openssl genpkey -algorithm RSA -out ./crypt_keys/jwt_keys/private_key.pem -pkeyopt rsa_keygen_bits:2048
openssl rsa -pubout -in ./crypt_keys/jwt_keys/private_key.pem -out ./crypt_keys/jwt_keys/public_key.pem
```

## D1 CMDB

The D1 integration target is `services/control_panel`. The legacy
`control_panel` tree is preserved for compatibility pending a team decision.
Plain `docker compose up` keeps CMDB disabled. Existing service settings are
preserved except for two corrected healthchecks: auth uses its actual HTTP port
443, and ClickHouse distroless uses `clickhouse-client` without a shell.

From this directory, configure the existing environment and public key, then:

```sh
docker compose --profile cmdb up -d --build
```

Set `CMDB_DATABASE_URL` and `CMDB_REDIS_URL` to URLs for the existing PostgreSQL
and Redis services (container hosts `postgres:5432` and `redis:6379`). URL-encode
credentials. These values are passed only to CMDB as `DATABASE_URL` and
`REDIS_URL`; their empty defaults preserve parsing when the profile is disabled.
CMDB fails startup if either is absent. Optional `JWT_ISSUER` and `JWT_AUDIENCE`
must match auth_service; the defaults match its current contract.

Provision `crypt_keys/jwt_keys/public_key.pem` from the same RSA key pair used by
auth_service. CMDB mounts only this public file, read only; ensure UID 10001 can
read it. Never mount a private key into CMDB. Access uses existing user access
JWTs for both HTTP and gRPC. Reads require a valid current user; writes require
`is_admin` or `is_super_admin`. A separate M2M/service-account model is pending.

CMDB waits for healthy PostgreSQL/Redis and the existing migration runner.
The runner must apply the full Alembic chain through `8b6e2f4a9c10`; CMDB does
not run migrations. Readiness checks all ten required D1 tables, Redis and the
loaded public key, without pinning future runtime readiness to an Alembic head.
Do not downgrade a populated deployment. Earlier feature deployments already
stamped at this revision need a separately reviewed compatibility migration.

Ports bind to localhost: HTTP `127.0.0.1:8081`, gRPC `127.0.0.1:50051`.
HTTP routes are under `/api/v1/cmdb`, with `/openapi.json` for the contract.
`/health/live` is process liveness; `/health/ready` returns 503 until its
dependencies/schema are usable. Default Docker SIGTERM triggers graceful drain.

The production Dockerfile uses the repository root as build context, Rust 1.89,
shared vendored protobuf generation, and a Debian slim runtime as UID/GID 10001.
Its Dockerfile-specific ignore file restricts build inputs; the runtime contains
the binary, CA certificates and curl for healthchecks, without repository source,
env files, keys or build caches. Root MSRV metadata remains 1.85; the existing
workspace dependency/MSRV inconsistency is separate technical debt.

Run static checks from the repository root:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --exclude cmdb-service --locked
```

Use a fresh project name for each isolated CMDB run:

```sh
docker compose -p cmdb-local-test -f services/control_panel/services/cmdb_service/tests/compose.core-test.yml up -d --build rust
docker compose -p cmdb-local-test -f services/control_panel/services/cmdb_service/tests/compose.core-test.yml exec -T rust cargo test --locked -p cmdb-service
docker compose -p cmdb-local-test -f services/control_panel/services/cmdb_service/tests/compose.core-test.yml down --volumes
```

This runs all 52 CMDB Rust tests against disposable PostgreSQL/Redis. The 27-test
migration suite, including downgrade/repeated upgrade with users preserved, has
its own independent Compose project; see
`services/database_migrations/tests/README.cmdb.md`. Rust CI retains existing
workspace/NETMON tests and adds CMDB static/unit, integration and migration jobs.
