# D1: initial CMDB schema

Revision `8b6e2f4a9c10` extends `72ca3097da29`. It uses the existing
Alembic runner and changes no existing migration, model, configuration,
or auth_service file. This step adds the database foundation; the Rust API
and CSV confirmation workflow follow separately.

## Repository audit

- Base main: `92759ec202b08d0ad507c2285627aa096ef771e8`.
  Implementation branch: `feature/d1-cmdb-api`.
- Root contains `control_panel`, `.github`, `scripts`, and `pyproject.toml`.
  Existing services are Python `auth_service` and `database_migrations`.
  No Rust service, Cargo manifest, workspace, or existing test suite was found.
  An isolated future Rust service can live at
  `control_panel/services/cmdb_service`.
- Shared Compose defines auth, migrations, PostgreSQL, Redis, NATS,
  ClickHouse, and Qdrant. Services use the implicit Compose default network.
  PostgreSQL is reached at `postgres:5432`; migrations wait for its healthcheck.
  The existing PostgreSQL storage is a host bind mount. Tests must not use it.
- CI runs Gitleaks and Python Bandit/pip-audit via `scripts/security-check.sh`.
  Bandit configuration excludes tests. No functional test or formatter/linter
  convention was found. No shared CI or dependency changes were made.
- Local ignored `.env`, `postgres_data`, and migration `__pycache__` already
  existed before the work. Their contents were not changed by the test setup.

## Existing database conventions

- SQLAlchemy 2.0.44, Alembic 1.17.0, psycopg2-binary 2.9.11.
- Revisions are twelve hexadecimal characters with descriptive snake_case
  filenames and explicit `upgrade`/`downgrade`. The old chain is
  `4c36a1315fda -> 5fe12f45695b -> 33df59363c17 -> 72ca3097da29`.
- The first migration creates `uuid-ossp`; existing users use PostgreSQL UUID
  with a server-side `uuid_generate_v4()` default. CMDB retains that generator.
  CMDB keys are named `id` as specified; references to existing users target
  `users.uuid`.
- Existing `users.created_at` is a timezone-naive TIMESTAMP. New CMDB timestamps
  use TIMESTAMPTZ as requested; no existing column is converted. The six canonical
  entity tables have server-side creation defaults and update triggers.
- `env.py` constructs a `postgresql+psycopg2` URL from `POSTGRES_USER`,
  `POSTGRES_PASSWORD`, and `POSTGRES_DB`, with fixed host `postgres:5432`.
  It loads `Base.metadata` through `models/__init__.py`.
- The existing Dockerfile installs the existing requirements and runs
  `alembic upgrade head`. Shared Compose mounts revision files into that runner.
  A manually authored new revision runs without registering new ORM models.
- CMDB adds `btree_gist` for UUID/range exclusion. The migration runner needs
  permission to install this extension. Downgrade retains it because it may
  predate CMDB or become shared; `uuid-ossp` is left untouched.

## Authentication audit (read only)

- User identity is `users.uuid` (PostgreSQL UUID); JWT `sub` is UUID4.
- Claims: `iss`, `sub`, `aud`, `exp`, `nbf`, `iat`, `jti`, `token_version`,
  `token_type`. Time claims serialize as Unix timestamps. `jti` and
  `token_version` are UUID4. Token types are `access`, `refresh`, and `csrf`.
- Both issuer and audience currently default to
  `Re:simple_videohosting_auth`. The decoder checks audience, restricts the
  algorithm to RS256, and validates the payload schema. It does not explicitly
  pass an expected issuer to PyJWT; a future verifier must make that policy clear.
- Tokens are signed with an RSA private PEM and verified with a public PEM.
  Key material was not read or printed. Compose mounts the key directory read only.
- Full token validation also rejects Redis blacklist entries at
  `blacklisted_jwt_token:<jti>` and checks the current user/token_version pair
  in PostgreSQL. Signature verification alone does not reproduce logout and
  password-change revocation.
- DB flags `is_admin` and `is_super_admin` exist. No roles/permissions tables,
  permission checks, or role claims were found.
- Cookie authentication uses `access_token`; mutating requests require a
  matching `X-CSRF-Token`. The existing `/api/token` endpoint accepts an access
  token and uses full validation.
- A future CMDB service can independently verify signatures with only the
  public key. To preserve existing revocation semantics it also needs Redis
  and user/token_version reads, or the existing token-validation endpoint.
  None of this requires editing auth_service.

## Tables and constraints

All ten tables have UUID primary keys with `uuid_generate_v4()` defaults.
Foreign keys use PostgreSQL's default NO ACTION; no destructive cascades are added.

| Table | Main constraints and behavior |
| --- | --- |
| `sites` | Required name; optional description; created_at/updated_at. Site names are not globally unique. |
| `rooms` | Required FK site_id; UNIQUE(site_id, name); timestamps. |
| `racks` | Required FK room_id; UNIQUE(room_id, name); height_u > 0; cannot shrink past any placed device. |
| `devices` | Nullable rack_id FK; placement requires start_unit and height_u; start_unit/height_u positive when present; lifecycle active/maintenance/decommissioned; rack bounds checked on placement/update; active U ranges cannot overlap in one rack. |
| `interfaces` | Required device_id FK; if_index > 0 when present; UNIQUE(device_id, if_index) permits multiple NULLs; native MACADDR, without global uniqueness. |
| `ip_addresses` | Required interface_id FK and INET address; UNIQUE(interface_id, address); management flag defaults false. IPv4 and IPv6 supported. |
| `field_overrides` | UNIQUE(entity_type, entity_id, field_name); six canonical entity types; nonblank field_name; locked_by FK users.uuid; locked_at; no duplicated value. |
| `audit_events` | Actor/source/action/entity metadata; before/after JSONB; optional request_id and batch FK; detected_at and optional changed_at; UPDATE, DELETE, TRUNCATE rejected. |
| `import_batches` | created_by FK users.uuid; filename; status pending/validated/confirmed/cancelled; confirmed_at exists exactly when status is confirmed. |
| `import_rows` | batch_id FK; positive row_number; UNIQUE(batch_id, row_number); raw/working JSONB objects; error/warning JSONB arrays; status pending/valid/invalid/imported. |

Hostname and serial have nonunique search indexes. Foreign-key access paths use
indexes or the leading columns of compound unique constraints. Audit has indexes
for entity/time, request_id, and import_batch_id. Four CMDB trigger functions and
nine triggers are created; downgrade removes them with the CMDB tables.

## Placement and 3NF decisions

- Location is `device -> rack -> room -> site`. Devices carry no site_id/room_id,
  IP, or MAC. Racks carry no site_id. Rack units are ranges, not per-U rows.
- An unplaced device has NULL rack_id/start_unit. It may still have a known,
  positive physical height_u. Positioned devices use one-based U numbering.
- An active device occupies `[start_unit, start_unit + height_u)` in its rack.
  Adjacent devices are allowed. Maintenance/decommissioned devices can overlap
  under the requested active-only rule, but all placed devices must fit the rack.
- A GiST exclusion constraint enforces active overlap under concurrency.
  PostgreSQL `btree_gist` provides the UUID equality operator class.
- Bounds cannot be a cross-table CHECK. Placement makes a no-op MVCC write to
  the target rack and validates its height; rack resize validates existing devices.
  This deliberately serializes placements/resizes in the same rack and rejects
  stale writes under REPEATABLE READ/SERIALIZABLE. Actual rack values and its
  updated_at remain unchanged on placement. A row lock alone would leave a
  stale-snapshot hole under stronger isolation.
- Bigint range arithmetic avoids overflow at the PostgreSQL integer limit.
- Import rows are staging, not a second canonical inventory. Raw and editable
  data are intentional staging copies. Audit snapshots are intentional history.
- No vendors/models/mac_addresses dictionaries or second migration mechanism
  are introduced.

References: [PostgreSQL constraints](https://www.postgresql.org/docs/18/ddl-constraints.html),
[btree_gist](https://www.postgresql.org/docs/18/btree-gist.html),
[transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html).

## Reproducible checks

Run from `control_panel/services/database_migrations`. Use a fresh, unique
Compose project name and verify that no containers already belong to it.
The dedicated test Compose uses PostgreSQL 18, tmpfs storage, no exposed ports,
no source bind mounts, and the existing database_migrations Dockerfile.
Trust authentication is confined to this isolated test network; it is not a
deployment configuration. It does not load `control_panel/.env`.

```sh
docker compose -p cronet-cmdb-d1-test-01a12211 -f tests/compose.cmdb-test.yml config --quiet
docker compose -p cronet-cmdb-d1-test-01a12211 -f tests/compose.cmdb-test.yml build database_migrations
docker compose -p cronet-cmdb-d1-test-01a12211 -f tests/compose.cmdb-test.yml up -d --wait postgres
docker compose -p cronet-cmdb-d1-test-01a12211 -f tests/compose.cmdb-test.yml run --rm --no-deps database_migrations python -B -m unittest discover -s tests -p test_cmdb_migration.py -v
docker compose -p cronet-cmdb-d1-test-01a12211 -f tests/compose.cmdb-test.yml down
```

The suite refuses a different database name and refuses a nonempty initial DB.
It upgrades the old chain, inserts a synthetic user, then upgrades CMDB, checks
the user schema/data, and performs downgrade/upgrade only in the ephemeral DB.
Concurrency tests wait for an observed PostgreSQL lock rather than assuming timing.
Python bytecode writing is disabled in test commands.

Validation on 2026-10-09: 17 integration tests passed in 6.952 seconds.
Checks cover migration head/history, table/type/3NF inspection, hierarchy/FKs,
positive dimensions, placement/resize, active overlap/reactivation/adjacency,
per-interface IP uniqueness, IPv4/IPv6/MAC validation, overrides, staging JSON
and statuses, audit immutability and transaction rollback, update timestamps,
four competing-transaction scenarios, two stronger isolation levels, and
downgrade/upgrade schema restoration with the existing user's data unchanged.
Local Python AST parsing and Git whitespace checks also passed.

## Delivery state

Four new D1 files are staged for review: this report, the new migration,
the isolated test Compose, and the integration test suite. All existing
tracked files are unchanged. The test project's containers and network
were removed after checking the table list, migration head, and extensions.
No new host database data, .env, IDE files, or bytecode files were created.

Commit and push are deferred at the user's request until the mentor confirms
the change. No commit was created and no branch was pushed. An earlier commit
attempt failed because Git had no author identity; the user subsequently
supplied an identity for eventual command-scoped use. Git configuration was
not changed and the identity is not stored in this report.

Planned commit message: `feat(cmdb): add initial CMDB database schema`.
Planned push target after confirmation: `origin/feature/d1-cmdb-api`.

## Next stage and open policies

- Build the isolated Rust cmdb_service with canonical CRUD and authorization.
- Check entity existence and valid field names for polymorphic field_overrides;
  discovery must consult these locks. A generic entity_id has no ordinary FK.
- Write entity changes and audit events in the same transaction. The schema
  supports this and rollback is tested; it does not force every SQL writer to
  create an audit event. DB owners can disable triggers; runtime roles must not
  have that privilege. Actor/entity references in history intentionally have no FK.
- Implement CSV parse -> validate -> preview -> edit -> revalidate -> confirm.
  Confirmation must lock the batch/staging rows, reject any validation errors,
  explicitly accept warnings, and commit the entire canonical import plus audit
  in one transaction. Status/JSON checks are not the all-or-nothing import API.
- Decide case normalization for names/hostname, actor/source/action vocabulary,
  import retention, and permissions before exposing writes. Existing username
  and CMDB name comparisons remain case-sensitive.
- Handle SQLSTATE 40001/40P01 with bounded transaction retries. Per-rack write
  serialization trades throughput for correct bounds at all supported isolation
  levels; the no-op writes also create MVCC versions/WAL. Bulk writers should
  use a consistent rack ordering. Device placement roles will need UPDATE on racks.
- Downgrade drops CMDB data; use it only on disposable databases or with a
  deliberate rollback/data-preservation plan. Existing users remain intact.

## Требуются изменения общего кода

No shared-code change is needed to apply this revision. The following changes
are deferred and were NOT applied:

1. `control_panel/services/database_migrations/models/__init__.py`.
   Its current metadata contains only User. Before future Alembic autogeneration,
   add new CMDB models in our own `models/cmdb.py` and register them, otherwise
   autogeneration can propose dropping tables absent from metadata.
   Minimal prospective registration diff (once those classes exist):

   ```diff
   +from .cmdb import (
   +    Site, Room, Rack, Device, Interface, IpAddress,
   +    FieldOverride, AuditEvent, ImportBatch, ImportRow,
   +)
   ```

   The model metadata must also represent database constraints/indexes; Alembic
   autogeneration requires review of PostgreSQL-specific objects and triggers.

2. `control_panel/docker-compose.yml`.
   The future API needs an additional service in the existing default network,
   database settings, startup after migrations, and public-key access. If it
   performs full revocation checks itself, it also needs existing Redis settings.
   Minimal prospective service addition (service Dockerfile/config paths must be
   implemented first, and any external API port decided at that stage):

   ```diff
    services:
   +    cmdb_service:
   +        build: ./services/cmdb_service
   +        environment:
   +            POSTGRES_USER: ${POSTGRES_USER}
   +            POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
   +            POSTGRES_DB: ${POSTGRES_DB}
   +            REDIS_PASSWORD: ${REDIS_PASSWORD}
   +        volumes:
   +            - ./crypt_keys/jwt_keys/public_key.pem:/cmdb_service/crypt_keys/jwt_keys/public_key.pem:ro
   +        depends_on:
   +            database_migrations:
   +                condition: service_completed_successfully
   +            postgres:
   +                condition: service_healthy
   +            redis:
   +                condition: service_healthy
   ```

   This is a proposed future diff, not a request to change the shared configuration
   during the schema stage. No auth_service change is required.
