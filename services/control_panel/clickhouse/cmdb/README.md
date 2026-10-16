# CMDB dictionaries (D5)

PostgreSQL is the source of truth. These ClickHouse 25.8 dictionaries read
stable PostgreSQL projections; no copying service or batch job is involved.
Alembic revision `d5c1a2b3e001` follows D4 `d4c1a2b3e001`. Apply it before
creating the dictionaries. No existing migration or production Compose service
is changed.

## Projection contract

`public.cmdb_clickhouse_devices` returns exactly one row per device, including
unplaced and decommissioned devices. UUIDs are canonical lowercase strings.
Location fields, including placement height, are NULL for unplaced devices.
For management IP, choose the lowest PostgreSQL INET address among IPv4
addresses first, otherwise IPv6; break ties by interface UUID and IP UUID.
Return only the host part, without CIDR. No management address means NULL.

`public.cmdb_clickhouse_interfaces` returns one row per interface, including
its device UUID, optional index/MAC, hostname and site. PostgreSQL formats MAC
addresses as lowercase colon-separated strings.

`public.cmdb_clickhouse_ips` returns one row per canonical host address.
IPv6 is compressed/lowercase; IPv4 stays dotted decimal. CIDR is separately
preserved as `address_cidr`/`prefix_length`. `is_management` is integer 0/1 for
ClickHouse UInt8. D1 permits the same IP on several interfaces, so the winner
is chosen by lifecycle (active, maintenance, decommissioned), then management
flag descending, then device/interface/IP UUID ascending. `address_count`
counts all candidates. A value >1 means the IP lookup is ambiguous: consumers
must not treat the representative as proof of unique ownership. There is no
VRF/tenant dimension in D1; use device/interface UUID keys when available.

All three dictionaries use `COMPLEX_KEY_HASHED` with a single String key.
Optional attributes retain NULL rather than conflating it with an empty
string. Lifetime is MIN 30 / MAX 120 seconds. Unknown keys can be distinguished
with `dictHas`; default nullable attributes are NULL.

## Deployment

The migration needs permission to create a cluster-wide role. It creates
`cronet_clickhouse_reader` NOLOGIN without a password, schema USAGE and SELECT
only on the three views. It grants no canonical table access. Existing PUBLIC
relation privileges that would broaden access fail the migration. An existing
role with this name also fails instead of being silently adopted or modified.
Review that collision before deployment.

Provision a separate LOGIN through deployment secret management and grant it
membership in `cronet_clickhouse_reader`. Do not grant it other CMDB roles or
table privileges. The views run with the migration owner's permissions and
have `security_barrier=true`; the reader cannot read underlying tables directly.

Mount `named-collection.xml` in the ClickHouse server's `config.d` directory.
Provide all of these through runtime configuration/secret management:

| Environment variable | Value |
| --- | --- |
| `CMDB_DICTIONARY_PG_HOST` | CMDB PostgreSQL hostname |
| `CMDB_DICTIONARY_PG_PORT` | PostgreSQL port |
| `CMDB_DICTIONARY_PG_DATABASE` | CMDB database |
| `CMDB_DICTIONARY_PG_SCHEMA` | `public` |
| `CMDB_DICTIONARY_PG_USER` | Deployment LOGIN with reader membership |
| `CMDB_DICTIONARY_PG_PASSWORD` | Secret supplied at runtime |

Connection/credential fields are non-overridable in the named collection.
Restrict configuration/named-collection administration to operators. Execute
`dictionaries.sql` in the chosen metrics database with the deployment's
authenticated ClickHouse client (`--multiquery`). It contains no credentials.
`IF NOT EXISTS` makes initial provisioning repeatable; it does not update an
already deployed dictionary definition. Future definition changes need an
explicit reviewed ALTER/replacement deployment.

Example lookups (run in that same ClickHouse database):

```sql
SELECT dictGet('cmdb_devices_dict', ('hostname','vendor','model','site_name','rack_name'),
               tuple(toString(toUUID('00000000-0000-0000-0000-000000000001'))));
SELECT dictGet('cmdb_interfaces_dict', ('name','mac_address','device_id'),
               tuple(toString(toUUID('00000000-0000-0000-0000-000000000002'))));
SELECT dictGet('cmdb_ips_dict', ('device_id','interface_id','address_count'),
               tuple(toString(toIPv6('2001:db8::1'))));
SELECT dictGet('cmdb_ips_dict', 'device_id', tuple(toString(toIPv4('192.0.2.3'))));
SYSTEM RELOAD DICTIONARY cmdb_devices_dict;
SYSTEM RELOAD DICTIONARY cmdb_interfaces_dict;
SYSTEM RELOAD DICTIONARY cmdb_ips_dict;
```

Reload refreshes an existing dictionary; it does not require re-creation.
Check `system.dictionaries` status/last_exception when the source is unavailable.
Stop dictionary consumers before downgrading D5. Downgrade removes only D5
views/grants/role, never D1/D4 data. It uses no CASCADE/DROP OWNED: unexpected
external dependencies fail atomically. Re-grant deployment LOGIN membership
after a downgrade/re-upgrade because the group role is recreated.

## Disposable integration and CI

From the repository root, with Docker Linux Engine running:

```text
python -B services/control_panel/services/cmdb_service/tests/run_clickhouse_integration.py
```

The stdlib-only runner generates random one-time passwords/login, uses an
explicit empty env file and a unique Compose project, and always cleans up its
containers/volumes. It never reads a repository .env. PostgreSQL and ClickHouse
25.8.33.6 have ephemeral storage, no published ports, and separate credentials.
The Rust acceptance test uses `sqlx` and the official `clickhouse` client; it is
explicitly ignored in ordinary workspace/CMDB runs and enabled by this runner.
It checks actual view permissions and denied canonical writes, dictGet for all
three keys, IPv4/IPv6, NULLs, collisions, and hostname/vendor/model/placement
refresh without dictionary UUID change. Migration tests retain the previous
32 cases and add D5 chain, view, privilege, downgrade and repeated upgrade checks.

`.github/workflows/cmdb-dictionaries.yml` runs this isolated test in its own job,
with an additional always-run cleanup step. Existing jobs remain unchanged.
The Rust ClickHouse dependency is dev-only. Production CMDB's existing filtered
build context excludes tests and these external deployment assets; its runtime
remains the production binary plus system libraries. Do not place credentials,
test data, logs or reports alongside these production assets.
