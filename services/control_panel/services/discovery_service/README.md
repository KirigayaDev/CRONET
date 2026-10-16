# Discovery service (D3)

One-shot, bounded SNMP discovery:

```text
literal CIDR -> optional ICMP hint -> SNMP GET -> classification
            -> CMDB ApplyDiscoveryObservation -> JSON summary
```

Requires Rust 1.89 or later. The root workspace and lockfile own dependency
resolution. There is no scheduler, NATS consumer, metrics polling, deep inventory,
topology collection or private Discovery database.

## Run

Provide a **short-lived admin user access JWT** through `CMDB_BEARER_TOKEN`.
Set `CMDB_GRPC_URL` to the CMDB gRPC endpoint (default
`http://127.0.0.1:50051`). HTTPS uses the WebPKI trust roots. Supply tokens through
your process environment/secret runner; do not put credentials in CLI arguments.

```sh
cargo run --locked -p discovery-service -- scan \
  --cidr 10.10.0.0/24 \
  --credential-ref vault:secret/snmp/site-a/core \
  --snmp-version v3 \
  --concurrency 64 --timeout-ms 2000 --icmp
```

`--snmp-version` accepts `v2c` and `v3`. `--json-results` includes per-host
records; otherwise stdout contains one JSON summary. Logs go to stderr and
contain fixed, sanitized messages. There is no error log per offline host.

| Option | Default / bounds |
| --- | --- |
| `--cidr` | Required literal IPv4/IPv6 CIDR; no target DNS resolution. |
| `--credential-ref` | Required reference, never a credential value. |
| `--concurrency` | 64; 1–512 in-flight hosts. |
| `--timeout-ms` | 2000; 1–30000, covers the entire SNMP exchange including v3 discovery. |
| `--max-hosts` | 4096; absolute maximum 65536. |
| `--snmp-port` | 161; custom port supports isolated fixtures. |
| `--icmp` | Off; at most 300 ms per host, within the same concurrency bound. |

CIDR size is checked before allocating/enumerating targets. IPv4 excludes network
and broadcast for /30 and larger subnets, includes both /31 addresses and the
single /32 address. Small IPv6 prefixes include all addresses. Huge IPv6/IPv4
prefixes are rejected immediately. Targets are generated lazily with
`buffer_unordered`; there is no task per address and no unbounded retry.

ICMP is a hint: false, timeout, unavailable socket or missing permissions still
lead to an SNMP attempt. A socket initialization failure produces one safe
warning. No privileged container is required.

Exit code **0** means the scan completed, including offline hosts or individual
CMDB conflicts. A nonzero exit means the operation could not run: invalid input,
unavailable credentials/CMDB, or rejected authentication/authorization. CMDB
connection/preflight and each RPC have five-second deadlines. Preflight reads one
device to check connectivity and token authentication; admin authorization is
enforced by the ingestion RPC on each observation. A reader-only token with no
SNMP responders may therefore finish without exercising the admin-only RPC.

## Credentials and SNMP

Uses `libs/creds`, with the poller's existing backend conventions:

- Default `NETMON_CRED_BACKEND=vault`: `VAULT_ADDR`, optional `VAULT_NAMESPACE`
  and `VAULT_CACERT`; AppRole `VAULT_ROLE_ID` + `VAULT_SECRET_ID`
  (`VAULT_APPROLE_MOUNT=approle`) or `VAULT_TOKEN`.
- Development/tests only: `NETMON_CRED_BACKEND=env`, reference `env:FIXTURE`
  reads the synthetic credential JSON from `NETMON_CRED_FIXTURE`.

The credential is resolved once, with a ten-second deadline, and remains in
`SecretString` through the existing credential types. Backend errors are replaced
with static messages. The CMDB token is a `SecretString`; gRPC authorization
metadata is marked sensitive. Neither credential contents nor token values enter
results, protobuf inventory fields, errors or application Debug output.

SNMP uses `snmp2` with heap buffers and literal `SocketAddr` targets. v3 performs
engine discovery and uses the credential's user/context/auth/privacy settings.
MD5, SHA1, DES and noAuthNoPriv require explicit
`NETMON_ALLOW_LEGACY_CRYPTO=true`, matching existing poller policy. SNMPv1 is
unsupported. Silent authentication drops are indistinguishable from timeouts;
explicit authentication errors are counted separately.

Each probe performs one GET for:

| Object | OID |
| --- | --- |
| sysDescr.0 | `1.3.6.1.2.1.1.1.0` |
| sysObjectID.0 | `1.3.6.1.2.1.1.2.0` |
| sysName.0 | `1.3.6.1.2.1.1.5.0` |

sysObjectID must be a valid ObjectIdentifier. Optional text is UTF-8, trimmed,
bounded and NUL-free. Empty text becomes absent. No MIB walk is performed.

## Classification

The pure classifier maps `1.3.6.1.4.1.<enterprise>`:

| ID | Vendor |
| --- | --- |
| 9 | Cisco |
| 11 | HPE |
| 2011 | Huawei |
| 2636 | Juniper |
| 14988 | MikroTik |
| 12356 | Fortinet |
| 30065 | Arista |
| 41112 | Ubiquiti |

Unknown enterprise yields no vendor, irrespective of vendor-like sysDescr text.
Exact words in sysDescr conservatively select firewall, wireless/wifi, switch,
router or server; fallback is `network_device`. Model is currently always absent:
no maintained exact-product OID mapping exists. sysObjectID is kept in the scan
result, not invented as a model or unique device identity.

## CMDB ownership and repeat scans

Production Discovery has no SQL dependency. `TonicCmdbClient` calls only the
additive admin-only `ApplyDiscoveryObservation` RPC for persistence. Ordinary
manual Create/Update RPCs would create ownership markers and are unsuitable for
automatic ingestion.

CMDB authenticates the existing JWT, sets `source=discovery`, `actor_type=user`
and the authenticated admin's user ID. This is transitional until a separate
service identity is designed. Existing HTTP, ordinary gRPC and CSV manual
semantics are preserved.

CMDB identifies a device by management IP via IP -> interface -> device,
independent of the stored prefix length. An unambiguous nonempty serial is a
fallback when provided by a future richer observation; this minimal worker does
not probe serial. Ambiguous IP/serial relations return a conflict, never a silent
IP move. A new device is active and unplaced, with a stable logical
`discovery-management` interface (`discovery/logical`) and a management IP.
An existing IP and its management flag are reused unchanged.

Device/interface/IP creation and audits are one CMDB transaction. A short
transaction-scoped relation lock serializes absent-IP decisions and prevents
concurrent duplicate ingestion without a new canonical schema constraint. This
coarse lock favors correctness for D3; it limits write throughput. No network I/O
occurs while the transaction holds it.

Only hostname/device_type/vendor/model/serial can be updated, and only when the
field has no manual override. Missing/blank observations do not clear values.
Placement and lifecycle are never updated. Responses list `skipped_fields`.
Discovery creates no manual overrides. Audit uses the existing `create`/`update`
vocabulary and request IDs; identical observations cause no audit/timestamp churn.
Repeated scans of a management IP reuse the device and endpoint. The summary's
`updated` count includes an existing device whose values were unchanged/skipped.

## Tests

Normal unit/static checks need no Docker or network:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --locked -p discovery-service --test unit
```

The four opt-in E2E tests return immediately unless `DISCOVERY_E2E=1`. Use a fresh
disposable Compose project for the real acceptance suite:

```sh
export COMPOSE_PROJECT_NAME=cronet-discovery-test
export COMPOSE_FILE=services/control_panel/services/discovery_service/tests/compose.discovery-test.yml
docker compose up -d --build --wait snmpsim
docker compose exec -T rust cargo build --locked -p cmdb-service
docker compose exec -T rust cargo test --locked -p discovery-service --test e2e -- --test-threads=1
docker compose down --volumes
```

The separate CI job runs exactly this fixture. It exposes no host ports, uses
synthetic SNMP credentials, ephemeral RSA test keys and an isolated test database.
SQLx is a **dev-dependency only**, for synthetic auth-user setup and read-only
acceptance assertions. Canonical inventory is written through the real CMDB
server. The fixture tests real v2c with Cisco/Juniper, v3 SHA256/AES128 and IPv6.
Full acceptance runs the real CLI, checks the stored graph/audit/no overrides,
sets a manual hostname with HTTP PATCH, then repeats discovery with a changed
SNMP hostname and verifies that the manual value survives. Offline hosts and
rejected credentials/JWTs are also covered. Existing NETMON fixtures are untouched.
