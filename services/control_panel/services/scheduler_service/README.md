# Scheduler service (D2)

CMDB gRPC inventory -> per-device interval slots -> DelayQueue -> distributed KV
coordination -> existing JetStream `TASKS_SNMP` -> existing SNMP poller.
Scheduler never executes SNMP, resolves targets through DNS, reads SNMP secrets,
assigns work to individual poller instances, or writes canonical inventory.
Rust 1.89+, root workspace/lockfile. D3 is not a dependency.

## Configuration

Set `SCHEDULER_CONFIG_PATH` to a JSON file, or explicitly provide
`SCHEDULER_CONFIG_JSON` for development/tests. A nonempty file path takes priority;
an invalid/missing file is an error, without fallback to JSON. Configuration is
validated at startup and stays fixed for that process; inventory resync is live.

```json
{
  "schedules": [{
    "id": "core-fast",
    "enabled": true,
    "site": "site-a",
    "interval": "30s",
    "jitter": "5s",
    "snmp_version": "v3",
    "credential_ref": "vault:secret/snmp/site-a/core",
    "get_oids": ["1.3.6.1.2.1.1.3.0"],
    "walk_roots": [],
    "timeout": "5s",
    "task_deadline": "30s",
    "max_lateness": "30s",
    "max_repetitions": 20,
    "port": 161
  }]
}
```

Durations use humantime-serde. IDs must be unique and nonempty. Schedule IDs and
sites follow `[A-Za-z0-9_-]+`, at most 128 bytes. Site matches CMDB site **name**
exactly; it is never invented from an unplaced device. Interval is positive,
millisecond-aligned and at most 24 hours; jitter is nonnegative and less than
interval. Timeout is 1 ms–30 s, task deadline 1 ms–1 h, max lateness 0–60 s.
All durations require millisecond precision. Port is 1–65535, repetitions 1–100.
GET has at most 100 OIDs, WALK at most 20 roots, at least one is required and each
OID is parsed with the existing snmp-types helper. Versions: v2c/v3 only.
credential_ref is required; its contents remain a reference, never a secret.
Configuration is at most 1 MiB/1024 schedules; inventory and owned queue each have
a 100000-entry safety limit. No new PostgreSQL schema is introduced.

| Environment | Default / behavior |
| --- | --- |
| `CMDB_GRPC_URL` | `http://127.0.0.1:50051`; HTTPS uses WebPKI roots. |
| `CMDB_BEARER_TOKEN_FILE` | Preferred production access-JWT file, reread before every complete resync. |
| `CMDB_BEARER_TOKEN` | Explicit development/test fallback when no token file is set. |
| `SCHEDULER_RESYNC_INTERVAL` | `60s`; 1 s–1 h, with bounded spreading. |
| `SCHEDULER_INSTANCE_ID` | Hostname/process/UUID fallback; explicit IDs must be unique among live replicas. |
| `SCHEDULER_NAMESPACE` | `SCHEDULER`; shared by replicas of this deployment. |
| `SCHEDULER_HEARTBEAT_INTERVAL` | `5s`; 1–30 s. |
| `SCHEDULER_MEMBER_TTL` | `15s`; at least 3 heartbeats, at most 120 s. |
| `SCHEDULER_CLAIM_TTL` | `20s`; 12–30 s, well below the existing 120 s stream dedup window. |
| `NATS_URL` | `nats://127.0.0.1:4222`; passed to existing Bus. |
| `NATS_CREDS_FILE` | Optional existing Bus credentials-file convention. |
| `SCHEDULER_BOOTSTRAP_STREAMS` | `false`; explicitly enable to call Bus::ensure_streams. |
| `SCHEDULER_STREAM_REPLICAS` | 1; 1–5 for scheduler KV/bootstrap. |
| `SCHEDULER_STREAM_PROFILE` | `prod`; `dev` provides smaller existing Bus limits for fixtures. |

CMDB uses the existing authenticated **user access JWT**. The read-only RPC does
not require admin privileges. This transitional user-token approach still needs
external rotation until service-account authentication is designed. File takes
priority over environment and must remain readable by the runtime user; rotate
the file atomically in its mounted directory. Tokens use SecretString, sensitive
gRPC metadata and fixed sanitized errors. Scheduler does not connect to Vault or
load SNMP credentials; the poller resolves credential_ref.

## Inventory and timing

Additive `ListPollTargets` reads only ACTIVE devices, with optional hostname/site
and **all** management IPs. UUID keyset pages are limited to 200. Site comes from
device -> rack -> room -> site; INET prefixes are stripped. Unplaced devices,
zero management IPs, multiple management IPs and unsafe site names are diagnosed
and skipped. The projection is read-only and creates no audit/override changes.

Successful resync replaces the snapshot. New devices are queued; removed,
decommissioned or unplaced devices lose future entries. IP changes cancel old
work; site changes select the new site's schedules. Identical snapshots preserve
timers. Failed auth/CMDB reads retain last known inventory. Before the first
successful read, no inventory is guessed. Resync runs independently of the due
queue with a 30 s overall deadline, five-second gRPC deadlines and bounded
exponential retry/spreading (1–60 s base).

DelayQueue stores one future entry per owned schedule/device pair; a map tracks
queue keys, generation and cancellation token. No sleeping task per device.
Unchanged heartbeats do not rebuild the inventory queue. Up to 32 due operations
run concurrently. Already acknowledged tasks cannot be revoked by a later CMDB
change; future queue entries and cancellable in-flight work are reconciled.

Slot is `floor(unix_milliseconds / interval_milliseconds)`. Occurrence identity is
schedule ID + canonical device UUID + canonical management IP + slot. UUID v5
with a fixed namespace is both PollTask.task_id and Nats-Msg-Id. Jitter is derived
from that UUID and never from process-local randomness. All replicas must share
the same schedule definitions and synchronized clocks. Retry spreading uses rand.
Deadline is due time + task_deadline, identical on all replicas. IPv6 target
formatting uses SocketAddr (`[2001:db8::1]:161`), attempt starts at 1.

Restart considers only the current eligible occurrence or the next one, never a
history backlog. Expired occurrences are skipped with bounded warning and a
cumulative structured-log counter. max_lateness is capped at 60 s to keep retry
eligibility inside the unchanged TASKS_SNMP deduplication window of at least 120 s.

## Membership and publication correctness

KV buckets default to SCHEDULER_MEMBERS / SCHEDULER_CLAIMS / SCHEDULER_FIRED.
They are created idempotently with existing Bus::kv; concurrent first creation is
handled locally. Existing bucket TTL/history/replica settings and TASKS_SNMP
WorkQueue/subject/dedup settings are validated; incompatible settings fail closed
and are not silently rewritten.

Membership uses an atomic initial create, a unique session value and revision-CAS
heartbeat. A duplicate live instance ID is not overwritten. TTL removes dead
members; graceful leave deletes only the caller's revision. Each member has 128
virtual nodes in hashring 0.3.6, using its fixed-seed SipHasher and explicit stable
byte hashing. Member order does not affect ownership. Only owned pairs are queued;
the current ownership view is checked again when a due operation starts.

Ownership reduces contention; correctness also uses the following protocol:

1. Check lateness/deadline and fired marker.
2. Atomic KV create claim for the deterministic occurrence UUID.
3. Recheck fired after acquisition to close the read/acquire race.
4. Publish the existing PollTask via Bus::publish_proto and await JetStream ACK.
5. Create fired marker only after ACK; release claim with its revision best-effort.
6. Complete the occurrence or retry within lateness/deadline.

Post-acquisition work is bounded to eight seconds; publish waits at most four.
The claim lasts at least twelve seconds; stale cleanup cannot delete a newer
revision. Crash before publish allows retry after lease expiry. Crash after
publish but before fired permits a retry of the **same** message ID; JetStream
deduplicates it in the existing window. Crash after fired does not republish.
Fired TTL is `max(2 * max_configured_interval, 1 hour)`, up to 48 h for the
validated 24-hour interval cap. Existing fired TTL may be longer, but at most
seven days. Marker expiry cannot normally occur while that slot is eligible.

NATS remains a required dependency. Existing Bus reconnect semantics are reused;
unacknowledged publication never marks fired. Retries retain their deterministic
occurrence identity; after max lateness they are visibly expired. This is bounded
deduplication, not an unbounded exactly-once transaction across KV and a stream:
arbitrarily delayed client buffers/partitions beyond the dedup window or severe
clock skew require operational recovery. Do not erase coordination buckets while
replicas are live. Config changes that need longer fired TTL require provisioning
a compatible bucket before restart. No poller replica assignment is introduced.

SIGINT/SIGTERM stop new due work, cancel background resync/in-flight futures,
stop heartbeats and best-effort remove the member revision with a two-second
shutdown bound for that operation. No infinite wait for claims.

## Docker and Compose

```sh
docker build -f services/control_panel/services/scheduler_service/Dockerfile -t cronet-scheduler .
```

Multi-stage build uses root context, a strict source allowlist and Rust 1.89.
Runtime is Debian slim + CA certificates + the release binary, UID/GID 10002.
Tests, fixtures, reports, logs, keys, .env, .git and target are excluded. The final
image never receives the source tree or test Compose. The small CMDB Dockerignore
addition only makes the new root workspace member available to its existing build.

Compose adds only the opt-in `scheduler` profile. Existing `docker compose up`
keeps its previous services/configuration. The new service waits for healthy NATS;
CMDB may come up later. Put schedules and the rotated JWT in an external runtime
directory, set SCHEDULER_RUNTIME_DIR to that host directory and use container paths:
SCHEDULER_CONFIG_PATH=/run/scheduler/schedules.json and
CMDB_BEARER_TOKEN_FILE=/run/scheduler/cmdb-token. The directory is mounted read-only.
The default local services/control_panel/scheduler_runtime is gitignored.
Configure SCHEDULER_NATS_URL or SCHEDULER_NATS_CREDS_FILE for the existing NATS auth.
No secret value is tracked. Enable the cmdb profile as well when using the local
CMDB endpoint, or point SCHEDULER_CMDB_GRPC_URL to an already running CMDB.

## Verification

Unit/static checks require no Docker or network:

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --locked -p scheduler-service --test unit
```

Real network suites are opt-in via SCHEDULER_E2E=1. Start a fresh isolated project:

```sh
export COMPOSE_PROJECT_NAME=cronet-scheduler-test
export COMPOSE_FILE=services/control_panel/services/scheduler_service/tests/compose.scheduler-test.yml
docker compose up -d --build --wait snmpsim
docker compose exec -T rust cargo build --locked -p cmdb-service -p snmp-poller
docker compose exec -T rust cargo test --locked -p scheduler-service --test integration --test acceptance -- --test-threads=1
docker compose down --volumes
```

The dedicated CI job runs these commands. Synthetic auth-user setup is confined
to the disposable Python fixture; Scheduler has no SQLx dependency, including dev.
All canonical test inventory is created/changed through CMDB HTTP. RSA test keys
live only in temporary test directories. No host ports or privileged containers.
Tests check actual stored JetStream messages under conflicting ownership views,
claim expiry, publish-before-marker crash deduplication, real two-process Scheduler
failover after killing the owner and waiting for member TTL, full
CMDB/DelayQueue/PollTask mapping, negative inventory cases, site moves, CMDB outage
retention, SIGTERM leave and the unchanged poller's real SNMP RawSnmpResult.
Normal cargo test entrypoints return without network when the opt-in env is absent.
