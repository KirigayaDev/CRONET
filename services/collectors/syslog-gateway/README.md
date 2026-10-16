# syslog-gateway (B1)

Receives plaintext UDP/TCP syslog and delivers raw protobuf transport envelopes
to JetStream. No vendor parsing, RFC3164/RFC5424 timestamp interpretation,
ClickHouse insert, CLI, NETCONF or voice processing happens here.
TLS and RELP belong to B2. Deploy B1 on trusted networks; use a TLS-capable ingress
for untrusted networks until B2 is available.

UDP: one datagram per event; non-UTF8 and empty payloads are valid. A fixed 64 KiB
receive buffer captures the entire IP datagram. Payload above the configured
limit is truncated and marked `truncated=true`.

TCP: LF-delimited messages (only LF is removed; CR is preserved), plus RFC6587
`<decimal length><SP><payload>` octet counting, including embedded LF and binary
bytes. Both forms can be received on a connection. A leading decimal token plus
SP is interpreted as octet counting; send digit-prefixed ambiguous messages with
an explicit octet prefix. Zero/leading-zero lengths, oversized frames and partial
frames at EOF close the connection. Oversized TCP frames are rejected, never
truncated. The idle timeout is a whole-frame deadline, so byte trickling cannot
hold a connection forever. Limits use a Semaphore and a bounded ingress channel.

`RawSyslogEvent` includes UUID v4 `event_id`, gateway ID, validated site, gateway
receive timestamp, normalized source address/port, transport, original payload
bytes and truncation flag. Subject: `raw.syslog.<site>`. Stream: `RAW_SYSLOG`,
Limits retention, file storage, 72h max age, 128 MiB (`dev`) / 8 GiB (`prod`),
120s duplicate window. Existing stream policies are unchanged. Limits retention
can evict older messages at the age/size boundary; size capacity must be planned
for the expected log rate.

## Durability and pressure

Receive → bound/frame → bounded channel → redb **Immediate commit** → publish via
`libs/bus` with `Nats-Msg-Id=event_id` → JetStream ACK → redb Immediate deletion.
Disk operations run on blocking workers. The spool file is exclusively locked;
replicas require separate spool volumes. Each record contains a version, creation
time, original serialized event and CRC32 covering its metadata and protobuf.
CRC detects accidental corruption; it is not a security hash. Invalid records
move atomically to `quarantine_v1`, remain on disk and count against capacity.
Recover quarantined records offline; B1 provides no repair CLI.

Before local commit a received frame is **not durably accepted**. UDP is inherently
best effort and TCP syslog has no application-level delivery ACK. A successful
socket write does not prove a local commit. After local commit, restart replays
the same event and ID. After remote ACK but before deletion, replay can publish
again: JetStream deduplicates within its 120s window. Longer outages/restarts may
produce duplicates; downstream must handle stable IDs (at least once delivery).

`SYSLOG_SPOOL_MAX_BYTES` is a **physical file cap**, enforced by a capped redb
FileBackend. Admission reserves copy-on-write/growth headroom: accounted capacity
is `max_bytes / 4 - 1 MiB`; each record costs its encoded bytes plus 16 KiB of
conservative page overhead. Default 1 GiB yields 255 MiB of accounted capacity.
This intentionally leaves room for metadata, page growth and ACK deletions.
The file may retain allocated space after deletion; its hard cap still applies.
Storage failures stop the service and preserve already committed records.
Use a persistent volume with adequate free space; a cap cannot reserve space
against unrelated disk writers.

At capacity TCP stops reading while its current frame awaits commit. UDP uses
explicit drops before acceptance, with a cumulative counter and logarithmically
sampled warnings. Publication retries use capped exponential backoff with jitter
(125ms–30s), retaining entries until ACK. Replay alternates oldest and newest
batches (100 by default), without loading the spool into memory. Ordering across
connections/backlog is not guaranteed. Memory is bounded by the channel, one
pending frame per connection, codec read buffers, replay batch and 8 MiB DB cache.

SIGTERM/Ctrl-C stop ingress, drain the bounded queue to disk where capacity
permits, and stop the publisher. Committed entries can remain for next startup.
Uncommitted frames blocked by a full spool are explicitly reported at shutdown.
Logs contain metadata/status only, never payloads or NATS credentials; third-party
log targets are suppressed even with a broad `RUST_LOG`.

## Configuration

Unknown `SYSLOG_*` keys and malformed values fail fast. Other collectors' unrelated
environment keys are ignored. Socket addresses require numeric IPs.

| Variable | Default / requirement |
|---|---|
| `NETMON_SITE` | Required, 1–128 ASCII letters/digits/`-`/`_` |
| `SYSLOG_GATEWAY_ID` | hostname + process ID; 1–256 characters |
| `SYSLOG_UDP_ADDR`, `SYSLOG_TCP_ADDR` | `0.0.0.0:5514` |
| `SYSLOG_MAX_FRAME_BYTES` | `65536`, range 1–65536 |
| `SYSLOG_MAX_CONNECTIONS` | `1024`, range 1–65536 |
| `SYSLOG_IDLE_SECS` | `120`, range 1–86400 |
| `SYSLOG_CHANNEL_CAPACITY` | `1024`, range 1–65536 |
| `SYSLOG_UDP_RCVBUF_BYTES` | `4194304`, range 65536–67108864; OS may clamp |
| `SYSLOG_SPOOL_PATH` | `var/syslog/spool.redb` (file) |
| `SYSLOG_SPOOL_MAX_BYTES` | `1073741824`, physical cap, 16 MiB–1 TiB |
| `SYSLOG_REPLAY_BATCH` | `100`, range 1–1000 |
| `SYSLOG_SHUTDOWN_SECS` | `30`, range 1–300 |
| `NATS_URL` | `nats://127.0.0.1:4222`; `nats`/`tls` URL |
| `NATS_CREDS_FILE` | Optional readable credentials file |
| `NETMON_BOOTSTRAP_STREAMS` | `true`; use `false` with externally provisioned streams |
| `NETMON_STREAM_PROFILE` | `dev` or `prod` |
| `NETMON_STREAM_REPLICAS` | `1`, range 1–5 |
| `RUST_LOG` | `syslog_gateway=info`; use `syslog_gateway=debug` for per-event status |

## Docker and testing

From repository root:

```sh
docker build -f services/collectors/syslog-gateway/Dockerfile -t cronet/syslog-gateway .
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges \
  -e NETMON_SITE=site-a -e NATS_URL=nats://nats:4222 \
  -p 5514:5514/udp -p 5514:5514/tcp \
  -v syslog-spool:/var/lib/cronet/syslog cronet/syslog-gateway
```

Runtime is UID/GID 10001; spool directory mode 0700, new DB mode 0600. Bind mounts
must be owned by 10001. Production **must** retain the spool volume across restarts.
The multistage image copies only the binary into the runtime base with CA/libs;
no project source, tests, fixtures, test Compose, keys, `.env`, logs or reports.
Port 5514 works without privileges; a production host can map 514 if required.
The control-panel Compose service is opt-in: `--profile syslog`; it exposes only
5514 and has a named spool volume. Configure `NATS_URL` for the chosen network.

```sh
cargo test -p syslog-gateway --locked
# nats-server on PATH (or NATS_SERVER=/absolute/path/to/nats-server):
cargo test -p syslog-gateway --test e2e --locked -- --ignored --test-threads=1
```

Integration tests own their NATS processes, random localhost ports and temporary
spools. They cover UDP/binary/truncation, multiple LF and octet-counted TCP
messages, TCP connection/idle/frame limits, NATS outage plus SIGKILL/restart replay,
stable IDs, ACK-before-delete deduplication and graceful SIGTERM shutdown on Unix.
