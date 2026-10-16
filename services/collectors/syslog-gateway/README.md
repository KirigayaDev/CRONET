# syslog-gateway (B1/B2)

Receives plaintext UDP/TCP syslog and delivers raw protobuf transport envelopes
to JetStream. No vendor parsing, RFC3164/RFC5424 timestamp interpretation,
ClickHouse insert, CLI, NETCONF or voice processing happens here.
Plain UDP/TCP listeners retain their B1 behavior. Optional B2 TLS adds server
authentication or mandatory client authentication (mTLS) on a separate listener.
Optional RELP adds application acknowledgements after durable local commit,
over either plain TCP or TLS.
Use TLS on untrusted networks and restrict access to plaintext endpoints.

## TLS (B2)

TLS is disabled unless `SYSLOG_TLS_ADDR` is set (typically `0.0.0.0:6514`).
Then `SYSLOG_TLS_CERT_FILE` and `SYSLOG_TLS_KEY_FILE` are required PEM file paths.
`SYSLOG_TLS_CLIENT_CA_FILE` enables mandatory mTLS: missing or untrusted client
identities fail the handshake. Without a client CA, only the server authenticates.
Cert/key/CA settings without an enabled TLS or RELP-TLS listener are configuration errors.
Bad paths, PEM, mismatched keys and expired/not-yet-valid certificates fail startup.

TLS 1.2/1.3 use rustls with the ring provider. Certificate parsing and current
validity checks use x509-parser; rustls checks the key/certificate match and verifies
mTLS chains. PEM files are bounded to 1 MiB each, and temporary key buffers are
zeroized. `rcgen` is a dev dependency only; tests generate all identities at runtime.

`SYSLOG_TLS_RELOAD_SECS` defaults to 30 (1–86400). Bounded polling reads content,
validates a full replacement and atomically swaps its `Arc<ServerConfig>` through
ArcSwap when content changes. Invalid replacements retain the previous config.
New connections get the new config; established sessions keep running. Rotate
the pair together; an intermediate mismatched pair is rejected and retried on the
next poll. Mount the secret directory so file replacement is visible to polling.
Existing sessions retain their authentication state until closed.

TLS shares B1's `SyslogCodec`, frame limits, idle deadline, durable Intake, redb
spool and publisher. Envelope transport is additive `TLS=3`; `UDP=1`/`TCP=2` and
all existing protobuf fields stay unchanged. `SYSLOG_MAX_CONNECTIONS` bounds each
listener, including handshakes. `SYSLOG_TLS_HANDSHAKE_SECS` defaults to 10 (1–60).
The plaintext listeners are still enabled by their existing configuration.

Mount certificate/key files read-only at runtime, readable by UID 10001; never
put key contents in environment variables or images. Example additions to the
Docker command below:

```sh
--mount type=bind,src=/srv/syslog-tls,dst=/run/syslog-tls,readonly \
-e SYSLOG_TLS_ADDR=0.0.0.0:6514 -p 6514:6514/tcp \
-e SYSLOG_TLS_CERT_FILE=/run/syslog-tls/server.pem \
-e SYSLOG_TLS_KEY_FILE=/run/syslog-tls/server.key
# Optional: -e SYSLOG_TLS_CLIENT_CA_FILE=/run/syslog-tls/client-ca.pem
```

The existing Compose `syslog` profile requires no certificate mounts and remains
compatible. Add TLS environment, read-only secret mounts and a 6514 port mapping
in a local Compose override when TLS is needed. The production image contains no
generated certificates or private keys.

## RELP (B2)

`SYSLOG_RELP_ADDR` enables plain RELP (suggested `0.0.0.0:20514`).
`SYSLOG_RELP_TLS_ADDR` enables RELP over TLS (suggested `0.0.0.0:20515`), using
the same certificate, mTLS, handshake timeout and hot reload settings as syslog
TLS. Either TLS listener can be enabled independently. Both RELP listeners are
disabled by default. Exposed Docker ports do not enable listeners or publish host
ports; add mappings explicitly or in a local Compose override.

The incremental `nom` parser implements RELP wire frames
`TXNR SP COMMAND SP DATALEN [SP DATA] LF`, bounded decimal headers and binary
data with no allocation based on an unchecked declared length. It supports
fragmented input, pipelined frames and exact byte payloads. The frame limit is
`SYSLOG_MAX_FRAME_BYTES`, including `open` offer data. A session processes one
transaction at a time and sends responses in request order. Existing connection
limits, frame deadline and bounded ingress also apply to RELP.

Supported commands are `open`, `syslog`, `close`. `open` must be transaction 1
and offer `relp_version` (0 or 1) plus `commands=syslog`; the response negotiates
those capabilities. Subsequent transaction numbers increment by one, wrapping
999999999 → 1. Unknown commands, bad sequence/state, malformed frames and
unsupported offers close the session (with a bounded 500 response where framing
is valid). `syslog` before `open` is rejected. `close` returns a response and
shuts the session down orderly. STARTTLS, compression and hint commands are not
implemented; RELP-TLS starts with a TLS handshake on its own endpoint.

For every `syslog` transaction:

```
RELP frame → original payload envelope → Intake → redb Immediate commit
           → RELP rsp "200 OK"
           → asynchronous JetStream publish/ACK → local deletion
```

The response confirms durable **local** acceptance; NATS need not be online.
The publisher can proceed concurrently after commit, so it may finish before the
client receives the RELP response. A full spool withholds success and backpressures
the connection. A failed commit closes the session without success. Retries after
a lost RELP response may create a new event: txnr is connection-local and never
used as a global event ID. Locally committed events retain their UUID across
restart/replay; JetStream deduplication still uses the B1 120s window.

Transport numbers are additive: `TLS=3`, `RELP=4`, `RELP_TLS=5`. Raw payloads,
timestamps, event IDs, subjects and stream policies retain B1 semantics.
Structured debug logs include command, txnr, source, transport and event ID;
payload, private key and certificate bodies are never logged.

Framing/offer behavior follows the [librelp protocol specification](https://sources.debian.org/data/main/libr/librelp/1.2.12-1~bpo8%2B1/doc/relp.html)
and [librelp session implementation](https://github.com/rsyslog/librelp/blob/master/src/relpsess.c).

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
They also generate test certificates through rcgen for TLS/mTLS, failed and valid
rotation, established-session continuity and slow handshakes; exercise RELP
open/syslog/close, fragmented/pipelined requests, malformed frames, local durable
ACK during NATS outage, full-spool backpressure and crash/replay; and verify RELP
over mTLS plus TLS outage persistence. No PEM fixtures are committed.
