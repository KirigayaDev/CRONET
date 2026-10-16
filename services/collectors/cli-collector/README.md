# cli-collector (Issue #2 / B3)

`CliTask → TASKS_CLI → SSH interactive shell → RawCliResult → RAW_CLI`.
No metric parsing, database writes, scheduler, topology or NETCONF.

## Deployment

Rust 1.89+ is required by russh 0.64.1; the production build uses Rust 1.99.
Build from repository root:

```sh
docker build -f services/collectors/cli-collector/Dockerfile -t cli-collector .
```

Required settings:

| Setting | Meaning |
| --- | --- |
| `NETMON_SITE` | One NATS subject token: ASCII letters, digits, `_`, `-`; at most 64 bytes |
| `CLI_KNOWN_HOSTS_FILE` | Readable mounted OpenSSH public host keys; mandatory, no automatic trust |
| `CLI_ALLOWED_COMMAND_REGEX` | Explicit device-specific diagnostic allow policy; full command must match |
| `VAULT_ADDR` + AppRole or token | Existing CredStore backend, default `vault` |

An example allow policy is `^(show|display|get) [a-z0-9 .-]+$`.
Review the actual device's commands before configuring it. There is no universal
read-only meaning for `diagnose`, `get`, or even `show` across vendors. The
collector therefore requires an explicit production policy. Controls, leading or
trailing whitespace and `; | & < > ` plus backtick, dollar and backslash are always
rejected, even if the configured regex would permit them. No local shell executes
task commands. At most 32 commands, 512 bytes each; patterns at most 512 bytes
and 256 KiB compiled size. Prompt regex cannot match empty text.

`NETMON_CRED_BACKEND=env` is for development only: `env:test` resolves
`NETMON_CRED_TEST` containing Basic credential JSON via existing `EnvCredStore`.
Production uses `vault:...` with `VAULT_ROLE_ID` / `VAULT_SECRET_ID`, optional
`VAULT_APPROLE_MOUNT`, or `VAULT_TOKEN`; `VAULT_NAMESPACE` and `VAULT_CACERT`
are supported. Credentials are cached for 60 seconds (capacity 10,000).
Only Basic SSH password authentication is supported. No secrets in task contracts
or diagnostic errors. Logs use a fixed collector-only INFO filter; dependency wire
logging cannot be enabled through `RUST_LOG`.

Known hosts accepts bare literal IPv4/IPv6 (port 22), `[IP]:port`, comma-separated
literal hosts and comments. Multiple keys per address allow planned rotation.
Only key material is compared (comments ignored). Unknown/changed keys fail
before authentication. DNS, wildcards, hashed entries, markers (including revoked
or CA entries) and certificates are intentionally unsupported and rejected; use a
dedicated literal-IP file. Updating that mounted file requires collector restart.
Private keys and production known_hosts are never part of the image or repository.

| Optional setting | Default / bounds |
| --- | --- |
| `NATS_URL`, `NATS_CREDS` | `nats://127.0.0.1:4222`, optional mounted NATS credentials |
| `NETMON_COLLECTOR_ID` | HOSTNAME or process ID; use a distinct ID per replica |
| `NETMON_BOOTSTRAP_STREAMS` | `false`; infrastructure normally creates streams |
| `NETMON_STREAM_PROFILE`, `NETMON_STREAM_REPLICAS` | `prod` (`dev` for local tests), 1 |
| `CLI_MAX_CONCURRENCY` | 16; 1–256 |
| `CLI_CONNECT_TIMEOUT_MS`, `CLI_AUTH_TIMEOUT_MS` | 5000 each; 10–60000 |
| `CLI_COMMAND_TIMEOUT_MS` | 10000; 10–120000 |
| `CLI_TASK_TIMEOUT_MS` | 60000; 10–600000; also bounded by task deadline |
| `CLI_MAX_COMMAND_BYTES`, `CLI_MAX_TOTAL_BYTES` | 1 MiB / 4 MiB, independently reducible to 1 byte |
| `CLI_SHUTDOWN_GRACE_MS` | 10000; 10–600000 |
| `CLI_PROMPT_REGEX` | `(?m)[#>][ \t]*$`; use a specific hostname/prompt where possible |

Configure NATS `max_payload: 32MB` for default output budgets. Collector startup
checks that `max_payload ≥ 5 × CLI_MAX_TOTAL_BYTES + 65536` to fit raw bytes,
worst-case decoded Unicode and protobuf overhead. With default NATS 1 MiB,
reduce `CLI_MAX_TOTAL_BYTES` to e.g. 131072. A reconnecting NATS server must keep
the same payload limit. Retention: TASKS_CLI WorkQueue/File, 1 hour, dev 32 MiB /
prod 512 MiB; RAW_CLI Limits/File, 24 hours, dev 128 MiB / prod 4096 MiB.

## Shell and delivery behavior

One TCP/SSH connection and PTY shell per task, all commands run sequentially.
No connection reuse across tasks: privilege, pagination, current CLI mode and
pending output are stateful. A deadpool keyed by target and credential reference
would still need vendor-specific reset/health logic and credential rotation
handling; a bounded worker is sufficient without an artificial pool dependency.

Task target is a literal `IP:port` (IPv6 `[IP]:port`), never DNS. IDs, credential
reference, required UTC deadline, timeouts, commands, encoding and regex are
validated before opening SSH. Initial prompt, PTY/shell acceptance, command reads,
authentication and connection each have timers; a whole-task timer includes
credential lookup. Errors produce typed results with fixed sanitized details.

Raw bytes include device echo and final prompt unchanged up to the byte budget.
The initial greeting/prompt counts against task bytes but is not a command result.
Commands are UTF-8 text followed by LF. Normalized output is decoded incrementally with encoding_rs (`utf-8` default or
`windows-1251`), then processed by vte with a fixed 1024-byte OSC buffer. Invalid
encoded bytes become replacement characters only in normalized text. ANSI styling,
OSC and other control sequences are suppressed; CR, LF, backspace and current-line
erase/cursor controls are normalized. Decoder/parser state survives command boundaries.
This is a transcript, not a full-screen
terminal emulator. Raw bytes remain the source of truth.

Prompt detection checks the final 4096 decoded characters and requires the regex
match to end at the buffer end; no fixed sleep determines command completion.
As with all prompt-driven CLIs, output resembling the configured prompt can end a
command early. Configure an unambiguous device prompt. Pagination is not handled
automatically: provision non-paginated device sessions. Timeout/closed shell keeps
partial command bytes. Byte overflow marks output/result truncated, closes the
connection and returns COMMAND_ERROR without executing further commands.

Durable `cli-collector-<site>` is shared by replicas; each pull requests one task
only when capacity is free. All replicas must use the same task timeout and durable
settings. ACK wait = whole task limit + 30 seconds; max deliveries 3. Result publish
waits at most 5 seconds and uses `Nats-Msg-Id = task_id:attempt`; only confirmed
result publication permits the input double ACK. Invalid task IDs use a safe
`invalid-cli:<input-stream-sequence>:<attempt>` message ID so BAD_TASK results
cannot inject NATS headers. Failure NAKs input with 1-second
delay. Dedup window is 120 seconds. Execution is at least once: after an ambiguous
ACK/crash, diagnostics may run again; persisted results are deduplicated within the
window. A supervisor must monitor exhausted deliveries.

Undecodable protobuf and envelopes over 64 KiB are terminated with a fixed warning
because they cannot provide reliable task identity. Decodable policy violations
produce BAD_TASK. SIGINT/SIGTERM stops pulls, waits the configured grace period,
then aborts outstanding jobs without ACK. A socket shutdown guard closes russh's
background connection on cancellation, including whole-task timeout and shutdown.
Unfinished tasks stay in JetStream for redelivery.

## Verification

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
CLI_E2E=1 NATS_SERVER=/path/to/nats-server cargo test -p cli-collector --locked -- --test-threads=1
NETMON_E2E=1 cargo test --workspace --locked -- --test-threads=1
```

Normal SSH tests use a real russh server, ephemeral Ed25519 keys and loopback
ports. CLI_E2E launches isolated NATS processes and real collector binaries,
including two replicas, dedup, result ACK barrier, publish failure redelivery,
SIGINT/SIGTERM and cancellation. The ACK barrier uses real JetStream input and a
controlled result ACK responder to observe ordering; the pipeline test stores
both input and results in actual JetStream streams. CI has a separate B3 job;
existing Rust and SNMP E2E jobs remain unchanged. The same E2E harness also runs
against the release binary inside the non-root production runtime; test executables
and the NATS test server are mounted read-only only for that run, never added to
the image layers.
