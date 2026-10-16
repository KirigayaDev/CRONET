#![allow(clippy::unwrap_used, clippy::expect_used)]
mod support;
use async_nats::jetstream::{
    consumer::{pull, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy, StorageType},
};
use bus::{Bus, BusConfig, StreamLimits};
use cli_collector::worker::Worker;
use contracts::{
    netmon::v1::{raw_cli_result::Status, RawCliResult},
    subjects,
};
use futures::StreamExt;
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use support::*;

struct Nats {
    bus: Bus,
    url: String,
    _child: tokio::process::Child,
    _dir: tempfile::TempDir,
}
async fn nats() -> Nats {
    let dir = tempfile::tempdir().unwrap();
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reserve.local_addr().unwrap().port();
    drop(reserve);
    let config = dir.path().join("nats.conf");
    std::fs::write(
        &config,
        format!(
            "host: 127.0.0.1\nport: {port}\nmax_payload: 32MB\njetstream {{ store_dir: {:?} }}\n",
            dir.path().join("store")
        ),
    )
    .unwrap();
    let child = tokio::process::Command::new(std::env::var("NATS_SERVER").unwrap_or_else(|_| "nats-server".into()))
        .arg("-c")
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("NATS_SERVER must point to a real nats-server");
    let url = format!("nats://127.0.0.1:{port}");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if async_nats::connect(&url).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let bus = Bus::connect(&BusConfig { url: url.clone(), creds_file: None, name: "cli-e2e".into() }).await.unwrap();
    Nats { bus, url, _child: child, _dir: dir }
}
struct Collector {
    child: tokio::process::Child,
    log: PathBuf,
}
async fn collector(n: &Nats, ssh: &SshServer, name: &str, grace: u64) -> Collector {
    let hosts = n._dir.path().join("known_hosts");
    std::fs::write(&hosts, &ssh.known_hosts).unwrap();
    let log = n._dir.path().join(format!("{name}.log"));
    let file = std::fs::File::create(&log).unwrap();
    let executable =
        std::env::var("CLI_TEST_COLLECTOR_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_cli-collector").into());
    let child = tokio::process::Command::new(executable)
        .env("NETMON_SITE", "b3")
        .env("NETMON_COLLECTOR_ID", name)
        .env("NATS_URL", &n.url)
        .env("NETMON_CRED_BACKEND", "env")
        .env("NETMON_CRED_TEST", format!(r#"{{"version":"basic","username":"{USER}","password":"{PASSWORD}"}}"#))
        .env("CLI_KNOWN_HOSTS_FILE", hosts)
        .env("CLI_ALLOWED_COMMAND_REGEX", ALLOW)
        .env("CLI_PROMPT_REGEX", "(?m)router# *$")
        .env("CLI_TASK_TIMEOUT_MS", "5000")
        .env("CLI_COMMAND_TIMEOUT_MS", "2000")
        .env("CLI_MAX_CONCURRENCY", "1")
        .env("CLI_SHUTDOWN_GRACE_MS", grace.to_string())
        .env("CLI_MAX_COMMAND_BYTES", "16384")
        .env("CLI_MAX_TOTAL_BYTES", "32768")
        .env("RUST_LOG", "trace") // Broad dependency logs must still stay disabled.
        .stdout(file.try_clone().unwrap())
        .stderr(file)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut c = Collector { child, log };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let logs = std::fs::read_to_string(&c.log).unwrap();
            assert!(c.child.try_wait().unwrap().is_none(), "collector exited: {logs}");
            if logs.contains("CLI worker ready") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    c
}
async fn pending(bus: &Bus) -> usize {
    bus.js
        .get_stream(subjects::TASKS_CLI_STREAM)
        .await
        .unwrap()
        .get_consumer::<pull::Config>("cli-collector-b3")
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .num_ack_pending
}
async fn wait_pending(bus: &Bus, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if pending(bus).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn stop(c: &mut Collector, signal: &str) {
    #[cfg(unix)]
    {
        let status = tokio::process::Command::new("sh")
            .args(["-c", "kill \"$1\" \"$2\"", "cli-test", signal])
            .arg(c.child.id().unwrap().to_string())
            .status()
            .await
            .unwrap();
        assert!(status.success());
    }
    #[cfg(not(unix))]
    {
        let _ = signal;
        c.child.start_kill().unwrap();
    }
    let status = tokio::time::timeout(Duration::from_secs(4), c.child.wait()).await.unwrap().unwrap();
    #[cfg(unix)]
    assert!(status.success(), "{}", std::fs::read_to_string(&c.log).unwrap());
    #[cfg(not(unix))]
    let _ = status;
    let logs = std::fs::read_to_string(&c.log).unwrap();
    assert!(!logs.contains(PASSWORD));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_two_replicas_real_ssh_jetstream_and_dedup() {
    if std::env::var("CLI_E2E").is_err() {
        return;
    }
    let n = nats().await;
    n.bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    let ssh = server().await;
    let mut a = collector(&n, &ssh, "replica-a", 2000).await;
    let mut b = collector(&n, &ssh, "replica-b", 2000).await;
    let stream = n.bus.js.get_stream(subjects::RAW_CLI_STREAM).await.unwrap();
    let consumer = stream
        .create_consumer(pull::OrderedConfig {
            filter_subject: subjects::cli_result("b3"),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut results = consumer.messages().await.unwrap();
    for i in 0..8 {
        let mut t = task(&ssh);
        t.task_id = format!("replica-task-{i}");
        t.attempt = 2;
        n.bus.publish_proto(subjects::cli_task("b3"), &format!("input-{i}"), &t).await.unwrap();
    }
    let mut ids = HashSet::new();
    let mut replicas = HashMap::new();
    for _ in 0..8 {
        let msg = tokio::time::timeout(Duration::from_secs(10), results.next()).await.unwrap().unwrap().unwrap();
        let r = RawCliResult::decode(msg.payload.as_ref()).unwrap();
        assert_eq!(r.status, Status::Ok as i32, "{r:?}");
        assert_eq!(r.outputs.len(), 2);
        assert_eq!(r.attempt, 2);
        assert!(ids.insert(r.task_id.clone()));
        *replicas.entry(r.collector_id).or_insert(0) += 1;
        assert_eq!(
            msg.headers.as_ref().unwrap().get(async_nats::header::NATS_MESSAGE_ID).unwrap().as_str(),
            format!("{}:2", r.task_id)
        );
    }
    wait_pending(&n.bus, 0).await;
    assert_eq!(replicas.len(), 2);
    assert_eq!(ssh.stats.shells.load(Ordering::SeqCst), 8);
    assert_eq!(ssh.stats.commands.load(Ordering::SeqCst), 16);
    assert!(tokio::time::timeout(Duration::from_millis(200), results.next()).await.is_err());
    // Explicit input redelivery produces one persisted result within dedup window.
    let mut t = task(&ssh);
    t.task_id = "replica-task-0".into();
    t.attempt = 2;
    n.bus.publish_proto(subjects::cli_task("b3"), "repeated-input", &t).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ssh.stats.shells.load(Ordering::SeqCst) == 9 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    wait_pending(&n.bus, 0).await;
    assert_eq!(stream.get_info().await.unwrap().state.messages, 8);
    stop(&mut a, "-TERM").await;
    stop(&mut b, "-INT").await;
    assert_eq!(ssh.stats.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_ack_barrier_and_publish_failure_redelivery() {
    if std::env::var("CLI_E2E").is_err() {
        return;
    }
    let n = nats().await;
    // Real JetStream input; controlled result ACK responder makes the barrier observable.
    n.bus
        .js
        .create_stream(StreamConfig {
            name: subjects::TASKS_CLI_STREAM.into(),
            subjects: vec![subjects::TASKS_CLI_FILTER.into()],
            retention: RetentionPolicy::WorkQueue,
            storage: StorageType::File,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut published = n.bus.client.subscribe(subjects::cli_result("b3")).await.unwrap();
    n.bus.client.flush().await.unwrap();
    let ssh = server().await;
    let mut c = collector(&n, &ssh, "barrier", 1000).await;
    n.bus.publish_proto(subjects::cli_task("b3"), "barrier-input", &task(&ssh)).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), published.next()).await.unwrap().unwrap();
    assert_eq!(RawCliResult::decode(result.payload.as_ref()).unwrap().outputs.len(), 2);
    assert_eq!(pending(&n.bus).await, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pending(&n.bus).await, 1);
    n.bus.client.publish(result.reply.unwrap(), r#"{"stream":"RAW_CLI","seq":1}"#.into()).await.unwrap();
    wait_pending(&n.bus, 0).await;
    // Missing result ACK -> publish timeout -> NAK -> another delivery, no input ACK.
    let mut failed = task(&ssh);
    failed.task_id = "failed-publish".into();
    n.bus.publish_proto(subjects::cli_task("b3"), "failed-input", &failed).await.unwrap();
    let _first = tokio::time::timeout(Duration::from_secs(5), published.next()).await.unwrap().unwrap();
    let _redelivery = tokio::time::timeout(Duration::from_secs(8), published.next()).await.unwrap().unwrap();
    let info = n
        .bus
        .js
        .get_stream(subjects::TASKS_CLI_STREAM)
        .await
        .unwrap()
        .get_consumer::<pull::Config>("cli-collector-b3")
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .clone();
    assert_eq!(info.ack_floor.consumer_sequence, 1);
    assert_eq!(info.num_ack_pending, 1);
    assert!(info.num_redelivered >= 1);
    stop(&mut c, "-TERM").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_aborts_active_shell_leaves_input_unacked_and_redelivers() {
    if std::env::var("CLI_E2E").is_err() {
        return;
    }
    let n = nats().await;
    n.bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    let ssh = server().await;
    let mut c = collector(&n, &ssh, "shutdown", 30).await;
    let mut t = task(&ssh);
    t.commands = vec!["show slow".into()];
    n.bus.publish_proto(subjects::cli_task("b3"), "shutdown-input", &t).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while ssh.stats.commands.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop(&mut c, "-TERM").await;
    assert_eq!(pending(&n.bus).await, 1);
    assert_eq!(
        n.bus.js.get_stream(subjects::RAW_CLI_STREAM).await.unwrap().get_info().await.unwrap().state.messages,
        0
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ssh.stats.active.load(Ordering::SeqCst), 0);
    // Force immediate redelivery instead of waiting the production ack_wait.
    let consumer = n
        .bus
        .js
        .get_stream(subjects::TASKS_CLI_STREAM)
        .await
        .unwrap()
        .get_consumer::<pull::Config>("cli-collector-b3")
        .await
        .unwrap();
    let mut cfg = consumer.get_info().await.unwrap().config.clone();
    cfg.ack_wait = Duration::from_millis(100);
    n.bus.js.get_stream(subjects::TASKS_CLI_STREAM).await.unwrap().update_consumer(cfg).await.unwrap();
    let mut retry = collector(&n, &ssh, "retry", 2000).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if n.bus.js.get_stream(subjects::RAW_CLI_STREAM).await.unwrap().get_info().await.unwrap().state.messages
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    wait_pending(&n.bus, 0).await;
    // Graceful path: active command finishes within the grace period and is ACKed.
    t.task_id = "finish-during-grace".into();
    let before = ssh.stats.commands.load(Ordering::SeqCst);
    n.bus.publish_proto(subjects::cli_task("b3"), "grace-input", &t).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while ssh.stats.commands.load(Ordering::SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop(&mut retry, "-TERM").await;
    wait_pending(&n.bus, 0).await;
    assert_eq!(
        n.bus.js.get_stream(subjects::RAW_CLI_STREAM).await.unwrap().get_info().await.unwrap().state.messages,
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_errors_produce_results_and_malformed_envelopes_terminate() {
    if std::env::var("CLI_E2E").is_err() {
        return;
    }
    let n = nats().await;
    n.bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    let ssh = server().await;
    let mut c = collector(&n, &ssh, "bad-tasks", 1000).await;
    let stream = n.bus.js.get_stream(subjects::RAW_CLI_STREAM).await.unwrap();
    let consumer = stream
        .create_consumer(pull::OrderedConfig { filter_subject: subjects::cli_result("b3"), ..Default::default() })
        .await
        .unwrap();
    let mut results = consumer.messages().await.unwrap();
    let mut blocked = task(&ssh);
    blocked.task_id = "blocked".into();
    blocked.commands = vec!["reload".into()];
    let mut bad_id = task(&ssh);
    bad_id.task_id = "bad\r\nheader".into();
    let mut expired = task(&ssh);
    expired.task_id = "expired".into();
    expired.deadline.as_mut().unwrap().seconds -= 100;
    for (i, t) in [blocked, bad_id, expired].into_iter().enumerate() {
        n.bus.publish_proto(subjects::cli_task("b3"), &format!("bad-input-{i}"), &t).await.unwrap();
        let m = tokio::time::timeout(Duration::from_secs(5), results.next()).await.unwrap().unwrap().unwrap();
        let r = RawCliResult::decode(m.payload.as_ref()).unwrap();
        assert_eq!(r.status, if i == 2 { Status::Expired } else { Status::BadTask } as i32);
        let id = m.headers.as_ref().unwrap().get(async_nats::header::NATS_MESSAGE_ID).unwrap().as_str();
        assert!(!id.contains(['\r', '\n']));
        if i == 1 {
            assert_eq!(id, "invalid-cli:2:0");
        }
    }
    for payload in [vec![255], vec![0; cli_collector::worker::MAX_TASK_BYTES + 1]] {
        n.bus.js.publish(subjects::cli_task("b3"), payload.into()).await.unwrap().await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if n.bus.js.get_stream(subjects::TASKS_CLI_STREAM).await.unwrap().get_info().await.unwrap().state.messages
                == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(ssh.stats.connections.load(Ordering::SeqCst), 0);
    assert_eq!(stream.get_info().await.unwrap().state.messages, 3);
    stop(&mut c, "-TERM").await;
}

#[tokio::test]
async fn worker_rejects_result_budget_above_server_payload() {
    if std::env::var("CLI_E2E").is_err() {
        return;
    }
    let n = nats().await;
    let ssh = server().await;
    let mut p = policy(&ssh);
    p.max_total_bytes = 32 * 1024 * 1024;
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let r = Worker {
        bus: n.bus,
        site: "b3".into(),
        policy: Arc::new(p),
        creds: Arc::new(store()),
        concurrency: 1,
        shutdown_grace: Duration::from_secs(1),
    }
    .run(rx)
    .await;
    assert!(r.is_err());
}
