#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Real processes and sockets. Run with a nats-server executable on PATH:
//! cargo test -p syslog-gateway --test e2e -- --ignored --test-threads=1

use bus::{Bus, BusConfig, StreamLimits};
use contracts::{
    netmon::v1::{RawSyslogEvent, SyslogTransport},
    subjects,
};
use prost::Message;
use serde_json::Value;
use std::{net::SocketAddr, path::Path, process::Stdio, time::Duration};
use syslog_gateway::{
    config::{Config, MIN_SPOOL_BYTES},
    model::envelope,
    spool::Spool,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket},
    process::{Child, Command},
    sync::mpsc,
};

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
fn url(port: u16) -> String {
    format!("nats://127.0.0.1:{port}")
}

struct Nats {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}
impl Nats {
    async fn start(port: u16) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let child = Command::new(std::env::var("NATS_SERVER").unwrap_or_else(|_| "nats-server".into()))
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start nats-server");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("NATS readiness");
        Self { child, port, _dir: dir }
    }
    async fn bus(&self) -> Bus {
        let bus = Bus::connect(&BusConfig { url: url(self.port), creds_file: None, name: "syslog-e2e".into() })
            .await
            .unwrap();
        bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
        bus
    }
    async fn kill(&mut self) {
        self.child.kill().await.unwrap();
    }
}

struct Gateway {
    child: Child,
    logs: mpsc::Receiver<Value>,
    udp: SocketAddr,
    tcp: SocketAddr,
}
impl Gateway {
    async fn start(path: &Path, nats_port: u16, frame_limit: usize) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_syslog-gateway"))
            .env_clear()
            .env("NATS_URL", url(nats_port))
            .env("NETMON_SITE", "syslog-e2e")
            .env("SYSLOG_GATEWAY_ID", "e2e-gateway")
            .env("NETMON_STREAM_PROFILE", "dev")
            .env("SYSLOG_UDP_ADDR", "127.0.0.1:0")
            .env("SYSLOG_TCP_ADDR", "127.0.0.1:0")
            .env("SYSLOG_MAX_FRAME_BYTES", frame_limit.to_string())
            .env("SYSLOG_MAX_CONNECTIONS", "1")
            .env("SYSLOG_IDLE_SECS", "1")
            .env("SYSLOG_CHANNEL_CAPACITY", "2")
            .env("SYSLOG_SPOOL_PATH", path)
            .env("SYSLOG_SPOOL_MAX_BYTES", MIN_SPOOL_BYTES.to_string())
            .env("RUST_LOG", "syslog_gateway=debug,async_nats::connector=trace")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, logs) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // Gateway logs are transport metadata; the test never prints payload.
                let log = serde_json::from_str::<Value>(&line).expect("structured gateway log");
                assert!(log["target"].as_str().is_none_or(|target| !target.starts_with("async_nats")));
                if tx.send(log["fields"].clone()).await.is_err() {
                    break;
                }
            }
        });
        let mut gateway =
            Self { child, logs, udp: "127.0.0.1:0".parse().unwrap(), tcp: "127.0.0.1:0".parse().unwrap() };
        let ready = gateway.wait("ready").await;
        gateway.udp = ready["udp_addr"].as_str().unwrap().parse().unwrap();
        gateway.tcp = ready["tcp_addr"].as_str().unwrap().parse().unwrap();
        gateway
    }
    async fn wait(&mut self, status: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let log = self.logs.recv().await.expect("gateway exited before expected log");
                if log["status"] == status {
                    return log;
                }
            }
        })
        .await
        .expect("gateway progress timeout")
    }
    async fn kill(&mut self) {
        self.child.kill().await.unwrap();
    }
    #[cfg(unix)]
    async fn terminate(&mut self) {
        let pid = self.child.id().unwrap().to_string();
        assert!(Command::new("kill").args(["-TERM", &pid]).status().await.unwrap().success());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await.unwrap().unwrap();
        assert!(status.success(), "graceful shutdown failed");
    }
}

async fn events(bus: &Bus, count: u64) -> Vec<RawSyslogEvent> {
    let mut stream = bus.js.get_stream(subjects::RAW_SYSLOG_STREAM).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if stream.info().await.unwrap().state.messages == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("JetStream message count");
    let mut events = Vec::new();
    for seq in 1..=count {
        let raw = stream.get_raw_message(seq).await.unwrap();
        let event = RawSyslogEvent::decode(raw.payload).unwrap();
        assert_eq!(raw.subject.as_str(), subjects::syslog("syslog-e2e"));
        assert_eq!(raw.headers.get(async_nats::header::NATS_MESSAGE_ID).unwrap().as_str(), event.event_id);
        events.push(event);
    }
    events
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn real_udp_and_tcp_preserve_binary_framing_and_truncation() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool.redb");
    let mut gateway = Gateway::start(&path, nats.port, 128).await;
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    udp.send_to(&[0xff, b'<', 0], gateway.udp).await.unwrap();
    gateway.wait("spooled").await;
    udp.send_to(&[b'x'; 129], gateway.udp).await.unwrap();
    gateway.wait("spooled").await;
    let mut tcp = TcpStream::connect(gateway.tcp).await.unwrap();
    let tcp_port = tcp.local_addr().unwrap().port();
    tcp.write_all(b"<1>one\n<2>two\r\n").await.unwrap();
    tcp.shutdown().await.unwrap();
    gateway.wait("spooled").await;
    gateway.wait("spooled").await;
    let mut octets = TcpStream::connect(gateway.tcp).await.unwrap();
    octets.write_all(b"4 a\n\xffb4 next").await.unwrap();
    octets.shutdown().await.unwrap();
    let events = events(&bus, 6).await;
    for event in &events {
        assert_eq!(event.source_ip, "127.0.0.1");
        assert_eq!(event.site, "syslog-e2e");
        assert_eq!(event.gateway_id, "e2e-gateway");
        assert!(event.received_at.is_some());
    }
    let binary = events.iter().find(|e| e.payload == [0xff, b'<', 0]).unwrap();
    assert_eq!(binary.transport, SyslogTransport::Udp as i32);
    assert_eq!(binary.source_port, u32::from(udp.local_addr().unwrap().port()));
    let truncated = events.iter().find(|e| e.truncated).unwrap();
    assert_eq!(truncated.payload, [b'x'; 128]);
    for payload in [b"<1>one".as_slice(), b"<2>two\r"] {
        let event = events.iter().find(|e| e.payload == payload).unwrap();
        assert_eq!(event.transport, SyslogTransport::Tcp as i32);
        assert_eq!(event.source_port, u32::from(tcp_port));
    }
    assert!(events.iter().any(|e| e.payload == b"a\n\xffb"));
    assert!(events.iter().any(|e| e.payload == b"next"));
    let ids: std::collections::HashSet<_> = events.iter().map(|e| &e.event_id).collect();
    assert_eq!(ids.len(), 6);
    #[cfg(unix)]
    gateway.terminate().await;
    #[cfg(not(unix))]
    gateway.kill().await;
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn outage_then_process_crash_and_restart_replays_stable_id() {
    let nats_port = port();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool.redb");
    let mut gateway = Gateway::start(&path, nats_port, 65536).await;
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    udp.send_to(b"durable\xff", gateway.udp).await.unwrap();
    let accepted = gateway.wait("spooled").await;
    let id = accepted["event_id"].as_str().unwrap().to_owned();
    gateway.kill().await;
    {
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        assert_eq!(spool.stats().unwrap().pending, 1);
        assert_eq!(spool.batch(1, false).unwrap()[0].event.event_id, id);
    }
    let mut restarted = Gateway::start(&path, nats_port, 65536).await;
    let mut nats = Nats::start(nats_port).await;
    let bus = nats.bus().await;
    let published = restarted.wait("published").await;
    assert_eq!(published["event_id"], id);
    let events = events(&bus, 1).await;
    assert_eq!(events[0].event_id, id);
    assert_eq!(events[0].payload, b"durable\xff");
    restarted.kill().await;
    assert_eq!(Spool::open(&path, MIN_SPOOL_BYTES).unwrap().stats().unwrap().pending, 0);
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn ack_without_local_delete_replays_with_jetstream_deduplication() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool.redb");
    let cfg = Config::from_map([("NETMON_SITE".into(), "syslog-e2e".into())].into()).unwrap();
    let event = envelope(&cfg, "127.0.0.1:42".parse().unwrap(), SyslogTransport::Tcp, b"ACK before crash".to_vec());
    {
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        spool.insert(&event).unwrap();
    }
    // Simulate a crash after the remote ACK but before the local deletion.
    bus.publish_proto(subjects::syslog(&event.site), &event.event_id, &event).await.unwrap();
    let mut gateway = Gateway::start(&path, nats.port, 65536).await;
    let replayed = gateway.wait("published").await;
    assert_eq!(replayed["event_id"], event.event_id);
    let stored = events(&bus, 1).await;
    assert_eq!(stored[0], event);
    gateway.kill().await;
    assert_eq!(Spool::open(&path, MIN_SPOOL_BYTES).unwrap().stats().unwrap().pending, 0);
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn tcp_connection_limit_idle_deadline_and_oversize_rejection() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let mut gateway = Gateway::start(&dir.path().join("spool.redb"), nats.port, 16).await;
    let mut idle = TcpStream::connect(gateway.tcp).await.unwrap();
    idle.write_all(b"3 ").await.unwrap();
    let mut waiting = TcpStream::connect(gateway.tcp).await.unwrap();
    waiting.write_all(b"<1>waiting\n").await.unwrap();
    // The second socket is queued while the sole active connection holds a permit.
    assert!(tokio::time::timeout(Duration::from_millis(100), gateway.wait("spooled")).await.is_err());
    let mut byte = [0];
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), idle.read(&mut byte)).await.unwrap().unwrap(), 0);
    gateway.wait("spooled").await;
    waiting.shutdown().await.unwrap();
    let mut oversized = TcpStream::connect(gateway.tcp).await.unwrap();
    oversized.write_all(b"12345678901234567\n").await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), oversized.read(&mut byte)).await.unwrap().unwrap(), 0);
    assert_eq!(events(&bus, 1).await[0].payload, b"<1>waiting");
    gateway.kill().await;
    nats.kill().await;
}
