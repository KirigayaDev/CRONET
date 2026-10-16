use super::certificates::{self, Ca};
use super::*;
use syslog_gateway::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};

fn frame(txnr: u32, command: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = format!("{txnr} {command} {}", payload.len()).into_bytes();
    if !payload.is_empty() {
        bytes.push(b' ');
        bytes.extend_from_slice(payload);
    }
    bytes.push(b'\n');
    bytes
}
// Independently decode the wire response instead of reusing the server codec.
async fn response<S: AsyncRead + Unpin>(stream: &mut S) -> (u32, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut header = Vec::new();
        let mut spaces = 0;
        while spaces < 3 {
            let byte = stream.read_u8().await.unwrap();
            assert!(header.len() < 64);
            header.push(byte);
            if byte == b' ' {
                spaces += 1;
            } else if byte == b'\n' {
                break;
            }
        }
        let text = std::str::from_utf8(&header).unwrap();
        let fields: Vec<_> = text.split_ascii_whitespace().collect();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1], "rsp");
        let txnr = fields[0].parse().unwrap();
        let length: usize = fields[2].parse().unwrap();
        assert!(length <= 4096);
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await.unwrap();
        if length > 0 {
            assert_eq!(stream.read_u8().await.unwrap(), b'\n');
        }
        (txnr, payload)
    })
    .await
    .unwrap()
}
async fn open<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) {
    stream.write_all(&frame(1, "open", b"relp_version=0\ncommands=syslog\n")).await.unwrap();
    let (txnr, data) = response(stream).await;
    assert_eq!(txnr, 1);
    assert!(data.starts_with(b"200 OK\n"));
    assert!(data.windows(b"commands=syslog\n".len()).any(|v| v == b"commands=syslog\n"));
}
fn env() -> Vec<(&'static str, String)> {
    vec![("SYSLOG_RELP_ADDR", "127.0.0.1:0".into())]
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn real_relp_open_fragmentation_pipeline_three_transactions_and_close() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let mut gateway = Gateway::start_with(&dir.path().join("spool.redb"), nats.port, 65536, &env()).await;
    let mut client = TcpStream::connect(gateway.addr("relp_addr")).await.unwrap();
    open(&mut client).await;
    for part in frame(2, "syslog", b"one\n\xff").chunks(2) {
        client.write_all(part).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(response(&mut client).await, (2, b"200 OK".to_vec()));
    let mut pipelined = frame(3, "syslog", b"two\x00");
    pipelined.extend(frame(4, "syslog", b"three"));
    client.write_all(&pipelined).await.unwrap();
    assert_eq!(response(&mut client).await, (3, b"200 OK".to_vec()));
    assert_eq!(response(&mut client).await, (4, b"200 OK".to_vec()));
    let stored = events(&bus, 3).await;
    assert!(stored.iter().all(|e| e.transport == SyslogTransport::Relp as i32));
    for payload in [b"one\n\xff".as_slice(), b"two\x00", b"three"] {
        assert!(stored.iter().any(|e| e.payload == payload));
    }
    client.write_all(&frame(5, "close", &[])).await.unwrap();
    assert_eq!(response(&mut client).await, (5, b"200 OK".to_vec()));
    let mut byte = [0];
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), client.read(&mut byte)).await.unwrap().unwrap(), 0);
    gateway.kill().await;
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn relp_ack_during_nats_outage_survives_crash_and_replays_same_id() {
    let nats_port = port();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool.redb");
    let mut gateway = Gateway::start_with(&path, nats_port, 65536, &env()).await;
    let mut client = TcpStream::connect(gateway.addr("relp_addr")).await.unwrap();
    open(&mut client).await;
    client.write_all(&frame(2, "syslog", b"offline\xff")).await.unwrap();
    assert_eq!(response(&mut client).await, (2, b"200 OK".to_vec()));
    let ack = gateway.wait("relp_ack").await;
    let id = ack["event_id"].as_str().unwrap().to_owned();
    gateway.kill().await;
    {
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        let pending = spool.batch(1, false).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event.event_id, id);
        assert_eq!(pending[0].event.payload, b"offline\xff");
    }
    let mut restarted = Gateway::start_with(&path, nats_port, 65536, &env()).await;
    let mut nats = Nats::start(nats_port).await;
    let bus = nats.bus().await;
    assert_eq!(restarted.wait("published").await["event_id"], id);
    assert_eq!(events(&bus, 1).await[0].event_id, id);
    restarted.kill().await;
    assert_eq!(Spool::open(&path, MIN_SPOOL_BYTES).unwrap().stats().unwrap().pending, 0);
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn relp_full_spool_withholds_ack_until_capacity_is_released() {
    let nats_port = port();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool.redb");
    let cfg = Config::from_map(
        [("NETMON_SITE".into(), "syslog-e2e".into()), ("SYSLOG_GATEWAY_ID".into(), "e2e-gateway".into())].into(),
    )
    .unwrap();
    let mut seeded = 0u64;
    {
        let spool = Spool::open(&path, MIN_SPOOL_BYTES).unwrap();
        loop {
            let event = envelope(&cfg, "127.0.0.1:42".parse().unwrap(), SyslogTransport::Relp, vec![0xfe; 65536]);
            match spool.insert(&event) {
                Ok(_) => seeded += 1,
                Err(Error::Full) => break,
                Err(error) => {
                    assert!(matches!(error, Error::Full));
                }
            }
        }
    }
    assert!(seeded > 0);
    let mut gateway = Gateway::start_with(&path, nats_port, 65536, &env()).await;
    let mut client = TcpStream::connect(gateway.addr("relp_addr")).await.unwrap();
    open(&mut client).await;
    client.write_all(&frame(2, "syslog", &vec![0xfd; 65536])).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(200), response(&mut client)).await.is_err());
    let mut nats = Nats::start(nats_port).await;
    let bus = nats.bus().await;
    assert_eq!(response(&mut client).await, (2, b"200 OK".to_vec()));
    assert!(events(&bus, seeded + 1).await.iter().any(|e| e.payload == vec![0xfd; 65536]));
    gateway.kill().await;
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn relp_rejects_invalid_protocol_and_requires_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut gateway = Gateway::start_with(&dir.path().join("spool.redb"), port(), 128, &env()).await;
    for bytes in [
        b"0 syslog 1 x\n".as_slice(),
        b"-1 syslog 1 x\n",
        b"1 syslog bad x\n",
        b"1 syslog 129 ",
        b"1 syslog 1 x\n",
        b"1 unknown 0\n",
        b"1 syslog 4 ab",
    ] {
        let mut client = TcpStream::connect(gateway.addr("relp_addr")).await.unwrap();
        client.write_all(bytes).await.unwrap();
        client.shutdown().await.unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), client.take(1024).read_to_end(&mut reply)).await.unwrap().unwrap();
        assert!(!reply.windows(6).any(|v| v == b"200 OK"));
    }
    for bad in [
        frame(2, "unknown", &[]),
        frame(3, "syslog", b"skipped transaction"),
        frame(1, "syslog", b"reused transaction"),
    ] {
        let mut client = TcpStream::connect(gateway.addr("relp_addr")).await.unwrap();
        open(&mut client).await;
        client.write_all(&bad).await.unwrap();
        let (_, reply) = response(&mut client).await;
        assert!(reply.starts_with(b"500 "));
    }
    gateway.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn relp_over_mtls_uses_the_same_session_and_durable_pipeline() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let server = ca.leaf(false);
    let client = ca.leaf(true);
    let settings = ca.files(dir.path(), &server, true);
    let mut env = certificates::env(&settings);
    env.retain(|(name, _)| *name != "SYSLOG_TLS_ADDR");
    env.push(("SYSLOG_RELP_TLS_ADDR", "127.0.0.1:0".into()));
    let mut gateway = Gateway::start_with(&dir.path().join("spool.redb"), nats.port, 65536, &env).await;
    assert_eq!(gateway.ready["tls_addr"], "disabled");
    let mut tls = tokio_rustls::TlsConnector::from(ca.client(Some(&client)))
        .connect("localhost".try_into().unwrap(), TcpStream::connect(gateway.addr("relp_tls_addr")).await.unwrap())
        .await
        .unwrap();
    open(&mut tls).await;
    tls.write_all(&frame(2, "syslog", b"secure-relp\xff")).await.unwrap();
    assert_eq!(response(&mut tls).await, (2, b"200 OK".to_vec()));
    let stored = events(&bus, 1).await;
    assert_eq!(stored[0].transport, SyslogTransport::RelpTls as i32);
    assert_eq!(stored[0].payload, b"secure-relp\xff");
    tls.write_all(&frame(3, "close", &[])).await.unwrap();
    assert_eq!(response(&mut tls).await, (3, b"200 OK".to_vec()));
    gateway.kill().await;
    nats.kill().await;
}
