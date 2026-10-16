use super::certificates::{self, Ca};
use super::*;
use std::sync::Arc;
use syslog_gateway::tls::ReloadingTls;
use tokio_rustls::{client::TlsStream, TlsConnector};

async fn connect(addr: SocketAddr, client: Arc<rustls::ClientConfig>) -> std::io::Result<TlsStream<TcpStream>> {
    TlsConnector::from(client).connect("localhost".try_into().unwrap(), TcpStream::connect(addr).await?).await
}

#[test]
fn strict_tls_config_requires_enabled_listener_and_real_paths() {
    let ca = Ca::new();
    let leaf = ca.leaf(false);
    let dir = tempfile::tempdir().unwrap();
    let settings = ca.files(dir.path(), &leaf, false);
    let mut env: std::collections::BTreeMap<String, String> =
        certificates::env(&settings).into_iter().map(|(k, v)| (k.into(), v)).collect();
    env.insert("NETMON_SITE".into(), "test".into());
    assert!(Config::from_map(env.clone()).is_ok());
    for (name, value) in [
        ("SYSLOG_TLS_RELOAD_SECS", "0"),
        ("SYSLOG_TLS_HANDSHAKE_SECS", "0"),
        ("SYSLOG_TLS_CERT_FILE", "missing"),
        ("SYSLOG_TLS_KEY_FILE", "missing"),
        ("SYSLOG_TLS_CLIENT_CA_FILE", "missing"),
        ("SYSLOG_TLS_ADDR", "bad"),
    ] {
        let mut bad = env.clone();
        bad.insert(name.into(), value.into());
        assert!(Config::from_map(bad).is_err());
    }
    env.remove("SYSLOG_TLS_ADDR");
    assert!(Config::from_map(env).is_err());
}

#[test]
fn tls_validation_rejects_bad_pem_key_mismatch_validity_and_ca_without_leaking_key() {
    let ca = Ca::new();
    let good = ca.leaf(false);
    let other = ca.leaf(false);
    let dir = tempfile::tempdir().unwrap();
    let settings = ca.files(dir.path(), &good, true);
    assert!(ReloadingTls::new(settings.clone()).is_ok());
    for bad in ["not PEM".to_string(), other.key.serialize_pem()] {
        std::fs::write(&settings.key_file, &bad).unwrap();
        let error = ReloadingTls::new(settings.clone()).err().unwrap().to_string();
        assert!(!error.contains(&bad));
        assert!(!error.contains("PRIVATE KEY"));
    }
    std::fs::write(&settings.key_file, good.key.serialize_pem()).unwrap();
    for leaf in [ca.dated_leaf(false, 2000, 2001), ca.dated_leaf(false, 2998, 2999)] {
        ca.files(dir.path(), &leaf, true);
        assert!(ReloadingTls::new(settings.clone()).is_err());
    }
    ca.files(dir.path(), &good, true);
    for bad in ["invalid PEM".to_string(), good.cert.pem()] {
        std::fs::write(settings.client_ca_file.as_ref().unwrap(), bad).unwrap();
        assert!(ReloadingTls::new(settings.clone()).is_err());
    }
    ca.files(dir.path(), &good, true);
    std::fs::write(&settings.cert_file, "-----BEGIN CERTIFICATE-----\nnot-valid\n-----END CERTIFICATE-----").unwrap();
    assert!(ReloadingTls::new(settings.clone()).is_err());
    std::fs::write(&settings.cert_file, vec![b'x'; 1024 * 1024 + 1]).unwrap();
    assert!(ReloadingTls::new(settings).is_err());
}

#[test]
fn failed_reload_keeps_the_previous_arc_and_unchanged_files_do_not_swap() {
    let ca = Ca::new();
    let leaf = ca.leaf(false);
    let dir = tempfile::tempdir().unwrap();
    let settings = ca.files(dir.path(), &leaf, false);
    let manager = ReloadingTls::new(settings.clone()).unwrap();
    let original = manager.snapshot();
    assert!(!manager.reload().unwrap());
    assert!(Arc::ptr_eq(&original, &manager.snapshot()));
    std::fs::write(&settings.key_file, "PRIVATE_TEST_SENTINEL").unwrap();
    assert!(manager.reload().is_err());
    assert!(Arc::ptr_eq(&original, &manager.snapshot()));
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn real_tls_uses_b1_lf_and_octet_codecs_and_jetstream() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let leaf = ca.leaf(false);
    let settings = ca.files(dir.path(), &leaf, false);
    let mut gateway =
        Gateway::start_with(&dir.path().join("spool.redb"), nats.port, 65536, &certificates::env(&settings)).await;
    let mut tls = connect(gateway.addr("tls_addr"), ca.client(None)).await.unwrap();
    tls.write_all(b"<1>tls\xff\n4 a\n\xffb").await.unwrap();
    let got = events(&bus, 2).await;
    assert!(got.iter().all(|e| e.transport == SyslogTransport::Tls as i32));
    assert!(got.iter().any(|e| e.payload == b"<1>tls\xff"));
    assert!(got.iter().any(|e| e.payload == b"a\n\xffb"));
    gateway.kill().await;
    nats.kill().await;
}

async fn rejected(addr: SocketAddr, client: Arc<rustls::ClientConfig>) {
    if let Ok(mut stream) = connect(addr, client).await {
        let _ = stream.write_all(b"must-not-be-accepted\n").await;
        let mut byte = [0];
        let result = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut byte)).await.unwrap();
        assert!(matches!(result, Ok(0) | Err(_)), "client without trusted identity was accepted");
    }
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn mtls_requires_a_trusted_client_certificate() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let server = ca.leaf(false);
    let client = ca.leaf(true);
    let untrusted = Ca::new().leaf(true);
    let settings = ca.files(dir.path(), &server, true);
    let mut gateway =
        Gateway::start_with(&dir.path().join("spool.redb"), nats.port, 65536, &certificates::env(&settings)).await;
    rejected(gateway.addr("tls_addr"), ca.client(None)).await;
    rejected(gateway.addr("tls_addr"), ca.client(Some(&untrusted))).await;
    let mut trusted = connect(gateway.addr("tls_addr"), ca.client(Some(&client))).await.unwrap();
    trusted.write_all(b"trusted\n").await.unwrap();
    assert_eq!(events(&bus, 1).await[0].payload, b"trusted");
    gateway.kill().await;
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn certificate_reload_preserves_sessions_and_rejects_bad_replacements() {
    let mut nats = Nats::start(port()).await;
    let bus = nats.bus().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let a = ca.leaf(false);
    let b = ca.leaf(false);
    let settings = ca.files(dir.path(), &a, false);
    let mut gateway =
        Gateway::start_with(&dir.path().join("spool.redb"), nats.port, 65536, &certificates::env(&settings)).await;
    let mut established = connect(gateway.addr("tls_addr"), ca.client(None)).await.unwrap();
    assert!(established.get_ref().1.peer_certificates().unwrap()[0].as_ref() == a.cert.der().as_ref());
    established.write_all(b"before\n").await.unwrap();
    gateway.wait("spooled").await;
    std::fs::write(&settings.cert_file, b.cert.pem()).unwrap();
    gateway.wait("tls_reload_rejected").await;
    let probe = connect(gateway.addr("tls_addr"), ca.client(None)).await.unwrap();
    assert!(probe.get_ref().1.peer_certificates().unwrap()[0].as_ref() == a.cert.der().as_ref());
    drop(probe);
    std::fs::write(&settings.key_file, b.key.serialize_pem()).unwrap();
    gateway.wait("tls_reloaded").await;
    let mut fresh = connect(gateway.addr("tls_addr"), ca.client(None)).await.unwrap();
    assert!(fresh.get_ref().1.peer_certificates().unwrap()[0].as_ref() == b.cert.der().as_ref());
    fresh.write_all(b"after\n").await.unwrap();
    established.write_all(b"existing\n").await.unwrap();
    assert_eq!(events(&bus, 3).await.len(), 3);
    gateway.kill().await;
    nats.kill().await;
}

#[tokio::test]
#[ignore = "requires real nats-server"]
async fn slow_tls_handshake_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::new();
    let leaf = ca.leaf(false);
    let settings = ca.files(dir.path(), &leaf, false);
    let mut gateway =
        Gateway::start_with(&dir.path().join("spool.redb"), port(), 65536, &certificates::env(&settings)).await;
    let mut stalled = TcpStream::connect(gateway.addr("tls_addr")).await.unwrap();
    let mut byte = [0];
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), stalled.read(&mut byte)).await.unwrap().unwrap(), 0);
    let _valid = connect(gateway.addr("tls_addr"), ca.client(None)).await.unwrap();
    gateway.kill().await;
}
