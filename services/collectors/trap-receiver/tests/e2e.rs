#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Сквозной тест: настоящий nats-server + процесс trap-receiver + UDP-клиент.
//!   nats-server -js &  ;  NETMON_E2E=1 cargo test -p trap-receiver --test e2e -- --nocapture

use std::{process::Stdio, time::Duration};

use async_nats::jetstream::consumer::{pull, DeliverPolicy};
use bus::{Bus, BusConfig, StreamLimits};
use contracts::netmon::v1::{snmp_value::Kind, RawTrap, SnmpVersion};
use futures::StreamExt;
use prost::Message;
use tokio::net::UdpSocket;

const PUBLIC: &str = "305202010104067075626c6963a74502012a020100020100303a300e06082b06010201010300430201003017060a2b06010603010104010006092b0601060301010503300f060a2b06010201020201010c02010c";
const WRONG: &str = "3051020101040577726f6e67a74502012a020100020100303a300e06082b06010201010300430201003017060a2b06010603010104010006092b0601060301010503300f060a2b06010201020201010c02010c";
const INFORM: &str = "302802010104067075626c6963a61b02014d0201000201003010300e06082b0601020101030043020100";

fn hex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn trap_receiver_end_to_end() {
    if std::env::var("NETMON_E2E").is_err() {
        eprintln!("NETMON_E2E не задан: сквозной тест пропущен");
        return;
    }
    let nats = "nats://127.0.0.1:4222";
    let bus = Bus::connect(&BusConfig { url: nats.into(), creds_file: None, name: "e2e-trap".into() }).await.unwrap();
    bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
    bus.js.get_stream("RAW_TRAP").await.unwrap().purge().await.unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_trap-receiver"))
        .env("NATS_URL", nats)
        .env("NETMON_SITE", "e2e")
        .env("NETMON_TRAP_LISTEN", "127.0.0.1:11620")
        .env("NETMON_ALLOWED_SOURCES", "127.0.0.0/8")
        .env("NETMON_CRED_BACKEND", "env")
        .env("NETMON_TRAP_COMMUNITY_REFS", "env:trap")
        .env("NETMON_CRED_TRAP", r#"{"version":"v2c","community":"public"}"#)
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = "127.0.0.1:11620";
    sock.send_to(&hex(PUBLIC), target).await.unwrap();
    sock.send_to(&hex(WRONG), target).await.unwrap(); // чужая community: отбрасывается
    sock.send_to(&[0xde, 0xad, 0xbe, 0xef], target).await.unwrap(); // мусор: отбрасывается
    sock.send_to(&hex(INFORM), target).await.unwrap();

    // на inform должен прийти Response с тем же request-id (77)
    let mut reply = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(3), sock.recv(&mut reply))
        .await
        .expect("нет ответа на inform")
        .unwrap();
    assert!(reply[..n].contains(&0xA2), "ожидался Response-PDU (0xA2)");

    let stream = bus.js.get_stream("RAW_TRAP").await.unwrap();
    let consumer = stream
        .create_consumer(pull::OrderedConfig {
            deliver_policy: DeliverPolicy::All,
            filter_subject: "raw.trap.e2e".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut msgs = consumer.messages().await.unwrap();
    let mut got = Vec::new();
    while got.len() < 2 {
        match tokio::time::timeout(Duration::from_secs(5), msgs.next()).await {
            Ok(Some(Ok(m))) => got.push(RawTrap::decode(m.payload.as_ref()).unwrap()),
            _ => break,
        }
    }
    let _ = child.start_kill();

    assert_eq!(got.len(), 2, "ожидались ровно трап и inform, получено {}", got.len());
    let trap = got.iter().find(|t| !t.inform).expect("трап");
    assert_eq!(trap.source_ip, "127.0.0.1");
    assert_eq!(trap.snmp_version, SnmpVersion::V2c as i32);
    assert_eq!(trap.varbinds[1].oid, "1.3.6.1.6.3.1.1.4.1.0");
    assert_eq!(trap.varbinds[1].value.as_ref().unwrap().kind, Some(Kind::ObjectId("1.3.6.1.6.3.1.1.5.3".into())));
    assert!(got.iter().any(|t| t.inform));
    for t in &got {
        assert!(!format!("{t:?}").contains("public"), "community попала в сообщение");
    }
    // ещё раз убеждаемся, что отброшенные пакеты не прошли
    assert!(
        tokio::time::timeout(Duration::from_millis(700), msgs.next()).await.is_err(),
        "лишние сообщения в RAW_TRAP"
    );
}
