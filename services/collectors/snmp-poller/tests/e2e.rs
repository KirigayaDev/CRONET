#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Сквозной тест «чёрным ящиком»: настоящий nats-server (JetStream), настоящий
//! процесс snmp-poller и эмулятор агента (snmpsim). Запускается вручную:
//!
//!   nats-server -js &
//!   snmpsim-command-responder --data-dir=<dir> --agent-udpv4-endpoint=127.0.0.1:1161 \
//!       --v3-user=mon --v3-auth-key=authpass123 --v3-auth-proto=SHA256 \
//!       --v3-priv-key=privpass123 --v3-priv-proto=AES128 &
//!   NETMON_E2E=1 cargo test -p snmp-poller --test e2e -- --nocapture
//!
//! Без NETMON_E2E тест пропускается, чтобы не ломать обычный `cargo test`.

use std::{collections::HashMap, process::Stdio, time::Duration};

use async_nats::jetstream::consumer::{pull, DeliverPolicy};
use bus::{Bus, BusConfig};
use contracts::netmon::v1::{snmp_value::Kind, PollStatus, PollTask, RawSnmpResult, SnmpVersion};
use futures::StreamExt;
use prost::Message;

const SITE: &str = "e2e";

fn ts(secs_from_now: i64) -> prost_types::Timestamp {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    prost_types::Timestamp { seconds: now + secs_from_now, nanos: 0 }
}

fn task(id: &str, target: &str, cred: &str, ver: SnmpVersion) -> PollTask {
    PollTask {
        task_id: id.into(),
        device_id: format!("dev-{id}"),
        target: target.into(),
        snmp_version: ver as i32,
        credential_ref: cred.into(),
        timeout_ms: 800,
        deadline: Some(ts(60)),
        attempt: 1,
        ..Default::default()
    }
}

async fn collect_results(bus: &Bus, want: usize) -> HashMap<String, RawSnmpResult> {
    let stream = bus.js.get_stream("RAW_SNMP").await.unwrap();
    let consumer = stream
        .create_consumer(pull::OrderedConfig {
            deliver_policy: DeliverPolicy::All,
            filter_subject: format!("raw.snmp.{SITE}"),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut msgs = consumer.messages().await.unwrap();
    let mut out = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while out.len() < want {
        match tokio::time::timeout_at(deadline, msgs.next()).await {
            Ok(Some(Ok(m))) => {
                let r = RawSnmpResult::decode(m.payload.as_ref()).unwrap();
                out.insert(r.task_id.clone(), r);
            }
            _ => break,
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn poller_end_to_end() {
    if std::env::var("NETMON_E2E").is_err() {
        eprintln!("NETMON_E2E не задан: сквозной тест пропущен");
        return;
    }
    let nats = std::env::var("NETMON_E2E_NATS").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let snmp = std::env::var("NETMON_E2E_SNMP").unwrap_or_else(|_| "127.0.0.1:1161".into());

    let bus = Bus::connect(&BusConfig { url: nats.clone(), creds_file: None, name: "e2e-test".into() }).await.unwrap();
    bus.ensure_streams(1, &bus::StreamLimits::dev()).await.unwrap();
    // чистый стенд: старые результаты и задачи прошлых прогонов не нужны
    for s in ["TASKS_SNMP", "RAW_SNMP"] {
        let st = bus.js.get_stream(s).await.unwrap();
        st.purge().await.unwrap();
    }

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_snmp-poller"))
        .env("NATS_URL", &nats)
        .env("NETMON_SITE", SITE)
        .env("NETMON_COLLECTOR_ID", "e2e-poller")
        .env("NETMON_CRED_BACKEND", "env")
        .env("NETMON_ALLOWED_TARGETS", "127.0.0.0/8")
        .env("NETMON_CRED_V2C_OK", r#"{"version":"v2c","community":"public"}"#)
        .env("NETMON_CRED_V2C_BAD", r#"{"version":"v2c","community":"nope"}"#)
        .env("NETMON_CRED_V3_OK", r#"{"version":"v3","username":"mon","auth_protocol":"sha256","auth_password":"authpass123","priv_protocol":"aes128","priv_password":"privpass123","context":"public"}"#)
        .env("NETMON_CRED_V3_BADPASS", r#"{"version":"v3","username":"mon","auth_protocol":"sha256","auth_password":"wrongpass123","priv_protocol":"aes128","priv_password":"privpass123","context":"public"}"#)
        .env("RUST_LOG", "info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("запуск snmp-poller");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let subject = contracts::subjects::snmp_task(SITE);
    let mut tasks = Vec::new();

    let mut t = task("get-v2c", &snmp, "env:v2c_ok", SnmpVersion::V2c);
    t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into(), "1.3.6.1.2.1.1.3.0".into(), "1.3.6.1.2.1.31.1.1.1.6.2".into()];
    tasks.push(t);

    // Counter64 >= 2^63 snmp2 не разбирает: ожидаем явную ошибку, а не тихую потерю данных
    let mut t = task("counter64-huge", &snmp, "env:v2c_ok", SnmpVersion::V2c);
    t.get_oids = vec!["1.3.6.1.2.1.1.3.0".into(), "1.3.6.1.2.1.31.1.1.1.6.1".into()];
    tasks.push(t);

    let mut t = task("walk-v2c", &snmp, "env:v2c_ok", SnmpVersion::V2c);
    t.walk_roots = vec!["1.3.6.1.2.1.2.2.1.2".into()]; // ifDescr
    t.max_repetitions = 1; // принудительно несколько запросов GETBULK
    tasks.push(t);

    tasks.push({
        let mut t = task("bad-community", &snmp, "env:v2c_bad", SnmpVersion::V2c);
        t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into()];
        t
    });
    tasks.push({
        let mut t = task("dead-port", "127.0.0.1:9", "env:v2c_ok", SnmpVersion::V2c);
        t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into()];
        t
    });
    tasks.push({
        let mut t = task("outside-allowlist", "10.9.9.9:161", "env:v2c_ok", SnmpVersion::V2c);
        t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into()];
        t
    });
    tasks.push({
        let mut t = task("expired", &snmp, "env:v2c_ok", SnmpVersion::V2c);
        t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into()];
        t.deadline = Some(ts(-10));
        t
    });
    tasks.push({
        let mut t = task("no-secret", &snmp, "env:missing", SnmpVersion::V2c);
        t.get_oids = vec!["1.3.6.1.2.1.1.1.0".into()];
        t
    });
    tasks.push({
        let mut t = task("get-v3", &snmp, "env:v3_ok", SnmpVersion::V3);
        t.get_oids = vec!["1.3.6.1.2.1.1.5.0".into()];
        t
    });
    tasks.push({
        let mut t = task("v3-bad-password", &snmp, "env:v3_badpass", SnmpVersion::V3);
        t.get_oids = vec!["1.3.6.1.2.1.1.5.0".into()];
        t
    });

    let n = tasks.len();
    for t in &tasks {
        bus.publish_proto(subject.clone(), &format!("task-{}", t.task_id), t).await.unwrap();
    }

    let res = collect_results(&bus, n).await;
    let _ = child.start_kill();
    for (id, r) in &res {
        eprintln!("{id}: status={} detail={:?} varbinds={}", r.status, r.error_detail, r.varbinds.len());
    }
    assert_eq!(res.len(), n, "получены результаты не по всем задачам: {:?}", res.keys().collect::<Vec<_>>());

    let st = |id: &str| PollStatus::try_from(res[id].status).unwrap();

    // GET: типы сохранены без интерпретации
    let r = &res["get-v2c"];
    assert_eq!(st("get-v2c"), PollStatus::Ok);
    assert_eq!(r.varbinds.len(), 3);
    assert_eq!(r.varbinds[0].value.as_ref().unwrap().kind, Some(Kind::OctetString(b"Test Router OS 1.0".to_vec())));
    assert_eq!(r.varbinds[1].value.as_ref().unwrap().kind, Some(Kind::Timeticks(123456)));
    assert_eq!(r.varbinds[2].value.as_ref().unwrap().kind, Some(Kind::Counter64(12345)));
    assert_eq!(st("counter64-huge"), PollStatus::Error);
    assert!(res["counter64-huge"].varbinds.is_empty(), "частичный результат отдавать нельзя");
    assert!(
        res["counter64-huge"].error_detail.contains("не удалось декодировать"),
        "{}",
        res["counter64-huge"].error_detail
    );
    assert_eq!(r.collector_id, "e2e-poller");

    // WALK: ровно два интерфейса и ничего лишнего из соседних поддеревьев
    assert_eq!(st("walk-v2c"), PollStatus::Ok);
    let oids: Vec<_> = res["walk-v2c"].varbinds.iter().map(|v| v.oid.as_str()).collect();
    assert_eq!(oids, ["1.3.6.1.2.1.2.2.1.2.1", "1.3.6.1.2.1.2.2.1.2.2"]);

    // ошибки - тоже результаты с понятным статусом
    assert_eq!(st("bad-community"), PollStatus::Timeout, "v2c с неверной community молчит: это таймаут");
    assert!(matches!(st("dead-port"), PollStatus::Timeout | PollStatus::Error));
    assert_eq!(st("outside-allowlist"), PollStatus::BadTask);
    assert_eq!(st("expired"), PollStatus::Expired);
    assert_eq!(st("no-secret"), PollStatus::CredError);

    // SNMPv3 authPriv (SHA-256 + AES-128)
    assert_eq!(st("get-v3"), PollStatus::Ok, "{}", res["get-v3"].error_detail);
    assert_eq!(res["get-v3"].varbinds[0].value.as_ref().unwrap().kind, Some(Kind::OctetString(b"r1".to_vec())));
    assert_eq!(st("v3-bad-password"), PollStatus::AuthFail, "{}", res["v3-bad-password"].error_detail);

    // секреты не должны попадать в результаты
    for r in res.values() {
        let text = format!("{r:?}");
        for secret in ["authpass123", "privpass123", "wrongpass123", "nope"] {
            assert!(!text.contains(secret), "секрет {secret} в результате {}", r.task_id);
        }
    }
}
