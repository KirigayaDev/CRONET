#![cfg(test)]
//! Opt-in disposable CMDB -> Scheduler binary -> JetStream -> existing poller acceptance.
use bus::{Bus, BusConfig};
use contracts::{
    netmon::v1::{PollStatus, PollTask, RawSnmpResult},
    subjects,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use prost::Message;
use scheduler_service::{
    config::Definition,
    membership::{members, Ownership},
    model::{Occurrence, ScheduleKey, Spec, Target},
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    fs::File,
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn free_addresses() -> (String, String) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (a.local_addr().unwrap().to_string(), b.local_addr().unwrap().to_string())
}
fn sign(key: &EncodingKey, identity: &Value) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    encode(&Header::new(Algorithm::RS256),&json!({"iss":"Re:simple_videohosting_auth","aud":"Re:simple_videohosting_auth","sub":identity["id"],"token_version":identity["version"],"jti":Uuid::new_v4(),"iat":now-1.0,"nbf":now-1.0,"exp":now+600.0,"token_type":"access"}),key).unwrap()
}
async fn post(client: &reqwest::Client, base: &str, token: &str, path: &str, body: Value) -> Value {
    let response =
        client.post(format!("{base}/api/v1/cmdb{path}")).bearer_auth(token).json(&body).send().await.unwrap();
    let status = response.status();
    let value: Value = response.json().await.unwrap();
    assert!(status.is_success(), "CMDB fixture request failed: {status} {value}");
    value
}
async fn location(client: &reqwest::Client, base: &str, token: &str, site: &str) -> Value {
    let s = post(client, base, token, "/sites", json!({"name":site})).await;
    let r = post(client, base, token, "/rooms", json!({"name":"Room","site_id":s["id"]})).await;
    post(client, base, token, "/racks", json!({"name":"Rack","room_id":r["id"],"height_u":42})).await
}
async fn add_ip(client: &reqwest::Client, base: &str, token: &str, device: &Value, address: &str, management: bool) {
    let interface =
        post(client, base, token, "/interfaces", json!({"device_id":device["id"],"name":Uuid::new_v4().to_string()}))
            .await;
    post(
        client,
        base,
        token,
        "/ip-addresses",
        json!({"interface_id":interface["id"],"address":address,"is_management":management}),
    )
    .await;
}
async fn stored(bus: &Bus, stream_name: &str) -> Vec<(String, Vec<u8>)> {
    let mut stream = bus.js.get_stream(stream_name).await.unwrap();
    let info = stream.info().await.unwrap().state.clone();
    let mut out = Vec::new();
    for seq in info.first_sequence..=info.last_sequence {
        if let Ok(raw) = stream.get_raw_message(seq).await {
            out.push((raw.subject.to_string(), raw.payload.to_vec()));
        }
    }
    out
}
async fn tasks(bus: &Bus, site: &str) -> Vec<PollTask> {
    stored(bus, subjects::TASKS_STREAM)
        .await
        .into_iter()
        .filter(|(subject, _)| subject == &subjects::snmp_task(site))
        .map(|(_, bytes)| PollTask::decode(bytes.as_slice()).unwrap())
        .collect()
}
async fn wait_tasks(bus: &Bus, site: &str) -> Vec<PollTask> {
    for _ in 0..50 {
        let values = tasks(bus, site).await;
        if !values.is_empty() {
            return values;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("Scheduler did not publish a stored task");
}
fn schedule(id: &str, site: &str) -> Value {
    json!({"id":id,"enabled":true,"site":site,"interval":"2s","jitter":"0s","snmp_version":"v2c","credential_ref":"env:FIXTURE","get_oids":["1.3.6.1.2.1.1.1.0"],"walk_roots":[],"timeout":"1s","task_deadline":"30s","max_lateness":"5s","max_repetitions":20,"port":1161})
}
fn terminate(process: &mut Process) {
    assert!(Command::new("kill").args(["-TERM", &process.0.id().to_string()]).status().unwrap().success());
}
async fn wait_exit(process: &mut Process) {
    for _ in 0..50 {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success());
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Scheduler graceful shutdown exceeded bound");
}
fn log(path: &Path) -> Stdio {
    Stdio::from(File::create(path).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn cmdb_delayqueue_jetstream_poller_and_inventory_resync() {
    if std::env::var("SCHEDULER_E2E").as_deref() != Ok("1") {
        return;
    }
    let database = std::env::var("CMDB_TEST_DATABASE_URL").unwrap();
    assert_eq!(database, "postgresql://cmdb_test@postgres/cronet_cmdb_test_schema?sslmode=disable");
    let redis = std::env::var("CMDB_TEST_REDIS_URL").unwrap();
    assert_eq!(redis, "redis://redis:6379");
    let identities: Value = serde_json::from_slice(&std::fs::read("/test-auth/identities.json").unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let private = dir.path().join("private.pem");
    let public = dir.path().join("public.pem");
    assert!(Command::new("openssl")
        .args(["genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out"])
        .arg(&private)
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    assert!(Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&private)
        .arg("-out")
        .arg(&public)
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    let key = EncodingKey::from_rsa_pem(&std::fs::read(private).unwrap()).unwrap();
    let admin = sign(&key, &identities["admin"]);
    let reader = sign(&key, &identities["reader"]);
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, &reader).unwrap();
    let (http, grpc) = free_addresses();
    let mut cmdb = Process(
        Command::new(std::env::var("CMDB_TEST_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", database)
            .env("REDIS_URL", redis)
            .env("JWT_PUBLIC_KEY_PATH", public)
            .env("CMDB_HTTP_ADDR", &http)
            .env("CMDB_GRPC_ADDR", &grpc)
            .stdout(Stdio::null())
            .stderr(log(&dir.path().join("cmdb.log")))
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    let base = format!("http://{http}");
    let mut ready = false;
    for _ in 0..50 {
        if client.get(format!("{base}/health/ready")).send().await.is_ok_and(|r| r.status().is_success()) {
            ready = true;
            break;
        }
        assert!(cmdb.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready);
    let suffix = Uuid::new_v4().simple().to_string();
    let site_a = format!("sched-a-{suffix}");
    let site_b = format!("sched-b-{suffix}");
    let rack_a = location(&client, &base, &admin, &site_a).await;
    let rack_b = location(&client, &base, &admin, &site_b).await;
    let mut devices = Vec::new();
    for (index, status) in
        ["active", "decommissioned", "maintenance", "active", "active", "active"].into_iter().enumerate()
    {
        let mut body = json!({"hostname":format!("fixture-{index}"),"lifecycle_status":status});
        if index != 3 {
            body["rack_id"] = rack_a["id"].clone();
            body["start_unit"] = json!(index * 2 + 1);
            body["height_u"] = json!(1);
        }
        let d = post(&client, &base, &admin, "/devices", body).await;
        add_ip(&client, &base, &admin, &d, &format!("127.0.0.{}", index + 1), index != 4).await;
        if index == 5 {
            add_ip(&client, &base, &admin, &d, "127.0.0.7", true).await;
        }
        devices.push(d);
    }
    let id: Uuid = devices[0]["id"].as_str().unwrap().parse().unwrap();
    let nats = std::env::var("SCHEDULER_TEST_NATS_URL").unwrap();
    let bus = Bus::connect(&BusConfig { url: nats.clone(), creds_file: None, name: "scheduler-acceptance".into() })
        .await
        .unwrap();
    bus.ensure_streams(1, &bus::StreamLimits::dev()).await.unwrap();
    let namespace = format!("ACCEPT_{}", Uuid::new_v4().simple());
    let definition = json!({"schedules":[schedule("site-a-fast",&site_a),schedule("site-b-fast",&site_b)]});
    let scheduler_log = dir.path().join("scheduler-a.log");
    let scheduler_b_log = dir.path().join("scheduler-b.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_scheduler-service"));
    command
        .env_clear()
        .env("NATS_URL", &nats)
        .env("CMDB_GRPC_URL", format!("http://{grpc}"))
        .env("CMDB_BEARER_TOKEN_FILE", &token_path)
        .env("SCHEDULER_CONFIG_JSON", definition.to_string())
        .env("SCHEDULER_INSTANCE_ID", "acceptance-A")
        .env("SCHEDULER_NAMESPACE", &namespace)
        .env("SCHEDULER_RESYNC_INTERVAL", "1s")
        .env("SCHEDULER_HEARTBEAT_INTERVAL", "1s")
        .env("SCHEDULER_MEMBER_TTL", "3s")
        .env("SCHEDULER_CLAIM_TTL", "12s")
        .stdout(Stdio::null());
    let a = Process(command.stderr(log(&scheduler_log)).spawn().unwrap());
    let b =
        Process(command.env("SCHEDULER_INSTANCE_ID", "acceptance-B").stderr(log(&scheduler_b_log)).spawn().unwrap());
    let mut schedulers = [a, b];
    let first = wait_tasks(&bus, &site_a).await;
    let membership = bus.js.get_key_value(format!("{namespace}_MEMBERS")).await.unwrap();
    let mut ids = Vec::new();
    for _ in 0..50 {
        ids = members(&membership).await.unwrap();
        if ids.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ids.len(), 2, "Both Scheduler processes must join membership");
    let key = ScheduleKey { schedule_id: "site-a-fast".into(), device_id: id };
    let ring = Ownership::new(ids);
    let owner = ring.owner(&key).unwrap();
    let owner_index = if owner == "acceptance-A" { 0 } else { 1 };
    let survivor = 1 - owner_index;
    let before_crash: HashSet<_> = tasks(&bus, &site_a).await.into_iter().map(|task| task.task_id).collect();
    schedulers[owner_index].0.kill().unwrap();
    schedulers[owner_index].0.wait().unwrap();
    for _ in 0..60 {
        if members(&membership).await.unwrap().len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let remaining = members(&membership).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_ne!(remaining[0], owner);
    let mut took_over = false;
    for _ in 0..40 {
        if tasks(&bus, &site_a).await.iter().any(|task| !before_crash.contains(&task.task_id)) {
            took_over = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(took_over, "Survivor failed to publish following owner crash and member TTL");
    let published = tasks(&bus, &site_a).await;
    assert!(published.len() >= first.len());
    let mut seen = HashSet::new();
    for task in &published {
        assert_eq!(task.device_id, id.to_string(), "Negative inventory case became schedulable");
        assert_eq!(task.target, "127.0.0.1:1161");
        assert_eq!(task.credential_ref, "env:FIXTURE");
        assert_eq!(task.get_oids, vec!["1.3.6.1.2.1.1.1.0"]);
        assert_eq!(task.attempt, 1);
        assert_eq!(task.snmp_version, contracts::netmon::v1::SnmpVersion::V2c as i32);
        assert!(seen.insert(task.task_id.clone()));
        let parsed: Definition = serde_json::from_value(definition.clone()).unwrap();
        let deadline = task.deadline.as_ref().unwrap();
        let due = deadline.seconds as u64 * 1000 + deadline.nanos as u64 / 1_000_000 - 30_000;
        let occurrence = Occurrence::new(
            Spec {
                schedule: parsed.schedules[0].clone(),
                target: Target { device_id: id, site: site_a.clone(), ip: "127.0.0.1".parse().unwrap() },
            },
            due / 2000,
        );
        assert_eq!(task.task_id, occurrence.task_id.to_string());
    }
    let mut poller = Process(
        Command::new(std::env::var("POLLER_TEST_BINARY").unwrap())
            .env_clear()
            .env("NATS_URL", &nats)
            .env("NETMON_SITE", &site_a)
            .env("NETMON_COLLECTOR_ID", "scheduler-acceptance-poller")
            .env("NETMON_CRED_BACKEND", "env")
            .env("NETMON_CRED_FIXTURE", r#"{"version":"v2c","community":"public"}"#)
            .env("NETMON_ALLOWED_TARGETS", "127.0.0.0/8")
            .env("NETMON_MAX_CONCURRENCY", "1")
            .stdout(Stdio::null())
            .stderr(log(&dir.path().join("poller.log")))
            .spawn()
            .unwrap(),
    );
    let mut result = false;
    for _ in 0..50 {
        for (subject, bytes) in stored(&bus, subjects::RAW_SNMP_STREAM).await {
            if subject == subjects::snmp_result(&site_a) {
                let r = RawSnmpResult::decode(bytes.as_slice()).unwrap();
                if r.device_id == id.to_string() && r.status == PollStatus::Ok as i32 {
                    assert!(!r.varbinds.is_empty());
                    result = true;
                }
            }
        }
        if result {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(result, "Existing snmp-poller did not produce an OK RawSnmpResult");
    poller.0.kill().unwrap();
    poller.0.wait().unwrap();
    let patch = client
        .patch(format!("{base}/api/v1/cmdb/devices/{id}"))
        .bearer_auth(&admin)
        .json(&json!({"rack_id":rack_b["id"],"start_unit":1}))
        .send()
        .await
        .unwrap();
    assert!(patch.status().is_success());
    wait_tasks(&bus, &site_b).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let old: HashSet<_> = tasks(&bus, &site_a).await.into_iter().map(|t| t.task_id).collect();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after: HashSet<_> = tasks(&bus, &site_a).await.into_iter().map(|t| t.task_id).collect();
    assert_eq!(old, after, "Old site schedule kept publishing after resync");
    let before = tasks(&bus, &site_b).await.len();
    cmdb.0.kill().unwrap();
    cmdb.0.wait().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(tasks(&bus, &site_b).await.len() > before, "CMDB outage erased last known schedules");
    terminate(&mut schedulers[survivor]);
    wait_exit(&mut schedulers[survivor]).await;
    assert!(members(&membership).await.unwrap().is_empty());
    let logs = std::fs::read_to_string(scheduler_log).unwrap() + &std::fs::read_to_string(scheduler_b_log).unwrap();
    assert!(logs.contains("Devices with multiple management IPs skipped"));
    assert!(!logs.contains(&reader));
    assert!(!logs.contains(&admin));
    assert!(logs.contains("Scheduler stopped"));
}
