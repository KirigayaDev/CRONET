#![cfg(test)]
use scheduler_service::{
    cmdb,
    config::{self, Definition, Schedule, TokenSource},
    coordinator::{CoordinationStore, Coordinator, Outcome},
    error::{Error, Result},
    membership::Ownership,
    model::{next_occurrence, unix_ms, Occurrence, ScheduleKey, Spec, Target},
    publisher::TaskPublisher,
    runtime,
    schedule::ScheduleQueue,
};
use secrecy::ExposeSecret;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

fn config_json() -> Value {
    json!({"schedules":[{"id":"core-fast","enabled":true,"site":"site-a","interval":"10s","jitter":"0s","snmp_version":"v2c","credential_ref":"env:FIXTURE","get_oids":["1.3.6.1.2.1.1.1.0"],"walk_roots":[],"timeout":"1s","task_deadline":"30s","max_repetitions":20,"port":161}]})
}
fn definition(value: Value) -> Result<Definition> {
    config::load_definition(None, Some(&value.to_string()))
}
fn schedule() -> Schedule {
    definition(config_json()).unwrap().schedules.remove(0)
}
fn spec() -> Spec {
    Spec {
        schedule: schedule(),
        target: Target { device_id: Uuid::from_u128(1), site: "site-a".into(), ip: "192.0.2.1".parse().unwrap() },
    }
}
fn wall(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}
fn occurrence() -> Occurrence {
    next_occurrence(spec(), SystemTime::now(), None)
}
#[test]
fn valid_human_duration_schedule() {
    let s = schedule();
    assert_eq!(s.interval, Duration::from_secs(10));
    assert_eq!(s.max_lateness, Duration::from_secs(30));
}
macro_rules! invalid {
    ($name:ident,$field:literal,$value:expr) => {
        #[test]
        fn $name() {
            let mut v = config_json();
            v["schedules"][0][$field] = $value;
            assert!(definition(v).is_err());
        }
    };
}
invalid!(zero_interval, "interval", json!("0s"));
invalid!(jitter_equal_interval, "jitter", json!("10s"));
invalid!(jitter_greater_interval, "jitter", json!("11s"));
invalid!(empty_credential, "credential_ref", json!("   "));
invalid!(empty_oids, "get_oids", json!([]));
invalid!(invalid_oid, "get_oids", json!(["1.3.bad"]));
invalid!(invalid_site, "site", json!("site.*"));
invalid!(snmp_v1_rejected, "snmp_version", json!("v1"));
invalid!(too_many_get_oids, "get_oids", json!(vec!["1.3.6"; 101]));
invalid!(too_many_walk_roots, "walk_roots", json!(vec!["1.3.6"; 21]));
invalid!(zero_port, "port", json!(0));
invalid!(zero_timeout, "timeout", json!("0s"));
invalid!(zero_deadline, "task_deadline", json!("0s"));
invalid!(unbounded_lateness_rejected, "max_lateness", json!("121s"));
invalid!(interval_safety_cap, "interval", json!("25h"));
invalid!(submillisecond_slot_rejected, "interval", json!("1ns"));
#[test]
fn duplicate_schedule_id_rejected() {
    let mut v = config_json();
    let duplicate = v["schedules"][0].clone();
    v["schedules"].as_array_mut().unwrap().push(duplicate);
    assert!(definition(v).is_err());
}
#[test]
fn config_file_takes_precedence_without_silent_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("schedule.json");
    std::fs::write(&path, config_json().to_string()).unwrap();
    assert!(config::load_definition(Some(&path), Some("invalid json")).is_ok());
    std::fs::write(&path, "invalid file").unwrap();
    assert!(config::load_definition(Some(&path), Some(&config_json().to_string())).is_err());
}
#[test]
fn token_file_precedence_and_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "synthetic-one\n").unwrap();
    let source = TokenSource::select(Some(path.clone()), Some("unused-synthetic".into())).unwrap();
    assert_eq!(source.load().unwrap().expose_secret(), "synthetic-one");
    std::fs::write(path, "synthetic-two").unwrap();
    assert_eq!(source.load().unwrap().expose_secret(), "synthetic-two");
}
#[test]
fn fired_ttl_covers_long_intervals() {
    let mut v = config_json();
    v["schedules"][0]["interval"] = json!("24h");
    assert_eq!(definition(v).unwrap().fired_ttl(), Duration::from_secs(48 * 3600));
    assert_eq!(definition(config_json()).unwrap().fired_ttl(), Duration::from_secs(3600));
}
#[test]
fn invalid_config_errors_never_echo_input_secrets() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scheduler-service"))
        .env_clear()
        .env("SCHEDULER_CONFIG_JSON", "D2_SYNTHETIC_SECRET_SENTINEL")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("D2_SYNTHETIC_SECRET_SENTINEL"));
    assert!(!format!("{} {:?}", Error::Config, Error::Cmdb).contains("SENTINEL"));
}
fn ring(ids: &[&str]) -> Ownership {
    Ownership::new(ids.iter().map(|id| (*id).to_owned()).collect())
}
fn keys() -> Vec<ScheduleKey> {
    (0..1000).map(|id| ScheduleKey { schedule_id: "core-fast".into(), device_id: Uuid::from_u128(id) }).collect()
}
#[test]
fn single_replica_owns_every_key() {
    let r = ring(&["A"]);
    for key in keys() {
        assert_eq!(r.owner(&key), Some("A"));
    }
}
#[test]
fn two_replicas_split_deterministically_and_order_does_not_matter() {
    let a = ring(&["A", "B"]);
    let b = ring(&["B", "A", "A"]);
    let mut owners = HashSet::new();
    for key in keys() {
        assert_eq!(a.owner(&key), b.owner(&key));
        owners.insert(a.owner(&key).unwrap());
    }
    assert_eq!(owners.len(), 2);
}
#[test]
fn removal_redistributes_only_removed_members_keys() {
    let a = ring(&["A", "B", "C"]);
    let b = ring(&["A", "B"]);
    let mut moved = 0;
    for key in keys() {
        if a.owner(&key) != b.owner(&key) {
            moved += 1;
            assert_eq!(a.owner(&key), Some("C"));
        }
    }
    assert!(moved > 0 && moved < 1000);
}
#[test]
fn addition_moves_only_a_subset_to_new_member() {
    let a = ring(&["A", "B"]);
    let b = ring(&["A", "B", "C"]);
    let mut moved = 0;
    for key in keys() {
        if a.owner(&key) != b.owner(&key) {
            moved += 1;
            assert_eq!(b.owner(&key), Some("C"));
        }
    }
    assert!(moved > 0 && moved < 800);
}
#[test]
fn deterministic_occurrence_identity_and_jitter() {
    let mut s = spec();
    s.schedule.jitter = Duration::from_secs(5);
    let a = Occurrence::new(s.clone(), 12);
    let b = Occurrence::new(s.clone(), 12);
    assert_eq!(a.task_id, b.task_id);
    assert_eq!(a.due_ms, b.due_ms);
    assert!(a.due_ms >= 120000 && a.due_ms <= 125000);
    assert_ne!(a.task_id, Occurrence::new(s.clone(), 13).task_id);
    s.target.device_id = Uuid::from_u128(2);
    assert_ne!(a.task_id, Occurrence::new(s.clone(), 12).task_id);
    s = spec();
    s.schedule.id = "different".into();
    assert_ne!(a.task_id, Occurrence::new(s.clone(), 12).task_id);
    s = spec();
    s.target.ip = "192.0.2.2".parse().unwrap();
    assert_ne!(a.task_id, Occurrence::new(s, 12).task_id);
}
#[test]
fn task_mapping_ipv6_and_deterministic_deadline() {
    let mut s = spec();
    s.target.ip = "2001:db8::1".parse().unwrap();
    let o = Occurrence::new(s, 12);
    let t = o.task();
    assert_eq!(t.target, "[2001:db8::1]:161");
    assert_eq!(t.attempt, 1);
    assert_eq!(t.timeout_ms, 1000);
    assert_eq!(t.credential_ref, "env:FIXTURE");
    assert_eq!(t.deadline.unwrap().seconds, 150);
    assert_eq!(t.task_id, o.task_id.to_string());
}
#[test]
fn restart_has_bounded_lateness_and_no_historical_backfill() {
    let mut s = spec();
    s.schedule.max_lateness = Duration::from_secs(1);
    let o = next_occurrence(s.clone(), wall(100003), None);
    assert_eq!(o.slot, 10001);
    assert_eq!(next_occurrence(s, wall(100000), None).slot, 10000);
}
#[tokio::test(start_paused = true)]
async fn delay_queue_fires_due_and_reschedules_without_real_wait() {
    let mut s = spec();
    s.schedule.max_lateness = Duration::ZERO;
    let mut q = ScheduleQueue::default();
    q.reconcile(vec![s], wall(101));
    assert!(tokio::time::timeout(Duration::from_secs(8), q.next()).await.is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    let work = q.next().await.unwrap();
    assert_eq!(work.occurrence.due_ms, 110000);
    let first = work.occurrence.task_id;
    q.complete(work, Outcome::Done, wall(110));
    let work = q.next().await.unwrap();
    assert_eq!(work.occurrence.slot, 12);
    assert_ne!(work.occurrence.task_id, first);
}
#[tokio::test(start_paused = true)]
async fn removal_and_ip_change_cancel_old_work() {
    let mut q = ScheduleQueue::default();
    let s = spec();
    q.reconcile(vec![s.clone()], wall(100));
    let old = q.next().await.unwrap();
    let mut changed = s;
    changed.target.ip = "192.0.2.2".parse().unwrap();
    q.reconcile(vec![changed], wall(100));
    assert!(old.cancel.is_cancelled());
    q.complete(old, Outcome::Retry, wall(100));
    assert_eq!(q.next().await.unwrap().occurrence.spec.target.ip.to_string(), "192.0.2.2");
    q.reconcile(Vec::new(), wall(100));
    assert_eq!(q.len(), 0);
    assert!(q.is_empty());
}
#[tokio::test(start_paused = true)]
async fn site_change_and_disabled_schedule_remove_future_entries() {
    let s = spec();
    let mut q = ScheduleQueue::default();
    q.reconcile(vec![s.clone()], wall(100));
    let mut target = s.target.clone();
    target.site = "site-b".into();
    let specs = runtime::specs(&[target], std::slice::from_ref(&s.schedule), &ring(&["A"]), "A").unwrap();
    q.reconcile(specs, wall(100));
    assert_eq!(q.len(), 0);
    let mut disabled = s;
    disabled.schedule.enabled = false;
    q.reconcile(vec![disabled], wall(100));
    assert_eq!(q.len(), 0);
}
#[test]
fn cmdb_missing_ambiguous_unplaced_and_invalid_site_are_skipped() {
    use contracts::cronet::cmdb::v1::PollTarget;
    let row = |id, site: Option<&str>, ips: Vec<&str>| PollTarget {
        device_id: Uuid::from_u128(id).to_string(),
        hostname: None,
        site_id: site.map(|_| Uuid::nil().to_string()),
        site_name: site.map(Into::into),
        management_ips: ips.into_iter().map(Into::into).collect(),
    };
    let (targets, d) = cmdb::normalize(vec![
        row(1, Some("site-a"), vec!["192.0.2.1"]),
        row(2, None, vec!["192.0.2.2"]),
        row(3, Some("site-a"), vec![]),
        row(4, Some("site-a"), vec!["192.0.2.3", "192.0.2.4"]),
        row(5, Some("unsafe.site"), vec!["192.0.2.5"]),
    ])
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!((d.unplaced, d.missing_ip, d.ambiguous_ip, d.invalid_site), (1, 1, 1, 1));
}

#[derive(Default)]
struct State {
    fired: HashSet<String>,
    claims: HashMap<String, (u64, tokio::time::Instant)>,
    revision: u64,
}
#[derive(Default)]
struct Store {
    state: Mutex<State>,
    fail_marker: AtomicBool,
}
#[async_trait::async_trait]
impl CoordinationStore for Store {
    async fn fired(&self, key: &str) -> Result<bool> {
        Ok(self.state.lock().unwrap().fired.contains(key))
    }
    async fn claim(&self, key: &str, _: &str) -> Result<Option<u64>> {
        let mut s = self.state.lock().unwrap();
        if s.claims.get(key).is_some_and(|(_, until)| *until > tokio::time::Instant::now()) {
            return Ok(None);
        }
        s.revision += 1;
        let revision = s.revision;
        s.claims.insert(key.to_owned(), (revision, tokio::time::Instant::now() + Duration::from_secs(2)));
        Ok(Some(revision))
    }
    async fn mark_fired(&self, key: &str) -> Result<()> {
        if self.fail_marker.load(Ordering::SeqCst) {
            return Err(Error::Coordination);
        }
        self.state.lock().unwrap().fired.insert(key.to_owned());
        Ok(())
    }
    async fn release(&self, key: &str, revision: u64) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if s.claims.get(key).is_some_and(|(rev, _)| *rev == revision) {
            s.claims.remove(key);
        }
        Ok(())
    }
}
#[derive(Default)]
struct Publisher {
    ids: Mutex<Vec<Uuid>>,
    fail: AtomicBool,
}
#[async_trait::async_trait]
impl TaskPublisher for Publisher {
    async fn publish(&self, o: &Occurrence) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::Publish);
        }
        self.ids.lock().unwrap().push(o.task_id);
        Ok(())
    }
}
fn coordinator(store: Arc<Store>, publisher: Arc<Publisher>) -> Coordinator {
    Coordinator { instance: "A".into(), store, publisher }
}
#[tokio::test]
async fn fired_marker_prevents_publish() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    let o = occurrence();
    s.mark_fired(&o.task_id.to_string()).await.unwrap();
    assert_eq!(coordinator(s, p.clone()).fire(&ring(&["A"]), &o).await, Outcome::Done);
    assert!(p.ids.lock().unwrap().is_empty());
}
#[tokio::test]
async fn claim_failure_and_non_owner_never_publish() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    let o = occurrence();
    s.claim(&o.task_id.to_string(), "B").await.unwrap();
    let c = coordinator(s, p.clone());
    assert_eq!(c.fire(&ring(&["A"]), &o).await, Outcome::Retry);
    assert_eq!(c.fire(&ring(&["B"]), &o).await, Outcome::NotOwner);
    assert!(p.ids.lock().unwrap().is_empty());
}
#[tokio::test]
async fn acknowledged_publish_marks_fired() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    let o = occurrence();
    assert_eq!(coordinator(s.clone(), p.clone()).fire(&ring(&["A"]), &o).await, Outcome::Done);
    assert!(s.fired(&o.task_id.to_string()).await.unwrap());
    assert_eq!(p.ids.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn failed_publish_does_not_mark_fired_and_can_retry() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    p.fail.store(true, Ordering::SeqCst);
    let o = occurrence();
    let c = coordinator(s.clone(), p.clone());
    assert_eq!(c.fire(&ring(&["A"]), &o).await, Outcome::Retry);
    assert!(!s.fired(&o.task_id.to_string()).await.unwrap());
    p.fail.store(false, Ordering::SeqCst);
    assert_eq!(c.fire(&ring(&["A"]), &o).await, Outcome::Done);
}
#[tokio::test(start_paused = true)]
async fn expired_claim_allows_retry_and_stale_release_cannot_delete_successor() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    let o = occurrence();
    let key = o.task_id.to_string();
    let old = s.claim(&key, "B").await.unwrap().unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    let successor = s.claim(&key, "A").await.unwrap().unwrap();
    s.release(&key, old).await.unwrap();
    assert!(s.claim(&key, "C").await.unwrap().is_none());
    s.release(&key, successor).await.unwrap();
    assert_eq!(coordinator(s, p).fire(&ring(&["A"]), &o).await, Outcome::Done);
}
#[tokio::test]
async fn crash_after_publish_reuses_identical_message_id() {
    let s = Arc::new(Store::default());
    let p = Arc::new(Publisher::default());
    let o = occurrence();
    let c = coordinator(s.clone(), p.clone());
    s.fail_marker.store(true, Ordering::SeqCst);
    assert_eq!(c.fire(&ring(&["A"]), &o).await, Outcome::Retry);
    s.fail_marker.store(false, Ordering::SeqCst);
    assert_eq!(c.fire(&ring(&["A"]), &o).await, Outcome::Done);
    let ids = p.ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
}
#[test]
fn slot_does_not_depend_on_process_start_time() {
    let a = next_occurrence(spec(), wall(100), None);
    let b = next_occurrence(spec(), wall(102), None);
    assert_eq!(a.task_id, b.task_id);
    assert_eq!(unix_ms(a.due()), 100000);
}
