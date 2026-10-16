#![cfg(test)]
use bus::{Bus, BusConfig};
use contracts::{netmon::v1::PollTask, subjects};
use prost::Message;
use scheduler_service::{
    config::{Definition, Schedule},
    coordinator::{CoordinationStore, Coordinator, NatsCoordination, Outcome},
    membership::{bucket, members, Membership, Ownership},
    model::{unix_ms, Occurrence, Spec, Target},
    publisher::{JetStreamPublisher, TaskPublisher},
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use uuid::Uuid;

fn enabled() -> bool {
    std::env::var("SCHEDULER_E2E").as_deref() == Ok("1")
}
async fn bus() -> Bus {
    let bus = Bus::connect(&BusConfig {
        url: std::env::var("SCHEDULER_TEST_NATS_URL").unwrap(),
        creds_file: None,
        name: "scheduler-integration".into(),
    })
    .await
    .unwrap();
    bus.ensure_streams(1, &bus::StreamLimits::dev()).await.unwrap();
    bus
}
fn namespace() -> String {
    format!("D2_{}", Uuid::new_v4().simple())
}
fn occurrence() -> Occurrence {
    let s:Definition=serde_json::from_value(serde_json::json!({"schedules":[{"id":format!("test-{}",Uuid::new_v4().simple()),"enabled":true,"site":format!("test-{}",Uuid::new_v4().simple()),"interval":"1s","jitter":"0s","snmp_version":"v2c","credential_ref":"env:FIXTURE","get_oids":["1.3.6.1.2.1.1.1.0"],"walk_roots":[],"timeout":"1s","task_deadline":"30s","max_lateness":"30s","max_repetitions":20,"port":161}]})).unwrap();
    let schedule: Schedule = s.schedules.into_iter().next().unwrap();
    let site = schedule.site.clone();
    Occurrence::new(
        Spec { schedule, target: Target { device_id: Uuid::new_v4(), site, ip: "192.0.2.1".parse().unwrap() } },
        unix_ms(SystemTime::now()) / 1000,
    )
}
async fn tasks(bus: &Bus, id: Uuid) -> Vec<PollTask> {
    let mut stream = bus.js.get_stream(subjects::TASKS_STREAM).await.unwrap();
    let state = stream.info().await.unwrap().state.clone();
    let mut out = Vec::new();
    for seq in state.first_sequence..=state.last_sequence {
        if let Ok(raw) = stream.get_raw_message(seq).await {
            let task = PollTask::decode(raw.payload).unwrap();
            if task.task_id == id.to_string() {
                out.push(task);
            }
        }
    }
    out
}
fn ring(id: &str) -> Ownership {
    Ownership::new(vec![id.into()])
}
#[tokio::test]
async fn two_divergent_owners_store_exactly_one_poll_task() {
    if !enabled() {
        return;
    }
    let bus = bus().await;
    let store = Arc::new(
        NatsCoordination::connect(&bus, &namespace(), Duration::from_secs(2), Duration::from_secs(3600), 1)
            .await
            .unwrap(),
    );
    let publisher = Arc::new(JetStreamPublisher(bus.clone()));
    let o = occurrence();
    let a = Coordinator { instance: "A".into(), store: store.clone(), publisher: publisher.clone() };
    let b = Coordinator { instance: "B".into(), store: store.clone(), publisher };
    let a_ring = ring("A");
    let b_ring = ring("B");
    let (x, y) = tokio::join!(a.fire(&a_ring, &o), b.fire(&b_ring, &o));
    assert!(x == Outcome::Done || y == Outcome::Done);
    assert_eq!(tasks(&bus, o.task_id).await.len(), 1);
    assert!(store.fired(&o.task_id.to_string()).await.unwrap());
    assert_eq!(b.fire(&ring("B"), &o).await, Outcome::Done);
    assert_eq!(tasks(&bus, o.task_id).await.len(), 1);
}
#[tokio::test]
async fn real_claim_expiry_and_publish_before_marker_crash_deduplicate() {
    if !enabled() {
        return;
    }
    let bus = bus().await;
    let store = Arc::new(
        NatsCoordination::connect(&bus, &namespace(), Duration::from_secs(1), Duration::from_secs(3600), 1)
            .await
            .unwrap(),
    );
    let publisher = Arc::new(JetStreamPublisher(bus.clone()));
    let c = Coordinator { instance: "B".into(), store: store.clone(), publisher: publisher.clone() };
    for after_publish in [false, true] {
        let o = occurrence();
        let key = o.task_id.to_string();
        let revision = store.claim(&key, "crashed-A").await.unwrap().unwrap();
        if after_publish {
            publisher.publish(&o).await.unwrap();
        }
        assert_eq!(c.fire(&ring("B"), &o).await, Outcome::Retry);
        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(c.fire(&ring("B"), &o).await, Outcome::Done);
        // A live successor lease must survive cleanup from the crashed session.
        let successor = store.claim(&key, "successor").await.unwrap().unwrap();
        assert!(store.release(&key, revision).await.is_err());
        assert!(store.claim(&key, "third-owner").await.unwrap().is_none());
        store.release(&key, successor).await.unwrap();
        assert_eq!(tasks(&bus, o.task_id).await.len(), 1);
    }
}
#[tokio::test]
async fn real_membership_ttl_transfers_next_occurrence_to_survivor() {
    if !enabled() {
        return;
    }
    let bus = bus().await;
    let ns = namespace();
    let kv = bucket(&bus, &format!("{ns}_MEMBERS"), Duration::from_secs(1), 1, false).await.unwrap();
    let mut a = Membership::join(kv.clone(), "A".into()).await.unwrap();
    let mut b = Membership::join(kv.clone(), "B".into()).await.unwrap();
    a.heartbeat().await.unwrap();
    b.heartbeat().await.unwrap();
    let ids = members(&kv).await.unwrap();
    assert_eq!(ids.len(), 2);
    let o = occurrence();
    let both = Ownership::new(ids);
    let owner = both.owner(&o.spec.key()).unwrap().to_owned();
    let store = Arc::new(
        NatsCoordination::connect(&bus, &ns, Duration::from_secs(2), Duration::from_secs(3600), 1).await.unwrap(),
    );
    let publisher = Arc::new(JetStreamPublisher(bus.clone()));
    let c = Coordinator { instance: owner.clone(), store: store.clone(), publisher: publisher.clone() };
    assert_eq!(c.fire(&both, &o).await, Outcome::Done);
    let (mut survivor, id) = if owner == "A" {
        drop(a);
        (b, "B")
    } else {
        drop(b);
        (a, "A")
    };
    for _ in 0..7 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        survivor.heartbeat().await.unwrap();
    }
    let remaining = members(&kv).await.unwrap();
    assert_eq!(remaining, vec![id.to_owned()]);
    let next = Occurrence::new(o.spec, unix_ms(SystemTime::now()) / 1000 + 1);
    tokio::time::sleep(next.due().duration_since(SystemTime::now()).unwrap_or_default() + Duration::from_millis(20))
        .await;
    let c = Coordinator { instance: id.into(), store, publisher };
    assert_eq!(c.fire(&Ownership::new(remaining), &next).await, Outcome::Done);
    assert_eq!(tasks(&bus, next.task_id).await.len(), 1);
    survivor.leave().await;
    assert!(members(&kv).await.unwrap().is_empty());
}
