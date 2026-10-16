use crate::config::{Schedule, Version};
use contracts::netmon::v1::{PollTask, SnmpVersion};
use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub device_id: Uuid,
    pub site: String,
    pub ip: IpAddr,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScheduleKey {
    pub schedule_id: String,
    pub device_id: Uuid,
}
impl ScheduleKey {
    pub fn stable(&self) -> String {
        format!("{}/{}", self.schedule_id, self.device_id)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spec {
    pub schedule: Schedule,
    pub target: Target,
}
impl Spec {
    pub fn key(&self) -> ScheduleKey {
        ScheduleKey { schedule_id: self.schedule.id.clone(), device_id: self.target.device_id }
    }
}
pub fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().min(u128::from(u64::MAX)) as u64
}
#[derive(Clone, Debug)]
pub struct Occurrence {
    pub spec: Spec,
    pub slot: u64,
    pub task_id: Uuid,
    pub due_ms: u64,
}
impl Occurrence {
    pub fn new(spec: Spec, slot: u64) -> Self {
        // Fixed protocol namespace. Canonical string fields are separated unambiguously.
        let name = format!("{}\n{}\n{}\n{}", spec.schedule.id, spec.target.device_id, spec.target.ip, slot);
        let task_id = Uuid::new_v5(&Uuid::from_u128(0x00bd59b21fa75ff89f505974ebf90b3a), name.as_bytes());
        let jitter_ms = spec.schedule.jitter.as_millis() as u64;
        let hash = u64::from_be_bytes(task_id.as_bytes()[..8].try_into().unwrap_or([0; 8]));
        let offset = if jitter_ms == 0 { 0 } else { hash % (jitter_ms + 1) };
        let due_ms = slot.saturating_mul(spec.schedule.interval.as_millis() as u64).saturating_add(offset);
        Self { spec, slot, task_id, due_ms }
    }
    pub fn expired(&self, now: SystemTime) -> bool {
        let now = unix_ms(now);
        now > self.due_ms.saturating_add(self.spec.schedule.max_lateness.as_millis() as u64)
            || now >= self.due_ms.saturating_add(self.spec.schedule.task_deadline.as_millis() as u64)
    }
    pub fn due(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.due_ms)
    }
    pub fn task(&self) -> PollTask {
        let s = &self.spec.schedule;
        let deadline = self.due_ms.saturating_add(s.task_deadline.as_millis() as u64);
        PollTask {
            task_id: self.task_id.to_string(),
            device_id: self.spec.target.device_id.to_string(),
            target: SocketAddr::new(self.spec.target.ip, s.port).to_string(),
            snmp_version: match s.snmp_version {
                Version::V2c => SnmpVersion::V2c,
                Version::V3 => SnmpVersion::V3,
            } as i32,
            credential_ref: s.credential_ref.clone(),
            get_oids: s.get_oids.clone(),
            walk_roots: s.walk_roots.clone(),
            timeout_ms: s.timeout.as_millis() as u32,
            deadline: Some(prost_types::Timestamp {
                seconds: (deadline / 1000) as i64,
                nanos: ((deadline % 1000) * 1_000_000) as i32,
            }),
            attempt: 1,
            max_repetitions: s.max_repetitions,
        }
    }
}
pub fn next_occurrence(spec: Spec, now: SystemTime, after: Option<u64>) -> Occurrence {
    let slot =
        (unix_ms(now) / (spec.schedule.interval.as_millis() as u64)).max(after.map_or(0, |s| s.saturating_add(1)));
    let current = Occurrence::new(spec.clone(), slot);
    if current.expired(now) {
        Occurrence::new(spec, slot.saturating_add(1))
    } else {
        current
    }
}
