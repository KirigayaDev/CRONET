use crate::{
    coordinator::Outcome,
    model::{next_occurrence, Occurrence, ScheduleKey, Spec},
};
use futures::StreamExt;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use tokio_util::{
    sync::CancellationToken,
    time::{delay_queue::Key, DelayQueue},
};

struct State {
    spec: Spec,
    key: Option<Key>,
    generation: u64,
    cancel: CancellationToken,
}
#[derive(Clone)]
pub struct Work {
    pub occurrence: Occurrence,
    pub generation: u64,
    pub cancel: CancellationToken,
}
#[derive(Default)]
pub struct ScheduleQueue {
    queue: DelayQueue<Work>,
    states: HashMap<ScheduleKey, State>,
    generation: u64,
}
impl ScheduleQueue {
    pub fn len(&self) -> usize {
        self.states.len()
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn reconcile(&mut self, specs: Vec<Spec>, now: SystemTime) {
        let desired: HashMap<_, _> = specs.into_iter().filter(|s| s.schedule.enabled).map(|s| (s.key(), s)).collect();
        let removed: Vec<_> =
            self.states.iter().filter(|(k, s)| desired.get(*k) != Some(&s.spec)).map(|(k, _)| k.clone()).collect();
        for key in removed {
            if let Some(state) = self.states.remove(&key) {
                state.cancel.cancel();
                if let Some(key) = state.key {
                    self.queue.try_remove(&key);
                }
            }
        }
        for (key, spec) in desired {
            if self.states.contains_key(&key) {
                continue;
            }
            self.generation = self.generation.wrapping_add(1);
            let cancel = CancellationToken::new();
            let occurrence = next_occurrence(spec.clone(), now, None);
            let work = Work { occurrence, generation: self.generation, cancel: cancel.clone() };
            let delay = work.occurrence.due().duration_since(now).unwrap_or_default();
            let queue_key = self.queue.insert(work, delay);
            self.states.insert(key, State { spec, key: Some(queue_key), generation: self.generation, cancel });
        }
    }
    pub async fn next(&mut self) -> Option<Work> {
        let work = self.queue.next().await?.into_inner();
        if let Some(state) = self.states.get_mut(&work.occurrence.spec.key()) {
            state.key = None;
        }
        Some(work)
    }
    pub fn complete(&mut self, work: Work, outcome: Outcome, now: SystemTime) {
        let Some(state) = self.states.get_mut(&work.occurrence.spec.key()) else { return };
        if state.generation != work.generation {
            return;
        }
        let (occurrence, delay) = if outcome == Outcome::Retry && !work.occurrence.expired(now) {
            (work.occurrence, Duration::from_millis(500))
        } else {
            let next = next_occurrence(state.spec.clone(), now, Some(work.occurrence.slot));
            let delay = next.due().duration_since(now).unwrap_or_default();
            (next, delay)
        };
        state.key = Some(
            self.queue.insert(Work { occurrence, generation: state.generation, cancel: state.cancel.clone() }, delay),
        );
    }
    pub fn clear(&mut self) {
        self.reconcile(Vec::new(), SystemTime::now());
    }
}
