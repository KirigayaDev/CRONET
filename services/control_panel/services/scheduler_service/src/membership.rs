use crate::{
    config::{safe_name, DEDUP_WINDOW},
    error::{Error, Result},
    model::ScheduleKey,
};
use async_nats::jetstream::{
    kv::{CreateErrorKind, Store},
    stream::RetentionPolicy,
};
use bus::Bus;
use futures::TryStreamExt;
use hashring::HashRing;
use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

#[derive(Clone, Debug, PartialEq)]
struct Node {
    instance: String,
    index: u16,
}
impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.instance.as_bytes());
        state.write(&self.index.to_be_bytes());
    }
}
struct Stable<'a>(&'a str);
impl Hash for Stable<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_bytes());
    }
}
pub struct Ownership {
    ring: HashRing<Node>,
    pub members: Vec<String>,
}
impl Ownership {
    pub fn new(mut members: Vec<String>) -> Self {
        members.sort();
        members.dedup();
        let mut ring = HashRing::new();
        ring.batch_add(
            members.iter().flat_map(|id| (0..128).map(move |index| Node { instance: id.clone(), index })).collect(),
        );
        Self { ring, members }
    }
    pub fn owner(&self, key: &ScheduleKey) -> Option<&str> {
        self.ring.get(&Stable(&key.stable())).map(|n| n.instance.as_str())
    }
}
pub async fn bucket(bus: &Bus, name: &str, ttl: Duration, replicas: usize, fired: bool) -> Result<Store> {
    // Bus::kv retains its existing behavior. Concurrent first creation is handled here.
    let store = match bus.kv(name, ttl, replicas).await {
        Ok(store) => store,
        Err(_) => bus.js.get_key_value(name).await.map_err(|_| Error::Coordination)?,
    };
    let status = store.status().await.map_err(|_| Error::Coordination)?;
    let age = status.max_age();
    if status.info.config.num_replicas != replicas
        || status.info.config.max_messages_per_subject != 1
        || (!fired && age != ttl)
        || (fired && (age < ttl || age > Duration::from_secs(7 * 86400)))
    {
        return Err(Error::Coordination);
    }
    Ok(store)
}
pub async fn validate_tasks_stream(bus: &Bus) -> Result<()> {
    let mut stream = bus.js.get_stream(contracts::subjects::TASKS_STREAM).await.map_err(|_| Error::Coordination)?;
    let config = &stream.info().await.map_err(|_| Error::Coordination)?.config;
    if config.retention != RetentionPolicy::WorkQueue
        || config.duplicate_window < DEDUP_WINDOW
        || !config.subjects.iter().any(|s| s == contracts::subjects::TASKS_FILTER)
    {
        return Err(Error::Coordination);
    }
    Ok(())
}
pub struct Membership {
    store: Store,
    id: String,
    value: String,
    revision: u64,
}
impl Membership {
    pub async fn join(store: Store, id: String) -> Result<Self> {
        let value = uuid::Uuid::new_v4().to_string();
        let revision = store.create(&id, value.clone().into()).await.map_err(|_| Error::Coordination)?;
        Ok(Self { store, id, value, revision })
    }
    pub async fn heartbeat(&mut self) -> Result<Vec<String>> {
        self.revision = match self.store.update(&self.id, self.value.clone().into(), self.revision).await {
            Ok(revision) => revision,
            Err(_) => {
                if self.store.get(&self.id).await.map_err(|_| Error::Coordination)?.is_some() {
                    return Err(Error::Coordination);
                }
                self.store.create(&self.id, self.value.clone().into()).await.map_err(|_| Error::Coordination)?
            }
        };
        members(&self.store).await
    }
    pub async fn leave(&self) {
        let _ = self.store.delete_expect_revision(&self.id, Some(self.revision)).await;
    }
}
pub async fn members(store: &Store) -> Result<Vec<String>> {
    let mut stream = store.keys().await.map_err(|_| Error::Coordination)?;
    let mut ids = Vec::new();
    while let Some(id) = stream.try_next().await.map_err(|_| Error::Coordination)? {
        if ids.len() >= 1024 || !safe_name(&id) {
            return Err(Error::Coordination);
        }
        ids.push(id);
    }
    ids.sort();
    ids.dedup();
    Ok(ids)
}
pub(crate) fn already_exists(error: &async_nats::jetstream::kv::CreateError) -> bool {
    error.kind() == CreateErrorKind::AlreadyExists
}
