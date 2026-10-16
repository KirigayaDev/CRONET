use crate::{
    error::{Error, Result},
    membership::{already_exists, bucket, Ownership},
    model::{unix_ms, Occurrence},
    publisher::TaskPublisher,
};
use async_nats::jetstream::kv::Store;
use bus::Bus;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[async_trait::async_trait]
pub trait CoordinationStore: Send + Sync {
    async fn fired(&self, key: &str) -> Result<bool>;
    async fn claim(&self, key: &str, owner: &str) -> Result<Option<u64>>;
    async fn mark_fired(&self, key: &str) -> Result<()>;
    async fn release(&self, key: &str, revision: u64) -> Result<()>;
}
pub struct NatsCoordination {
    pub claims: Store,
    pub fired: Store,
}
impl NatsCoordination {
    pub async fn connect(
        bus: &Bus,
        namespace: &str,
        claim_ttl: Duration,
        fired_ttl: Duration,
        replicas: usize,
    ) -> Result<Self> {
        Ok(Self {
            claims: bucket(bus, &format!("{namespace}_CLAIMS"), claim_ttl, replicas, false).await?,
            fired: bucket(bus, &format!("{namespace}_FIRED"), fired_ttl, replicas, true).await?,
        })
    }
}
#[async_trait::async_trait]
impl CoordinationStore for NatsCoordination {
    async fn fired(&self, key: &str) -> Result<bool> {
        Ok(self.fired.get(key).await.map_err(|_| Error::Coordination)?.is_some())
    }
    async fn claim(&self, key: &str, owner: &str) -> Result<Option<u64>> {
        match self.claims.create(key, owner.to_owned().into()).await {
            Ok(revision) => Ok(Some(revision)),
            Err(e) if already_exists(&e) => Ok(None),
            Err(_) => Err(Error::Coordination),
        }
    }
    async fn mark_fired(&self, key: &str) -> Result<()> {
        match self.fired.create(key, "1".into()).await {
            Ok(_) => Ok(()),
            Err(e) if already_exists(&e) => Ok(()),
            Err(_) => Err(Error::Coordination),
        }
    }
    async fn release(&self, key: &str, revision: u64) -> Result<()> {
        self.claims.delete_expect_revision(key, Some(revision)).await.map_err(|_| Error::Coordination)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Retry,
    NotOwner,
    Expired,
    Cancelled,
}
pub struct Coordinator {
    pub instance: String,
    pub store: Arc<dyn CoordinationStore>,
    pub publisher: Arc<dyn TaskPublisher>,
}
impl Coordinator {
    pub async fn fire(&self, ring: &Ownership, occurrence: &Occurrence) -> Outcome {
        if ring.owner(&occurrence.spec.key()) != Some(self.instance.as_str()) {
            return Outcome::NotOwner;
        }
        if occurrence.expired(SystemTime::now()) {
            return Outcome::Expired;
        }
        if unix_ms(SystemTime::now()) < occurrence.due_ms {
            return Outcome::Retry;
        }
        let key = occurrence.task_id.to_string();
        let acquire = async {
            if self.store.fired(&key).await? {
                return Ok(None);
            }
            let revision = self.store.claim(&key, &self.instance).await?;
            Ok::<_, Error>(Some(revision))
        };
        let revision = match tokio::time::timeout(Duration::from_secs(4), acquire).await {
            Ok(Ok(None)) => return Outcome::Done,
            Ok(Ok(Some(Some(revision)))) => revision,
            _ => return Outcome::Retry,
        };
        // Under 8s of work after acquisition; production claim TTL is at least 12s.
        let operation = async {
            // Another owner may have finished between the first read and our CAS create.
            if self.store.fired(&key).await? {
                return Ok(());
            }
            if occurrence.expired(SystemTime::now()) {
                return Err(Error::Publish);
            }
            tokio::time::timeout(Duration::from_secs(4), self.publisher.publish(occurrence))
                .await
                .map_err(|_| Error::Publish)??;
            self.store.mark_fired(&key).await?;
            Ok::<_, Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(8), operation).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.store.release(&key, revision)).await;
        if matches!(result, Ok(Ok(()))) {
            Outcome::Done
        } else {
            Outcome::Retry
        }
    }
}
