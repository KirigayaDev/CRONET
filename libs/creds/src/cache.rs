use std::{sync::Arc, time::Duration};

use moka::future::Cache;

use crate::{CredError, CredStore, Credential};

/// Кэш с TTL поверх любого `CredStore`. Ошибки не кэшируются; параллельные
/// запросы одной ссылки выполняют один обращение к backend.
pub struct CachedCredStore<S> {
    inner: Arc<S>,
    cache: Cache<String, Arc<Credential>>,
}

impl<S: CredStore + 'static> CachedCredStore<S> {
    pub fn new(inner: S, ttl: Duration, max_entries: u64) -> Self {
        Self { inner: Arc::new(inner), cache: Cache::builder().time_to_live(ttl).max_capacity(max_entries).build() }
    }
}

#[async_trait::async_trait]
impl<S: CredStore + 'static> CredStore for CachedCredStore<S> {
    async fn get(&self, reference: &str) -> Result<Arc<Credential>, CredError> {
        let inner = self.inner.clone();
        let key = reference.to_owned();
        self.cache
            .try_get_with(key.clone(), async move { inner.get(&key).await })
            .await
            .map_err(|e: Arc<CredError>| (*e).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting(AtomicUsize, bool);

    #[async_trait::async_trait]
    impl CredStore for Counting {
        async fn get(&self, _r: &str) -> Result<Arc<Credential>, CredError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(30)).await;
            if self.1 {
                Credential::from_json(br#"{"version":"v2c","community":"c"}"#).map(Arc::new)
            } else {
                Err(CredError::Backend("down".into()))
            }
        }
    }

    #[tokio::test]
    async fn coalesces_and_caches() {
        let store = Arc::new(CachedCredStore::new(Counting(AtomicUsize::new(0), true), Duration::from_secs(60), 10));
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let s = store.clone();
            tasks.push(tokio::spawn(async move { s.get("vault:x").await.is_ok() }));
        }
        for t in tasks {
            assert!(t.await.unwrap());
        }
        assert!(store.get("vault:x").await.is_ok());
        assert_eq!(store.inner.0.load(Ordering::SeqCst), 1, "backend должен вызываться один раз");
    }

    #[tokio::test]
    async fn errors_are_not_cached() {
        let store = CachedCredStore::new(Counting(AtomicUsize::new(0), false), Duration::from_secs(60), 10);
        assert!(store.get("vault:x").await.is_err());
        assert!(store.get("vault:x").await.is_err());
        assert_eq!(store.inner.0.load(Ordering::SeqCst), 2);
    }
}
