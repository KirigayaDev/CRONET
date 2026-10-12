//! Доступ к учётным данным устройств.
//!
//! * Сообщения в NATS несут только ссылку (`vault:secret/snmp/site-a/core`).
//! * Секреты живут в `SecretString` (не попадают в `Debug`, обнуляются при drop).
//! * `CachedCredStore` кэширует ответ Vault с TTL (`moka`), одновременные запросы
//!   одной и той же ссылки схлопываются в один поход в Vault.

mod cache;
mod credential;
mod env;
mod vault;

pub use cache::CachedCredStore;
pub use credential::{AuthProto, CredError, Credential, PrivProto, V3Credential};
pub use env::EnvCredStore;
pub use vault::{VaultAuth, VaultConfig, VaultCredStore};

use std::sync::Arc;

#[async_trait::async_trait]
pub trait CredStore: Send + Sync {
    /// Возвращает учётные данные по ссылке. Ошибки не содержат секретов.
    async fn get(&self, reference: &str) -> Result<Arc<Credential>, CredError>;
}
