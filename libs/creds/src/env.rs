use std::sync::Arc;

use crate::{CredError, CredStore, Credential};

/// Хранилище для локальной разработки и тестов: ссылка `env:NAME` читает
/// JSON из переменной окружения `NETMON_CRED_NAME`.
/// В проде не использовать: секреты в окружении видны через /proc и дампы.
pub struct EnvCredStore;

#[async_trait::async_trait]
impl CredStore for EnvCredStore {
    async fn get(&self, reference: &str) -> Result<Arc<Credential>, CredError> {
        let name = reference.strip_prefix("env:").ok_or(CredError::BadReference)?;
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(CredError::BadReference);
        }
        let var = format!("NETMON_CRED_{}", name.to_ascii_uppercase());
        let json = std::env::var(&var).map_err(|_| CredError::NotFound)?;
        Ok(Arc::new(Credential::from_json(json.as_bytes())?))
    }
}
