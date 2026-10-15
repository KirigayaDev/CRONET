//! Read-only ACTIVE inventory projection. Scheduler owns schedulability decisions.
use crate::{models::Pagination, repository::Repository, Result, Service};
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct PollTarget {
    pub device_id: Uuid,
    pub hostname: Option<String>,
    pub site_id: Option<Uuid>,
    pub site_name: Option<String>,
    pub management_ips: Vec<String>,
}
impl Service {
    pub async fn list_poll_targets(&self, limit: i64, after: Option<Uuid>) -> Result<Vec<PollTarget>> {
        Pagination::new(limit, 0)?;
        Repository::list_poll_targets(&mut *self.pool.acquire().await?, limit, after).await
    }
}
