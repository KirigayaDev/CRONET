use crate::{
    error::{Error, Result},
    model::Occurrence,
};
use bus::Bus;

#[async_trait::async_trait]
pub trait TaskPublisher: Send + Sync {
    async fn publish(&self, occurrence: &Occurrence) -> Result<()>;
}
pub struct JetStreamPublisher(pub Bus);
#[async_trait::async_trait]
impl TaskPublisher for JetStreamPublisher {
    async fn publish(&self, occurrence: &Occurrence) -> Result<()> {
        self.0
            .publish_proto(
                contracts::subjects::snmp_task(&occurrence.spec.target.site),
                &occurrence.task_id.to_string(),
                &occurrence.task(),
            )
            .await
            .map_err(|_| Error::Publish)
    }
}
