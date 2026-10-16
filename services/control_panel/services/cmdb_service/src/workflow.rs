//! Transactional audit and manual field ownership shared by both transports.
use crate::{models::*, repository::Repository, service::TransactionFuture, Error, Result, Service};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum ActorType {
    User,
    Service,
}
impl ActorType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Service => "service",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Http,
    Grpc,
    CsvImport,
    Discovery,
}
impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::CsvImport => "csv_import",
            Self::Discovery => "discovery",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct MutationContext {
    pub actor_type: ActorType,
    pub actor_id: Uuid,
    pub source: Source,
    pub request_id: Uuid,
    pub import_batch_id: Option<Uuid>,
}
impl MutationContext {
    pub fn user(actor_id: Uuid, source: Source, request_id: Uuid) -> Self {
        Self { actor_type: ActorType::User, actor_id, source, request_id, import_batch_id: None }
    }
    pub fn validate(self) -> Result<()> {
        if (self.source == Source::CsvImport) != self.import_batch_id.is_some() {
            return Err(Error::validation("Import context must contain exactly the CSV batch reference"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Create,
    Update,
    Delete,
    Decommission,
    FieldOverrideSet,
    FieldOverrideClear,
}
impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Decommission => "decommission",
            Self::FieldOverrideSet => "field_override_set",
            Self::FieldOverrideClear => "field_override_clear",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, utoipa::ToSchema)]
pub struct AuditEvent {
    pub id: Uuid,
    pub actor_type: String,
    pub actor_id: Option<Uuid>,
    pub source: String,
    pub action: String,
    pub entity_type: String,
    pub entity_id: Uuid,
    #[schema(value_type=Option<Object>,additional_properties=true)]
    pub before_data: Option<Value>,
    #[schema(value_type=Option<Object>,additional_properties=true)]
    pub after_data: Option<Value>,
    pub request_id: Option<Uuid>,
    pub import_batch_id: Option<Uuid>,
    pub detected_at: DateTime<Utc>,
    pub changed_at: Option<DateTime<Utc>>,
}
#[derive(Clone, Debug, Default, Deserialize)]
pub struct AuditFilter {
    pub entity_type: Option<EntityType>,
    pub entity_id: Option<Uuid>,
    pub actor_id: Option<Uuid>,
    pub request_id: Option<Uuid>,
    pub import_batch_id: Option<Uuid>,
}
#[derive(Clone, Debug, Serialize, Deserialize, FromRow, utoipa::ToSchema)]
pub struct FieldOverride {
    pub id: Uuid,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub field_name: String,
    pub locked_by: Uuid,
    pub locked_at: DateTime<Utc>,
}

pub fn override_fields(entity: EntityType) -> &'static [&'static str] {
    match entity {
        EntityType::Devices => &["hostname", "device_type", "vendor", "model", "serial"],
        EntityType::Interfaces => &["if_index", "name", "description", "interface_type", "mac_address"],
        EntityType::IpAddresses => &["address"],
        EntityType::Sites | EntityType::Rooms | EntityType::Racks => &[],
    }
}
pub(crate) fn manual_fields(fields: &[(&'static str, bool)]) -> Vec<&'static str> {
    fields.iter().filter_map(|(field, changed)| changed.then_some(*field)).collect()
}
pub(crate) fn json<T: Serialize>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|_| Error::Internal("Entity serialization failed"))
}

impl Service {
    /// Scope one transport request. Raw credentials never enter this context.
    pub fn for_mutation(&self, context: MutationContext) -> Self {
        let mut service = self.clone();
        service.context = Some(context);
        service
    }

    pub(crate) async fn mutation<T, F>(
        &self,
        entity: EntityType,
        id: Option<Uuid>,
        action: Action,
        fields: Vec<&'static str>,
        operation: F,
    ) -> Result<T>
    where
        T: Serialize + Send,
        F: for<'a> Fn(&'a mut Transaction<'static, Postgres>) -> TransactionFuture<'a, T>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let context = self.context;
        if let Some(context) = context {
            context.validate()?;
        }
        self.retryable_transaction(|tx| {
            let operation = operation.clone();
            let fields = fields.clone();
            Box::pin(async move {
                // Actor-free core helpers remain usable for caller-owned transactions.
                let Some(context) = context else {
                    return operation(tx).await;
                };
                let before = match id {
                    Some(id) => Some(Repository::lock_entity(tx, entity, id).await?),
                    None => None,
                };
                if action == Action::Delete {
                    Repository::delete_entity_overrides(tx, entity, id.ok_or(Error::Internal("Delete ID missing"))?)
                        .await?;
                }
                let value = operation(tx).await?;
                let after = if action == Action::Delete { None } else { Some(json(&value)?) };
                let id = match id {
                    Some(id) => id,
                    None => serde_json::from_value(
                        after
                            .as_ref()
                            .and_then(|v| v.get("id"))
                            .cloned()
                            .ok_or(Error::Internal("Created entity ID missing"))?,
                    )
                    .map_err(|_| Error::Internal("Created entity ID invalid"))?,
                };
                Self::record_mutation_in(tx, context, entity, id, action, before, after, &fields).await?;
                Ok(value)
            })
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_mutation_in(
        tx: &mut Transaction<'_, Postgres>,
        context: MutationContext,
        entity: EntityType,
        id: Uuid,
        action: Action,
        before: Option<Value>,
        after: Option<Value>,
        fields: &[&str],
    ) -> Result<()> {
        context.validate()?;
        Repository::append_audit(tx, context, entity, id, action, before, after).await?;
        for field in fields {
            if !override_fields(entity).contains(field) {
                return Err(Error::validation("Unsupported manual override field"));
            }
            Repository::upsert_override(tx, entity, id, field, context.actor_id).await?;
        }
        Ok(())
    }

    pub async fn list_audit_events(&self, filter: &AuditFilter, page: Pagination) -> Result<Vec<AuditEvent>> {
        page.validate()?;
        Repository::list_audit(&mut *self.pool.acquire().await?, filter, page).await
    }
    pub async fn get_audit_event(&self, id: Uuid) -> Result<AuditEvent> {
        Repository::get_audit(&mut *self.pool.acquire().await?, id)
            .await?
            .ok_or(Error::NotFound { entity: "audit event", id })
    }
    pub async fn list_field_overrides(
        &self,
        entity: Option<EntityType>,
        id: Option<Uuid>,
        page: Pagination,
    ) -> Result<Vec<FieldOverride>> {
        page.validate()?;
        Repository::list_overrides(&mut *self.pool.acquire().await?, entity, id, page).await
    }
    pub async fn set_field_override(&self, entity: EntityType, id: Uuid, field: &str) -> Result<FieldOverride> {
        self.change_override(entity, id, field, true).await?.ok_or(Error::Internal("Override result missing"))
    }
    pub async fn clear_field_override(&self, entity: EntityType, id: Uuid, field: &str) -> Result<()> {
        self.change_override(entity, id, field, false).await.map(|_| ())
    }
    async fn change_override(
        &self,
        entity: EntityType,
        id: Uuid,
        field: &str,
        set: bool,
    ) -> Result<Option<FieldOverride>> {
        if !override_fields(entity).contains(&field) {
            return Err(Error::validation("Unsupported manual override field"));
        }
        let context = self.context.ok_or(Error::Internal("Mutation actor missing"))?;
        context.validate()?;
        self.retryable_transaction(|tx| {
            let field = field.to_owned();
            Box::pin(async move {
                Repository::lock_entity(tx, entity, id).await?;
                let before = Repository::get_override(tx, entity, id, &field).await?;
                let after = if set {
                    Some(Repository::upsert_override(tx, entity, id, &field, context.actor_id).await?)
                } else {
                    Repository::delete_override(tx, entity, id, &field).await?;
                    None
                };
                Repository::append_audit(
                    tx,
                    context,
                    entity,
                    id,
                    if set { Action::FieldOverrideSet } else { Action::FieldOverrideClear },
                    before.as_ref().map(json).transpose()?,
                    after.as_ref().map(json).transpose()?,
                )
                .await?;
                Ok(after)
            })
        })
        .await
    }
}
