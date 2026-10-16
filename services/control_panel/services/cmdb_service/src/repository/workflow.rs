//! Queries for existing audit, override and import tables. No schema changes.
use super::Repository;
use crate::{
    imports::{ImportBatch, ImportRow},
    models::*,
    workflow::*,
    Error, Result,
};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

impl Repository {
    pub async fn lock_entity(conn: &mut PgConnection, entity: EntityType, id: Uuid) -> Result<Value> {
        macro_rules! snapshot {
            ($model:ty, $sql:literal) => {
                sqlx::query_as::<_, $model>($sql).bind(id).fetch_optional(conn).await?.map(|v| json(&v)).transpose()?
            };
        }
        let value = match entity {
            EntityType::Sites => snapshot!(Site, "SELECT * FROM sites WHERE id=$1 FOR UPDATE"),
            EntityType::Rooms => snapshot!(Room, "SELECT * FROM rooms WHERE id=$1 FOR UPDATE"),
            EntityType::Racks => snapshot!(Rack, "SELECT * FROM racks WHERE id=$1 FOR UPDATE"),
            EntityType::Devices => snapshot!(Device, "SELECT * FROM devices WHERE id=$1 FOR UPDATE"),
            EntityType::Interfaces => snapshot!(Interface, "SELECT * FROM interfaces WHERE id=$1 FOR UPDATE"),
            EntityType::IpAddresses => snapshot!(IpAddress, "SELECT * FROM ip_addresses WHERE id=$1 FOR UPDATE"),
        };
        value.ok_or(Error::NotFound { entity: entity.as_str(), id })
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn append_audit(
        conn: &mut PgConnection,
        context: MutationContext,
        entity: EntityType,
        id: Uuid,
        action: Action,
        before: Option<Value>,
        after: Option<Value>,
    ) -> Result<AuditEvent> {
        Ok(sqlx::query_as("INSERT INTO audit_events
            (actor_type,actor_id,source,action,entity_type,entity_id,before_data,after_data,request_id,import_batch_id,detected_at,changed_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp(),clock_timestamp()) RETURNING *")
            .bind(context.actor_type.as_str()).bind(context.actor_id).bind(context.source.as_str())
            .bind(action.as_str()).bind(entity.as_str()).bind(id).bind(before).bind(after)
            .bind(context.request_id).bind(context.import_batch_id).fetch_one(conn).await?)
    }
    pub async fn get_audit(conn: &mut PgConnection, id: Uuid) -> Result<Option<AuditEvent>> {
        Ok(sqlx::query_as("SELECT * FROM audit_events WHERE id=$1").bind(id).fetch_optional(conn).await?)
    }
    pub async fn list_audit(
        conn: &mut PgConnection,
        filter: &AuditFilter,
        page: Pagination,
    ) -> Result<Vec<AuditEvent>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM audit_events WHERE
            ($1::text IS NULL OR entity_type=$1) AND ($2::uuid IS NULL OR entity_id=$2)
            AND ($3::uuid IS NULL OR actor_id=$3) AND ($4::uuid IS NULL OR request_id=$4)
            AND ($5::uuid IS NULL OR import_batch_id=$5) ORDER BY detected_at DESC,id DESC LIMIT $6 OFFSET $7",
        )
        .bind(filter.entity_type.map(EntityType::as_str))
        .bind(filter.entity_id)
        .bind(filter.actor_id)
        .bind(filter.request_id)
        .bind(filter.import_batch_id)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }
    pub async fn get_override(
        conn: &mut PgConnection,
        entity: EntityType,
        id: Uuid,
        field: &str,
    ) -> Result<Option<FieldOverride>> {
        Ok(sqlx::query_as("SELECT * FROM field_overrides WHERE entity_type=$1 AND entity_id=$2 AND field_name=$3")
            .bind(entity.as_str())
            .bind(id)
            .bind(field)
            .fetch_optional(conn)
            .await?)
    }
    pub async fn upsert_override(
        conn: &mut PgConnection,
        entity: EntityType,
        id: Uuid,
        field: &str,
        actor: Uuid,
    ) -> Result<FieldOverride> {
        Ok(sqlx::query_as(
            "INSERT INTO field_overrides(entity_type,entity_id,field_name,locked_by,locked_at)
            VALUES($1,$2,$3,$4,clock_timestamp()) ON CONFLICT(entity_type,entity_id,field_name)
            DO UPDATE SET locked_by=EXCLUDED.locked_by,locked_at=EXCLUDED.locked_at RETURNING *",
        )
        .bind(entity.as_str())
        .bind(id)
        .bind(field)
        .bind(actor)
        .fetch_one(conn)
        .await?)
    }
    pub async fn delete_override(conn: &mut PgConnection, entity: EntityType, id: Uuid, field: &str) -> Result<()> {
        sqlx::query("DELETE FROM field_overrides WHERE entity_type=$1 AND entity_id=$2 AND field_name=$3")
            .bind(entity.as_str())
            .bind(id)
            .bind(field)
            .execute(conn)
            .await?;
        Ok(())
    }
    pub async fn delete_entity_overrides(conn: &mut PgConnection, entity: EntityType, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM field_overrides WHERE entity_type=$1 AND entity_id=$2")
            .bind(entity.as_str())
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
    pub async fn list_overrides(
        conn: &mut PgConnection,
        entity: Option<EntityType>,
        id: Option<Uuid>,
        page: Pagination,
    ) -> Result<Vec<FieldOverride>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM field_overrides WHERE ($1::text IS NULL OR entity_type=$1)
            AND ($2::uuid IS NULL OR entity_id=$2) ORDER BY locked_at DESC,id DESC LIMIT $3 OFFSET $4",
        )
        .bind(entity.map(EntityType::as_str))
        .bind(id)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }
    pub async fn create_import_batch(conn: &mut PgConnection, actor: Uuid, filename: &str) -> Result<ImportBatch> {
        Ok(sqlx::query_as("INSERT INTO import_batches(created_by,filename) VALUES($1,$2) RETURNING *")
            .bind(actor)
            .bind(filename)
            .fetch_one(conn)
            .await?)
    }
    pub async fn get_import_batch(conn: &mut PgConnection, id: Uuid, lock: bool) -> Result<ImportBatch> {
        let sql = if lock {
            "SELECT * FROM import_batches WHERE id=$1 FOR UPDATE"
        } else {
            "SELECT * FROM import_batches WHERE id=$1"
        };
        sqlx::query_as(sql).bind(id).fetch_optional(conn).await?.ok_or(Error::NotFound { entity: "import batch", id })
    }
    pub async fn import_counts(conn: &mut PgConnection, id: Uuid) -> Result<(i64, i64, i64, i64)> {
        Ok(sqlx::query_as(
            "SELECT count(*),count(*) FILTER(WHERE status='valid'),count(*) FILTER(WHERE status='invalid'),
            count(*) FILTER(WHERE jsonb_array_length(validation_warnings)>0) FROM import_rows WHERE batch_id=$1",
        )
        .bind(id)
        .fetch_one(conn)
        .await?)
    }
    pub async fn insert_import_row(
        conn: &mut PgConnection,
        batch: Uuid,
        number: i32,
        raw: Value,
        working: Value,
    ) -> Result<ImportRow> {
        Ok(sqlx::query_as(
            "INSERT INTO import_rows(batch_id,row_number,raw_data,working_data) VALUES($1,$2,$3,$4) RETURNING *",
        )
        .bind(batch)
        .bind(number)
        .bind(raw)
        .bind(working)
        .fetch_one(conn)
        .await?)
    }
    pub async fn import_rows_locked(conn: &mut PgConnection, batch: Uuid) -> Result<Vec<ImportRow>> {
        Ok(sqlx::query_as("SELECT * FROM import_rows WHERE batch_id=$1 ORDER BY row_number,id LIMIT 10001 FOR UPDATE")
            .bind(batch)
            .fetch_all(conn)
            .await?)
    }
    pub async fn list_import_rows(conn: &mut PgConnection, batch: Uuid, page: Pagination) -> Result<Vec<ImportRow>> {
        page.validate()?;
        Ok(sqlx::query_as("SELECT * FROM import_rows WHERE batch_id=$1 ORDER BY row_number,id LIMIT $2 OFFSET $3")
            .bind(batch)
            .bind(page.limit())
            .bind(page.offset())
            .fetch_all(conn)
            .await?)
    }
    pub async fn save_import_row(conn: &mut PgConnection, row: &ImportRow) -> Result<()> {
        // raw_data is immutable through every Service operation.
        sqlx::query("UPDATE import_rows SET working_data=$3,validation_errors=$4,validation_warnings=$5,status=$6 WHERE id=$1 AND batch_id=$2")
            .bind(row.id).bind(row.batch_id).bind(&row.working_data).bind(&row.validation_errors)
            .bind(&row.validation_warnings).bind(&row.status).execute(conn).await?;
        Ok(())
    }
    pub async fn set_import_state(conn: &mut PgConnection, batch: Uuid, status: &str) -> Result<ImportBatch> {
        Ok(sqlx::query_as("UPDATE import_batches SET status=$2,confirmed_at=CASE WHEN $2='confirmed' THEN clock_timestamp() ELSE NULL END
            WHERE id=$1 RETURNING *").bind(batch).bind(status).fetch_one(conn).await?)
    }
    pub async fn mark_imported(conn: &mut PgConnection, batch: Uuid) -> Result<()> {
        sqlx::query("UPDATE import_rows SET status='imported' WHERE batch_id=$1").bind(batch).execute(conn).await?;
        Ok(())
    }
    pub async fn rack_overlaps(conn: &mut PgConnection, rack: Uuid, start: i32, height: i32) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM devices WHERE rack_id=$1
            AND start_unit::bigint < $2::bigint+$3::bigint AND start_unit::bigint+height_u::bigint > $2::bigint)",
        )
        .bind(rack)
        .bind(start)
        .bind(height)
        .fetch_one(conn)
        .await?)
    }
    pub async fn inventory_duplicates(
        conn: &mut PgConnection,
        hostname: Option<&str>,
        serial: Option<&str>,
    ) -> Result<(bool, bool)> {
        Ok(sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM devices WHERE hostname=$1),EXISTS(SELECT 1 FROM devices WHERE serial=$2)",
        )
        .bind(hostname)
        .bind(serial)
        .fetch_one(conn)
        .await?)
    }
}
