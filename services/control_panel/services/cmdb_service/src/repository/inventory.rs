use super::Repository;
use crate::{
    inventory::{InventoryComponent, SnapshotMetadata},
    Result,
};
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

impl Repository {
    pub async fn latest_inventory(conn: &mut PgConnection, device: Uuid) -> Result<Option<SnapshotMetadata>> {
        Ok(sqlx::query_as("SELECT * FROM inventory_snapshots WHERE device_id=$1 ORDER BY collected_at DESC,created_at DESC,id DESC LIMIT 1")
            .bind(device).fetch_optional(conn).await?)
    }
    pub async fn inventory_metadata(
        conn: &mut PgConnection,
        device: Uuid,
        snapshot: Uuid,
    ) -> Result<Option<SnapshotMetadata>> {
        Ok(sqlx::query_as("SELECT * FROM inventory_snapshots WHERE device_id=$1 AND id=$2")
            .bind(device)
            .bind(snapshot)
            .fetch_optional(conn)
            .await?)
    }
    pub async fn inventory_components(conn: &mut PgConnection, snapshot: Uuid) -> Result<Vec<InventoryComponent>> {
        Ok(sqlx::query_as("SELECT * FROM inventory_components WHERE snapshot_id=$1 ORDER BY ent_physical_index")
            .bind(snapshot)
            .fetch_all(conn)
            .await?)
    }
    pub async fn insert_inventory(
        conn: &mut PgConnection,
        device: Uuid,
        collected_at: DateTime<Utc>,
        hash: &str,
        components: &[InventoryComponent],
    ) -> Result<SnapshotMetadata> {
        let metadata: SnapshotMetadata = sqlx::query_as("INSERT INTO inventory_snapshots(device_id,collected_at,content_hash,component_count) VALUES($1,$2,$3,$4) RETURNING *")
            .bind(device).bind(collected_at).bind(hash).bind(components.len() as i32).fetch_one(&mut *conn).await?;
        // One INSERT regardless of component count. Typed arrays preserve NULLs.
        macro_rules! column {
            ($field:ident) => {
                components.iter().map(|c| c.$field.to_owned()).collect::<Vec<_>>()
            };
        }
        sqlx::query("INSERT INTO inventory_components(snapshot_id,ent_physical_index,contained_in,parent_rel_pos,class,name,description,vendor_type,serial,model,hardware_rev,firmware_rev,software_rev)
            SELECT $1,u.* FROM UNNEST($2::int[],$3::int[],$4::int[],$5::text[],$6::text[],$7::text[],$8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::text[]) AS u")
            .bind(metadata.id).bind(column!(ent_physical_index)).bind(column!(contained_in)).bind(column!(parent_rel_pos))
            .bind(column!(class)).bind(column!(name)).bind(column!(description)).bind(column!(vendor_type)).bind(column!(serial))
            .bind(column!(model)).bind(column!(hardware_rev)).bind(column!(firmware_rev)).bind(column!(software_rev))
            .execute(conn).await?;
        Ok(metadata)
    }
}
