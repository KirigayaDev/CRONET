use super::Repository;
use crate::{poll_targets::PollTarget, Result};
use sqlx::PgConnection;
use uuid::Uuid;

impl Repository {
    pub(crate) async fn list_poll_targets(
        conn: &mut PgConnection,
        limit: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<PollTarget>> {
        Ok(sqlx::query_as(
            "SELECT d.id AS device_id,d.hostname,s.id AS site_id,s.name AS site_name,
             ARRAY(SELECT host(p.address) FROM interfaces i JOIN ip_addresses p ON p.interface_id=i.id
                   WHERE i.device_id=d.id AND p.is_management ORDER BY p.id) AS management_ips
             FROM devices d LEFT JOIN racks r ON r.id=d.rack_id
             LEFT JOIN rooms rm ON rm.id=r.room_id LEFT JOIN sites s ON s.id=rm.site_id
             WHERE d.lifecycle_status='active' AND ($1::uuid IS NULL OR d.id>$1)
             ORDER BY d.id LIMIT $2",
        )
        .bind(after)
        .bind(limit)
        .fetch_all(conn)
        .await?)
    }
}
