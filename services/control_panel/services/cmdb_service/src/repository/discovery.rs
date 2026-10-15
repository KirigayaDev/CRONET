//! Discovery identity queries. Existing IP addresses need not be globally unique.
use super::Repository;
use crate::{models::Interface, Result};
use sqlx::PgConnection;
use std::net::IpAddr;
use uuid::Uuid;

impl Repository {
    pub(crate) async fn lock_discovery_identity(conn: &mut PgConnection) -> Result<()> {
        // Serialize discovery with ordinary writes to management relations, including
        // the initially absent-IP case. No global unique constraint is invented.
        // Acquire before device locks; keep this transaction free of network I/O.
        sqlx::query("SET LOCAL lock_timeout = '3s'").execute(&mut *conn).await?;
        sqlx::query("LOCK TABLE interfaces, ip_addresses IN SHARE ROW EXCLUSIVE MODE").execute(conn).await?;
        Ok(())
    }

    pub(crate) async fn discovery_ip_devices(conn: &mut PgConnection, ip: IpAddr) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar(
            "SELECT i.device_id FROM ip_addresses p JOIN interfaces i ON i.id=p.interface_id
             WHERE host(p.address)::inet=$1::inet ORDER BY p.id LIMIT 2",
        )
        .bind(ip.to_string())
        .fetch_all(conn)
        .await?)
    }

    pub(crate) async fn discovery_serial_devices(conn: &mut PgConnection, serial: &str) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("SELECT id FROM devices WHERE serial=$1 ORDER BY id LIMIT 2")
            .bind(serial)
            .fetch_all(conn)
            .await?)
    }

    pub(crate) async fn discovery_interfaces(conn: &mut PgConnection, device: Uuid) -> Result<Vec<Interface>> {
        Ok(sqlx::query_as(
            "SELECT * FROM interfaces WHERE device_id=$1 AND name='discovery-management'
             AND interface_type='discovery/logical' ORDER BY id LIMIT 2",
        )
        .bind(device)
        .fetch_all(conn)
        .await?)
    }
}
