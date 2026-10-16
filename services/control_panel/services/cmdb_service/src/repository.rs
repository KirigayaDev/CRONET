//! SQL only. Callers own connections and transaction boundaries.
use crate::{models::*, Result};
use sqlx::PgConnection;
use uuid::Uuid;

mod inventory;
mod workflow;

pub struct Repository;

impl Repository {
    pub async fn get_auth_user(conn: &mut PgConnection, id: Uuid) -> Result<Option<AuthUser>> {
        Ok(sqlx::query_as("SELECT uuid, token_version_uuid, is_admin, is_super_admin FROM users WHERE uuid = $1")
            .bind(id)
            .fetch_optional(conn)
            .await?)
    }

    pub async fn create_site(conn: &mut PgConnection, input: &CreateSite) -> Result<Site> {
        Ok(sqlx::query_as("INSERT INTO sites (name, description) VALUES ($1, $2) RETURNING *")
            .bind(&input.name)
            .bind(&input.description)
            .fetch_one(conn)
            .await?)
    }

    pub async fn get_site(conn: &mut PgConnection, id: Uuid) -> Result<Option<Site>> {
        Ok(sqlx::query_as("SELECT * FROM sites WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_sites(conn: &mut PgConnection, page: Pagination) -> Result<Vec<Site>> {
        page.validate()?;
        Ok(sqlx::query_as("SELECT * FROM sites ORDER BY name, id LIMIT $1 OFFSET $2")
            .bind(page.limit())
            .bind(page.offset())
            .fetch_all(conn)
            .await?)
    }

    pub async fn find_site_by_name(conn: &mut PgConnection, name: &str) -> Result<Option<Site>> {
        Ok(sqlx::query_as(
            "SELECT * FROM sites WHERE
            lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g'))) =
            lower(btrim(regexp_replace($1, '[[:space:]]+', ' ', 'g')))",
        )
        .bind(name)
        .fetch_optional(conn)
        .await?)
    }

    pub async fn update_site(conn: &mut PgConnection, id: Uuid, input: &UpdateSite) -> Result<Option<Site>> {
        Ok(sqlx::query_as(
            "UPDATE sites SET
            name = CASE WHEN $2 THEN $3 ELSE name END,
            description = CASE WHEN $4 THEN $5 ELSE description END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.name.is_changed())
        .bind(input.name.value())
        .bind(input.description.is_changed())
        .bind(input.description.value())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn delete_site(conn: &mut PgConnection, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM sites WHERE id = $1").bind(id).execute(conn).await?.rows_affected() == 1)
    }

    pub async fn create_room(conn: &mut PgConnection, input: &CreateRoom) -> Result<Room> {
        Ok(sqlx::query_as("INSERT INTO rooms (site_id, name, description) VALUES ($1, $2, $3) RETURNING *")
            .bind(input.site_id)
            .bind(&input.name)
            .bind(&input.description)
            .fetch_one(conn)
            .await?)
    }

    pub async fn get_room(conn: &mut PgConnection, id: Uuid) -> Result<Option<Room>> {
        Ok(sqlx::query_as("SELECT * FROM rooms WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_rooms(conn: &mut PgConnection, site_id: Option<Uuid>, page: Pagination) -> Result<Vec<Room>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM rooms WHERE ($1::uuid IS NULL OR site_id = $1) ORDER BY name, id LIMIT $2 OFFSET $3",
        )
        .bind(site_id)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }

    pub async fn find_room_by_name(conn: &mut PgConnection, site_id: Uuid, name: &str) -> Result<Option<Room>> {
        Ok(sqlx::query_as(
            "SELECT * FROM rooms WHERE site_id = $1 AND
            lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g'))) =
            lower(btrim(regexp_replace($2, '[[:space:]]+', ' ', 'g')))",
        )
        .bind(site_id)
        .bind(name)
        .fetch_optional(conn)
        .await?)
    }

    pub async fn update_room(conn: &mut PgConnection, id: Uuid, input: &UpdateRoom) -> Result<Option<Room>> {
        Ok(sqlx::query_as(
            "UPDATE rooms SET
            site_id = CASE WHEN $2 THEN $3 ELSE site_id END,
            name = CASE WHEN $4 THEN $5 ELSE name END,
            description = CASE WHEN $6 THEN $7 ELSE description END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.site_id.is_changed())
        .bind(input.site_id.value().copied())
        .bind(input.name.is_changed())
        .bind(input.name.value())
        .bind(input.description.is_changed())
        .bind(input.description.value())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn delete_room(conn: &mut PgConnection, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM rooms WHERE id = $1").bind(id).execute(conn).await?.rows_affected() == 1)
    }

    pub async fn create_rack(conn: &mut PgConnection, input: &CreateRack) -> Result<Rack> {
        Ok(sqlx::query_as(
            "INSERT INTO racks (room_id, name, height_u, description)
            VALUES ($1, $2, $3, $4) RETURNING *",
        )
        .bind(input.room_id)
        .bind(&input.name)
        .bind(input.height_u)
        .bind(&input.description)
        .fetch_one(conn)
        .await?)
    }

    pub async fn get_rack(conn: &mut PgConnection, id: Uuid) -> Result<Option<Rack>> {
        Ok(sqlx::query_as("SELECT * FROM racks WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_racks(conn: &mut PgConnection, room_id: Option<Uuid>, page: Pagination) -> Result<Vec<Rack>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM racks WHERE ($1::uuid IS NULL OR room_id = $1) ORDER BY name, id LIMIT $2 OFFSET $3",
        )
        .bind(room_id)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }

    pub async fn find_rack_by_name(conn: &mut PgConnection, room_id: Uuid, name: &str) -> Result<Option<Rack>> {
        Ok(sqlx::query_as(
            "SELECT * FROM racks WHERE room_id = $1 AND
            lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g'))) =
            lower(btrim(regexp_replace($2, '[[:space:]]+', ' ', 'g')))",
        )
        .bind(room_id)
        .bind(name)
        .fetch_optional(conn)
        .await?)
    }

    pub async fn update_rack(conn: &mut PgConnection, id: Uuid, input: &UpdateRack) -> Result<Option<Rack>> {
        Ok(sqlx::query_as(
            "UPDATE racks SET
            room_id = CASE WHEN $2 THEN $3 ELSE room_id END,
            name = CASE WHEN $4 THEN $5 ELSE name END,
            height_u = CASE WHEN $6 THEN $7 ELSE height_u END,
            description = CASE WHEN $8 THEN $9 ELSE description END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.room_id.is_changed())
        .bind(input.room_id.value().copied())
        .bind(input.name.is_changed())
        .bind(input.name.value())
        .bind(input.height_u.is_changed())
        .bind(input.height_u.value().copied())
        .bind(input.description.is_changed())
        .bind(input.description.value())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn delete_rack(conn: &mut PgConnection, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM racks WHERE id = $1").bind(id).execute(conn).await?.rows_affected() == 1)
    }

    pub async fn create_device(conn: &mut PgConnection, input: &CreateDevice) -> Result<Device> {
        Ok(sqlx::query_as(
            "INSERT INTO devices
            (rack_id, start_unit, height_u, hostname, device_type, vendor, model, serial, lifecycle_status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *",
        )
        .bind(input.rack_id)
        .bind(input.start_unit)
        .bind(input.height_u)
        .bind(&input.hostname)
        .bind(&input.device_type)
        .bind(&input.vendor)
        .bind(&input.model)
        .bind(&input.serial)
        .bind(input.lifecycle_status)
        .fetch_one(conn)
        .await?)
    }

    pub async fn get_device(conn: &mut PgConnection, id: Uuid) -> Result<Option<Device>> {
        Ok(sqlx::query_as("SELECT * FROM devices WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_devices(conn: &mut PgConnection, filter: &DeviceFilter, page: Pagination) -> Result<Vec<Device>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM devices WHERE
            ($1::uuid IS NULL OR rack_id = $1) AND ($2::text IS NULL OR hostname = $2) AND
            ($3::text IS NULL OR serial = $3) AND ($4::text IS NULL OR lifecycle_status = $4)
            ORDER BY created_at, id LIMIT $5 OFFSET $6",
        )
        .bind(filter.rack_id)
        .bind(&filter.hostname)
        .bind(&filter.serial)
        .bind(filter.lifecycle_status)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }

    pub async fn update_device(conn: &mut PgConnection, id: Uuid, input: &UpdateDevice) -> Result<Option<Device>> {
        Ok(sqlx::query_as(
            "UPDATE devices SET
            rack_id = CASE WHEN $2 THEN $3 ELSE rack_id END,
            start_unit = CASE WHEN $4 THEN $5 ELSE start_unit END,
            height_u = CASE WHEN $6 THEN $7 ELSE height_u END,
            hostname = CASE WHEN $8 THEN $9 ELSE hostname END,
            device_type = CASE WHEN $10 THEN $11 ELSE device_type END,
            vendor = CASE WHEN $12 THEN $13 ELSE vendor END,
            model = CASE WHEN $14 THEN $15 ELSE model END,
            serial = CASE WHEN $16 THEN $17 ELSE serial END,
            lifecycle_status = CASE WHEN $18 THEN $19 ELSE lifecycle_status END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.rack_id.is_changed())
        .bind(input.rack_id.value().copied())
        .bind(input.start_unit.is_changed())
        .bind(input.start_unit.value().copied())
        .bind(input.height_u.is_changed())
        .bind(input.height_u.value().copied())
        .bind(input.hostname.is_changed())
        .bind(input.hostname.value())
        .bind(input.device_type.is_changed())
        .bind(input.device_type.value())
        .bind(input.vendor.is_changed())
        .bind(input.vendor.value())
        .bind(input.model.is_changed())
        .bind(input.model.value())
        .bind(input.serial.is_changed())
        .bind(input.serial.value())
        .bind(input.lifecycle_status.is_changed())
        .bind(input.lifecycle_status.value().copied())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn decommission_device(conn: &mut PgConnection, id: Uuid) -> Result<Option<Device>> {
        Ok(sqlx::query_as("UPDATE devices SET lifecycle_status = 'decommissioned' WHERE id = $1 RETURNING *")
            .bind(id)
            .fetch_optional(conn)
            .await?)
    }

    pub async fn create_interface(conn: &mut PgConnection, input: &CreateInterface) -> Result<Interface> {
        Ok(sqlx::query_as(
            "INSERT INTO interfaces
            (device_id, if_index, name, description, interface_type, mac_address)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
        )
        .bind(input.device_id)
        .bind(input.if_index)
        .bind(&input.name)
        .bind(&input.description)
        .bind(&input.interface_type)
        .bind(input.mac_address)
        .fetch_one(conn)
        .await?)
    }

    pub async fn get_interface(conn: &mut PgConnection, id: Uuid) -> Result<Option<Interface>> {
        Ok(sqlx::query_as("SELECT * FROM interfaces WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_interfaces(conn: &mut PgConnection, device_id: Uuid, page: Pagination) -> Result<Vec<Interface>> {
        page.validate()?;
        Ok(sqlx::query_as(
            "SELECT * FROM interfaces WHERE device_id = $1 ORDER BY if_index NULLS LAST, name, id LIMIT $2 OFFSET $3",
        )
        .bind(device_id)
        .bind(page.limit())
        .bind(page.offset())
        .fetch_all(conn)
        .await?)
    }

    pub async fn update_interface(
        conn: &mut PgConnection,
        id: Uuid,
        input: &UpdateInterface,
    ) -> Result<Option<Interface>> {
        Ok(sqlx::query_as(
            "UPDATE interfaces SET
            device_id = CASE WHEN $2 THEN $3 ELSE device_id END,
            if_index = CASE WHEN $4 THEN $5 ELSE if_index END,
            name = CASE WHEN $6 THEN $7 ELSE name END,
            description = CASE WHEN $8 THEN $9 ELSE description END,
            interface_type = CASE WHEN $10 THEN $11 ELSE interface_type END,
            mac_address = CASE WHEN $12 THEN $13 ELSE mac_address END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.device_id.is_changed())
        .bind(input.device_id.value().copied())
        .bind(input.if_index.is_changed())
        .bind(input.if_index.value().copied())
        .bind(input.name.is_changed())
        .bind(input.name.value())
        .bind(input.description.is_changed())
        .bind(input.description.value())
        .bind(input.interface_type.is_changed())
        .bind(input.interface_type.value())
        .bind(input.mac_address.is_changed())
        .bind(input.mac_address.value().copied())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn delete_interface(conn: &mut PgConnection, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM interfaces WHERE id = $1").bind(id).execute(conn).await?.rows_affected() == 1)
    }

    pub async fn create_ip_address(conn: &mut PgConnection, input: &CreateIpAddress) -> Result<IpAddress> {
        Ok(sqlx::query_as(
            "INSERT INTO ip_addresses (interface_id, address, is_management)
            VALUES ($1,$2,$3) RETURNING *",
        )
        .bind(input.interface_id)
        .bind(input.address)
        .bind(input.is_management)
        .fetch_one(conn)
        .await?)
    }

    pub async fn get_ip_address(conn: &mut PgConnection, id: Uuid) -> Result<Option<IpAddress>> {
        Ok(sqlx::query_as("SELECT * FROM ip_addresses WHERE id = $1").bind(id).fetch_optional(conn).await?)
    }

    pub async fn list_ip_addresses(
        conn: &mut PgConnection,
        interface_id: Uuid,
        page: Pagination,
    ) -> Result<Vec<IpAddress>> {
        page.validate()?;
        Ok(sqlx::query_as("SELECT * FROM ip_addresses WHERE interface_id = $1 ORDER BY address, id LIMIT $2 OFFSET $3")
            .bind(interface_id)
            .bind(page.limit())
            .bind(page.offset())
            .fetch_all(conn)
            .await?)
    }

    pub async fn update_ip_address(
        conn: &mut PgConnection,
        id: Uuid,
        input: &UpdateIpAddress,
    ) -> Result<Option<IpAddress>> {
        Ok(sqlx::query_as(
            "UPDATE ip_addresses SET
            interface_id = CASE WHEN $2 THEN $3 ELSE interface_id END,
            address = CASE WHEN $4 THEN $5 ELSE address END,
            is_management = CASE WHEN $6 THEN $7 ELSE is_management END
            WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(input.interface_id.is_changed())
        .bind(input.interface_id.value().copied())
        .bind(input.address.is_changed())
        .bind(input.address.value().copied())
        .bind(input.is_management.is_changed())
        .bind(input.is_management.value().copied())
        .fetch_optional(conn)
        .await?)
    }

    pub async fn delete_ip_address(conn: &mut PgConnection, id: Uuid) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM ip_addresses WHERE id = $1").bind(id).execute(conn).await?.rows_affected() == 1)
    }

    pub async fn is_field_overridden(
        conn: &mut PgConnection,
        entity_type: EntityType,
        entity_id: Uuid,
        field_name: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM field_overrides
            WHERE entity_type = $1 AND entity_id = $2 AND field_name = $3)",
        )
        .bind(entity_type.as_str())
        .bind(entity_id)
        .bind(field_name)
        .fetch_one(conn)
        .await?)
    }
}
