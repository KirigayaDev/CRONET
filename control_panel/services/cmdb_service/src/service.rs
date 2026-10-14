//! Business operations shared by future transports. SQL belongs to Repository.
use crate::{models::*, repository::Repository, Error, Result};
use sqlx::{PgPool, Postgres, Transaction};
use std::{future::Future, pin::Pin, time::Duration};
use uuid::Uuid;

pub type TransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    max_attempts: u32,
    delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: Duration::from_millis(25),
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, delay: Duration) -> Result<Self> {
        if !(1..=5).contains(&max_attempts) || delay > Duration::from_secs(1) {
            return Err(Error::validation(
                "Retry policy requires 1..=5 attempts and delay <= 1 second",
            ));
        }
        Ok(Self {
            max_attempts,
            delay,
        })
    }
}

#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    retry: RetryPolicy,
}

impl Service {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: RetryPolicy::default(),
        }
    }
    pub fn with_retry_policy(pool: PgPool, retry: RetryPolicy) -> Self {
        Self { pool, retry }
    }

    /// Opt-in replay of a whole transaction. Closure must have no external side
    /// effects: database writes are rolled back before retry. Unknown commit
    /// outcomes, connection failures, validation and conflicts are never retried.
    pub async fn retryable_transaction<T, F>(&self, mut operation: F) -> Result<T>
    where
        T: Send,
        F: for<'a> FnMut(&'a mut Transaction<'static, Postgres>) -> TransactionFuture<'a, T>,
    {
        for attempt in 1..=self.retry.max_attempts {
            let mut tx = self.pool.begin().await?;
            let error = match operation(&mut tx).await {
                Ok(value) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(error) => Error::from(error),
                },
                Err(error) => {
                    tx.rollback().await?;
                    error
                }
            };
            if !error.is_retryable() || attempt == self.retry.max_attempts {
                return Err(error);
            }
            tokio::time::sleep(self.retry.delay * attempt).await;
        }
        unreachable!("Retry policy always contains at least one attempt")
    }

    pub async fn create_site(&self, input: CreateSite) -> Result<Site> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_site_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_site_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateSite,
    ) -> Result<Site> {
        validate_name(&input.name)?;
        Repository::create_site(tx, input).await
    }
    pub async fn get_site(&self, id: Uuid) -> Result<Site> {
        found(
            Repository::get_site(&mut *self.pool.acquire().await?, id).await?,
            "site",
            id,
        )
    }
    pub async fn list_sites(&self) -> Result<Vec<Site>> {
        self.list_sites_page(Pagination::default()).await
    }
    pub async fn list_sites_page(&self, page: Pagination) -> Result<Vec<Site>> {
        page.validate()?;
        Repository::list_sites(&mut *self.pool.acquire().await?, page).await
    }
    pub async fn find_site_by_name(&self, name: &str) -> Result<Option<Site>> {
        validate_name(name)?;
        Repository::find_site_by_name(&mut *self.pool.acquire().await?, name).await
    }
    pub async fn update_site(&self, id: Uuid, input: UpdateSite) -> Result<Site> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_site_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_site_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateSite,
    ) -> Result<Site> {
        validate_name_patch(&input.name)?;
        found(Repository::update_site(tx, id, input).await?, "site", id)
    }
    pub async fn delete_site(&self, id: Uuid) -> Result<()> {
        self.retryable_transaction(|tx| Box::pin(Self::delete_site_in(tx, id)))
            .await
    }
    pub async fn delete_site_in(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<()> {
        deleted(Repository::delete_site(tx, id).await?, "site", id)
    }

    pub async fn create_room(&self, input: CreateRoom) -> Result<Room> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_room_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_room_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateRoom,
    ) -> Result<Room> {
        validate_name(&input.name)?;
        Repository::create_room(tx, input).await
    }
    pub async fn get_room(&self, id: Uuid) -> Result<Room> {
        found(
            Repository::get_room(&mut *self.pool.acquire().await?, id).await?,
            "room",
            id,
        )
    }
    pub async fn list_rooms(&self, site_id: Option<Uuid>) -> Result<Vec<Room>> {
        self.list_rooms_page(site_id, Pagination::default()).await
    }
    pub async fn list_rooms_page(
        &self,
        site_id: Option<Uuid>,
        page: Pagination,
    ) -> Result<Vec<Room>> {
        page.validate()?;
        Repository::list_rooms(&mut *self.pool.acquire().await?, site_id, page).await
    }
    pub async fn find_room_by_name(&self, site_id: Uuid, name: &str) -> Result<Option<Room>> {
        validate_name(name)?;
        Repository::find_room_by_name(&mut *self.pool.acquire().await?, site_id, name).await
    }
    pub async fn update_room(&self, id: Uuid, input: UpdateRoom) -> Result<Room> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_room_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_room_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateRoom,
    ) -> Result<Room> {
        required(&input.site_id, "site_id")?;
        validate_name_patch(&input.name)?;
        found(Repository::update_room(tx, id, input).await?, "room", id)
    }
    pub async fn delete_room(&self, id: Uuid) -> Result<()> {
        self.retryable_transaction(|tx| Box::pin(Self::delete_room_in(tx, id)))
            .await
    }
    pub async fn delete_room_in(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<()> {
        deleted(Repository::delete_room(tx, id).await?, "room", id)
    }

    pub async fn create_rack(&self, input: CreateRack) -> Result<Rack> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_rack_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_rack_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateRack,
    ) -> Result<Rack> {
        validate_name(&input.name)?;
        positive("height_u", Some(input.height_u))?;
        Repository::create_rack(tx, input).await
    }
    pub async fn get_rack(&self, id: Uuid) -> Result<Rack> {
        found(
            Repository::get_rack(&mut *self.pool.acquire().await?, id).await?,
            "rack",
            id,
        )
    }
    pub async fn list_racks(&self, room_id: Option<Uuid>) -> Result<Vec<Rack>> {
        self.list_racks_page(room_id, Pagination::default()).await
    }
    pub async fn list_racks_page(
        &self,
        room_id: Option<Uuid>,
        page: Pagination,
    ) -> Result<Vec<Rack>> {
        page.validate()?;
        Repository::list_racks(&mut *self.pool.acquire().await?, room_id, page).await
    }
    pub async fn find_rack_by_name(&self, room_id: Uuid, name: &str) -> Result<Option<Rack>> {
        validate_name(name)?;
        Repository::find_rack_by_name(&mut *self.pool.acquire().await?, room_id, name).await
    }
    pub async fn update_rack(&self, id: Uuid, input: UpdateRack) -> Result<Rack> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_rack_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_rack_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateRack,
    ) -> Result<Rack> {
        required(&input.room_id, "room_id")?;
        required(&input.height_u, "height_u")?;
        validate_name_patch(&input.name)?;
        positive("height_u", input.height_u.value().copied())?;
        found(Repository::update_rack(tx, id, input).await?, "rack", id)
    }
    pub async fn delete_rack(&self, id: Uuid) -> Result<()> {
        self.retryable_transaction(|tx| Box::pin(Self::delete_rack_in(tx, id)))
            .await
    }
    pub async fn delete_rack_in(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<()> {
        deleted(Repository::delete_rack(tx, id).await?, "rack", id)
    }

    pub async fn create_device(&self, input: CreateDevice) -> Result<Device> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_device_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_device_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateDevice,
    ) -> Result<Device> {
        positive("start_unit", input.start_unit)?;
        positive("height_u", input.height_u)?;
        Repository::create_device(tx, input).await
    }
    pub async fn get_device(&self, id: Uuid) -> Result<Device> {
        found(
            Repository::get_device(&mut *self.pool.acquire().await?, id).await?,
            "device",
            id,
        )
    }
    pub async fn list_devices(&self, filter: &DeviceFilter) -> Result<Vec<Device>> {
        self.list_devices_page(filter, Pagination::default()).await
    }
    pub async fn list_devices_page(
        &self,
        filter: &DeviceFilter,
        page: Pagination,
    ) -> Result<Vec<Device>> {
        page.validate()?;
        Repository::list_devices(&mut *self.pool.acquire().await?, filter, page).await
    }
    pub async fn update_device(&self, id: Uuid, input: UpdateDevice) -> Result<Device> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_device_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_device_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateDevice,
    ) -> Result<Device> {
        required(&input.lifecycle_status, "lifecycle_status")?;
        positive("start_unit", input.start_unit.value().copied())?;
        positive("height_u", input.height_u.value().copied())?;
        found(
            Repository::update_device(tx, id, input).await?,
            "device",
            id,
        )
    }
    pub async fn decommission_device(&self, id: Uuid) -> Result<Device> {
        self.retryable_transaction(|tx| Box::pin(Self::decommission_device_in(tx, id)))
            .await
    }
    pub async fn decommission_device_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
    ) -> Result<Device> {
        // Keep physical placement; removing occupancy is an explicit PATCH.
        found(Repository::decommission_device(tx, id).await?, "device", id)
    }

    pub async fn create_interface(&self, input: CreateInterface) -> Result<Interface> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_interface_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_interface_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateInterface,
    ) -> Result<Interface> {
        validate_name(&input.name)?;
        positive("if_index", input.if_index)?;
        Repository::create_interface(tx, input).await
    }
    pub async fn get_interface(&self, id: Uuid) -> Result<Interface> {
        found(
            Repository::get_interface(&mut *self.pool.acquire().await?, id).await?,
            "interface",
            id,
        )
    }
    pub async fn list_interfaces(&self, device_id: Uuid) -> Result<Vec<Interface>> {
        self.list_interfaces_page(device_id, Pagination::default())
            .await
    }
    pub async fn list_interfaces_page(
        &self,
        device_id: Uuid,
        page: Pagination,
    ) -> Result<Vec<Interface>> {
        page.validate()?;
        Repository::list_interfaces(&mut *self.pool.acquire().await?, device_id, page).await
    }
    pub async fn update_interface(&self, id: Uuid, input: UpdateInterface) -> Result<Interface> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_interface_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_interface_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateInterface,
    ) -> Result<Interface> {
        required(&input.device_id, "device_id")?;
        validate_name_patch(&input.name)?;
        positive("if_index", input.if_index.value().copied())?;
        found(
            Repository::update_interface(tx, id, input).await?,
            "interface",
            id,
        )
    }
    pub async fn delete_interface(&self, id: Uuid) -> Result<()> {
        self.retryable_transaction(|tx| Box::pin(Self::delete_interface_in(tx, id)))
            .await
    }
    pub async fn delete_interface_in(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<()> {
        deleted(Repository::delete_interface(tx, id).await?, "interface", id)
    }

    pub async fn create_ip_address(&self, input: CreateIpAddress) -> Result<IpAddress> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::create_ip_address_in(tx, &input).await })
        })
        .await
    }
    pub async fn create_ip_address_in(
        tx: &mut Transaction<'_, Postgres>,
        input: &CreateIpAddress,
    ) -> Result<IpAddress> {
        Repository::create_ip_address(tx, input).await
    }
    pub async fn get_ip_address(&self, id: Uuid) -> Result<IpAddress> {
        found(
            Repository::get_ip_address(&mut *self.pool.acquire().await?, id).await?,
            "IP address",
            id,
        )
    }
    pub async fn list_ip_addresses(&self, interface_id: Uuid) -> Result<Vec<IpAddress>> {
        self.list_ip_addresses_page(interface_id, Pagination::default())
            .await
    }
    pub async fn list_ip_addresses_page(
        &self,
        interface_id: Uuid,
        page: Pagination,
    ) -> Result<Vec<IpAddress>> {
        page.validate()?;
        Repository::list_ip_addresses(&mut *self.pool.acquire().await?, interface_id, page).await
    }
    pub async fn update_ip_address(&self, id: Uuid, input: UpdateIpAddress) -> Result<IpAddress> {
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move { Self::update_ip_address_in(tx, id, &input).await })
        })
        .await
    }
    pub async fn update_ip_address_in(
        tx: &mut Transaction<'_, Postgres>,
        id: Uuid,
        input: &UpdateIpAddress,
    ) -> Result<IpAddress> {
        required(&input.interface_id, "interface_id")?;
        required(&input.address, "address")?;
        required(&input.is_management, "is_management")?;
        found(
            Repository::update_ip_address(tx, id, input).await?,
            "IP address",
            id,
        )
    }
    pub async fn delete_ip_address(&self, id: Uuid) -> Result<()> {
        self.retryable_transaction(|tx| Box::pin(Self::delete_ip_address_in(tx, id)))
            .await
    }
    pub async fn delete_ip_address_in(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<()> {
        deleted(
            Repository::delete_ip_address(tx, id).await?,
            "IP address",
            id,
        )
    }

    pub async fn is_field_overridden(
        &self,
        entity_type: EntityType,
        entity_id: Uuid,
        field_name: &str,
    ) -> Result<bool> {
        validate_name(field_name)?;
        Repository::is_field_overridden(
            &mut *self.pool.acquire().await?,
            entity_type,
            entity_id,
            field_name,
        )
        .await
    }
    pub async fn is_field_overridden_in(
        tx: &mut Transaction<'_, Postgres>,
        entity_type: EntityType,
        entity_id: Uuid,
        field_name: &str,
    ) -> Result<bool> {
        validate_name(field_name)?;
        Repository::is_field_overridden(tx, entity_type, entity_id, field_name).await
    }
}

fn found<T>(value: Option<T>, entity: &'static str, id: Uuid) -> Result<T> {
    value.ok_or(Error::NotFound { entity, id })
}
fn deleted(value: bool, entity: &'static str, id: Uuid) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(Error::NotFound { entity, id })
    }
}
fn validate_name(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::validation(
            "Name must contain a non-whitespace character",
        ))
    } else {
        Ok(())
    }
}
fn validate_name_patch(value: &Patch<String>) -> Result<()> {
    required(value, "name")?;
    if let Patch::Set(value) = value {
        validate_name(value)?;
    }
    Ok(())
}
fn required<T>(value: &Patch<T>, field: &str) -> Result<()> {
    if matches!(value, Patch::Clear) {
        Err(Error::validation(format!("{field} cannot be NULL")))
    } else {
        Ok(())
    }
}
fn positive(field: &str, value: Option<i32>) -> Result<()> {
    if value.is_some_and(|value| value <= 0) {
        Err(Error::validation(format!("{field} must be positive")))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_are_bounded_and_policy_rejects_unbounded_settings() {
        assert!(RetryPolicy::new(0, Duration::ZERO).is_err());
        assert!(RetryPolicy::new(6, Duration::ZERO).is_err());
        assert!(RetryPolicy::new(3, Duration::from_secs(2)).is_err());
        assert!(RetryPolicy::new(3, Duration::ZERO).is_ok());
    }
}
