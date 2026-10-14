use crate::{models as domain, models::Patch, Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiLifecycle {
    #[default]
    Active,
    Maintenance,
    Decommissioned,
}
impl From<ApiLifecycle> for domain::LifecycleStatus {
    fn from(v: ApiLifecycle) -> Self {
        match v {
            ApiLifecycle::Active => Self::Active,
            ApiLifecycle::Maintenance => Self::Maintenance,
            ApiLifecycle::Decommissioned => Self::Decommissioned,
        }
    }
}
impl From<domain::LifecycleStatus> for ApiLifecycle {
    fn from(v: domain::LifecycleStatus) -> Self {
        match v {
            domain::LifecycleStatus::Active => Self::Active,
            domain::LifecycleStatus::Maintenance => Self::Maintenance,
            domain::LifecycleStatus::Decommissioned => Self::Decommissioned,
        }
    }
}
pub fn parse<T: std::str::FromStr>(value: &str, field: &str) -> Result<T> {
    value.parse().map_err(|_| Error::validation(format!("Invalid {field}")))
}
fn try_patch<T, U>(value: Patch<T>, f: impl FnOnce(T) -> Result<U>) -> Result<Patch<U>> {
    Ok(match value {
        Patch::Unchanged => Patch::Unchanged,
        Patch::Clear => Patch::Clear,
        Patch::Set(v) => Patch::Set(f(v)?),
    })
}
#[derive(Debug, Default, Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub site_id: Option<Uuid>,
    pub room_id: Option<Uuid>,
    pub rack_id: Option<Uuid>,
    pub hostname: Option<String>,
    pub serial: Option<String>,
    pub lifecycle_status: Option<ApiLifecycle>,
}
impl ListParams {
    pub fn page(&self) -> Result<domain::Pagination> {
        domain::Pagination::new(self.limit.unwrap_or(50), self.offset.unwrap_or(0))
    }
    pub fn device_filter(&self) -> domain::DeviceFilter {
        domain::DeviceFilter {
            rack_id: self.rack_id,
            hostname: self.hostname.clone(),
            serial: self.serial.clone(),
            lifecycle_status: self.lifecycle_status.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SiteDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::Site> for SiteDto {
    fn from(v: domain::Site) -> Self {
        Self { id: v.id, created_at: v.created_at, updated_at: v.updated_at, name: v.name, description: v.description }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSiteDto {
    pub name: String,
    pub description: Option<String>,
}
impl TryFrom<CreateSiteDto> for domain::CreateSite {
    type Error = Error;
    fn try_from(v: CreateSiteDto) -> Result<Self> {
        Ok(Self { name: v.name, description: v.description })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateSiteDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub description: Patch<String>,
}
impl TryFrom<UpdateSiteDto> for domain::UpdateSite {
    type Error = Error;
    fn try_from(v: UpdateSiteDto) -> Result<Self> {
        Ok(Self { name: v.name, description: v.description })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RoomDto {
    pub id: Uuid,
    pub site_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::Room> for RoomDto {
    fn from(v: domain::Room) -> Self {
        Self {
            id: v.id,
            created_at: v.created_at,
            updated_at: v.updated_at,
            site_id: v.site_id,
            name: v.name,
            description: v.description,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateRoomDto {
    pub site_id: Uuid,
    pub name: String,
    pub description: Option<String>,
}
impl TryFrom<CreateRoomDto> for domain::CreateRoom {
    type Error = Error;
    fn try_from(v: CreateRoomDto) -> Result<Self> {
        Ok(Self { site_id: v.site_id, name: v.name, description: v.description })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateRoomDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<Uuid>)]
    pub site_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub description: Patch<String>,
}
impl TryFrom<UpdateRoomDto> for domain::UpdateRoom {
    type Error = Error;
    fn try_from(v: UpdateRoomDto) -> Result<Self> {
        Ok(Self { site_id: v.site_id, name: v.name, description: v.description })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RackDto {
    pub id: Uuid,
    pub room_id: Uuid,
    pub name: String,
    pub height_u: i32,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::Rack> for RackDto {
    fn from(v: domain::Rack) -> Self {
        Self {
            id: v.id,
            created_at: v.created_at,
            updated_at: v.updated_at,
            room_id: v.room_id,
            name: v.name,
            height_u: v.height_u,
            description: v.description,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateRackDto {
    pub room_id: Uuid,
    pub name: String,
    pub height_u: i32,
    pub description: Option<String>,
}
impl TryFrom<CreateRackDto> for domain::CreateRack {
    type Error = Error;
    fn try_from(v: CreateRackDto) -> Result<Self> {
        Ok(Self { room_id: v.room_id, name: v.name, height_u: v.height_u, description: v.description })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateRackDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<Uuid>)]
    pub room_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<i32>)]
    pub height_u: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub description: Patch<String>,
}
impl TryFrom<UpdateRackDto> for domain::UpdateRack {
    type Error = Error;
    fn try_from(v: UpdateRackDto) -> Result<Self> {
        Ok(Self { room_id: v.room_id, name: v.name, height_u: v.height_u, description: v.description })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeviceDto {
    pub id: Uuid,
    pub rack_id: Option<Uuid>,
    pub start_unit: Option<i32>,
    pub height_u: Option<i32>,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub lifecycle_status: ApiLifecycle,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::Device> for DeviceDto {
    fn from(v: domain::Device) -> Self {
        Self {
            id: v.id,
            created_at: v.created_at,
            updated_at: v.updated_at,
            rack_id: v.rack_id,
            start_unit: v.start_unit,
            height_u: v.height_u,
            hostname: v.hostname,
            device_type: v.device_type,
            vendor: v.vendor,
            model: v.model,
            serial: v.serial,
            lifecycle_status: v.lifecycle_status.into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateDeviceDto {
    pub rack_id: Option<Uuid>,
    pub start_unit: Option<i32>,
    pub height_u: Option<i32>,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    #[serde(default)]
    pub lifecycle_status: ApiLifecycle,
}
impl TryFrom<CreateDeviceDto> for domain::CreateDevice {
    type Error = Error;
    fn try_from(v: CreateDeviceDto) -> Result<Self> {
        Ok(Self {
            rack_id: v.rack_id,
            start_unit: v.start_unit,
            height_u: v.height_u,
            hostname: v.hostname,
            device_type: v.device_type,
            vendor: v.vendor,
            model: v.model,
            serial: v.serial,
            lifecycle_status: v.lifecycle_status.into(),
        })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateDeviceDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<Uuid>)]
    pub rack_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<i32>)]
    pub start_unit: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<i32>)]
    pub height_u: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub hostname: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub device_type: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub vendor: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub model: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub serial: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<ApiLifecycle>)]
    pub lifecycle_status: Patch<ApiLifecycle>,
}
impl TryFrom<UpdateDeviceDto> for domain::UpdateDevice {
    type Error = Error;
    fn try_from(v: UpdateDeviceDto) -> Result<Self> {
        Ok(Self {
            rack_id: v.rack_id,
            start_unit: v.start_unit,
            height_u: v.height_u,
            hostname: v.hostname,
            device_type: v.device_type,
            vendor: v.vendor,
            model: v.model,
            serial: v.serial,
            lifecycle_status: try_patch(v.lifecycle_status, |v| Ok(v.into()))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InterfaceDto {
    pub id: Uuid,
    pub device_id: Uuid,
    pub if_index: Option<i32>,
    pub name: String,
    pub description: Option<String>,
    pub interface_type: Option<String>,
    pub mac_address: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::Interface> for InterfaceDto {
    fn from(v: domain::Interface) -> Self {
        Self {
            id: v.id,
            created_at: v.created_at,
            updated_at: v.updated_at,
            device_id: v.device_id,
            if_index: v.if_index,
            name: v.name,
            description: v.description,
            interface_type: v.interface_type,
            mac_address: v.mac_address.map(|v| v.to_string()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateInterfaceDto {
    pub device_id: Uuid,
    pub if_index: Option<i32>,
    pub name: String,
    pub description: Option<String>,
    pub interface_type: Option<String>,
    pub mac_address: Option<String>,
}
impl TryFrom<CreateInterfaceDto> for domain::CreateInterface {
    type Error = Error;
    fn try_from(v: CreateInterfaceDto) -> Result<Self> {
        Ok(Self {
            device_id: v.device_id,
            if_index: v.if_index,
            name: v.name,
            description: v.description,
            interface_type: v.interface_type,
            mac_address: v.mac_address.map(|v| parse(&v, "mac_address")).transpose()?,
        })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateInterfaceDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<Uuid>)]
    pub device_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<i32>)]
    pub if_index: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub description: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub interface_type: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub mac_address: Patch<String>,
}
impl TryFrom<UpdateInterfaceDto> for domain::UpdateInterface {
    type Error = Error;
    fn try_from(v: UpdateInterfaceDto) -> Result<Self> {
        Ok(Self {
            device_id: v.device_id,
            if_index: v.if_index,
            name: v.name,
            description: v.description,
            interface_type: v.interface_type,
            mac_address: try_patch(v.mac_address, |v| parse(&v, "mac_address"))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IpAddressDto {
    pub id: Uuid,
    pub interface_id: Uuid,
    pub address: String,
    pub is_management: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl From<domain::IpAddress> for IpAddressDto {
    fn from(v: domain::IpAddress) -> Self {
        Self {
            id: v.id,
            created_at: v.created_at,
            updated_at: v.updated_at,
            interface_id: v.interface_id,
            address: v.address.to_string(),
            is_management: v.is_management,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateIpAddressDto {
    pub interface_id: Uuid,
    pub address: String,
    #[serde(default)]
    pub is_management: bool,
}
impl TryFrom<CreateIpAddressDto> for domain::CreateIpAddress {
    type Error = Error;
    fn try_from(v: CreateIpAddressDto) -> Result<Self> {
        Ok(Self {
            interface_id: v.interface_id,
            address: parse(&v.address, "IP address")?,
            is_management: v.is_management,
        })
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateIpAddressDto {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<Uuid>)]
    pub interface_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub address: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<bool>)]
    pub is_management: Patch<bool>,
}
impl TryFrom<UpdateIpAddressDto> for domain::UpdateIpAddress {
    type Error = Error;
    fn try_from(v: UpdateIpAddressDto) -> Result<Self> {
        Ok(Self {
            interface_id: v.interface_id,
            address: try_patch(v.address, |v| parse(&v, "IP address"))?,
            is_management: v.is_management,
        })
    }
}
