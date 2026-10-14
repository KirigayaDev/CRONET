use chrono::{DateTime, Utc};
use ipnetwork::IpNetwork;
use mac_address::MacAddress;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Pagination {
    limit: i64,
    offset: i64,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            limit: 50,
            offset: 0,
        }
    }
}

impl Pagination {
    pub fn new(limit: i64, offset: i64) -> crate::Result<Self> {
        let page = Self { limit, offset };
        page.validate()?;
        Ok(page)
    }
    pub fn validate(self) -> crate::Result<()> {
        if !(1..=200).contains(&self.limit) || self.offset < 0 {
            return Err(crate::Error::validation(
                "Pagination requires limit 1..=200 and offset >= 0",
            ));
        }
        Ok(())
    }
    pub fn limit(self) -> i64 {
        self.limit
    }
    pub fn offset(self) -> i64 {
        self.offset
    }
}

#[derive(sqlx::FromRow)]
pub struct AuthUser {
    pub uuid: Uuid,
    pub token_version_uuid: Uuid,
    pub is_admin: bool,
    pub is_super_admin: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum LifecycleStatus {
    #[default]
    Active,
    Maintenance,
    Decommissioned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Sites,
    Rooms,
    Racks,
    Devices,
    Interfaces,
    IpAddresses,
}

impl EntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sites => "sites",
            Self::Rooms => "rooms",
            Self::Racks => "racks",
            Self::Devices => "devices",
            Self::Interfaces => "interfaces",
            Self::IpAddresses => "ip_addresses",
        }
    }
}

/// Missing field = Unchanged, JSON null = Clear, present value = Set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Patch<T> {
    #[default]
    Unchanged,
    Set(T),
    Clear,
}

impl<T> Patch<T> {
    pub fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }
    pub fn is_changed(&self) -> bool {
        !self.is_unchanged()
    }
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Set(value) => Some(value),
            _ => None,
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Patch<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(match Option::<T>::deserialize(deserializer)? {
            Some(value) => Self::Set(value),
            None => Self::Clear,
        })
    }
}

impl<T: Serialize> Serialize for Patch<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Set(value) => value.serialize(serializer),
            Self::Unchanged | Self::Clear => serializer.serialize_none(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Site {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Room {
    pub id: Uuid,
    pub site_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Rack {
    pub id: Uuid,
    pub room_id: Uuid,
    pub name: String,
    pub height_u: i32,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Device {
    pub id: Uuid,
    pub rack_id: Option<Uuid>,
    pub start_unit: Option<i32>,
    pub height_u: Option<i32>,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub lifecycle_status: LifecycleStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Interface {
    pub id: Uuid,
    pub device_id: Uuid,
    pub if_index: Option<i32>,
    pub name: String,
    pub description: Option<String>,
    pub interface_type: Option<String>,
    pub mac_address: Option<MacAddress>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IpAddress {
    pub id: Uuid,
    pub interface_id: Uuid,
    pub address: IpNetwork,
    pub is_management: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSite {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateSite {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub description: Patch<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoom {
    pub site_id: Uuid,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateRoom {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub site_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub description: Patch<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRack {
    pub room_id: Uuid,
    pub name: String,
    pub height_u: i32,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateRack {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub room_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub height_u: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub description: Patch<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateDevice {
    pub rack_id: Option<Uuid>,
    pub start_unit: Option<i32>,
    pub height_u: Option<i32>,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    #[serde(default = "active")]
    pub lifecycle_status: LifecycleStatus,
}

fn active() -> LifecycleStatus {
    LifecycleStatus::Active
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateDevice {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub rack_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub start_unit: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub height_u: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub hostname: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub device_type: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub vendor: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub model: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub serial: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub lifecycle_status: Patch<LifecycleStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceFilter {
    pub rack_id: Option<Uuid>,
    pub hostname: Option<String>,
    pub serial: Option<String>,
    pub lifecycle_status: Option<LifecycleStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateInterface {
    pub device_id: Uuid,
    pub if_index: Option<i32>,
    pub name: String,
    pub description: Option<String>,
    pub interface_type: Option<String>,
    pub mac_address: Option<MacAddress>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateInterface {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub device_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub if_index: Patch<i32>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub name: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub description: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub interface_type: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub mac_address: Patch<MacAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIpAddress {
    pub interface_id: Uuid,
    pub address: IpNetwork,
    pub is_management: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateIpAddress {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub interface_id: Patch<Uuid>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub address: Patch<IpNetwork>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    pub is_management: Patch<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_distinguishes_missing_null_and_value_and_round_trips() {
        for (json, expected) in [
            ("{}", Patch::Unchanged),
            (r#"{"description":null}"#, Patch::Clear),
            (r#"{"description":"new"}"#, Patch::Set("new".to_owned())),
        ] {
            let input: UpdateSite = serde_json::from_str(json).unwrap();
            assert_eq!(input.description, expected);
            let again: UpdateSite =
                serde_json::from_value(serde_json::to_value(input).unwrap()).unwrap();
            assert_eq!(again.description, expected);
        }
    }

    #[test]
    fn lifecycle_and_network_inputs_are_typed() {
        assert_eq!(
            serde_json::to_string(&LifecycleStatus::Decommissioned).unwrap(),
            "\"decommissioned\""
        );
        assert!(serde_json::from_str::<LifecycleStatus>("\"other\"").is_err());
        assert!("192.0.2.1/24".parse::<IpNetwork>().is_ok());
        assert!("2001:db8::1/64".parse::<IpNetwork>().is_ok());
        assert!("invalid".parse::<IpNetwork>().is_err());
        assert!("aa:bb:cc:dd:ee:ff".parse::<MacAddress>().is_ok());
        assert!("invalid".parse::<MacAddress>().is_err());
    }
}
