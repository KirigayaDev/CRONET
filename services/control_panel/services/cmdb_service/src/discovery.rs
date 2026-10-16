//! Automatic ingestion; classification and network/credential handling stay outside CMDB.
use crate::{
    models::*,
    repository::Repository,
    workflow::{json, Action, ActorType, Source},
    Error, Result, Service,
};
use std::net::IpAddr;

#[derive(Clone, Debug)]
pub struct DiscoveryObservation {
    pub target_ip: IpAddr,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
}

#[derive(Debug)]
pub struct DiscoveryApplied {
    pub device: Device,
    pub created: bool,
    pub skipped_fields: Vec<String>,
}

fn conflict() -> Error {
    Error::Conflict { sqlstate: "23P01".into(), constraint: Some("discovery_identity_ambiguous".into()) }
}

impl DiscoveryObservation {
    fn normalize(mut self) -> Result<Self> {
        for value in [&mut self.hostname, &mut self.device_type, &mut self.vendor, &mut self.model, &mut self.serial] {
            if let Some(text) = value {
                if text.contains('\0') || text.len() > 1024 {
                    return Err(Error::validation("Invalid discovery text"));
                }
                *text = text.trim().to_owned();
                if text.is_empty() {
                    *value = None;
                }
            }
        }
        Ok(self)
    }
}

impl Service {
    pub async fn apply_discovery_observation(&self, input: DiscoveryObservation) -> Result<DiscoveryApplied> {
        let context = self.context.ok_or(Error::validation("Discovery actor context is required"))?;
        if context.source != Source::Discovery || !matches!(context.actor_type, ActorType::User) {
            return Err(Error::validation("Discovery requires its authenticated user context"));
        }
        context.validate()?;
        let input = input.normalize()?;
        self.retryable_transaction(|tx| {
            let input = input.clone();
            Box::pin(async move {
                Repository::lock_discovery_identity(tx).await?;
                let matches = Repository::discovery_ip_devices(tx, input.target_ip).await?;
                if matches.len() > 1 {
                    return Err(conflict());
                }
                let has_ip = !matches.is_empty();
                let mut id = matches.first().copied();
                if let (Some(ip_device), Some(serial)) = (id, &input.serial) {
                    if Repository::discovery_serial_devices(tx, serial)
                        .await?
                        .iter()
                        .any(|serial_device| *serial_device != ip_device)
                    {
                        return Err(conflict());
                    }
                }
                if id.is_none() {
                    if let Some(serial) = &input.serial {
                        let matches = Repository::discovery_serial_devices(tx, serial).await?;
                        if matches.len() > 1 {
                            return Err(conflict());
                        }
                        id = matches.first().copied();
                    }
                }
                let created = id.is_none();
                let mut skipped_fields = Vec::new();
                let device = if let Some(id) = id {
                    let before = Repository::lock_entity(tx, EntityType::Devices, id).await?;
                    let old: Device = serde_json::from_value(before.clone())
                        .map_err(|_| Error::Internal("Device snapshot invalid"))?;
                    let mut patch = UpdateDevice::default();
                    let mut changed = false;
                    macro_rules! observed {
                        ($field:ident) => {
                            if let Some(value) = &input.$field {
                                if Repository::get_override(tx, EntityType::Devices, id, stringify!($field))
                                    .await?
                                    .is_some()
                                {
                                    skipped_fields.push(stringify!($field).to_owned());
                                } else if old.$field.as_ref() != Some(value) {
                                    patch.$field = Patch::Set(value.clone());
                                    changed = true;
                                }
                            }
                        };
                    }
                    observed!(hostname);
                    observed!(device_type);
                    observed!(vendor);
                    observed!(model);
                    observed!(serial);
                    if changed {
                        let device = Self::update_device_in(tx, id, &patch).await?;
                        Self::record_mutation_in(
                            tx,
                            context,
                            EntityType::Devices,
                            id,
                            Action::Update,
                            Some(before),
                            Some(json(&device)?),
                            &[],
                        )
                        .await?;
                        device
                    } else {
                        old
                    }
                } else {
                    let device = Self::create_device_in(
                        tx,
                        &CreateDevice {
                            hostname: input.hostname,
                            device_type: input.device_type,
                            vendor: input.vendor,
                            model: input.model,
                            serial: input.serial,
                            ..Default::default()
                        },
                    )
                    .await?;
                    Self::record_mutation_in(
                        tx,
                        context,
                        EntityType::Devices,
                        device.id,
                        Action::Create,
                        None,
                        Some(json(&device)?),
                        &[],
                    )
                    .await?;
                    device
                };
                if !has_ip {
                    let interfaces = Repository::discovery_interfaces(tx, device.id).await?;
                    if interfaces.len() > 1 {
                        return Err(conflict());
                    }
                    let interface = if let Some(interface) = interfaces.into_iter().next() {
                        interface
                    } else {
                        let interface = Self::create_interface_in(
                            tx,
                            &CreateInterface {
                                device_id: device.id,
                                if_index: None,
                                name: "discovery-management".into(),
                                description: Some("Logical discovery endpoint; not an identified physical port".into()),
                                interface_type: Some("discovery/logical".into()),
                                mac_address: None,
                            },
                        )
                        .await?;
                        Self::record_mutation_in(
                            tx,
                            context,
                            EntityType::Interfaces,
                            interface.id,
                            Action::Create,
                            None,
                            Some(json(&interface)?),
                            &[],
                        )
                        .await?;
                        interface
                    };
                    let address = Self::create_ip_address_in(
                        tx,
                        &CreateIpAddress {
                            interface_id: interface.id,
                            address: input.target_ip.into(),
                            is_management: true,
                        },
                    )
                    .await?;
                    Self::record_mutation_in(
                        tx,
                        context,
                        EntityType::IpAddresses,
                        address.id,
                        Action::Create,
                        None,
                        Some(json(&address)?),
                        &[],
                    )
                    .await?;
                }
                Ok(DiscoveryApplied { device, created, skipped_fields })
            })
        })
        .await
    }
}
