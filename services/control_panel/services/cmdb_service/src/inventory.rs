//! Normalized ENTITY-MIB snapshots. No SNMP transport or polling lives here.
use crate::{models::EntityType, repository::Repository, workflow::Action, Error, Result, Service};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::collections::{BTreeMap, BTreeSet};
use utoipa::ToSchema;
use uuid::Uuid;
mod export;
pub use export::{export_csv, export_xlsx};

pub const MAX_COMPONENTS: usize = 10_000;
// 10,000 bounded components including JSON escaping; shared by both transports.
pub const MAX_MESSAGE_BYTES: usize = 192 * 1024 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromRow, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InventoryComponent {
    pub ent_physical_index: i32,
    pub contained_in: Option<i32>,
    pub parent_rel_pos: Option<i32>,
    /// Normalized ENTITY-MIB physical class label, e.g. chassis, module, fan.
    pub class: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub vendor_type: Option<String>,
    pub serial: Option<String>,
    pub model: Option<String>,
    pub hardware_rev: Option<String>,
    pub firmware_rev: Option<String>,
    pub software_rev: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmitInventory {
    pub collected_at: DateTime<Utc>,
    pub components: Vec<InventoryComponent>,
}
#[derive(Clone, Debug, Serialize, Deserialize, FromRow, ToSchema)]
pub struct SnapshotMetadata {
    pub id: Uuid,
    pub device_id: Uuid,
    pub collected_at: DateTime<Utc>,
    pub content_hash: String,
    pub component_count: i32,
    pub created_at: DateTime<Utc>,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct InventorySnapshot {
    pub metadata: SnapshotMetadata,
    pub components: Vec<InventoryComponent>,
}
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SubmitInventoryResult {
    pub snapshot: InventorySnapshot,
    pub unchanged: bool,
}
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComponentChange {
    pub old: InventoryComponent,
    pub new: InventoryComponent,
    pub changed_fields: Vec<String>,
    pub text_diff: String,
}
#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct InventoryDiff {
    pub added: Vec<InventoryComponent>,
    pub removed: Vec<InventoryComponent>,
    pub changed: Vec<ComponentChange>,
    pub replaced: Vec<ComponentChange>,
    pub unchanged: Vec<InventoryComponent>,
}

pub fn canonicalize(mut components: Vec<InventoryComponent>) -> Result<(Vec<InventoryComponent>, String)> {
    if components.len() > MAX_COMPONENTS {
        return Err(Error::validation("Inventory exceeds 10000 components"));
    }
    let mut indices = BTreeSet::new();
    for c in &mut components {
        if c.ent_physical_index <= 0
            || !indices.insert(c.ent_physical_index)
            || c.contained_in.is_some_and(|v| v < 0 || v == c.ent_physical_index)
            || c.parent_rel_pos.is_some_and(|v| v < -1)
        {
            return Err(Error::validation("Invalid or duplicate ENTITY-MIB index/position"));
        }
        c.class = c.class.trim().to_ascii_lowercase();
        if c.class.is_empty() || c.class.len() > 64 || c.class.contains('\0') {
            return Err(Error::validation("Invalid physical class"));
        }
        for (value, limit) in [
            (&mut c.name, 255),
            (&mut c.description, 1024),
            (&mut c.vendor_type, 255),
            (&mut c.serial, 255),
            (&mut c.model, 255),
            (&mut c.hardware_rev, 255),
            (&mut c.firmware_rev, 255),
            (&mut c.software_rev, 255),
        ] {
            *value = value.take().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
            if value.as_ref().is_some_and(|v| v.chars().count() > limit || v.contains('\0')) {
                return Err(Error::validation("Inventory string exceeds limit or contains NUL"));
            }
        }
    }
    components.sort_by_key(|c| c.ent_physical_index);
    let bytes = serde_json::to_vec(&components).map_err(|_| Error::Internal("Inventory serialization failed"))?;
    Ok((components, blake3::hash(&bytes).to_hex().to_string()))
}

// Unknown (-1) relative positions and root containment cannot identify a slot.
fn slot(c: &InventoryComponent) -> (String, i32, i32) {
    match (c.contained_in, c.parent_rel_pos) {
        (Some(parent), Some(pos)) if parent > 0 && pos >= 0 => (c.class.clone(), parent, pos),
        _ => (c.class.clone(), -1, c.ent_physical_index),
    }
}

/// Match unique class/serial identities first (including moved components), then
/// unique logical slots. Ambiguous slots are never arbitrarily paired.
pub fn compare_components(old: &[InventoryComponent], new: &[InventoryComponent]) -> InventoryDiff {
    let mut old_left: BTreeMap<_, _> = old.iter().map(|c| (c.ent_physical_index, c)).collect();
    let mut new_left: BTreeMap<_, _> = new.iter().map(|c| (c.ent_physical_index, c)).collect();
    let mut pairs = Vec::new();
    for by_serial in [true, false] {
        let key = |c: &InventoryComponent| {
            if by_serial {
                c.serial.as_ref().map(|s| (c.class.clone(), s.clone()))
            } else {
                let (class, parent, pos) = slot(c);
                Some((class, format!("{parent}:{pos}")))
            }
        };
        let group = |values: &BTreeMap<i32, &InventoryComponent>| {
            let mut groups: BTreeMap<_, Vec<i32>> = BTreeMap::new();
            for (&i, c) in values {
                if let Some(key) = key(c) {
                    groups.entry(key).or_default().push(i);
                }
            }
            groups
        };
        let a = group(&old_left);
        let b = group(&new_left);
        for (key, ids) in a {
            if let (Some(&left), Some(right)) =
                (ids.first().filter(|_| ids.len() == 1), b.get(&key).filter(|v| v.len() == 1))
            {
                if let (Some(a), Some(b)) = (old_left.remove(&left), new_left.remove(&right[0])) {
                    pairs.push((a, b));
                }
            }
        }
    }
    // Missing serials in repeated/unknown slots can still be the exact same
    // indexed component. Never infer replacement from an ambiguous slot.
    let same_indices: Vec<_> = old_left.keys().filter(|i| new_left.contains_key(i)).copied().collect();
    for index in same_indices {
        if let (Some(a), Some(b)) = (old_left.get(&index), new_left.get(&index)) {
            if a.class != b.class || (a.serial.is_some() && b.serial.is_some() && a.serial != b.serial) {
                continue;
            }
        }
        if let (Some(a), Some(b)) = (old_left.remove(&index), new_left.remove(&index)) {
            pairs.push((a, b));
        }
    }
    pairs.sort_by_key(|(_, b)| b.ent_physical_index);
    let mut diff = InventoryDiff {
        removed: old_left.into_values().cloned().collect(),
        added: new_left.into_values().cloned().collect(),
        ..Default::default()
    };
    for (a, b) in pairs {
        if a == b {
            diff.unchanged.push(b.clone());
            continue;
        }
        let mut fields = Vec::new();
        let mut at = String::new();
        let mut bt = String::new();
        macro_rules! field { ($($f:ident),*) => { $(
            if a.$f != b.$f { fields.push(stringify!($f).to_owned()); }
            at.push_str(&format!("{}: {:?}\n", stringify!($f), a.$f));
            bt.push_str(&format!("{}: {:?}\n", stringify!($f), b.$f));
        )* }; }
        field!(
            ent_physical_index,
            contained_in,
            parent_rel_pos,
            class,
            name,
            description,
            vendor_type,
            serial,
            model,
            hardware_rev,
            firmware_rev,
            software_rev
        );
        let change = ComponentChange {
            old: a.clone(),
            new: b.clone(),
            changed_fields: fields,
            text_diff: similar::TextDiff::from_lines(&at, &bt).unified_diff().header("previous", "current").to_string(),
        };
        if a.serial.is_some() && b.serial.is_some() && a.serial != b.serial {
            diff.replaced.push(change);
        } else {
            diff.changed.push(change);
        }
    }
    diff
}

impl Service {
    pub async fn submit_inventory(&self, device_id: Uuid, input: SubmitInventory) -> Result<SubmitInventoryResult> {
        let context = self.context.ok_or(Error::validation("Inventory submission requires an audit context"))?;
        context.validate()?;
        if !(-62135596800..=253402300799).contains(&input.collected_at.timestamp()) {
            return Err(Error::validation("collected_at must be in years 0001..9999"));
        }
        let (components, hash) = canonicalize(input.components)?;
        self.retryable_transaction(move |tx| {
            let components = components.clone();
            let hash = hash.clone();
            Box::pin(async move {
                // Serialize per-device compare+insert, also preventing deletion mid-write.
                Repository::lock_entity(tx, EntityType::Devices, device_id).await?;
                if let Some(latest) = Repository::latest_inventory(tx, device_id).await? {
                    if latest.content_hash == hash {
                        let components = Repository::inventory_components(tx, latest.id).await?;
                        return Ok(SubmitInventoryResult {
                            snapshot: InventorySnapshot { metadata: latest, components },
                            unchanged: true,
                        });
                    }
                }
                let metadata =
                    Repository::insert_inventory(tx, device_id, input.collected_at, &hash, &components).await?;
                Repository::append_audit(
                    tx,
                    context,
                    EntityType::Devices,
                    device_id,
                    Action::Update,
                    None,
                    Some(serde_json::json!({"inventory_snapshot": &metadata})),
                )
                .await?;
                Ok(SubmitInventoryResult { snapshot: InventorySnapshot { metadata, components }, unchanged: false })
            })
        })
        .await
    }
    pub async fn latest_inventory(&self, device_id: Uuid) -> Result<InventorySnapshot> {
        let mut conn = self.pool.acquire().await?;
        let metadata = Repository::latest_inventory(&mut conn, device_id)
            .await?
            .ok_or(Error::NotFound { entity: "inventory", id: device_id })?;
        let components = Repository::inventory_components(&mut conn, metadata.id).await?;
        Ok(InventorySnapshot { metadata, components })
    }
    pub async fn compare_inventory(&self, device_id: Uuid, previous: Uuid, current: Uuid) -> Result<InventoryDiff> {
        let mut conn = self.pool.acquire().await?;
        for id in [previous, current] {
            Repository::inventory_metadata(&mut conn, device_id, id)
                .await?
                .ok_or(Error::NotFound { entity: "inventory snapshot", id })?;
        }
        let old = Repository::inventory_components(&mut conn, previous).await?;
        let new = Repository::inventory_components(&mut conn, current).await?;
        Ok(compare_components(&old, &new))
    }
}
