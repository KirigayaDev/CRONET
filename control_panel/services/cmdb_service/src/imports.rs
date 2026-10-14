//! Bounded device CSV staging and atomic confirmation using the existing schema.
use crate::{
    models::*,
    repository::Repository,
    workflow::{json, Action, Source},
    Error, Result, Service,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Postgres, Transaction};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

pub const MAX_FILE_BYTES: usize = 5 * 1024 * 1024;
pub const MAX_ROWS: usize = 10_000;
pub const COLUMNS: [&str; 11] = [
    "site",
    "room",
    "rack",
    "start_unit",
    "height_u",
    "hostname",
    "device_type",
    "vendor",
    "model",
    "serial",
    "lifecycle_status",
];

#[derive(Clone, Debug, Serialize, Deserialize, Default, utoipa::ToSchema)]
pub struct ImportData {
    pub site: Option<String>,
    pub room: Option<String>,
    pub rack: Option<String>,
    /// Decimal text preserves invalid uploaded values until the user corrects them.
    pub start_unit: Option<String>,
    pub height_u: Option<String>,
    pub hostname: Option<String>,
    pub device_type: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub lifecycle_status: Option<String>,
    #[serde(default, rename = "_cleared_fields")]
    #[schema(ignore)]
    pub(crate) cleared_fields: BTreeSet<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Default, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ImportRowPatch {
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub site: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub room: Patch<String>,
    #[serde(default, skip_serializing_if = "Patch::is_unchanged")]
    #[schema(value_type=Option<String>)]
    pub rack: Patch<String>,
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
    /// active, maintenance or decommissioned.
    #[schema(value_type=Option<String>)]
    pub lifecycle_status: Patch<LifecycleStatus>,
}
impl ImportData {
    fn apply(&mut self, patch: &ImportRowPatch) -> Result<()> {
        macro_rules! text {
            ($field:ident, $technical:expr) => {
                match &patch.$field {
                    Patch::Unchanged => {}
                    Patch::Set(value) => {
                        if value.contains('\0') {
                            return Err(Error::validation("Import text must not contain NUL characters"));
                        }
                        self.$field = nonempty(value);
                        if $technical {
                            if self.$field.is_none() {
                                self.cleared_fields.insert(stringify!($field).into());
                            } else {
                                self.cleared_fields.remove(stringify!($field));
                            }
                        }
                    }
                    Patch::Clear => {
                        self.$field = None;
                        if $technical {
                            self.cleared_fields.insert(stringify!($field).into());
                        }
                    }
                }
            };
        }
        text!(site, false);
        text!(room, false);
        text!(rack, false);
        text!(hostname, true);
        text!(device_type, true);
        text!(vendor, true);
        text!(model, true);
        text!(serial, true);
        for (target, change) in [(&mut self.start_unit, &patch.start_unit), (&mut self.height_u, &patch.height_u)] {
            match change {
                Patch::Unchanged => {}
                Patch::Set(v) => *target = Some(v.to_string()),
                Patch::Clear => *target = None,
            }
        }
        match &patch.lifecycle_status {
            Patch::Unchanged => {}
            Patch::Clear => self.lifecycle_status = None,
            Patch::Set(v) => {
                self.lifecycle_status = Some(
                    match v {
                        LifecycleStatus::Active => "active",
                        LifecycleStatus::Maintenance => "maintenance",
                        LifecycleStatus::Decommissioned => "decommissioned",
                    }
                    .into(),
                )
            }
        }
        Ok(())
    }
    fn manual_fields(&self) -> Vec<&'static str> {
        [
            ("hostname", &self.hostname),
            ("device_type", &self.device_type),
            ("vendor", &self.vendor),
            ("model", &self.model),
            ("serial", &self.serial),
        ]
        .into_iter()
        .filter_map(|(field, value)| (value.is_some() || self.cleared_fields.contains(field)).then_some(field))
        .collect()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, FromRow, utoipa::ToSchema)]
pub struct ImportBatch {
    pub id: Uuid,
    pub created_by: Uuid,
    pub filename: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct BatchSummary {
    #[serde(flatten)]
    pub batch: ImportBatch,
    pub total_rows: i64,
    pub valid_rows: i64,
    pub invalid_rows: i64,
    pub warning_rows: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, FromRow, utoipa::ToSchema)]
pub struct ImportRow {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub row_number: i32,
    #[schema(value_type=Object,additional_properties=true)]
    pub raw_data: Value,
    #[schema(value_type=ImportData)]
    pub working_data: Value,
    #[schema(value_type=Vec<ValidationIssue>)]
    pub validation_errors: Value,
    #[schema(value_type=Vec<ValidationIssue>)]
    pub validation_warnings: Value,
    pub status: String,
}
impl ImportRow {
    /// Do not publish the internal PATCH intent bookkeeping.
    pub fn public(mut self) -> Self {
        if let Some(data) = self.working_data.as_object_mut() {
            data.remove("_cleared_fields");
        }
        self
    }
    fn data(&self) -> Result<ImportData> {
        serde_json::from_value(self.working_data.clone()).map_err(|_| Error::Internal("Stored import row invalid"))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}
fn issue(field: &str, code: &str, message: &str) -> ValidationIssue {
    ValidationIssue { field: field.into(), code: code.into(), message: message.into() }
}
fn nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

#[derive(Clone)]
pub struct ParsedRow {
    pub raw: Value,
    pub working: ImportData,
}
pub fn parse_csv(bytes: &[u8]) -> Result<Vec<ParsedRow>> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err(Error::validation("CSV exceeds 5 MiB"));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| Error::validation("CSV must be UTF-8"))?;
    if text.contains('\0') {
        return Err(Error::validation("CSV must not contain NUL characters"));
    }
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    strict_quotes(text.as_bytes())?;
    let mut reader = csv::ReaderBuilder::new().flexible(false).from_reader(text.as_bytes());
    let headers = reader.headers().map_err(|_| Error::validation("Malformed CSV header"))?.clone();
    let unique: BTreeSet<_> = headers.iter().collect();
    if headers.len() != COLUMNS.len() || unique.len() != COLUMNS.len() || !COLUMNS.iter().all(|h| unique.contains(h)) {
        return Err(Error::validation("CSV needs exactly the eleven unique template headers"));
    }
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|_| Error::validation("Malformed CSV row"))?;
        if rows.len() == MAX_ROWS {
            return Err(Error::validation("CSV exceeds 10000 rows"));
        }
        let raw: serde_json::Map<String, Value> =
            headers.iter().zip(record.iter()).map(|(h, v)| (h.to_owned(), Value::String(v.to_owned()))).collect();
        let working: serde_json::Map<String, Value> = raw
            .iter()
            .map(|(h, v)| {
                let value = v.as_str().and_then(nonempty).map(Value::String).unwrap_or(Value::Null);
                (h.clone(), value)
            })
            .collect();
        let mut working: ImportData =
            serde_json::from_value(Value::Object(working)).map_err(|_| Error::Internal("CSV conversion failed"))?;
        if working.lifecycle_status.is_none() {
            working.lifecycle_status = Some("active".into());
        }
        rows.push(ParsedRow { raw: Value::Object(raw), working });
    }
    if rows.is_empty() {
        return Err(Error::validation("CSV has no device rows"));
    }
    Ok(rows)
}
// csv deliberately accepts some unfinished/stray quotes. Enforce a strict
// comma/quote grammar first, while still accepting quoted newlines and CRLF/LF.
fn strict_quotes(bytes: &[u8]) -> Result<()> {
    let mut state = 0u8; // 0 field start, 1 unquoted, 2 quoted, 3 closing quote
    for &byte in bytes {
        state = match (state, byte) {
            (0, b'"') => 2,
            (0, b',' | b'\r' | b'\n') => 0,
            (0, _) => 1,
            (1, b'"') => return Err(Error::validation("Malformed CSV quoting")),
            (1, b',' | b'\r' | b'\n') => 0,
            (1, _) => 1,
            (2, b'"') => 3,
            (2, _) => 2,
            (3, b'"') => 2,
            (3, b',' | b'\r' | b'\n') => 0,
            _ => return Err(Error::validation("Malformed CSV quoting")),
        };
    }
    if state == 2 {
        return Err(Error::validation("Unclosed CSV quoted field"));
    }
    Ok(())
}
fn editable(batch: &ImportBatch) -> Result<()> {
    if !matches!(batch.status.as_str(), "pending" | "validated") {
        return Err(Error::InvalidState("Batch is finalized"));
    }
    Ok(())
}
fn dimension(value: &Option<String>, field: &str, errors: &mut Vec<ValidationIssue>) -> Option<i32> {
    match value.as_deref() {
        None => None,
        Some(value) => match value.parse::<i32>() {
            Ok(v) if v > 0 && value.bytes().all(|b| b.is_ascii_digit()) => Some(v),
            _ => {
                errors.push(issue(field, "positive_integer", "Expected a positive 32-bit integer"));
                None
            }
        },
    }
}

impl Service {
    async fn summary_in(tx: &mut Transaction<'_, Postgres>, batch: ImportBatch) -> Result<BatchSummary> {
        let (total_rows, valid_rows, invalid_rows, warning_rows) = Repository::import_counts(tx, batch.id).await?;
        Ok(BatchSummary { batch, total_rows, valid_rows, invalid_rows, warning_rows })
    }
    pub async fn get_import(&self, id: Uuid) -> Result<BatchSummary> {
        let mut tx = self.pool.begin().await?;
        // Hold the batch row lock so summary and counts describe one state.
        let batch = Repository::get_import_batch(&mut tx, id, true).await?;
        let summary = Self::summary_in(&mut tx, batch).await?;
        tx.commit().await?;
        Ok(summary)
    }
    pub async fn list_import_rows(&self, id: Uuid, page: Pagination) -> Result<Vec<ImportRow>> {
        let mut conn = self.pool.acquire().await?;
        Repository::get_import_batch(&mut conn, id, false).await?;
        Repository::list_import_rows(&mut conn, id, page).await
    }
    pub async fn upload_devices(&self, filename: &str, bytes: &[u8]) -> Result<BatchSummary> {
        let rows = parse_csv(bytes)?;
        let filename = filename.rsplit(['/', '\\']).next().unwrap_or("");
        if filename.is_empty() || filename.len() > 255 || filename.chars().any(char::is_control) {
            return Err(Error::validation("Invalid CSV filename"));
        }
        let actor = self.context.ok_or(Error::Internal("Import actor missing"))?.actor_id;
        self.retryable_transaction(|tx| {
            let rows = rows.clone();
            let filename = filename.to_owned();
            Box::pin(async move {
                let batch = Repository::create_import_batch(tx, actor, &filename).await?;
                let mut staged = Vec::with_capacity(rows.len());
                for (index, row) in rows.into_iter().enumerate() {
                    staged.push(
                        Repository::insert_import_row(tx, batch.id, index as i32 + 1, row.raw, json(&row.working)?)
                            .await?,
                    );
                }
                Self::validate_rows(tx, &mut staged).await?;
                let batch = Self::save_validation(tx, batch.id, &staged).await?;
                Self::summary_in(tx, batch).await
            })
        })
        .await
    }
    pub async fn edit_import_row(&self, batch_id: Uuid, row_id: Uuid, patch: ImportRowPatch) -> Result<ImportRow> {
        self.retryable_transaction(|tx| {
            let patch = patch.clone();
            Box::pin(async move {
                editable(&Repository::get_import_batch(tx, batch_id, true).await?)?;
                let mut rows = Repository::import_rows_locked(tx, batch_id).await?;
                let index = rows
                    .iter()
                    .position(|r| r.id == row_id)
                    .ok_or(Error::NotFound { entity: "import row", id: row_id })?;
                let mut data = rows[index].data()?;
                data.apply(&patch)?;
                rows[index].working_data = json(&data)?;
                Self::validate_rows(tx, &mut rows).await?;
                Self::save_validation(tx, batch_id, &rows).await?;
                Ok(rows.remove(index))
            })
        })
        .await
    }
    pub async fn revalidate_import(&self, id: Uuid) -> Result<BatchSummary> {
        self.retryable_transaction(|tx| {
            Box::pin(async move {
                editable(&Repository::get_import_batch(tx, id, true).await?)?;
                let mut rows = Repository::import_rows_locked(tx, id).await?;
                Self::validate_rows(tx, &mut rows).await?;
                let batch = Self::save_validation(tx, id, &rows).await?;
                Self::summary_in(tx, batch).await
            })
        })
        .await
    }
    pub async fn cancel_import(&self, id: Uuid) -> Result<BatchSummary> {
        self.retryable_transaction(|tx| {
            Box::pin(async move {
                editable(&Repository::get_import_batch(tx, id, true).await?)?;
                let batch = Repository::set_import_state(tx, id, "cancelled").await?;
                Self::summary_in(tx, batch).await
            })
        })
        .await
    }
    pub async fn confirm_import(&self, id: Uuid, accept_warnings: bool) -> Result<BatchSummary> {
        let mut context = self.context.ok_or(Error::Internal("Import actor missing"))?;
        context.source = Source::CsvImport;
        context.import_batch_id = Some(id);
        let result = self
            .retryable_transaction(|tx| {
                Box::pin(async move {
                    editable(&Repository::get_import_batch(tx, id, true).await?)?;
                    let mut rows = Repository::import_rows_locked(tx, id).await?;
                    let initial = Self::validate_rows(tx, &mut rows).await?;
                    let racks: BTreeSet<_> = initial.iter().flatten().filter_map(|v| v.rack_id).collect();
                    // Existing placement triggers serialize on these same rack rows.
                    // Lock in UUID order, then validate again against the current DB.
                    for rack in racks {
                        Repository::lock_entity(tx, EntityType::Racks, rack).await?;
                    }
                    let inputs = Self::validate_rows(tx, &mut rows).await?;
                    if inputs.iter().any(Option::is_none) {
                        return Err(Error::ImportValidation);
                    }
                    if !accept_warnings
                        && rows.iter().any(|r| r.validation_warnings.as_array().is_some_and(|v| !v.is_empty()))
                    {
                        return Err(Error::InvalidState("Import warnings need explicit acceptance"));
                    }
                    Self::save_validation(tx, id, &rows).await?;
                    for (row, input) in rows.iter().zip(inputs) {
                        let input = input.ok_or(Error::ImportValidation)?;
                        let device = Self::create_device_in(tx, &input).await?;
                        Self::record_mutation_in(
                            tx,
                            context,
                            EntityType::Devices,
                            device.id,
                            Action::Create,
                            None,
                            Some(json(&device)?),
                            &row.data()?.manual_fields(),
                        )
                        .await?;
                    }
                    Repository::mark_imported(tx, id).await?;
                    let batch = Repository::set_import_state(tx, id, "confirmed").await?;
                    Self::summary_in(tx, batch).await
                })
            })
            .await;
        if matches!(&result, Err(Error::ImportValidation | Error::Conflict { .. })) {
            // A separate staging-only transaction refreshes preview after rollback.
            // A concurrent cancellation/confirmation can prevent this refresh;
            // it must never replace or hide the original canonical failure.
            let _ = self.revalidate_import(id).await;
        }
        result
    }
    async fn save_validation(tx: &mut Transaction<'_, Postgres>, id: Uuid, rows: &[ImportRow]) -> Result<ImportBatch> {
        for row in rows {
            Repository::save_import_row(tx, row).await?;
        }
        Repository::set_import_state(
            tx,
            id,
            if rows.iter().all(|r| r.status == "valid") { "validated" } else { "pending" },
        )
        .await
    }
    async fn validate_rows(
        tx: &mut Transaction<'_, Postgres>,
        rows: &mut [ImportRow],
    ) -> Result<Vec<Option<CreateDevice>>> {
        if rows.is_empty() || rows.len() > MAX_ROWS {
            return Err(Error::validation("Invalid import row count"));
        }
        let mut inputs = Vec::with_capacity(rows.len());
        let mut errors = vec![Vec::<ValidationIssue>::new(); rows.len()];
        let mut warnings = vec![Vec::<ValidationIssue>::new(); rows.len()];
        let mut locations: HashMap<(String, String, String), Option<Rack>> = HashMap::new();
        for (index, row) in rows.iter().enumerate() {
            let data = row.data()?;
            let e = &mut errors[index];
            let start = dimension(&data.start_unit, "start_unit", e);
            let height = dimension(&data.height_u, "height_u", e);
            let lifecycle = match data.lifecycle_status.as_deref().unwrap_or("active") {
                "active" => LifecycleStatus::Active,
                "maintenance" => LifecycleStatus::Maintenance,
                "decommissioned" => LifecycleStatus::Decommissioned,
                _ => {
                    e.push(issue("lifecycle_status", "invalid", "Unknown lifecycle status"));
                    LifecycleStatus::Active
                }
            };
            let placed = data.site.is_some() || data.room.is_some() || data.rack.is_some() || data.start_unit.is_some();
            let mut rack_id = None;
            if placed {
                if let (Some(site), Some(room), Some(rack), Some(start), Some(height)) =
                    (&data.site, &data.room, &data.rack, start, height)
                {
                    let key = (site.clone(), room.clone(), rack.clone());
                    let found = if let Some(found) = locations.get(&key) {
                        found.clone()
                    } else {
                        let found = match Repository::find_site_by_name(tx, site).await? {
                            Some(site) => match Repository::find_room_by_name(tx, site.id, room).await? {
                                Some(room) => Repository::find_rack_by_name(tx, room.id, rack).await?,
                                None => None,
                            },
                            None => None,
                        };
                        locations.insert(key, found.clone());
                        found
                    };
                    if let Some(rack) = found {
                        rack_id = Some(rack.id);
                        if i64::from(start) + i64::from(height) - 1 > i64::from(rack.height_u) {
                            e.push(issue("start_unit", "out_of_bounds", "Placement exceeds rack height"));
                        }
                        if Repository::rack_overlaps(tx, rack.id, start, height).await? {
                            e.push(issue("start_unit", "occupied", "Rack units are occupied"));
                        }
                    } else {
                        e.push(issue("rack", "not_found", "Site/room/rack hierarchy does not exist"));
                    }
                } else {
                    e.push(issue(
                        "placement",
                        "incomplete",
                        "Placed devices require site, room, rack, start_unit and height_u",
                    ));
                }
            }
            let (hostname, serial) =
                Repository::inventory_duplicates(tx, data.hostname.as_deref(), data.serial.as_deref()).await?;
            if hostname {
                warnings[index].push(issue("hostname", "duplicate", "Hostname already exists in canonical inventory"));
            }
            if serial {
                warnings[index].push(issue("serial", "duplicate", "Serial already exists in canonical inventory"));
            }
            inputs.push(e.is_empty().then_some(CreateDevice {
                rack_id,
                start_unit: start,
                height_u: height,
                hostname: data.hostname,
                device_type: data.device_type,
                vendor: data.vendor,
                model: data.model,
                serial: data.serial,
                lifecycle_status: lifecycle,
            }));
        }
        let mut ranges: HashMap<Uuid, Vec<(i64, i64, usize)>> = HashMap::new();
        for (index, input) in inputs.iter().enumerate() {
            if let Some(input) = input {
                if let (Some(rack), Some(start), Some(height)) = (input.rack_id, input.start_unit, input.height_u) {
                    ranges.entry(rack).or_default().push((
                        i64::from(start),
                        i64::from(start) + i64::from(height),
                        index,
                    ));
                }
            }
        }
        let mut overlaps = BTreeSet::new();
        for ranges in ranges.values_mut() {
            ranges.sort_unstable();
            let mut previous: Option<(i64, usize)> = None;
            for &(start, end, index) in ranges.iter() {
                if let Some((max_end, other)) = previous {
                    if start < max_end {
                        overlaps.insert(index);
                        overlaps.insert(other);
                    }
                    if end > max_end {
                        previous = Some((end, index));
                    }
                } else {
                    previous = Some((end, index));
                }
            }
        }
        for index in overlaps {
            errors[index].push(issue(
                "start_unit",
                "batch_overlap",
                "Placement overlaps another valid row in this batch",
            ));
        }
        for (index, row) in rows.iter_mut().enumerate() {
            row.status = if errors[index].is_empty() {
                "valid"
            } else {
                inputs[index] = None;
                "invalid"
            }
            .into();
            row.validation_errors = json(&errors[index])?;
            row.validation_warnings = json(&warnings[index])?;
        }
        Ok(inputs)
    }
}
