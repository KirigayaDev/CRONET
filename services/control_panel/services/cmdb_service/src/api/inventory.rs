use super::{
    grpc::pb,
    http::{ApiJson, ApiPath, ApiQuery, HttpError, RequestIdentity},
    ApiState,
};
use crate::{
    auth::Principal,
    inventory::*,
    workflow::{MutationContext, Source},
    Error,
};
use axum::{
    extract::{DefaultBodyLimit, Extension, State},
    http::header,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use utoipa::OpenApi;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/devices/{device_id}/inventory",
            get(latest).post(submit).layer(DefaultBodyLimit::max(MAX_MESSAGE_BYTES)),
        )
        .route("/devices/{device_id}/inventory/diff", get(diff))
        .route("/devices/{device_id}/inventory/export", get(export))
}
#[derive(Deserialize)]
pub(super) struct DiffQuery {
    previous_id: Uuid,
    current_id: Uuid,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ExportFormat {
    Csv,
    Xlsx,
}
#[derive(Deserialize)]
pub(super) struct ExportQuery {
    format: ExportFormat,
}

#[utoipa::path(post,path="/devices/{device_id}/inventory",params(("device_id"=Uuid,Path)),request_body=SubmitInventory,security(("bearer_jwt"=[])),responses((status=200,body=SubmitInventoryResult),(status=400),(status=401),(status=403),(status=404),(status=409),(status=503)))]
async fn submit(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath(device): ApiPath<Uuid>,
    ApiJson(input): ApiJson<SubmitInventory>,
) -> Result<Json<SubmitInventoryResult>, HttpError> {
    Ok(Json(
        state
            .service
            .for_mutation(MutationContext::user(principal.user_id, Source::Http, id.0))
            .submit_inventory(device, input)
            .await?,
    ))
}
#[utoipa::path(get,path="/devices/{device_id}/inventory",params(("device_id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=200,body=InventorySnapshot),(status=401),(status=404),(status=503)))]
async fn latest(
    State(state): State<ApiState>,
    ApiPath(device): ApiPath<Uuid>,
) -> Result<Json<InventorySnapshot>, HttpError> {
    Ok(Json(state.service.latest_inventory(device).await?))
}
#[utoipa::path(get,path="/devices/{device_id}/inventory/diff",params(("device_id"=Uuid,Path),("previous_id"=Uuid,Query),("current_id"=Uuid,Query)),security(("bearer_jwt"=[])),responses((status=200,body=InventoryDiff),(status=400),(status=401),(status=404),(status=503)))]
async fn diff(
    State(state): State<ApiState>,
    ApiPath(device): ApiPath<Uuid>,
    ApiQuery(q): ApiQuery<DiffQuery>,
) -> Result<Json<InventoryDiff>, HttpError> {
    Ok(Json(state.service.compare_inventory(device, q.previous_id, q.current_id).await?))
}
#[utoipa::path(get,path="/devices/{device_id}/inventory/export",params(("device_id"=Uuid,Path),("format"=String,Query,description="csv or xlsx")),security(("bearer_jwt"=[])),responses((status=200,description="In-memory CSV or XLSX inventory attachment",body=String,content_type="application/octet-stream"),(status=400),(status=401),(status=404),(status=503)))]
async fn export(
    State(state): State<ApiState>,
    ApiPath(device): ApiPath<Uuid>,
    ApiQuery(q): ApiQuery<ExportQuery>,
) -> Result<Response, HttpError> {
    let snapshot = state.service.latest_inventory(device).await?;
    let (content_type, filename) = match q.format {
        ExportFormat::Csv => ("text/csv; charset=utf-8", "attachment; filename=inventory.csv"),
        ExportFormat::Xlsx => {
            ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "attachment; filename=inventory.xlsx")
        }
    };
    let bytes = tokio::task::spawn_blocking(move || match q.format {
        ExportFormat::Csv => export_csv(&snapshot),
        ExportFormat::Xlsx => export_xlsx(&snapshot),
    })
    .await
    .map_err(|_| Error::Internal("Inventory export task failed"))??;
    Ok(([(header::CONTENT_TYPE, content_type), (header::CONTENT_DISPOSITION, filename)], bytes).into_response())
}
#[derive(OpenApi)]
#[openapi(
    paths(submit, latest, diff, export),
    components(schemas(
        InventoryComponent,
        SubmitInventory,
        SnapshotMetadata,
        InventorySnapshot,
        SubmitInventoryResult,
        ComponentChange,
        InventoryDiff
    ))
)]
pub(super) struct InventoryDoc;

macro_rules! component_conversion {
    ($from:ty => $to:ty) => {
        impl From<$from> for $to {
            fn from(c: $from) -> Self {
                Self {
                    ent_physical_index: c.ent_physical_index,
                    contained_in: c.contained_in,
                    parent_rel_pos: c.parent_rel_pos,
                    class: c.class,
                    name: c.name,
                    description: c.description,
                    vendor_type: c.vendor_type,
                    serial: c.serial,
                    model: c.model,
                    hardware_rev: c.hardware_rev,
                    firmware_rev: c.firmware_rev,
                    software_rev: c.software_rev,
                }
            }
        }
    };
}
component_conversion!(InventoryComponent => pb::InventoryComponent);
component_conversion!(pb::InventoryComponent => InventoryComponent);
impl From<InventorySnapshot> for pb::InventorySnapshot {
    fn from(v: InventorySnapshot) -> Self {
        let m = v.metadata;
        let timestamp = |t: chrono::DateTime<chrono::Utc>| {
            Some(prost_types::Timestamp { seconds: t.timestamp(), nanos: t.timestamp_subsec_nanos() as i32 })
        };
        Self {
            id: m.id.to_string(),
            device_id: m.device_id.to_string(),
            collected_at: timestamp(m.collected_at),
            content_hash: m.content_hash,
            component_count: m.component_count,
            created_at: timestamp(m.created_at),
            components: v.components.into_iter().map(Into::into).collect(),
        }
    }
}
impl From<ComponentChange> for pb::InventoryComponentChange {
    fn from(v: ComponentChange) -> Self {
        Self {
            old: Some(v.old.into()),
            new: Some(v.new.into()),
            changed_fields: v.changed_fields,
            text_diff: v.text_diff,
        }
    }
}
impl From<InventoryDiff> for pb::InventoryDiff {
    fn from(v: InventoryDiff) -> Self {
        Self {
            added: v.added.into_iter().map(Into::into).collect(),
            removed: v.removed.into_iter().map(Into::into).collect(),
            changed: v.changed.into_iter().map(Into::into).collect(),
            replaced: v.replaced.into_iter().map(Into::into).collect(),
            unchanged: v.unchanged.into_iter().map(Into::into).collect(),
        }
    }
}
