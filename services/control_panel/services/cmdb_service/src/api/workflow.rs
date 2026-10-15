//! HTTP staging/ownership DTOs and adapters; business rules stay in Service.
use super::{
    http::{ApiJson, ApiPath, ApiQuery, HttpError, RequestIdentity},
    ApiState,
};
use crate::{
    auth::Principal,
    imports::*,
    models::{EntityType, Pagination},
    workflow::*,
    Error,
};
use axum::{
    extract::{DefaultBodyLimit, Extension, FromRequest, Multipart, Request, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
    Json, Router,
};
use serde::Deserialize;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/audit-events", get(list_audit))
        .route("/audit-events/{id}", get(get_audit))
        .route("/field-overrides", get(list_overrides).post(set_override))
        .route("/field-overrides/{entity_type}/{entity_id}/{field_name}", delete(clear_override))
        .route("/imports/devices", post(upload).layer(DefaultBodyLimit::max(MAX_FILE_BYTES + 64 * 1024)))
        .route("/imports/{batch_id}", get(get_batch))
        .route("/imports/{batch_id}/rows", get(list_rows))
        .route("/imports/{batch_id}/rows/{row_id}", patch(edit_row))
        .route("/imports/{batch_id}/revalidate", post(revalidate))
        .route("/imports/{batch_id}/confirm", post(confirm))
        .route("/imports/{batch_id}/cancel", post(cancel))
}
#[derive(Default, Deserialize)]
pub(super) struct WorkflowQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    entity_type: Option<EntityType>,
    entity_id: Option<Uuid>,
    actor_id: Option<Uuid>,
    request_id: Option<Uuid>,
    import_batch_id: Option<Uuid>,
}
impl WorkflowQuery {
    fn page(&self) -> crate::Result<Pagination> {
        Pagination::new(self.limit.unwrap_or(50), self.offset.unwrap_or(0))
    }
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct OverrideInput {
    /// devices, interfaces or ip_addresses; fields use the documented allowlist.
    #[schema(value_type=String)]
    entity_type: EntityType,
    entity_id: Uuid,
    field_name: String,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfirmInput {
    accept_warnings: bool,
}
#[derive(ToSchema)]
pub(super) struct CsvUpload {
    #[schema(format=Binary)]
    #[allow(dead_code)]
    file: String,
}
fn scoped(state: &ApiState, principal: Principal, id: RequestIdentity) -> crate::Service {
    state.service.for_mutation(MutationContext::user(principal.user_id, Source::Http, id.0))
}
pub(super) struct ApiMultipart(Multipart);
impl<S: Send + Sync> FromRequest<S> for ApiMultipart {
    type Rejection = HttpError;
    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        Multipart::from_request(request, state)
            .await
            .map(Self)
            .map_err(|_| Error::validation("Invalid multipart upload").into())
    }
}

#[utoipa::path(get,path="/audit-events",params(
    ("entity_type"=Option<String>,Query),("entity_id"=Option<Uuid>,Query),("actor_id"=Option<Uuid>,Query),
    ("request_id"=Option<Uuid>,Query),("import_batch_id"=Option<Uuid>,Query),
    ("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0")),
    security(("bearer_jwt"=[])),responses((status=200,body=[AuditEvent]),(status=400,body=super::http::ErrorBody),(status=401,body=super::http::ErrorBody),(status=503,body=super::http::ErrorBody)))]
pub(super) async fn list_audit(
    State(state): State<ApiState>,
    ApiQuery(query): ApiQuery<WorkflowQuery>,
) -> Result<Json<Vec<AuditEvent>>, HttpError> {
    let filter = AuditFilter {
        entity_type: query.entity_type,
        entity_id: query.entity_id,
        actor_id: query.actor_id,
        request_id: query.request_id,
        import_batch_id: query.import_batch_id,
    };
    Ok(Json(state.service.list_audit_events(&filter, query.page()?).await?))
}
#[utoipa::path(get,path="/audit-events/{id}",params(("id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=200,body=AuditEvent),(status=404,body=super::http::ErrorBody),(status=401,body=super::http::ErrorBody)))]
pub(super) async fn get_audit(
    State(state): State<ApiState>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<AuditEvent>, HttpError> {
    Ok(Json(state.service.get_audit_event(id).await?))
}
#[utoipa::path(get,path="/field-overrides",params(("entity_type"=Option<String>,Query),("entity_id"=Option<Uuid>,Query),
    ("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0")),
    security(("bearer_jwt"=[])),responses((status=200,body=[FieldOverride]),(status=400,body=super::http::ErrorBody),(status=401,body=super::http::ErrorBody)))]
pub(super) async fn list_overrides(
    State(state): State<ApiState>,
    ApiQuery(query): ApiQuery<WorkflowQuery>,
) -> Result<Json<Vec<FieldOverride>>, HttpError> {
    Ok(Json(state.service.list_field_overrides(query.entity_type, query.entity_id, query.page()?).await?))
}
#[utoipa::path(post,path="/field-overrides",request_body=OverrideInput,security(("bearer_jwt"=[])),responses((status=401,body=super::http::ErrorBody),(status=201,body=FieldOverride),(status=400,body=super::http::ErrorBody),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=500,body=super::http::ErrorBody)))]
pub(super) async fn set_override(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<OverrideInput>,
) -> Result<(StatusCode, Json<FieldOverride>), HttpError> {
    Ok((
        StatusCode::CREATED,
        Json(
            scoped(&state, principal, id)
                .set_field_override(input.entity_type, input.entity_id, &input.field_name)
                .await?,
        ),
    ))
}
#[utoipa::path(delete,path="/field-overrides/{entity_type}/{entity_id}/{field_name}",params(("entity_type"=String,Path),("entity_id"=Uuid,Path),("field_name"=String,Path)),
    security(("bearer_jwt"=[])),responses((status=401,body=super::http::ErrorBody),(status=204,description="Override cleared"),(status=400,body=super::http::ErrorBody),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody)))]
pub(super) async fn clear_override(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath((entity, entity_id, field)): ApiPath<(EntityType, Uuid, String)>,
) -> Result<StatusCode, HttpError> {
    scoped(&state, principal, id).clear_field_override(entity, entity_id, &field).await?;
    Ok(StatusCode::NO_CONTENT)
}
#[utoipa::path(post,path="/imports/devices",request_body(content=CsvUpload,content_type="multipart/form-data"),security(("bearer_jwt"=[])),
    responses((status=401,body=super::http::ErrorBody),(status=201,body=BatchSummary),(status=400,body=super::http::ErrorBody),(status=403,body=super::http::ErrorBody),(status=500,body=super::http::ErrorBody)))]
pub(super) async fn upload(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiMultipart(mut multipart): ApiMultipart,
) -> Result<(StatusCode, Json<BatchSummary>), HttpError> {
    let mut file = None;
    while let Some(mut field) =
        multipart.next_field().await.map_err(|_| Error::validation("Invalid multipart upload"))?
    {
        if field.name() != Some("file") || file.is_some() {
            return Err(Error::validation("Expected exactly one CSV file part").into());
        }
        let filename = field.file_name().unwrap_or("devices.csv").to_owned();
        let mut bytes = Vec::new();
        while let Some(chunk) =
            field.chunk().await.map_err(|_| Error::validation("Invalid or oversized multipart upload"))?
        {
            if bytes.len() + chunk.len() > MAX_FILE_BYTES {
                return Err(Error::validation("CSV exceeds 5 MiB").into());
            }
            bytes.extend_from_slice(&chunk);
        }
        file = Some((filename, bytes));
    }
    let (filename, bytes) = file.ok_or_else(|| Error::validation("CSV file part is required"))?;
    Ok((StatusCode::CREATED, Json(scoped(&state, principal, id).upload_devices(&filename, &bytes).await?)))
}
#[utoipa::path(get,path="/imports/{batch_id}",params(("batch_id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=200,body=BatchSummary),(status=404,body=super::http::ErrorBody),(status=401,body=super::http::ErrorBody)))]
pub(super) async fn get_batch(
    State(state): State<ApiState>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<BatchSummary>, HttpError> {
    Ok(Json(state.service.get_import(id).await?))
}
#[utoipa::path(get,path="/imports/{batch_id}/rows",params(("batch_id"=Uuid,Path),("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0")),
    security(("bearer_jwt"=[])),responses((status=200,body=[ImportRow]),(status=400,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=401,body=super::http::ErrorBody)))]
pub(super) async fn list_rows(
    State(state): State<ApiState>,
    ApiPath(id): ApiPath<Uuid>,
    ApiQuery(query): ApiQuery<WorkflowQuery>,
) -> Result<Json<Vec<ImportRow>>, HttpError> {
    Ok(Json(state.service.list_import_rows(id, query.page()?).await?.into_iter().map(ImportRow::public).collect()))
}
#[utoipa::path(patch,path="/imports/{batch_id}/rows/{row_id}",request_body=ImportRowPatch,params(("batch_id"=Uuid,Path),("row_id"=Uuid,Path)),security(("bearer_jwt"=[])),
    responses((status=401,body=super::http::ErrorBody),(status=200,body=ImportRow),(status=400,body=super::http::ErrorBody),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=409,body=super::http::ErrorBody)))]
pub(super) async fn edit_row(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath((batch, row)): ApiPath<(Uuid, Uuid)>,
    ApiJson(input): ApiJson<ImportRowPatch>,
) -> Result<Json<ImportRow>, HttpError> {
    Ok(Json(scoped(&state, principal, id).edit_import_row(batch, row, input).await?.public()))
}
#[utoipa::path(post,path="/imports/{batch_id}/revalidate",params(("batch_id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=401,body=super::http::ErrorBody),(status=200,body=BatchSummary),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=409,body=super::http::ErrorBody)))]
pub(super) async fn revalidate(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath(batch): ApiPath<Uuid>,
) -> Result<Json<BatchSummary>, HttpError> {
    Ok(Json(scoped(&state, principal, id).revalidate_import(batch).await?))
}
#[utoipa::path(post,path="/imports/{batch_id}/confirm",request_body=ConfirmInput,params(("batch_id"=Uuid,Path)),security(("bearer_jwt"=[])),
    responses((status=401,body=super::http::ErrorBody),(status=200,body=BatchSummary),(status=400,body=super::http::ErrorBody),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=409,body=super::http::ErrorBody),(status=500,body=super::http::ErrorBody),(status=503,body=super::http::ErrorBody)))]
pub(super) async fn confirm(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath(batch): ApiPath<Uuid>,
    ApiJson(input): ApiJson<ConfirmInput>,
) -> Result<Json<BatchSummary>, HttpError> {
    Ok(Json(scoped(&state, principal, id).confirm_import(batch, input.accept_warnings).await?))
}
#[utoipa::path(post,path="/imports/{batch_id}/cancel",params(("batch_id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=401,body=super::http::ErrorBody),(status=200,body=BatchSummary),(status=403,body=super::http::ErrorBody),(status=404,body=super::http::ErrorBody),(status=409,body=super::http::ErrorBody)))]
pub(super) async fn cancel(
    State(state): State<ApiState>,
    Extension(principal): Extension<Principal>,
    Extension(id): Extension<RequestIdentity>,
    ApiPath(batch): ApiPath<Uuid>,
) -> Result<Json<BatchSummary>, HttpError> {
    Ok(Json(scoped(&state, principal, id).cancel_import(batch).await?))
}
#[derive(OpenApi)]
#[openapi(
    paths(
        list_audit,
        get_audit,
        list_overrides,
        set_override,
        clear_override,
        upload,
        get_batch,
        list_rows,
        edit_row,
        revalidate,
        confirm,
        cancel
    ),
    components(schemas(
        AuditEvent,
        FieldOverride,
        ImportBatch,
        BatchSummary,
        ImportRow,
        ImportData,
        ImportRowPatch,
        ValidationIssue,
        OverrideInput,
        ConfirmInput,
        CsvUpload
    ))
)]
pub(super) struct WorkflowDoc;
