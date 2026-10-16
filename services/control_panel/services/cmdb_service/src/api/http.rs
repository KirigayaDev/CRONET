use super::{dto::*, ApiState};
use crate::{
    auth::{AuthError, Principal},
    Error,
};
use axum::{
    body::Body,
    extract::{Extension, FromRequest, FromRequestParts, Path, Query, Request, State},
    http::{header, request::Parts, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{de::DeserializeOwned, Serialize};
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}
#[derive(Serialize, ToSchema)]
pub struct HealthBody {
    pub status: String,
}
pub enum HttpError {
    Core(Error),
    Auth(AuthError),
}
impl From<Error> for HttpError {
    fn from(v: Error) -> Self {
        Self::Core(v)
    }
}
impl From<AuthError> for HttpError {
    fn from(v: AuthError) -> Self {
        Self::Auth(v)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Core(Error::Validation(_)) => (StatusCode::BAD_REQUEST, "validation", "Invalid request"),
            Self::Core(Error::ImportValidation) => {
                (StatusCode::BAD_REQUEST, "import_validation", "Import validation failed; revalidate for row details")
            }
            Self::Core(Error::InvalidState(_)) => {
                (StatusCode::CONFLICT, "invalid_state", "Operation is not allowed in the current workflow state")
            }
            Self::Core(Error::NotFound { .. }) => (StatusCode::NOT_FOUND, "not_found", "Resource not found"),
            Self::Core(Error::Conflict { .. }) => (StatusCode::CONFLICT, "conflict", "Resource constraint conflict"),
            Self::Core(Error::Retryable { .. }) => {
                (StatusCode::SERVICE_UNAVAILABLE, "retryable", "Transaction could not complete")
            }
            Self::Core(Error::Database(_) | Error::Internal(_)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", "Internal server error")
            }
            Self::Auth(AuthError::Unauthorized) => {
                (StatusCode::UNAUTHORIZED, "unauthorized", "Authentication required")
            }
            Self::Auth(AuthError::Forbidden) => {
                (StatusCode::FORBIDDEN, "forbidden", "Write access requires admin privileges")
            }
            Self::Auth(AuthError::Unavailable) => {
                (StatusCode::SERVICE_UNAVAILABLE, "auth_unavailable", "Authentication dependencies unavailable")
            }
        };
        let mut response = (status, Json(ErrorBody { code: code.into(), message: message.into() })).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(header::WWW_AUTHENTICATE, axum::http::HeaderValue::from_static("Bearer"));
        }
        response
    }
}
pub(super) struct ApiJson<T>(pub(super) T);
impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = HttpError;
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(v)| Self(v))
            .map_err(|_| Error::validation("Invalid JSON").into())
    }
}
pub(super) struct ApiQuery<T>(pub(super) T);
impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = HttpError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(v)| Self(v))
            .map_err(|_| Error::validation("Invalid query").into())
    }
}
pub(super) struct ApiPath<T>(pub(super) T);
impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = HttpError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(v)| Self(v))
            .map_err(|_| Error::validation("Invalid path").into())
    }
}

async fn authenticate(State(state): State<ApiState>, mut request: Request, next: Next) -> Result<Response, HttpError> {
    if request.headers().get_all(header::AUTHORIZATION).iter().count() != 1 {
        return Err(AuthError::Unauthorized.into());
    }
    let header = request.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let principal = state.auth.authenticate(header).await?;
    if !matches!(*request.method(), axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS) {
        principal.require_write()?;
    }
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}
pub fn router(state: ApiState) -> Router {
    let protected = Router::new()
        .route("/sites", get(list_sites).post(create_site))
        .route("/sites/{id}", get(get_site).patch(update_site).delete(delete_site))
        .route("/rooms", get(list_rooms).post(create_room))
        .route("/rooms/{id}", get(get_room).patch(update_room).delete(delete_room))
        .route("/racks", get(list_racks).post(create_rack))
        .route("/racks/{id}", get(get_rack).patch(update_rack).delete(delete_rack))
        .route("/devices", get(list_devices).post(create_device))
        .route("/devices/{id}", get(get_device).patch(update_device))
        .route("/devices/{id}/decommission", post(decommission_device))
        .route("/devices/{device_id}/interfaces", get(list_interfaces))
        .route("/interfaces", post(create_interface))
        .route("/interfaces/{id}", get(get_interface).patch(update_interface).delete(delete_interface))
        .route("/interfaces/{interface_id}/ip-addresses", get(list_ip_addresses))
        .route("/ip-addresses", post(create_ip_address))
        .route("/ip-addresses/{id}", get(get_ip_address).patch(update_ip_address).delete(delete_ip_address))
        .merge(super::workflow::routes())
        .merge(super::inventory::routes())
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .nest("/api/v1/cmdb", protected)
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/openapi.json", get(openapi))
        .fallback(|| async { HttpError::Core(Error::NotFound { entity: "route", id: Uuid::nil() }).into_response() })
        .method_not_allowed_fallback(|| async {
            (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(ErrorBody { code: "method_not_allowed".into(), message: "Method not allowed".into() }),
            )
        })
        .with_state(state)
        .layer(middleware::from_fn(request_identity))
        .layer(tower_http::trace::TraceLayer::new_for_http().make_span_with(|r: &axum::http::Request<Body>| {
            let path = r.extensions().get::<axum::extract::MatchedPath>().map(|p| p.as_str()).unwrap_or("unmatched");
            tracing::info_span!("http_request",method=%r.method(),path=path)
        }))
}
#[derive(Clone, Copy)]
pub struct RequestIdentity(pub Uuid);

async fn request_identity(mut request: Request, next: Next) -> Response {
    let id = if request.headers().get_all("x-request-id").iter().count() == 1 {
        request.headers().get("x-request-id").and_then(|v| v.to_str().ok()).and_then(|v| Uuid::parse_str(v).ok())
    } else {
        None
    }
    .unwrap_or_else(Uuid::new_v4);
    request.extensions_mut().insert(RequestIdentity(id));
    let mut response = next.run(request).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&id.to_string()) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}
#[utoipa::path(get,path="/health/live",responses((status=200,body=HealthBody)))]
async fn live() -> Json<HealthBody> {
    Json(HealthBody { status: "ok".into() })
}
#[utoipa::path(get,path="/health/ready",responses((status=200,body=HealthBody),(status=503,body=HealthBody)))]
async fn ready(State(state): State<ApiState>) -> (StatusCode, Json<HealthBody>) {
    let ready = state.auth.ready().await;
    if !ready {
        tracing::warn!("readiness dependency check failed");
    }
    (
        if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE },
        Json(HealthBody { status: if ready { "ok" } else { "unavailable" }.into() }),
    )
}
#[utoipa::path(get,path="/openapi.json",responses((status=200,description="OpenAPI specification")))]
async fn openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

#[utoipa::path(get,path="/api/v1/cmdb/sites",params(("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),),security(("bearer_jwt"=[])),responses((status=200,body=[SiteDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_sites(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<SiteDto>>, HttpError> {
    Ok(Json(state.service.list_sites_page(query.page()?).await?.into_iter().map(Into::into).collect()))
}

#[utoipa::path(get,path="/api/v1/cmdb/sites/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=SiteDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_site(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<SiteDto>, HttpError> {
    Ok(Json(state.service.get_site(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/sites",request_body=CreateSiteDto,security(("bearer_jwt"=[])),responses((status=201,body=SiteDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_site(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateSiteDto>,
) -> Result<(StatusCode, Json<SiteDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_site(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/sites/{id}",request_body=UpdateSiteDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=SiteDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_site(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateSiteDto>,
) -> Result<Json<SiteDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_site(id, input.try_into()?).await?.into()))
}

#[utoipa::path(delete,path="/api/v1/cmdb/sites/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=204,description="Deleted"),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn delete_site(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<StatusCode, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    service.delete_site(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get,path="/api/v1/cmdb/rooms",params(("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),("site_id"=Option<Uuid>,Query),),security(("bearer_jwt"=[])),responses((status=200,body=[RoomDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_rooms(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<RoomDto>>, HttpError> {
    Ok(Json(state.service.list_rooms_page(query.site_id, query.page()?).await?.into_iter().map(Into::into).collect()))
}

#[utoipa::path(get,path="/api/v1/cmdb/rooms/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=RoomDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_room(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<RoomDto>, HttpError> {
    Ok(Json(state.service.get_room(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/rooms",request_body=CreateRoomDto,security(("bearer_jwt"=[])),responses((status=201,body=RoomDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_room(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateRoomDto>,
) -> Result<(StatusCode, Json<RoomDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_room(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/rooms/{id}",request_body=UpdateRoomDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=RoomDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_room(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateRoomDto>,
) -> Result<Json<RoomDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_room(id, input.try_into()?).await?.into()))
}

#[utoipa::path(delete,path="/api/v1/cmdb/rooms/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=204,description="Deleted"),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn delete_room(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<StatusCode, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    service.delete_room(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get,path="/api/v1/cmdb/racks",params(("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),("room_id"=Option<Uuid>,Query),),security(("bearer_jwt"=[])),responses((status=200,body=[RackDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_racks(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<RackDto>>, HttpError> {
    Ok(Json(state.service.list_racks_page(query.room_id, query.page()?).await?.into_iter().map(Into::into).collect()))
}

#[utoipa::path(get,path="/api/v1/cmdb/racks/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=RackDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_rack(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<RackDto>, HttpError> {
    Ok(Json(state.service.get_rack(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/racks",request_body=CreateRackDto,security(("bearer_jwt"=[])),responses((status=201,body=RackDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_rack(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateRackDto>,
) -> Result<(StatusCode, Json<RackDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_rack(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/racks/{id}",request_body=UpdateRackDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=RackDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_rack(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateRackDto>,
) -> Result<Json<RackDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_rack(id, input.try_into()?).await?.into()))
}

#[utoipa::path(delete,path="/api/v1/cmdb/racks/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=204,description="Deleted"),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn delete_rack(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<StatusCode, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    service.delete_rack(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get,path="/api/v1/cmdb/devices",params(("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),("rack_id"=Option<Uuid>,Query),("hostname"=Option<String>,Query,description="Exact filter"),("serial"=Option<String>,Query,description="Exact filter"),("lifecycle_status"=Option<ApiLifecycle>,Query),),security(("bearer_jwt"=[])),responses((status=200,body=[DeviceDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_devices(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<DeviceDto>>, HttpError> {
    Ok(Json(
        state
            .service
            .list_devices_page(&query.device_filter(), query.page()?)
            .await?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

#[utoipa::path(get,path="/api/v1/cmdb/devices/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=DeviceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_device(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<DeviceDto>, HttpError> {
    Ok(Json(state.service.get_device(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/devices",request_body=CreateDeviceDto,security(("bearer_jwt"=[])),responses((status=201,body=DeviceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_device(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateDeviceDto>,
) -> Result<(StatusCode, Json<DeviceDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_device(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/devices/{id}",request_body=UpdateDeviceDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=DeviceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_device(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateDeviceDto>,
) -> Result<Json<DeviceDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_device(id, input.try_into()?).await?.into()))
}

#[utoipa::path(get,path="/api/v1/cmdb/devices/{device_id}/interfaces",params(("device_id"=Uuid,Path),("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),),security(("bearer_jwt"=[])),responses((status=200,body=[InterfaceDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_interfaces(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(device_id): ApiPath<Uuid>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<InterfaceDto>>, HttpError> {
    Ok(Json(state.service.list_interfaces_page(device_id, query.page()?).await?.into_iter().map(Into::into).collect()))
}

#[utoipa::path(get,path="/api/v1/cmdb/interfaces/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=InterfaceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_interface(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<InterfaceDto>, HttpError> {
    Ok(Json(state.service.get_interface(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/interfaces",request_body=CreateInterfaceDto,security(("bearer_jwt"=[])),responses((status=201,body=InterfaceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_interface(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateInterfaceDto>,
) -> Result<(StatusCode, Json<InterfaceDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_interface(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/interfaces/{id}",request_body=UpdateInterfaceDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=InterfaceDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_interface(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateInterfaceDto>,
) -> Result<Json<InterfaceDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_interface(id, input.try_into()?).await?.into()))
}

#[utoipa::path(delete,path="/api/v1/cmdb/interfaces/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=204,description="Deleted"),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn delete_interface(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<StatusCode, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    service.delete_interface(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get,path="/api/v1/cmdb/interfaces/{interface_id}/ip-addresses",params(("interface_id"=Uuid,Path),("limit"=Option<i64>,Query,minimum=1,maximum=200,description="Default 50"),("offset"=Option<i64>,Query,minimum=0,description="Default 0"),),security(("bearer_jwt"=[])),responses((status=200,body=[IpAddressDto]),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn list_ip_addresses(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(interface_id): ApiPath<Uuid>,
    ApiQuery(query): ApiQuery<ListParams>,
) -> Result<Json<Vec<IpAddressDto>>, HttpError> {
    Ok(Json(
        state.service.list_ip_addresses_page(interface_id, query.page()?).await?.into_iter().map(Into::into).collect(),
    ))
}

#[utoipa::path(get,path="/api/v1/cmdb/ip-addresses/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=IpAddressDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn get_ip_address(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<IpAddressDto>, HttpError> {
    Ok(Json(state.service.get_ip_address(id).await?.into()))
}

#[utoipa::path(post,path="/api/v1/cmdb/ip-addresses",request_body=CreateIpAddressDto,security(("bearer_jwt"=[])),responses((status=201,body=IpAddressDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn create_ip_address(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiJson(input): ApiJson<CreateIpAddressDto>,
) -> Result<(StatusCode, Json<IpAddressDto>), HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok((StatusCode::CREATED, Json(service.create_ip_address(input.try_into()?).await?.into())))
}

#[utoipa::path(patch,path="/api/v1/cmdb/ip-addresses/{id}",request_body=UpdateIpAddressDto,params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=200,body=IpAddressDto),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn update_ip_address(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
    ApiJson(input): ApiJson<UpdateIpAddressDto>,
) -> Result<Json<IpAddressDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.update_ip_address(id, input.try_into()?).await?.into()))
}

#[utoipa::path(delete,path="/api/v1/cmdb/ip-addresses/{id}",params(("id"=Uuid,Path),),security(("bearer_jwt"=[])),responses((status=204,description="Deleted"),(status=400,body=ErrorBody),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn delete_ip_address(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<StatusCode, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    service.delete_ip_address(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post,path="/api/v1/cmdb/devices/{id}/decommission",params(("id"=Uuid,Path)),security(("bearer_jwt"=[])),responses((status=200,body=DeviceDto),(status=401,body=ErrorBody),(status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=500,body=ErrorBody),(status=503,body=ErrorBody)))]
async fn decommission_device(
    State(state): State<ApiState>,
    Extension(_principal): Extension<Principal>,
    Extension(request_id): Extension<RequestIdentity>,
    ApiPath(id): ApiPath<Uuid>,
) -> Result<Json<DeviceDto>, HttpError> {
    let service = state.service.for_mutation(crate::workflow::MutationContext::user(
        _principal.user_id,
        crate::workflow::Source::Http,
        request_id.0,
    ));
    Ok(Json(service.decommission_device(id).await?.into()))
}
struct Security;
impl utoipa::Modify for Security {
    fn modify(&self, api: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = api.components.as_mut() {
            components.add_security_scheme(
                "bearer_jwt",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::HttpBuilder::new()
                        .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}
#[derive(OpenApi)]
#[openapi(paths(live,ready,openapi,list_sites,get_site,create_site,update_site,delete_site,list_rooms,get_room,create_room,update_room,delete_room,list_racks,get_rack,create_rack,update_rack,delete_rack,list_devices,get_device,create_device,update_device,list_interfaces,get_interface,create_interface,update_interface,delete_interface,list_ip_addresses,get_ip_address,create_ip_address,update_ip_address,delete_ip_address,decommission_device),components(schemas(ErrorBody,HealthBody,ApiLifecycle,SiteDto,CreateSiteDto,UpdateSiteDto,RoomDto,CreateRoomDto,UpdateRoomDto,RackDto,CreateRackDto,UpdateRackDto,DeviceDto,CreateDeviceDto,UpdateDeviceDto,InterfaceDto,CreateInterfaceDto,UpdateInterfaceDto,IpAddressDto,CreateIpAddressDto,UpdateIpAddressDto)),nest((path="/api/v1/cmdb",api=super::workflow::WorkflowDoc),(path="/api/v1/cmdb",api=super::inventory::InventoryDoc)),modifiers(&Security))]
pub struct ApiDoc;
