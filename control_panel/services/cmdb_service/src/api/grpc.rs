use super::{dto::parse, ApiState};
use crate::{
    auth::{AuthError, Principal},
    models as domain, Error,
};
use tonic::{Request, Response, Status};

pub mod pb {
    tonic::include_proto!("cronet.cmdb.v1");
}

#[derive(Clone)]
pub struct GrpcApi {
    state: ApiState,
}

pub fn domain_status(error: Error) -> Status {
    match error {
        Error::Validation(_) => Status::invalid_argument("Invalid request"),
        Error::ImportValidation => Status::invalid_argument("Import validation failed"),
        Error::InvalidState(_) => Status::failed_precondition("Invalid workflow state"),
        Error::NotFound { .. } => Status::not_found("Resource not found"),
        Error::Conflict { sqlstate, .. } if sqlstate == "23505" => Status::already_exists("Resource already exists"),
        Error::Conflict { .. } => Status::failed_precondition("Resource constraint conflict"),
        Error::Retryable { .. } => Status::unavailable("Transaction could not complete"),
        Error::Database(_) | Error::Internal(_) => Status::internal("Internal server error"),
    }
}
pub fn auth_status(error: AuthError) -> Status {
    match error {
        AuthError::Unauthorized => Status::unauthenticated("Authentication required"),
        AuthError::Forbidden => Status::permission_denied("Write access requires admin privileges"),
        AuthError::Unavailable => Status::unavailable("Authentication dependencies unavailable"),
    }
}

impl GrpcApi {
    pub fn new(state: ApiState) -> Self {
        Self { state }
    }
    async fn authorize<T>(&self, request: &mut Request<T>, write: bool) -> Result<Principal, Status> {
        tracing::info!(rpc = std::any::type_name::<T>(), "gRPC request");
        if request.metadata().get_all("authorization").iter().count() != 1 {
            return Err(auth_status(AuthError::Unauthorized));
        }
        let header = request.metadata().get("authorization").and_then(|v| v.to_str().ok());
        let principal = self.state.auth.authenticate(header).await.map_err(auth_status)?;
        if write {
            principal.require_write().map_err(auth_status)?;
        }
        request.extensions_mut().insert(principal);
        Ok(principal)
    }
}

fn page(limit: Option<i64>, offset: Option<i64>) -> Result<domain::Pagination, Status> {
    domain::Pagination::new(limit.unwrap_or(50), offset.unwrap_or(0)).map_err(domain_status)
}
fn uuid(value: &str) -> Result<uuid::Uuid, Status> {
    parse(value, "UUID").map_err(domain_status)
}
fn optional_uuid(value: Option<String>) -> Result<Option<uuid::Uuid>, Status> {
    value.map(|v| uuid(&v)).transpose()
}
fn lifecycle(value: i32) -> Result<domain::LifecycleStatus, Status> {
    match pb::LifecycleStatus::try_from(value) {
        Ok(pb::LifecycleStatus::Active) => Ok(domain::LifecycleStatus::Active),
        Ok(pb::LifecycleStatus::Maintenance) => Ok(domain::LifecycleStatus::Maintenance),
        Ok(pb::LifecycleStatus::Decommissioned) => Ok(domain::LifecycleStatus::Decommissioned),
        _ => Err(Status::invalid_argument("Invalid lifecycle_status")),
    }
}
fn string_patch(value: Option<pb::StringPatch>) -> Result<domain::Patch<String>, Status> {
    use pb::string_patch::Change;
    match value {
        None => Ok(domain::Patch::Unchanged),
        Some(pb::StringPatch { change: Some(Change::Value(v)) }) => Ok(domain::Patch::Set(v)),
        Some(pb::StringPatch { change: Some(Change::Clear(0)) }) => Ok(domain::Patch::Clear),
        _ => Err(Status::invalid_argument("Patch must specify value or clear")),
    }
}
fn int_patch(value: Option<pb::Int32Patch>) -> Result<domain::Patch<i32>, Status> {
    use pb::int32_patch::Change;
    match value {
        None => Ok(domain::Patch::Unchanged),
        Some(pb::Int32Patch { change: Some(Change::Value(v)) }) => Ok(domain::Patch::Set(v)),
        Some(pb::Int32Patch { change: Some(Change::Clear(0)) }) => Ok(domain::Patch::Clear),
        _ => Err(Status::invalid_argument("Patch must specify value or clear")),
    }
}
fn parsed_patch<T: std::str::FromStr>(value: Option<pb::StringPatch>, field: &str) -> Result<domain::Patch<T>, Status> {
    Ok(match string_patch(value)? {
        domain::Patch::Unchanged => domain::Patch::Unchanged,
        domain::Patch::Clear => domain::Patch::Clear,
        domain::Patch::Set(v) => domain::Patch::Set(parse(&v, field).map_err(domain_status)?),
    })
}

#[tonic::async_trait]
impl pb::cmdb_server::Cmdb for GrpcApi {
    async fn get_device(&self, mut request: Request<pb::IdRequest>) -> Result<Response<pb::Device>, Status> {
        let _principal = self.authorize(&mut request, false).await?;
        let id = uuid(&request.into_inner().id)?;
        Ok(Response::new(self.state.service.get_device(id).await.map_err(domain_status)?.into()))
    }
    async fn list_devices(
        &self,
        mut request: Request<pb::ListDevicesRequest>,
    ) -> Result<Response<pb::ListDevicesResponse>, Status> {
        let _principal = self.authorize(&mut request, false).await?;
        let v = request.into_inner();
        let page = page(v.limit, v.offset)?;
        let filter = domain::DeviceFilter {
            rack_id: optional_uuid(v.rack_id)?,
            hostname: v.hostname,
            serial: v.serial,
            lifecycle_status: v.lifecycle_status.map(lifecycle).transpose()?,
        };
        let devices = self.state.service.list_devices_page(&filter, page).await.map_err(domain_status)?;
        Ok(Response::new(pb::ListDevicesResponse { devices: devices.into_iter().map(Into::into).collect() }))
    }
    async fn create_device(
        &self,
        mut request: Request<pb::CreateDeviceRequest>,
    ) -> Result<Response<pb::Device>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::CreateDevice {
            rack_id: optional_uuid(v.rack_id)?,
            start_unit: v.start_unit,
            height_u: v.height_u,
            hostname: v.hostname,
            device_type: v.device_type,
            vendor: v.vendor,
            model: v.model,
            serial: v.serial,
            lifecycle_status: if v.lifecycle_status == 0 {
                domain::LifecycleStatus::Active
            } else {
                lifecycle(v.lifecycle_status)?
            },
        };
        Ok(Response::new(service.create_device(input).await.map_err(domain_status)?.into()))
    }
    async fn update_device(
        &self,
        mut request: Request<pb::UpdateDeviceRequest>,
    ) -> Result<Response<pb::Device>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::UpdateDevice {
            rack_id: parsed_patch(v.rack_id, "rack_id")?,
            start_unit: int_patch(v.start_unit)?,
            height_u: int_patch(v.height_u)?,
            hostname: string_patch(v.hostname)?,
            device_type: string_patch(v.device_type)?,
            vendor: string_patch(v.vendor)?,
            model: string_patch(v.model)?,
            serial: string_patch(v.serial)?,
            lifecycle_status: match v.lifecycle_status {
                Some(v) => domain::Patch::Set(lifecycle(v)?),
                None => domain::Patch::Unchanged,
            },
        };
        Ok(Response::new(service.update_device(uuid(&v.id)?, input).await.map_err(domain_status)?.into()))
    }
    async fn decommission_device(&self, mut request: Request<pb::IdRequest>) -> Result<Response<pb::Device>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        Ok(Response::new(
            service.decommission_device(uuid(&request.into_inner().id)?).await.map_err(domain_status)?.into(),
        ))
    }
    async fn list_interfaces(
        &self,
        mut request: Request<pb::ListInterfacesRequest>,
    ) -> Result<Response<pb::ListInterfacesResponse>, Status> {
        let _principal = self.authorize(&mut request, false).await?;
        let v = request.into_inner();
        let rows = self
            .state
            .service
            .list_interfaces_page(uuid(&v.device_id)?, page(v.limit, v.offset)?)
            .await
            .map_err(domain_status)?;
        Ok(Response::new(pb::ListInterfacesResponse { interfaces: rows.into_iter().map(Into::into).collect() }))
    }
    async fn create_interface(
        &self,
        mut request: Request<pb::CreateInterfaceRequest>,
    ) -> Result<Response<pb::Interface>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::CreateInterface {
            device_id: uuid(&v.device_id)?,
            if_index: v.if_index,
            name: v.name,
            description: v.description,
            interface_type: v.interface_type,
            mac_address: v.mac_address.map(|v| parse(&v, "mac_address").map_err(domain_status)).transpose()?,
        };
        Ok(Response::new(service.create_interface(input).await.map_err(domain_status)?.into()))
    }
    async fn update_interface(
        &self,
        mut request: Request<pb::UpdateInterfaceRequest>,
    ) -> Result<Response<pb::Interface>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::UpdateInterface {
            device_id: match v.device_id {
                Some(v) => domain::Patch::Set(uuid(&v)?),
                None => domain::Patch::Unchanged,
            },
            if_index: int_patch(v.if_index)?,
            name: v.name.map(domain::Patch::Set).unwrap_or_default(),
            description: string_patch(v.description)?,
            interface_type: string_patch(v.interface_type)?,
            mac_address: parsed_patch(v.mac_address, "mac_address")?,
        };
        Ok(Response::new(service.update_interface(uuid(&v.id)?, input).await.map_err(domain_status)?.into()))
    }
    async fn list_ip_addresses(
        &self,
        mut request: Request<pb::ListIpAddressesRequest>,
    ) -> Result<Response<pb::ListIpAddressesResponse>, Status> {
        let _principal = self.authorize(&mut request, false).await?;
        let v = request.into_inner();
        let rows = self
            .state
            .service
            .list_ip_addresses_page(uuid(&v.interface_id)?, page(v.limit, v.offset)?)
            .await
            .map_err(domain_status)?;
        Ok(Response::new(pb::ListIpAddressesResponse { ip_addresses: rows.into_iter().map(Into::into).collect() }))
    }
    async fn create_ip_address(
        &self,
        mut request: Request<pb::CreateIpAddressRequest>,
    ) -> Result<Response<pb::IpAddress>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::CreateIpAddress {
            interface_id: uuid(&v.interface_id)?,
            address: parse(&v.address, "IP address").map_err(domain_status)?,
            is_management: v.is_management,
        };
        Ok(Response::new(service.create_ip_address(input).await.map_err(domain_status)?.into()))
    }
    async fn update_ip_address(
        &self,
        mut request: Request<pb::UpdateIpAddressRequest>,
    ) -> Result<Response<pb::IpAddress>, Status> {
        let _principal = self.authorize(&mut request, true).await?;
        let context = mutation_context(_principal, &request);
        let service = self.state.service.for_mutation(context);
        let v = request.into_inner();
        let input = domain::UpdateIpAddress {
            interface_id: match v.interface_id {
                Some(v) => domain::Patch::Set(uuid(&v)?),
                None => domain::Patch::Unchanged,
            },
            address: match v.address {
                Some(v) => domain::Patch::Set(parse(&v, "IP address").map_err(domain_status)?),
                None => domain::Patch::Unchanged,
            },
            is_management: v.is_management.map(domain::Patch::Set).unwrap_or_default(),
        };
        Ok(Response::new(service.update_ip_address(uuid(&v.id)?, input).await.map_err(domain_status)?.into()))
    }
    async fn get_rack(&self, mut request: Request<pb::IdRequest>) -> Result<Response<pb::Rack>, Status> {
        let _principal = self.authorize(&mut request, false).await?;
        Ok(Response::new(
            self.state.service.get_rack(uuid(&request.into_inner().id)?).await.map_err(domain_status)?.into(),
        ))
    }
}

fn timestamp(value: chrono::DateTime<chrono::Utc>) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds: value.timestamp(), nanos: value.timestamp_subsec_nanos() as i32 })
}
fn status(value: domain::LifecycleStatus) -> i32 {
    match value {
        domain::LifecycleStatus::Active => pb::LifecycleStatus::Active as i32,
        domain::LifecycleStatus::Maintenance => pb::LifecycleStatus::Maintenance as i32,
        domain::LifecycleStatus::Decommissioned => pb::LifecycleStatus::Decommissioned as i32,
    }
}
impl From<domain::Device> for pb::Device {
    fn from(v: domain::Device) -> Self {
        Self {
            id: v.id.to_string(),
            rack_id: v.rack_id.map(|v| v.to_string()),
            start_unit: v.start_unit,
            height_u: v.height_u,
            hostname: v.hostname,
            device_type: v.device_type,
            vendor: v.vendor,
            model: v.model,
            serial: v.serial,
            lifecycle_status: status(v.lifecycle_status),
            created_at: timestamp(v.created_at),
            updated_at: timestamp(v.updated_at),
        }
    }
}
impl From<domain::Rack> for pb::Rack {
    fn from(v: domain::Rack) -> Self {
        Self {
            id: v.id.to_string(),
            room_id: v.room_id.to_string(),
            name: v.name,
            height_u: v.height_u,
            description: v.description,
            created_at: timestamp(v.created_at),
            updated_at: timestamp(v.updated_at),
        }
    }
}
impl From<domain::Interface> for pb::Interface {
    fn from(v: domain::Interface) -> Self {
        Self {
            id: v.id.to_string(),
            device_id: v.device_id.to_string(),
            if_index: v.if_index,
            name: v.name,
            description: v.description,
            interface_type: v.interface_type,
            mac_address: v.mac_address.map(|v| v.to_string()),
            created_at: timestamp(v.created_at),
            updated_at: timestamp(v.updated_at),
        }
    }
}
impl From<domain::IpAddress> for pb::IpAddress {
    fn from(v: domain::IpAddress) -> Self {
        Self {
            id: v.id.to_string(),
            interface_id: v.interface_id.to_string(),
            address: v.address.to_string(),
            is_management: v.is_management,
            created_at: timestamp(v.created_at),
            updated_at: timestamp(v.updated_at),
        }
    }
}

fn mutation_context<T>(principal: Principal, request: &Request<T>) -> crate::workflow::MutationContext {
    let id = if request.metadata().get_all("x-request-id").iter().count() == 1 {
        request.metadata().get("x-request-id").and_then(|v| v.to_str().ok()).and_then(|v| uuid::Uuid::parse_str(v).ok())
    } else {
        None
    }
    .unwrap_or_else(uuid::Uuid::new_v4);
    crate::workflow::MutationContext::user(principal.user_id, crate::workflow::Source::Grpc, id)
}
