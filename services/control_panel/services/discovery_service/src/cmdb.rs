use crate::{
    classify::Classification,
    error::{Error, Result},
    probe::Identity,
};
use contracts::cronet::cmdb::v1::{self as pb, cmdb_client::CmdbClient};
use secrecy::{ExposeSecret, SecretString};
use std::{net::IpAddr, time::Duration};
use tonic::{
    transport::{Channel, Endpoint},
    Request,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkError {
    Unauthorized,
    Forbidden,
    Unavailable,
    Rejected,
}
impl SinkError {
    pub fn fatal(self) -> Option<Error> {
        match self {
            Self::Unauthorized | Self::Forbidden => Some(Error::CmdbAuth),
            Self::Unavailable => Some(Error::CmdbUnavailable),
            Self::Rejected => None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Applied {
    pub device_id: Uuid,
    pub created: bool,
    pub skipped_fields: Vec<String>,
}
#[async_trait::async_trait]
pub trait CmdbSink: Send + Sync {
    async fn check_ready(&self) -> std::result::Result<(), SinkError>;
    async fn apply(
        &self,
        ip: IpAddr,
        identity: &Identity,
        class: &Classification,
    ) -> std::result::Result<Applied, SinkError>;
}
pub struct TonicCmdbClient {
    client: CmdbClient<Channel>,
    token: SecretString,
}
impl std::fmt::Debug for TonicCmdbClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TonicCmdbClient(<redacted>)")
    }
}
impl TonicCmdbClient {
    pub async fn connect(endpoint: String, token: SecretString, timeout: Duration) -> Result<Self> {
        let mut endpoint = Endpoint::from_shared(endpoint).map_err(|_| Error::Config)?;
        if endpoint.uri().authority().is_some_and(|a| a.as_str().contains('@')) {
            return Err(Error::Config);
        }
        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_webpki_roots())
                .map_err(|_| Error::Config)?;
        }
        let channel =
            endpoint.connect_timeout(timeout).timeout(timeout).connect().await.map_err(|_| Error::CmdbUnavailable)?;
        let out = Self { client: CmdbClient::new(channel), token };
        let _: Request<()> = out.request(()).map_err(|_| Error::CmdbAuth)?;
        Ok(out)
    }
    fn request<T>(&self, value: T) -> std::result::Result<Request<T>, SinkError> {
        let mut request = Request::new(value);
        let mut token: tonic::metadata::MetadataValue<_> =
            format!("Bearer {}", self.token.expose_secret()).parse().map_err(|_| SinkError::Unauthorized)?;
        token.set_sensitive(true);
        request.metadata_mut().insert("authorization", token);
        request
            .metadata_mut()
            .insert("x-request-id", Uuid::new_v4().to_string().parse().map_err(|_| SinkError::Rejected)?);
        Ok(request)
    }
}
fn status(error: tonic::Status) -> SinkError {
    match error.code() {
        tonic::Code::Unauthenticated => SinkError::Unauthorized,
        tonic::Code::PermissionDenied => SinkError::Forbidden,
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => SinkError::Unavailable,
        _ => SinkError::Rejected,
    }
}
#[async_trait::async_trait]
impl CmdbSink for TonicCmdbClient {
    async fn check_ready(&self) -> std::result::Result<(), SinkError> {
        self.client
            .clone()
            .list_devices(self.request(pb::ListDevicesRequest { limit: Some(1), ..Default::default() })?)
            .await
            .map_err(status)?;
        Ok(())
    }
    async fn apply(
        &self,
        ip: IpAddr,
        identity: &Identity,
        class: &Classification,
    ) -> std::result::Result<Applied, SinkError> {
        let response = self
            .client
            .clone()
            .apply_discovery_observation(self.request(pb::ApplyDiscoveryObservationRequest {
                target_ip: ip.to_string(),
                hostname: identity.sys_name.clone(),
                device_type: Some(class.device_type.clone()),
                vendor: class.vendor.clone(),
                model: class.model.clone(),
                serial: None,
            })?)
            .await
            .map_err(status)?
            .into_inner();
        let device = response.device.ok_or(SinkError::Unavailable)?;
        Ok(Applied {
            device_id: device.id.parse().map_err(|_| SinkError::Unavailable)?,
            created: response.created,
            skipped_fields: response.skipped_fields,
        })
    }
}
