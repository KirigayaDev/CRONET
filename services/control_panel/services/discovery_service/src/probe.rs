use crate::{
    config::SnmpVersion,
    error::{Error, Result},
};
use creds::{AuthProto, Credential, PrivProto};
use secrecy::ExposeSecret;
use snmp2::{
    v3::{Auth, AuthProtocol, Cipher, Security},
    AsyncSession, Oid, Value,
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

pub const SYS_DESCR: &str = "1.3.6.1.2.1.1.1.0";
pub const SYS_OBJECT_ID: &str = "1.3.6.1.2.1.1.2.0";
pub const SYS_NAME: &str = "1.3.6.1.2.1.1.5.0";
#[derive(Clone, Debug)]
pub struct Identity {
    pub sys_object_id: String,
    pub sys_name: Option<String>,
    pub sys_descr: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProbeError {
    #[error("SNMP timeout")]
    Timeout,
    #[error("SNMP authentication failed")]
    Auth,
    #[error("No SNMP response")]
    NoSnmp,
    #[error("Invalid SNMP identity")]
    InvalidIdentity,
    #[error("Invalid SNMP response")]
    Protocol,
}
impl ProbeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::Timeout => "snmp_timeout",
            Self::Auth => "snmp_auth",
            Self::NoSnmp => "no_snmp",
            Self::InvalidIdentity => "invalid_identity",
            Self::Protocol => "snmp_protocol",
        }
    }
}
#[async_trait::async_trait]
pub trait SnmpProbe: Send + Sync {
    async fn probe(&self, target: SocketAddr) -> std::result::Result<Identity, ProbeError>;
}

pub struct Snmp2Probe {
    credential: Arc<Credential>,
}
impl std::fmt::Debug for Snmp2Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Snmp2Probe(<redacted>)")
    }
}
impl Snmp2Probe {
    pub fn new(credential: Arc<Credential>, version: SnmpVersion, allow_legacy: bool) -> Result<Self> {
        match (&*credential, version) {
            (Credential::V2c { .. }, SnmpVersion::V2c) => {}
            (Credential::V3(v3), SnmpVersion::V3) if allow_legacy || (!v3.is_legacy() && v3.auth.is_some()) => {}
            _ => return Err(Error::Credential),
        }
        Ok(Self { credential })
    }
    async fn session(&self, target: SocketAddr) -> std::result::Result<AsyncSession, ProbeError> {
        match &*self.credential {
            Credential::V2c { community } => AsyncSession::new_v2c(target, community.expose_secret().as_bytes(), 1)
                .await
                .map_err(|_| ProbeError::NoSnmp),
            Credential::V3(v3) => {
                let password = v3.auth.as_ref().map(|(_, pw)| pw.expose_secret()).unwrap_or("");
                let level = match (&v3.auth, &v3.privacy) {
                    (None, _) => Auth::NoAuthNoPriv,
                    (Some(_), None) => Auth::AuthNoPriv,
                    (Some(_), Some((cipher, password))) => Auth::AuthPriv {
                        cipher: match cipher {
                            PrivProto::Des => Cipher::Des,
                            PrivProto::Aes128 => Cipher::Aes128,
                            PrivProto::Aes192 => Cipher::Aes192,
                            PrivProto::Aes256 => Cipher::Aes256,
                        },
                        privacy_password: password.expose_secret().as_bytes().to_vec(),
                    },
                };
                let mut security = Security::new(v3.username.as_bytes(), password.as_bytes()).with_auth(level);
                if !v3.context.is_empty() {
                    security = security.with_context_name(&v3.context);
                }
                if let Some((protocol, _)) = &v3.auth {
                    security = security.with_auth_protocol(match protocol {
                        AuthProto::Md5 => AuthProtocol::Md5,
                        AuthProto::Sha1 => AuthProtocol::Sha1,
                        AuthProto::Sha224 => AuthProtocol::Sha224,
                        AuthProto::Sha256 => AuthProtocol::Sha256,
                        AuthProto::Sha384 => AuthProtocol::Sha384,
                        AuthProto::Sha512 => AuthProtocol::Sha512,
                    });
                }
                let mut session = AsyncSession::new_v3(target, 1, security).await.map_err(|_| ProbeError::NoSnmp)?;
                session.init().await.map_err(snmp_error)?;
                Ok(session)
            }
            Credential::Basic { .. } => Err(ProbeError::Auth),
        }
    }
}
fn snmp_error(error: snmp2::Error) -> ProbeError {
    match error {
        snmp2::Error::AuthFailure(_) | snmp2::Error::CommunityMismatch => ProbeError::Auth,
        snmp2::Error::Send | snmp2::Error::Receive => ProbeError::NoSnmp,
        _ => ProbeError::Protocol,
    }
}
pub fn parse_identity<'a>(
    values: impl Iterator<Item = (Oid<'a>, Value<'a>)>,
) -> std::result::Result<Identity, ProbeError> {
    let mut out = Identity { sys_object_id: String::new(), sys_name: None, sys_descr: None };
    for (oid, value) in values {
        let name = snmp_types::oid_to_string(&oid);
        match (name.as_str(), value) {
            (SYS_OBJECT_ID, Value::ObjectIdentifier(value)) => out.sys_object_id = snmp_types::oid_to_string(&value),
            (SYS_NAME, Value::OctetString(value)) => out.sys_name = text(value, 1024)?,
            (SYS_DESCR, Value::OctetString(value)) => out.sys_descr = text(value, 4096)?,
            _ => {}
        }
    }
    snmp_types::parse_oid(&out.sys_object_id).map_err(|_| ProbeError::InvalidIdentity)?;
    Ok(out)
}
fn text(value: &[u8], max: usize) -> std::result::Result<Option<String>, ProbeError> {
    if value.len() > max || value.contains(&0) {
        return Err(ProbeError::InvalidIdentity);
    }
    let value = std::str::from_utf8(value).map_err(|_| ProbeError::InvalidIdentity)?.trim();
    Ok((!value.is_empty()).then(|| value.to_owned()))
}
#[async_trait::async_trait]
impl SnmpProbe for Snmp2Probe {
    async fn probe(&self, target: SocketAddr) -> std::result::Result<Identity, ProbeError> {
        let mut session = self.session(target).await?;
        let oids = [SYS_DESCR, SYS_OBJECT_ID, SYS_NAME].map(snmp_types::parse_oid);
        let oids: Vec<_> = oids.into_iter().collect::<std::result::Result<_, _>>().map_err(|_| ProbeError::Protocol)?;
        let refs: Vec<_> = oids.iter().collect();
        let pdu = session.get_many(&refs).await.map_err(snmp_error)?;
        if pdu.error_status != 0 {
            return Err(ProbeError::Protocol);
        }
        parse_identity(pdu.varbinds)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HintUnavailable;
#[async_trait::async_trait]
pub trait ReachabilityProbe: Send + Sync {
    async fn reachable(&self, ip: IpAddr, timeout: Duration) -> std::result::Result<bool, HintUnavailable>;
}
pub struct SurgeReachability {
    client: Option<surge_ping::Client>,
}
impl SurgeReachability {
    pub fn new(ipv6: bool) -> Self {
        let config =
            surge_ping::Config::builder().kind(if ipv6 { surge_ping::ICMP::V6 } else { surge_ping::ICMP::V4 }).build();
        let client = surge_ping::Client::new(&config).ok();
        if client.is_none() {
            tracing::warn!("ICMP hint unavailable; continuing with SNMP");
        }
        Self { client }
    }
    pub fn disabled() -> Self {
        Self { client: None }
    }
}
#[async_trait::async_trait]
impl ReachabilityProbe for SurgeReachability {
    async fn reachable(&self, ip: IpAddr, timeout: Duration) -> std::result::Result<bool, HintUnavailable> {
        let client = self.client.as_ref().ok_or(HintUnavailable)?;
        let mut ping = client.pinger(ip, surge_ping::PingIdentifier(1)).await;
        ping.timeout(timeout);
        Ok(ping.ping(surge_ping::PingSequence(0), &[0; 8]).await.is_ok())
    }
}
