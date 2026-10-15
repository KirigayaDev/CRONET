//! Fixed, secret-free errors: discard backend, SNMP and gRPC error payloads.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid discovery configuration")]
    Config,
    #[error("CIDR exceeds the configured host limit")]
    TooManyHosts,
    #[error("Credential backend/configuration is unavailable or disallowed")]
    Credential,
    #[error("CMDB authentication or admin authorization failed")]
    CmdbAuth,
    #[error("Required CMDB service is unavailable")]
    CmdbUnavailable,
}
pub type Result<T> = std::result::Result<T, Error>;
