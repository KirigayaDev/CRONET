#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid Scheduler configuration")]
    Config,
    #[error("CMDB inventory/authentication unavailable")]
    Cmdb,
    #[error("NATS coordination unavailable or incompatible")]
    Coordination,
    #[error("JetStream publication not acknowledged")]
    Publish,
}
pub type Result<T> = std::result::Result<T, Error>;
