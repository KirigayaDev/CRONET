#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration: {0}")]
    Config(String),
    #[error("listener I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("spool: {0}")]
    Spool(String),
    #[error("spool capacity exhausted")]
    Full,
    #[error("invalid spool record")]
    Corrupt,
    #[error("task failed")]
    Task,
    #[error("NATS configuration or credentials could not be loaded")]
    NatsConfig,
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn storage(error: impl std::fmt::Display) -> Error {
    Error::Spool(error.to_string())
}
