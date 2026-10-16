#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration: {0}")]
    Config(String),
    #[error("NATS operation failed")]
    Bus,
    #[error("ClickHouse batch insert failed")]
    Write,
    #[error("worker stopped unexpectedly")]
    Worker,
    #[error("shutdown deadline reached; uncommitted messages remain unacknowledged")]
    Shutdown,
}
