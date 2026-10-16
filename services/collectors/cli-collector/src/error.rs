use contracts::netmon::v1::raw_cli_result::Status;

/// Never retain an upstream error: SSH/Vault payloads can contain secrets.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("{detail}")]
pub struct Failure {
    pub status: Status,
    pub detail: &'static str,
}
impl Failure {
    pub const fn new(status: Status, detail: &'static str) -> Self {
        Self { status, detail }
    }
    pub const fn timeout() -> Self {
        Self::new(Status::Timeout, "SSH operation timed out")
    }
    pub const fn command() -> Self {
        Self::new(Status::CommandError, "SSH shell closed or rejected request")
    }
    pub const fn bad() -> Self {
        Self::new(Status::BadTask, "task rejected by CLI policy")
    }
}
