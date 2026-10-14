use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Internal(&'static str),
    #[error("{entity} {id} was not found")]
    NotFound { entity: &'static str, id: Uuid },
    #[error("Invalid input: {0}")]
    Validation(String),
    #[error("Database constraint conflict ({sqlstate}, {constraint:?})")]
    Conflict {
        sqlstate: String,
        constraint: Option<String>,
    },
    #[error("Transaction must be retried ({sqlstate})")]
    Retryable { sqlstate: String },
    // Do not echo driver messages, SQL values or connection credentials in Display.
    #[error("Database operation failed")]
    Database(#[source] sqlx::Error),
}

impl Error {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { sqlstate } if sqlstate == "40001" || sqlstate == "40P01")
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(database) = &error {
            let code = database
                .code()
                .map(|code| code.into_owned())
                .unwrap_or_default();
            match code.as_str() {
                "23505" | "23503" | "23P01" => {
                    return Self::Conflict {
                        sqlstate: code,
                        constraint: database.constraint().map(str::to_owned),
                    }
                }
                "23514" | "23502" | "22P02" | "22003" => {
                    return Self::validation(format!("PostgreSQL rejected the input ({code})"));
                }
                "40001" | "40P01" => return Self::Retryable { sqlstate: code },
                _ => {}
            }
        }
        Self::Database(error)
    }
}
