use crate::{Error, Result};
use sqlx::postgres::PgConnectOptions;
use std::{env, str::FromStr};

// Deliberately no Debug: connection URLs may contain credentials.
pub struct Config {
    database_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let value = env::var("DATABASE_URL")
            .map_err(|_| Error::validation("DATABASE_URL is required and must be valid Unicode"))?;
        Self::from_database_url(&value)
    }

    pub fn from_database_url(value: &str) -> Result<Self> {
        if value.trim().is_empty() {
            return Err(Error::validation("DATABASE_URL must not be empty"));
        }
        if !(value.starts_with("postgres://") || value.starts_with("postgresql://"))
            || PgConnectOptions::from_str(value).is_err()
        {
            return Err(Error::validation(
                "DATABASE_URL must be a valid PostgreSQL URL",
            ));
        }
        Ok(Self {
            database_url: value.to_owned(),
        })
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_postgres_urls_and_preserves_process_configuration() {
        let url = "postgresql://test@localhost:5432/example";
        assert_eq!(Config::from_database_url(url).unwrap().database_url(), url);
    }

    #[test]
    fn rejects_empty_wrong_scheme_and_invalid_url_without_leaking_it() {
        for value in ["", "   ", "mysql://secret@example/db", "postgres://[broken"] {
            let error = Config::from_database_url(value).err().unwrap();
            assert!(matches!(error, Error::Validation(_)));
            assert!(!error.to_string().contains("secret"));
        }
    }
}
