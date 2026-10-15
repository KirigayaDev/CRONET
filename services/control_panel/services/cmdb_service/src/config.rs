use crate::{Error, Result};
use sqlx::postgres::PgConnectOptions;
use std::{env, net::SocketAddr, path::PathBuf, str::FromStr};

// Deliberately no Debug: connection URLs may contain credentials.
pub struct Config {
    database_url: String,
    redis_url: Option<String>,
    public_key_path: Option<PathBuf>,
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    pub jwt_issuer: String,
    pub jwt_audience: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let value = env::var("DATABASE_URL")
            .map_err(|_| Error::validation("DATABASE_URL is required and must be valid Unicode"))?;
        let mut config = Self::from_database_url(&value)?;
        let redis_url = required_env("REDIS_URL")?;
        redis::Client::open(redis_url.as_str())
            .map_err(|_| Error::validation("REDIS_URL must be a valid Redis URL"))?;
        config.redis_url = Some(redis_url);
        config.public_key_path = Some(PathBuf::from(required_env("JWT_PUBLIC_KEY_PATH")?));
        config.http_addr = env::var("CMDB_HTTP_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8081".into())
            .parse()
            .map_err(|_| Error::validation("CMDB_HTTP_ADDR must be a socket address"))?;
        config.grpc_addr = env::var("CMDB_GRPC_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".into())
            .parse()
            .map_err(|_| Error::validation("CMDB_GRPC_ADDR must be a socket address"))?;
        config.jwt_issuer = env::var("JWT_ISSUER").unwrap_or(config.jwt_issuer);
        config.jwt_audience = env::var("JWT_AUDIENCE").unwrap_or(config.jwt_audience);
        if config.jwt_issuer.trim().is_empty() || config.jwt_audience.trim().is_empty() {
            return Err(Error::validation("JWT issuer/audience must not be empty"));
        }
        Ok(config)
    }

    pub fn from_database_url(value: &str) -> Result<Self> {
        if value.trim().is_empty() {
            return Err(Error::validation("DATABASE_URL must not be empty"));
        }
        if !(value.starts_with("postgres://") || value.starts_with("postgresql://"))
            || PgConnectOptions::from_str(value).is_err()
        {
            return Err(Error::validation("DATABASE_URL must be a valid PostgreSQL URL"));
        }
        Ok(Self {
            database_url: value.to_owned(),
            redis_url: None,
            public_key_path: None,
            http_addr: "0.0.0.0:8081".parse().expect("constant address"),
            grpc_addr: "0.0.0.0:50051".parse().expect("constant address"),
            jwt_issuer: "Re:simple_videohosting_auth".into(),
            jwt_audience: "Re:simple_videohosting_auth".into(),
        })
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }
    pub fn redis_url(&self) -> Result<&str> {
        self.redis_url.as_deref().ok_or_else(|| Error::validation("REDIS_URL is required"))
    }
    pub fn public_key_path(&self) -> Result<&std::path::Path> {
        self.public_key_path.as_deref().ok_or_else(|| Error::validation("JWT_PUBLIC_KEY_PATH is required"))
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty()).ok_or_else(|| Error::validation(format!("{name} is required")))
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
