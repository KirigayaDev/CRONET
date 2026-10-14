//! One verifier for HTTP and gRPC. No login, signing, cookies or bypasses.
use crate::{db, repository::Repository};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use rsa::{
    pkcs1::DecodeRsaPublicKey, pkcs8::DecodePublicKey, traits::PublicKeyParts, RsaPublicKey,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

const AUTH_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Principal {
    pub user_id: Uuid,
    pub is_admin: bool,
    pub is_super_admin: bool,
}

impl Principal {
    pub fn require_write(self) -> Result<(), AuthError> {
        if self.is_admin || self.is_super_admin {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("Authentication required")]
    Unauthorized,
    #[error("Write access requires admin privileges")]
    Forbidden,
    #[error("Authentication dependencies unavailable")]
    Unavailable,
}

// Python's datetime.timestamp() emits fractional numbers, not just integers.
#[derive(Clone, Serialize, Deserialize)]
pub struct AccessClaims {
    pub iss: String,
    pub sub: Uuid,
    pub aud: String,
    pub exp: f64,
    pub nbf: f64,
    pub iat: f64,
    pub jti: Uuid,
    pub token_version: Uuid,
    pub token_type: String,
}

#[derive(Clone)]
pub struct Authenticator {
    pool: PgPool,
    redis: ConnectionManager,
    key: DecodingKey,
    validation: Validation,
}

pub async fn connect_redis(url: &str) -> Result<ConnectionManager, AuthError> {
    let client = redis::Client::open(url).map_err(|_| AuthError::Unavailable)?;
    let options = ConnectionManagerConfig::new()
        .set_response_timeout(AUTH_TIMEOUT)
        .set_connection_timeout(AUTH_TIMEOUT);
    tokio::time::timeout(
        AUTH_TIMEOUT,
        client.get_connection_manager_with_config(options),
    )
    .await
    .map_err(|_| AuthError::Unavailable)?
    .map_err(|_| AuthError::Unavailable)
}

pub fn load_public_key(path: &Path) -> Result<Vec<u8>, AuthError> {
    std::fs::read(path).map_err(|_| AuthError::Unavailable)
}

impl Authenticator {
    pub fn new(
        pool: PgPool,
        redis: ConnectionManager,
        public_pem: &[u8],
        issuer: &str,
        audience: &str,
    ) -> Result<Self, AuthError> {
        let text = std::str::from_utf8(public_pem).map_err(|_| AuthError::Unavailable)?;
        if !(text.trim_start().starts_with("-----BEGIN PUBLIC KEY-----")
            || text
                .trim_start()
                .starts_with("-----BEGIN RSA PUBLIC KEY-----"))
        {
            return Err(AuthError::Unavailable);
        }
        // DecodingKey parses the PEM container; validate RSA itself at startup.
        let rsa = RsaPublicKey::from_public_key_pem(text)
            .or_else(|_| RsaPublicKey::from_pkcs1_pem(text))
            .map_err(|_| AuthError::Unavailable)?;
        if rsa.n().bits() < 2048 {
            return Err(AuthError::Unavailable);
        }
        let key = DecodingKey::from_rsa_pem(public_pem).map_err(|_| AuthError::Unavailable)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "sub", "aud"]);
        // Check required fractional times ourselves without truncation/leeway.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.leeway = 0;
        Ok(Self {
            pool,
            redis,
            key,
            validation,
        })
    }

    pub async fn authenticate(&self, authorization: Option<&str>) -> Result<Principal, AuthError> {
        let header = authorization.ok_or(AuthError::Unauthorized)?;
        let mut parts = header.split_whitespace();
        if !parts
            .next()
            .is_some_and(|p| p.eq_ignore_ascii_case("bearer"))
        {
            return Err(AuthError::Unauthorized);
        }
        let token = parts.next().ok_or(AuthError::Unauthorized)?;
        if parts.next().is_some() {
            return Err(AuthError::Unauthorized);
        }
        let claims = decode::<AccessClaims>(token, &self.key, &self.validation)
            .map_err(|_| AuthError::Unauthorized)?
            .claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::Unavailable)?
            .as_secs_f64();
        if claims.token_type != "access"
            || ![claims.exp, claims.nbf, claims.iat]
                .iter()
                .all(|t| t.is_finite() && *t >= 0.0)
            || claims.exp <= now
            || claims.nbf > now
            || claims.iat > now
            || [claims.sub, claims.jti, claims.token_version]
                .iter()
                .any(|id| id.get_version_num() != 4)
        {
            return Err(AuthError::Unauthorized);
        }
        let mut redis = self.redis.clone();
        let key = format!("blacklisted_jwt_token:{}", claims.jti);
        let blacklisted: bool = tokio::time::timeout(
            AUTH_TIMEOUT,
            redis::cmd("EXISTS").arg(key).query_async(&mut redis),
        )
        .await
        .map_err(|_| AuthError::Unavailable)?
        .map_err(|_| AuthError::Unavailable)?;
        if blacklisted {
            return Err(AuthError::Unauthorized);
        }
        let user = tokio::time::timeout(AUTH_TIMEOUT, async {
            let mut conn = self.pool.acquire().await?;
            Repository::get_auth_user(&mut conn, claims.sub).await
        })
        .await
        .map_err(|_| AuthError::Unavailable)?
        .map_err(|_| AuthError::Unavailable)?
        .ok_or(AuthError::Unauthorized)?;
        if user.token_version_uuid != claims.token_version {
            return Err(AuthError::Unauthorized);
        }
        Ok(Principal {
            user_id: user.uuid,
            is_admin: user.is_admin,
            is_super_admin: user.is_super_admin,
        })
    }

    pub async fn ready(&self) -> bool {
        let db = tokio::time::timeout(AUTH_TIMEOUT, db::check_connection(&self.pool)).await;
        let mut redis = self.redis.clone();
        let redis = tokio::time::timeout(
            AUTH_TIMEOUT,
            redis::cmd("PING").query_async::<String>(&mut redis),
        )
        .await;
        matches!(db, Ok(Ok(()))) && matches!(redis, Ok(Ok(ref value)) if value == "PONG")
    }
}
