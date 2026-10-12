use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::{CredError, CredStore, Credential};

pub enum VaultAuth {
    /// Статический токен: только для разработки.
    Token(SecretString),
    /// AppRole: токен выдаётся по role_id/secret_id и перевыпускается при истечении.
    AppRole { mount: String, role_id: String, secret_id: SecretString },
}

pub struct VaultConfig {
    pub addr: String, // "https://vault.internal:8200"
    pub auth: VaultAuth,
    /// Namespace (Vault Enterprise), необязательно.
    pub namespace: Option<String>,
    /// PEM корневого сертификата, если Vault использует внутренний CA.
    pub ca_pem: Option<Vec<u8>>,
    pub request_timeout: Duration,
}

struct Session {
    token: SecretString,
    expires: Option<Instant>,
}

pub struct VaultCredStore {
    http: reqwest::Client,
    cfg: VaultConfig,
    session: RwLock<Option<Session>>,
}

#[derive(Deserialize)]
struct LoginResp {
    auth: LoginAuth,
}
#[derive(Deserialize)]
struct LoginAuth {
    client_token: SecretString,
    lease_duration: u64,
}
#[derive(Deserialize)]
struct KvResp {
    data: KvData,
}
#[derive(Deserialize)]
struct KvData {
    data: serde_json::Value,
}

impl VaultCredStore {
    pub fn new(cfg: VaultConfig) -> Result<Self, CredError> {
        if !cfg.addr.starts_with("https://")
            && !cfg.addr.starts_with("http://127.0.0.1")
            && !cfg.addr.starts_with("http://localhost")
        {
            return Err(CredError::Backend(
                "VAULT_ADDR должен быть https:// (http допустим только для localhost)".into(),
            ));
        }
        let mut b = reqwest::Client::builder().timeout(cfg.request_timeout);
        if let Some(pem) = &cfg.ca_pem {
            let cert = reqwest::Certificate::from_pem(pem)
                .map_err(|_| CredError::Backend("некорректный CA-сертификат Vault".into()))?;
            b = b.add_root_certificate(cert);
        }
        let http = b.build().map_err(|e| CredError::Backend(e.to_string()))?;
        Ok(Self { http, cfg, session: RwLock::new(None) })
    }

    async fn token(&self, force_refresh: bool) -> Result<SecretString, CredError> {
        match &self.cfg.auth {
            VaultAuth::Token(t) => Ok(t.clone()),
            VaultAuth::AppRole { mount, role_id, secret_id } => {
                if !force_refresh {
                    if let Some(s) = self.session.read().await.as_ref() {
                        // обновляем заранее, за 30 секунд до конца срока
                        if s.expires.is_none_or(|e| e > Instant::now() + Duration::from_secs(30)) {
                            return Ok(s.token.clone());
                        }
                    }
                }
                let mut guard = self.session.write().await;
                let url = format!("{}/v1/auth/{}/login", self.cfg.addr.trim_end_matches('/'), mount);
                let mut req = self.http.post(url).json(&serde_json::json!({
                    "role_id": role_id,
                    "secret_id": secret_id.expose_secret(),
                }));
                if let Some(ns) = &self.cfg.namespace {
                    req = req.header("X-Vault-Namespace", ns);
                }
                let resp = req.send().await.map_err(|e| CredError::Backend(format!("login: {}", e.without_url())))?;
                if !resp.status().is_success() {
                    return Err(CredError::Backend(format!("login: HTTP {}", resp.status().as_u16())));
                }
                let body: LoginResp =
                    resp.json().await.map_err(|_| CredError::Backend("login: неожиданный ответ".into()))?;
                let expires = (body.auth.lease_duration > 0)
                    .then(|| Instant::now() + Duration::from_secs(body.auth.lease_duration));
                *guard = Some(Session { token: body.auth.client_token.clone(), expires });
                Ok(body.auth.client_token)
            }
        }
    }

    async fn read(&self, token: &SecretString, mount: &str, path: &str) -> Result<reqwest::Response, CredError> {
        let url = format!("{}/v1/{}/data/{}", self.cfg.addr.trim_end_matches('/'), mount, path);
        let mut req = self.http.get(url).header("X-Vault-Token", token.expose_secret());
        if let Some(ns) = &self.cfg.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        req.send().await.map_err(|e| CredError::Backend(e.without_url().to_string()))
    }
}

/// "vault:secret/snmp/site-a/core" -> ("secret", "snmp/site-a/core")
fn split_ref(reference: &str) -> Result<(&str, &str), CredError> {
    let rest = reference.strip_prefix("vault:").ok_or(CredError::BadReference)?;
    let (mount, path) = rest.split_once('/').ok_or(CredError::BadReference)?;
    let ok = |s: &str| {
        !s.is_empty()
            && s.split('/').all(|seg| {
                !seg.is_empty()
                    && seg != ".."
                    && seg != "."
                    && seg.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
            })
    };
    if ok(mount) && ok(path) {
        Ok((mount, path))
    } else {
        Err(CredError::BadReference)
    }
}

#[async_trait::async_trait]
impl CredStore for VaultCredStore {
    async fn get(&self, reference: &str) -> Result<Arc<Credential>, CredError> {
        let (mount, path) = split_ref(reference)?;
        let mut token = self.token(false).await?;
        let mut resp = self.read(&token, mount, path).await?;
        // токен мог быть отозван или истечь раньше срока: один повтор с новым токеном
        if resp.status() == reqwest::StatusCode::FORBIDDEN && matches!(self.cfg.auth, VaultAuth::AppRole { .. }) {
            token = self.token(true).await?;
            resp = self.read(&token, mount, path).await?;
        }
        match resp.status().as_u16() {
            200 => {}
            404 => return Err(CredError::NotFound),
            403 => return Err(CredError::Forbidden),
            code => return Err(CredError::Backend(format!("HTTP {code}"))),
        }
        let body: KvResp = resp.json().await.map_err(|_| CredError::Backend("неожиданный ответ Vault".into()))?;
        let bytes = serde_json::to_vec(&body.data.data).map_err(|_| CredError::Invalid("не JSON".into()))?;
        Ok(Arc::new(Credential::from_json(&bytes)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        matchers::{header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn cfg(addr: String, auth: VaultAuth) -> VaultConfig {
        VaultConfig { addr, auth, namespace: None, ca_pem: None, request_timeout: Duration::from_secs(2) }
    }

    #[test]
    fn reference_parsing() {
        assert_eq!(split_ref("vault:secret/snmp/a").unwrap(), ("secret", "snmp/a"));
        for bad in [
            "secret/x",
            "vault:secret",
            "vault:/x",
            "vault:secret/../x",
            "vault:secret//x",
            "vault:secret/a b",
            "env:x",
        ] {
            assert!(split_ref(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn refuses_plain_http_for_remote() {
        assert!(VaultCredStore::new(cfg("http://vault.example:8200".into(), VaultAuth::Token("t".to_string().into())))
            .is_err());
    }

    #[tokio::test]
    async fn reads_kv_v2_with_approle_and_relogins_on_403() {
        let srv = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/auth/approle/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"auth": {"client_token": "tok1", "lease_duration": 3600}})),
            )
            .mount(&srv)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/snmp/core"))
            .and(header("X-Vault-Token", "tok1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"data": {"data": {"version": "v2c", "community": "abc"}}})),
            )
            .mount(&srv)
            .await;
        let store = VaultCredStore::new(cfg(
            srv.uri(),
            VaultAuth::AppRole { mount: "approle".into(), role_id: "r".into(), secret_id: "s".to_string().into() },
        ))
        .unwrap();
        assert!(matches!(&*store.get("vault:secret/snmp/core").await.unwrap(), Credential::V2c { .. }));
        assert_eq!(store.get("vault:secret/snmp/missing").await.unwrap_err(), CredError::NotFound);
    }
}
