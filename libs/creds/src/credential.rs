use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CredError {
    #[error("секрет не найден")]
    NotFound,
    #[error("неподдерживаемая или некорректная ссылка на секрет")]
    BadReference,
    #[error("некорректное содержимое секрета: {0}")]
    Invalid(String),
    #[error("хранилище секретов недоступно: {0}")]
    Backend(String),
    #[error("доступ к секрету запрещён")]
    Forbidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthProto {
    Md5,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

impl AuthProto {
    /// MD5 и SHA-1 считаются устаревшими.
    pub fn is_legacy(self) -> bool {
        matches!(self, AuthProto::Md5 | AuthProto::Sha1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrivProto {
    Des,
    Aes128,
    Aes192,
    Aes256,
}

impl PrivProto {
    pub fn is_legacy(self) -> bool {
        matches!(self, PrivProto::Des)
    }
}

pub struct V3Credential {
    pub username: String,
    /// Контекст SNMPv3 (contextName); на большинстве устройств пустой.
    pub context: String,
    pub auth: Option<(AuthProto, SecretString)>,
    pub privacy: Option<(PrivProto, SecretString)>,
}

impl V3Credential {
    pub fn is_legacy(&self) -> bool {
        self.auth.as_ref().is_some_and(|(p, _)| p.is_legacy())
            || self.privacy.as_ref().is_some_and(|(p, _)| p.is_legacy())
    }
}

pub enum Credential {
    V2c {
        community: SecretString,
    },
    V3(V3Credential),
    /// Логин и пароль (gNMI и другие API с аутентификацией по паре).
    Basic {
        username: String,
        password: SecretString,
    },
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credential::V2c { .. } => f.write_str("Credential::V2c(<redacted>)"),
            Credential::V3(v) => write!(f, "Credential::V3(user={:?}, <redacted>)", v.username),
            Credential::Basic { username, .. } => write!(f, "Credential::Basic(user={username:?}, <redacted>)"),
        }
    }
}

/// Формат JSON в Vault / dev-хранилище.
#[derive(Deserialize)]
#[serde(tag = "version", rename_all = "lowercase", deny_unknown_fields)]
enum Raw {
    Basic {
        username: String,
        password: SecretString,
    },
    V2c {
        community: SecretString,
    },
    V3 {
        username: String,
        #[serde(default)]
        context: String,
        auth_protocol: Option<AuthProto>,
        auth_password: Option<SecretString>,
        priv_protocol: Option<PrivProto>,
        priv_password: Option<SecretString>,
    },
}

/// RFC 3414: пароль USM должен быть не короче 8 символов.
const MIN_USM_PASSWORD: usize = 8;

impl Credential {
    pub fn from_json(bytes: &[u8]) -> Result<Self, CredError> {
        // Ошибку serde не пробрасываем целиком: в ней могут оказаться куски секрета.
        let raw: Raw = serde_json::from_slice(bytes)
            .map_err(|e| CredError::Invalid(format!("JSON не соответствует схеме ({})", e.classify_name())))?;
        match raw {
            Raw::Basic { username, password } => {
                if username.is_empty() || password.expose_secret().is_empty() {
                    return Err(CredError::Invalid("пустые логин или пароль".into()));
                }
                Ok(Credential::Basic { username, password })
            }
            Raw::V2c { community } => {
                if community.expose_secret().is_empty() {
                    return Err(CredError::Invalid("пустая community".into()));
                }
                Ok(Credential::V2c { community })
            }
            Raw::V3 { username, context, auth_protocol, auth_password, priv_protocol, priv_password } => {
                if username.is_empty() {
                    return Err(CredError::Invalid("пустое имя пользователя".into()));
                }
                let auth = match (auth_protocol, auth_password) {
                    (Some(p), Some(pw)) => Some((p, pw)),
                    (None, None) => None,
                    _ => return Err(CredError::Invalid("auth_protocol и auth_password задаются вместе".into())),
                };
                let privacy = match (priv_protocol, priv_password) {
                    (Some(p), Some(pw)) => Some((p, pw)),
                    (None, None) => None,
                    _ => return Err(CredError::Invalid("priv_protocol и priv_password задаются вместе".into())),
                };
                if privacy.is_some() && auth.is_none() {
                    return Err(CredError::Invalid("шифрование без аутентификации (privNoAuth) невозможно".into()));
                }
                for pw in auth.iter().map(|a| &a.1).chain(privacy.iter().map(|p| &p.1)) {
                    if pw.expose_secret().len() < MIN_USM_PASSWORD {
                        return Err(CredError::Invalid(format!("пароль USM короче {MIN_USM_PASSWORD} символов")));
                    }
                }
                Ok(Credential::V3(V3Credential { username, context, auth, privacy }))
            }
        }
    }
}

trait ClassifyName {
    fn classify_name(&self) -> &'static str;
}
impl ClassifyName for serde_json::Error {
    fn classify_name(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Io => "io",
            serde_json::error::Category::Syntax => "синтаксис",
            serde_json::error::Category::Data => "данные",
            serde_json::error::Category::Eof => "неожиданный конец",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_v2c() {
        let c = Credential::from_json(br#"{"version":"v2c","community":"s3cret"}"#).unwrap();
        assert!(matches!(c, Credential::V2c { .. }));
        assert!(!format!("{c:?}").contains("s3cret"));
    }

    #[test]
    fn parses_basic() {
        let c = Credential::from_json(br#"{"version":"basic","username":"gnmi","password":"pw-123456"}"#).unwrap();
        assert!(matches!(c, Credential::Basic { .. }));
        assert!(!format!("{c:?}").contains("pw-123456"));
        assert!(Credential::from_json(br#"{"version":"basic","username":"","password":"x"}"#).is_err());
    }

    #[test]
    fn parses_v3_authpriv() {
        let c = Credential::from_json(
            br#"{"version":"v3","username":"mon","auth_protocol":"sha256","auth_password":"authpass123",
                 "priv_protocol":"aes128","priv_password":"privpass123"}"#,
        )
        .unwrap();
        match c {
            Credential::V3(v) => {
                assert_eq!(v.username, "mon");
                assert!(!v.is_legacy());
            }
            _ => panic!("ожидался v3"),
        }
    }

    #[test]
    fn flags_legacy_crypto() {
        let c = Credential::from_json(
            br#"{"version":"v3","username":"mon","auth_protocol":"md5","auth_password":"authpass123"}"#,
        )
        .unwrap();
        assert!(matches!(c, Credential::V3(v) if v.is_legacy()));
    }

    #[test]
    fn rejects_bad_secrets() {
        for bad in [
            r#"{"version":"v2c","community":""}"#,
            r#"{"version":"v3","username":"","auth_protocol":"sha256","auth_password":"authpass123"}"#,
            r#"{"version":"v3","username":"u","auth_protocol":"sha256"}"#,
            r#"{"version":"v3","username":"u","auth_protocol":"sha256","auth_password":"short"}"#,
            r#"{"version":"v3","username":"u","priv_protocol":"aes128","priv_password":"privpass123"}"#,
            r#"{"version":"v4"}"#,
            r#"{"version":"v2c","community":"x","extra":1}"#,
            "not json",
        ] {
            assert!(Credential::from_json(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn error_does_not_echo_secret() {
        let e = Credential::from_json(
            br#"{"version":"v3","username":"u","auth_protocol":"sha256","auth_password":"hunter2hunter2","oops":1}"#,
        )
        .unwrap_err();
        assert!(!e.to_string().contains("hunter2"));
    }
}
