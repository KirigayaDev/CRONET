use std::time::Duration;

use contracts::netmon::v1::VarBind;
use creds::{AuthProto, Credential, PrivProto};
use secrecy::ExposeSecret;
use snmp2::{
    v3::{Auth, AuthProtocol, Cipher, Security},
    AsyncSession, Oid, Value,
};
use snmp_types::{oid_arcs, oid_to_string, value_to_proto};

pub struct Request<'a> {
    pub target: std::net::SocketAddr,
    pub cred: &'a Credential,
    pub get_oids: &'a [Oid<'static>],
    pub walk_roots: &'a [Oid<'static>],
    /// Таймаут одного запроса.
    pub timeout: Duration,
    pub max_repetitions: u32,
    pub max_varbinds: usize,
}

#[derive(Debug, Default)]
pub struct Collected {
    pub varbinds: Vec<VarBind>,
    pub truncated: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("таймаут")]
    Timeout,
    #[error("ошибка аутентификации: {0}")]
    Auth(String),
    #[error("ошибка протокола: {0}")]
    Protocol(String),
    #[error("сетевая ошибка: {0}")]
    Io(String),
}

#[async_trait::async_trait]
pub trait SnmpTransport: Send + Sync {
    async fn collect(&self, req: &Request<'_>) -> Result<Collected, TransportError>;
}

/// Реализация на `snmp2::AsyncSession`. Сессия создаётся на каждую задачу:
/// коллектор stateless и не держит соединений между опросами.
pub struct Snmp2Transport;

/// Сколько OID за один GET. Небольшое число, чтобы ответ влезал в датаграмму.
const GET_CHUNK: usize = 10;

fn map_err(e: snmp2::Error) -> TransportError {
    match e {
        snmp2::Error::AuthFailure(kind) => TransportError::Auth(format!("{kind:?}")),
        snmp2::Error::CommunityMismatch => TransportError::Auth("community не совпала".into()),
        snmp2::Error::Send | snmp2::Error::Receive => TransportError::Io(e.to_string()),
        other => TransportError::Protocol(other.to_string()),
    }
}

fn auth_protocol(p: AuthProto) -> AuthProtocol {
    match p {
        AuthProto::Md5 => AuthProtocol::Md5,
        AuthProto::Sha1 => AuthProtocol::Sha1,
        AuthProto::Sha224 => AuthProtocol::Sha224,
        AuthProto::Sha256 => AuthProtocol::Sha256,
        AuthProto::Sha384 => AuthProtocol::Sha384,
        AuthProto::Sha512 => AuthProtocol::Sha512,
    }
}

fn cipher(p: PrivProto) -> Cipher {
    match p {
        PrivProto::Des => Cipher::Des,
        PrivProto::Aes128 => Cipher::Aes128,
        PrivProto::Aes192 => Cipher::Aes192,
        PrivProto::Aes256 => Cipher::Aes256,
    }
}

async fn open_session(req: &Request<'_>) -> Result<AsyncSession, TransportError> {
    let req_id = 1;
    match req.cred {
        Credential::V2c { community } => {
            AsyncSession::new_v2c(req.target, community.expose_secret().as_bytes(), req_id)
                .await
                .map_err(|e| TransportError::Io(e.to_string()))
        }
        Credential::Basic { .. } => Err(TransportError::Protocol("секрет типа basic не подходит для SNMP".into())),
        Credential::V3(v3) => {
            let (auth_pass, level) = match (&v3.auth, &v3.privacy) {
                (None, _) => (String::new(), Auth::NoAuthNoPriv),
                (Some((_, pw)), None) => (pw.expose_secret().to_owned(), Auth::AuthNoPriv),
                (Some((_, pw)), Some((c, priv_pw))) => (
                    pw.expose_secret().to_owned(),
                    Auth::AuthPriv {
                        cipher: cipher(*c),
                        privacy_password: priv_pw.expose_secret().as_bytes().to_vec(),
                    },
                ),
            };
            let mut security = Security::new(v3.username.as_bytes(), auth_pass.as_bytes()).with_auth(level);
            if !v3.context.is_empty() {
                security = security.with_context_name(&v3.context);
            }
            if let Some((p, _)) = &v3.auth {
                security = security.with_auth_protocol(auth_protocol(*p));
            }
            let mut session = AsyncSession::new_v3(req.target, req_id, security)
                .await
                .map_err(|e| TransportError::Io(e.to_string()))?;
            // обнаружение engineID и окна времени (RFC 3414)
            session.init().await.map_err(map_err)?;
            Ok(session)
        }
    }
}

async fn within<T>(
    t: Duration,
    f: impl std::future::Future<Output = Result<T, snmp2::Error>>,
) -> Result<T, TransportError> {
    match tokio::time::timeout(t, f).await {
        Err(_) => Err(TransportError::Timeout),
        Ok(r) => r.map_err(map_err),
    }
}

#[async_trait::async_trait]
impl SnmpTransport for Snmp2Transport {
    async fn collect(&self, req: &Request<'_>) -> Result<Collected, TransportError> {
        // открытие сессии для v3 включает обмен для обнаружения engineID
        let mut session =
            tokio::time::timeout(req.timeout, open_session(req)).await.map_err(|_| TransportError::Timeout)??;

        let mut out = Collected::default();

        for chunk in req.get_oids.chunks(GET_CHUNK) {
            let refs: Vec<&Oid<'_>> = chunk.iter().collect();
            let pdu = within(req.timeout, session.get_many(&refs)).await?;
            if pdu.error_status != 0 {
                return Err(TransportError::Protocol(format!(
                    "error-status {} (index {})",
                    pdu.error_status, pdu.error_index
                )));
            }
            let mut received = 0usize;
            for (oid, value) in pdu.varbinds {
                received += 1;
                if out.varbinds.len() >= req.max_varbinds {
                    out.truncated = true;
                    return Ok(out);
                }
                out.varbinds.push(VarBind { oid: oid_to_string(&oid), value: Some(value_to_proto(&value)) });
            }
            // snmp2 при ошибке разбора значения молча обрывает итератор varbind'ов
            // (например, Counter64 >= 2^63 в версии 0.5.2), и остаток ответа пропадает.
            // Неполный ответ лучше явной ошибки, чем тихой потери метрик.
            if received != chunk.len() {
                return Err(TransportError::Protocol(format!(
                    "разобрано {received} varbind'ов из {} запрошенных (часть ответа не удалось декодировать)",
                    chunk.len()
                )));
            }
        }

        for root in req.walk_roots {
            let mut cursor: Oid<'static> = root.clone();
            let mut last: Vec<u64> = Vec::new();
            'walk: loop {
                let pdu = within(req.timeout, session.getbulk(&[&cursor], 0, req.max_repetitions)).await?;
                if pdu.error_status != 0 {
                    return Err(TransportError::Protocol(format!("error-status {}", pdu.error_status)));
                }
                let mut got_any = false;
                let mut pdu_was_empty = true;
                let mut next_cursor = None;
                for (oid, value) in pdu.varbinds {
                    pdu_was_empty = false;
                    if matches!(value, Value::EndOfMibView) || !oid.starts_with(root) {
                        break 'walk;
                    }
                    let arcs = oid_arcs(&oid);
                    // устройство, зациклившее обход, не должно подвесить задачу
                    if arcs <= last {
                        return Err(TransportError::Protocol("OID не возрастает (зацикленный обход)".into()));
                    }
                    last = arcs;
                    if out.varbinds.len() >= req.max_varbinds {
                        out.truncated = true;
                        return Ok(out);
                    }
                    got_any = true;
                    out.varbinds.push(VarBind { oid: oid_to_string(&oid), value: Some(value_to_proto(&value)) });
                    next_cursor = Some(oid.to_owned());
                }
                if !got_any && pdu_was_empty {
                    return Err(TransportError::Protocol(
                        "ответ GETBULK без varbind'ов: значение не удалось декодировать".into(),
                    ));
                }
                match (got_any, next_cursor) {
                    (true, Some(c)) => cursor = c,
                    _ => break,
                }
            }
        }
        Ok(out)
    }
}
