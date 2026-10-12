use std::{
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use contracts::netmon::v1::{PollStatus, PollTask, RawSnmpResult, SnmpVersion};
use creds::{CredError, CredStore, Credential};
use ipnet::IpNet;
use snmp2::Oid;
use snmp_types::parse_oid;

use crate::transport::{Request, SnmpTransport, TransportError};

pub struct Policy {
    pub collector_id: String,
    pub default_timeout: Duration,
    pub default_max_repetitions: u32,
    pub max_varbinds: usize,
    pub max_task_time: Duration,
    pub allowed_targets: Vec<IpNet>,
    pub allow_legacy_crypto: bool,
}

const MAX_GET_OIDS: usize = 100;
const MAX_WALK_ROOTS: usize = 20;
const MAX_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REPETITIONS: u32 = 100;

struct Validated {
    target: SocketAddr,
    get_oids: Vec<Oid<'static>>,
    walk_roots: Vec<Oid<'static>>,
}

fn validate(task: &PollTask, policy: &Policy) -> Result<Validated, String> {
    if task.task_id.is_empty() || task.device_id.is_empty() {
        return Err("пустой task_id или device_id".into());
    }
    // только IP:порт: имена не резолвим, чтобы задача не могла направить опрос куда угодно через DNS
    let target: SocketAddr = task.target.parse().map_err(|_| "target должен быть вида IP:порт".to_string())?;
    if !policy.allowed_targets.is_empty() && !policy.allowed_targets.iter().any(|n| n.contains(&target.ip())) {
        return Err("target вне разрешённых сетей".into());
    }
    if task.get_oids.is_empty() && task.walk_roots.is_empty() {
        return Err("нет ни get_oids, ни walk_roots".into());
    }
    if task.get_oids.len() > MAX_GET_OIDS || task.walk_roots.len() > MAX_WALK_ROOTS {
        return Err("слишком много OID в задаче".into());
    }
    let get_oids = task
        .get_oids
        .iter()
        .map(|s| parse_oid(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("get_oids: {e}"))?;
    let walk_roots = task
        .walk_roots
        .iter()
        .map(|s| parse_oid(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("walk_roots: {e}"))?;
    Ok(Validated { target, get_oids, walk_roots })
}

fn timestamp(t: SystemTime) -> prost_types::Timestamp {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    prost_types::Timestamp { seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX), nanos: d.subsec_nanos() as i32 }
}

fn result(
    task: &PollTask,
    policy: &Policy,
    started: SystemTime,
    status: PollStatus,
    detail: impl Into<String>,
) -> RawSnmpResult {
    RawSnmpResult {
        task_id: task.task_id.clone(),
        device_id: task.device_id.clone(),
        collector_id: policy.collector_id.clone(),
        attempt: task.attempt,
        collected_at: Some(timestamp(started)),
        duration_ms: u32::try_from(started.elapsed().unwrap_or_default().as_millis()).unwrap_or(u32::MAX),
        status: status as i32,
        error_detail: detail.into(),
        varbinds: Vec::new(),
        truncated: false,
    }
}

/// Выполняет задачу и всегда возвращает результат (ошибки - тоже результат).
pub async fn execute(
    task: &PollTask,
    now: SystemTime,
    policy: &Policy,
    creds: &dyn CredStore,
    transport: &dyn SnmpTransport,
) -> RawSnmpResult {
    let started = now;

    // дедлайн: опрос по устаревшей задаче только грузит оборудование
    let remaining = match task.deadline.as_ref() {
        Some(d) => {
            let dl = SystemTime::UNIX_EPOCH
                + Duration::new(u64::try_from(d.seconds).unwrap_or(0), u32::try_from(d.nanos).unwrap_or(0));
            match dl.duration_since(now) {
                Ok(r) if !r.is_zero() => r,
                _ => {
                    return result(task, policy, started, PollStatus::Expired, "дедлайн задачи прошёл до начала опроса")
                }
            }
        }
        None => policy.max_task_time,
    };

    let v = match validate(task, policy) {
        Ok(v) => v,
        Err(e) => return result(task, policy, started, PollStatus::BadTask, e),
    };

    let version = SnmpVersion::try_from(task.snmp_version).unwrap_or(SnmpVersion::Unspecified);
    if !matches!(version, SnmpVersion::V2c | SnmpVersion::V3) {
        return result(task, policy, started, PollStatus::BadTask, "поддерживаются только SNMPv2c и SNMPv3");
    }

    let cred = match creds.get(&task.credential_ref).await {
        Ok(c) => c,
        // ошибки Vault не содержат секретов (см. creds), но ссылку логируем только здесь
        Err(
            e @ (CredError::BadReference
            | CredError::NotFound
            | CredError::Invalid(_)
            | CredError::Forbidden
            | CredError::Backend(_)),
        ) => {
            return result(task, policy, started, PollStatus::CredError, e.to_string());
        }
    };

    match (&*cred, version) {
        (Credential::V2c { .. }, SnmpVersion::V2c) => {}
        (Credential::V3(v3), SnmpVersion::V3) => {
            if v3.is_legacy() && !policy.allow_legacy_crypto {
                return result(
                    task,
                    policy,
                    started,
                    PollStatus::CredError,
                    "учётка использует MD5/SHA-1/DES, а NETMON_ALLOW_LEGACY_CRYPTO выключен",
                );
            }
            if v3.auth.is_none() && !policy.allow_legacy_crypto {
                return result(task, policy, started, PollStatus::CredError, "noAuthNoPriv запрещён политикой");
            }
        }
        _ => {
            return result(
                task,
                policy,
                started,
                PollStatus::CredError,
                "тип секрета не соответствует версии SNMP в задаче",
            )
        }
    }

    let timeout =
        if task.timeout_ms == 0 { policy.default_timeout } else { Duration::from_millis(u64::from(task.timeout_ms)) }
            .min(MAX_TIMEOUT);
    let max_rep = if task.max_repetitions == 0 { policy.default_max_repetitions } else { task.max_repetitions }
        .min(MAX_REPETITIONS);
    let total = remaining.min(policy.max_task_time);

    let req = Request {
        target: v.target,
        cred: &cred,
        get_oids: &v.get_oids,
        walk_roots: &v.walk_roots,
        timeout,
        max_repetitions: max_rep,
        max_varbinds: policy.max_varbinds,
    };

    match tokio::time::timeout(total, transport.collect(&req)).await {
        Err(_) => result(task, policy, started, PollStatus::Timeout, "общий лимит времени задачи исчерпан"),
        Ok(Err(TransportError::Timeout)) => {
            result(task, policy, started, PollStatus::Timeout, "устройство не ответило")
        }
        Ok(Err(TransportError::Auth(m))) => result(task, policy, started, PollStatus::AuthFail, m),
        Ok(Err(e)) => result(task, policy, started, PollStatus::Error, e.to_string()),
        Ok(Ok(c)) => {
            let mut r = result(task, policy, started, PollStatus::Ok, "");
            r.varbinds = c.varbinds;
            r.truncated = c.truncated;
            r
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Collected;
    use contracts::netmon::v1::{snmp_value::Kind, SnmpValue, VarBind};
    use std::sync::{Arc, Mutex};

    struct FakeCreds(Result<Arc<Credential>, CredError>);
    #[async_trait::async_trait]
    impl CredStore for FakeCreds {
        async fn get(&self, _: &str) -> Result<Arc<Credential>, CredError> {
            self.0.clone()
        }
    }

    enum Behavior {
        Ok,
        Timeout,
        Auth,
        Hang,
    }
    struct FakeTransport(Behavior, Mutex<usize>);
    #[async_trait::async_trait]
    impl SnmpTransport for FakeTransport {
        async fn collect(&self, _r: &Request<'_>) -> Result<Collected, TransportError> {
            *self.1.lock().unwrap() += 1;
            match self.0 {
                Behavior::Ok => Ok(Collected {
                    varbinds: vec![VarBind {
                        oid: "1.3.6.1.2.1.1.3.0".into(),
                        value: Some(SnmpValue { kind: Some(Kind::Timeticks(5)) }),
                    }],
                    truncated: false,
                }),
                Behavior::Timeout => Err(TransportError::Timeout),
                Behavior::Auth => Err(TransportError::Auth("UsernameMismatch".into())),
                Behavior::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(Collected::default())
                }
            }
        }
    }

    fn policy() -> Policy {
        Policy {
            collector_id: "test".into(),
            default_timeout: Duration::from_secs(1),
            default_max_repetitions: 10,
            max_varbinds: 100,
            max_task_time: Duration::from_millis(200),
            allowed_targets: vec!["10.0.0.0/8".parse().unwrap()],
            allow_legacy_crypto: false,
        }
    }

    fn v2c() -> Arc<Credential> {
        Arc::new(Credential::from_json(br#"{"version":"v2c","community":"c"}"#).unwrap())
    }

    fn task() -> PollTask {
        PollTask {
            task_id: "t1".into(),
            device_id: "r1".into(),
            target: "10.1.2.3:161".into(),
            snmp_version: SnmpVersion::V2c as i32,
            credential_ref: "vault:secret/x".into(),
            get_oids: vec!["1.3.6.1.2.1.1.3.0".into()],
            attempt: 1,
            ..Default::default()
        }
    }

    fn future_deadline() -> Option<prost_types::Timestamp> {
        Some(timestamp(SystemTime::now() + Duration::from_secs(60)))
    }

    async fn run(t: PollTask, b: Behavior, c: Result<Arc<Credential>, CredError>) -> (RawSnmpResult, usize) {
        let tr = FakeTransport(b, Mutex::new(0));
        let r = execute(&t, SystemTime::now(), &policy(), &FakeCreds(c), &tr).await;
        let calls = *tr.1.lock().unwrap();
        (r, calls)
    }

    #[tokio::test]
    async fn ok_result_carries_raw_varbinds() {
        let mut t = task();
        t.deadline = future_deadline();
        let (r, calls) = run(t, Behavior::Ok, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::Ok as i32);
        assert_eq!(r.varbinds.len(), 1);
        assert_eq!(r.task_id, "t1");
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn expired_task_is_not_polled() {
        let mut t = task();
        t.deadline = Some(timestamp(SystemTime::now() - Duration::from_secs(5)));
        let (r, calls) = run(t, Behavior::Ok, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::Expired as i32);
        assert_eq!(calls, 0, "устройство не должно опрашиваться");
    }

    #[tokio::test]
    async fn errors_become_results() {
        let (r, _) = run(task(), Behavior::Timeout, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::Timeout as i32);
        let (r, _) = run(task(), Behavior::Auth, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::AuthFail as i32);
        let (r, calls) = run(task(), Behavior::Ok, Err(CredError::NotFound)).await;
        assert_eq!(r.status, PollStatus::CredError as i32);
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn hanging_device_hits_task_time_limit() {
        let started = std::time::Instant::now();
        let (r, _) = run(task(), Behavior::Hang, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::Timeout as i32);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn bad_tasks_are_rejected_without_polling() {
        type Mutation = Box<dyn Fn(&mut PollTask)>;
        let cases: Vec<Mutation> = vec![
            Box::new(|t| t.target = "192.168.1.1:161".into()),    // вне allowlist
            Box::new(|t| t.target = "router.example:161".into()), // имя, а не IP
            Box::new(|t| t.get_oids = vec!["1.3.x".into()]),      // кривой OID
            Box::new(|t| {
                t.get_oids.clear();
            }), // нечего опрашивать
            Box::new(|t| t.snmp_version = 1),                     // v1 не поддерживается
            Box::new(|t| t.get_oids = vec!["1.3.6.1.2.1.1.3.0".into(); 101]),
        ];
        for mutate in cases {
            let mut t = task();
            mutate(&mut t);
            let (r, calls) = run(t, Behavior::Ok, Ok(v2c())).await;
            assert_eq!(r.status, PollStatus::BadTask as i32, "{}", r.error_detail);
            assert_eq!(calls, 0);
        }
    }

    #[tokio::test]
    async fn legacy_v3_crypto_is_refused_by_default() {
        let md5 = Arc::new(
            Credential::from_json(
                br#"{"version":"v3","username":"u","auth_protocol":"md5","auth_password":"authpass123"}"#,
            )
            .unwrap(),
        );
        let mut t = task();
        t.snmp_version = SnmpVersion::V3 as i32;
        let (r, calls) = run(t, Behavior::Ok, Ok(md5)).await;
        assert_eq!(r.status, PollStatus::CredError as i32);
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn credential_type_must_match_version() {
        let mut t = task();
        t.snmp_version = SnmpVersion::V3 as i32;
        let (r, _) = run(t, Behavior::Ok, Ok(v2c())).await;
        assert_eq!(r.status, PollStatus::CredError as i32);
    }
}
