use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use contracts::{
    gnmi::{
        g_nmi_client::GNmiClient, subscribe_request, subscription_list, Encoding, SubscribeRequest, SubscribeResponse,
        Subscription, SubscriptionList, SubscriptionMode,
    },
    netmon::v1::{GnmiMode, GnmiSubscribeTask, RawGnmiUpdate},
};
use creds::Credential;
use prost::Message;
use rand::RngExt;
use secrecy::ExposeSecret;
use tokio::sync::{mpsc, watch};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

use crate::path::{parse_path, PathError};

#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn publish(&self, update: RawGnmiUpdate) -> Result<(), String>;
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("некорректная задача: {0}")]
    BadTask(String),
    #[error("подключение: {0}")]
    Connect(String),
    #[error("gNMI: {0}")]
    Rpc(String),
    #[error("устройство молчит дольше {0:?}")]
    Idle(Duration),
    #[error("публикация: {0}")]
    Sink(String),
}

pub struct Params {
    pub task: GnmiSubscribeTask,
    pub cred: Option<Arc<Credential>>,
    pub ca_pem: Option<Vec<u8>>,
    pub collector_id: String,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

impl From<PathError> for SessionError {
    fn from(e: PathError) -> Self {
        SessionError::BadTask(e.to_string())
    }
}

pub fn build_request(task: &GnmiSubscribeTask) -> Result<SubscribeRequest, SessionError> {
    if task.paths.is_empty() {
        return Err(SessionError::BadTask("нет путей для подписки".into()));
    }
    if task.paths.len() > 200 {
        return Err(SessionError::BadTask("слишком много путей".into()));
    }
    let mode = match GnmiMode::try_from(task.mode) {
        Ok(GnmiMode::Sample) => SubscriptionMode::Sample,
        Ok(GnmiMode::OnChange) => SubscriptionMode::OnChange,
        _ => return Err(SessionError::BadTask("mode должен быть SAMPLE или ON_CHANGE".into())),
    };
    let mut subs = Vec::new();
    for p in &task.paths {
        subs.push(Subscription {
            path: Some(parse_path(&p.path, &task.origin)?),
            mode: mode as i32,
            sample_interval: task.sample_interval_ns,
            heartbeat_interval: task.heartbeat_interval_ns,
            suppress_redundant: false,
        });
    }
    Ok(SubscribeRequest {
        request: Some(subscribe_request::Request::Subscribe(SubscriptionList {
            subscription: subs,
            mode: subscription_list::Mode::Stream as i32,
            encoding: Encoding::Proto as i32,
            ..Default::default()
        })),
        extension: Vec::new(),
    })
}

fn idle_limit(task: &GnmiSubscribeTask) -> Duration {
    let ns = if task.heartbeat_interval_ns > 0 {
        task.heartbeat_interval_ns.saturating_mul(3)
    } else if task.sample_interval_ns > 0 {
        task.sample_interval_ns.saturating_mul(5)
    } else {
        0
    };
    Duration::from_nanos(ns).max(Duration::from_secs(60))
}

async fn connect(p: &Params) -> Result<tonic::transport::Channel, SessionError> {
    let scheme = if p.task.tls { "https" } else { "http" };
    let mut ep = Endpoint::from_shared(format!("{scheme}://{}", p.task.target))
        .map_err(|e| SessionError::Connect(e.to_string()))?
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if p.task.tls {
        let mut tls = ClientTlsConfig::new().with_webpki_roots();
        if !p.task.tls_server_name.is_empty() {
            tls = tls.domain_name(p.task.tls_server_name.clone());
        }
        if let Some(pem) = &p.ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        ep = ep.tls_config(tls).map_err(|e| SessionError::Connect(e.to_string()))?;
    }
    ep.connect().await.map_err(|e| SessionError::Connect(e.to_string()))
}

async fn run_once(
    p: &Params,
    sink: &dyn Sink,
    cancel: &mut watch::Receiver<bool>,
    got_data: &mut bool,
) -> Result<(), SessionError> {
    let request = build_request(&p.task)?;
    let channel = connect(p).await?;

    let auth = match p.cred.as_deref() {
        Some(Credential::Basic { username, password }) => Some((
            MetadataValue::try_from(username.as_str())
                .map_err(|_| SessionError::BadTask("недопустимый логин".into()))?,
            MetadataValue::try_from(password.expose_secret())
                .map_err(|_| SessionError::BadTask("недопустимый пароль".into()))?,
        )),
        Some(_) => return Err(SessionError::BadTask("для gNMI нужен секрет типа basic".into())),
        None => None,
    };
    let mut client = GNmiClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        if let Some((u, pw)) = &auth {
            req.metadata_mut().insert("username", u.clone());
            req.metadata_mut().insert("password", pw.clone());
        }
        Ok(req)
    });

    let (tx, rx) = mpsc::channel::<SubscribeRequest>(1);
    tx.send(request).await.map_err(|_| SessionError::Rpc("канал запроса закрыт".into()))?;
    let mut stream = client
        .subscribe(ReceiverStream::new(rx))
        .await
        .map_err(|s| SessionError::Rpc(format!("{}: {}", s.code(), s.message())))?
        .into_inner();

    let session_id = uuid::Uuid::new_v4().to_string();
    let idle = idle_limit(&p.task);
    let mut seq: u64 = 0;
    tracing::info!(device = %p.task.device_id, %session_id, "gNMI-подписка установлена");

    loop {
        let next = tokio::select! {
            _ = cancel.changed() => return Ok(()),
            r = tokio::time::timeout(idle, stream.next()) => r,
        };
        let msg = match next {
            Err(_) => return Err(SessionError::Idle(idle)),
            Ok(None) => return Err(SessionError::Rpc("устройство закрыло поток".into())),
            Ok(Some(Err(s))) => return Err(SessionError::Rpc(format!("{}: {}", s.code(), s.message()))),
            Ok(Some(Ok(m))) => m,
        };
        let m: SubscribeResponse = msg;
        let d = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
        let update = RawGnmiUpdate {
            device_id: p.task.device_id.clone(),
            collector_id: p.collector_id.clone(),
            sequence: seq,
            session_id: session_id.clone(),
            received_at: Some(prost_types::Timestamp {
                seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                nanos: d.subsec_nanos() as i32,
            }),
            subscribe_response: m.encode_to_vec(),
        };
        seq += 1;
        *got_data = true;
        sink.publish(update).await.map_err(SessionError::Sink)?;
    }
}

pub async fn run_forever(p: Params, sink: Arc<dyn Sink>, mut cancel: watch::Receiver<bool>) {
    let mut backoff = p.backoff_min;
    loop {
        if *cancel.borrow() {
            return;
        }
        let started = tokio::time::Instant::now();
        let mut got_data = false;
        match run_once(&p, &*sink, &mut cancel, &mut got_data).await {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(device = %p.task.device_id, error = %e, "gNMI-сессия прервана");
                if got_data || started.elapsed() > Duration::from_secs(60) {
                    backoff = p.backoff_min;
                }
            }
        }
        let jitter = rand::rng().random_range(0.5..1.5);
        let wait = backoff.mul_f64(jitter);
        tokio::select! {
            _ = cancel.changed() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        backoff = (backoff * 2).min(p.backoff_max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{
        gnmi::{
            g_nmi_server::{GNmi, GNmiServer},
            subscribe_response, CapabilityRequest, CapabilityResponse, GetRequest, GetResponse, Notification,
            SetRequest, SetResponse,
        },
        netmon::v1::GnmiPath,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status, Streaming};

    /// Поддельное устройство: на каждую подписку отдаёт три обновления и закрывает поток.
    struct Mock {
        subs: Arc<AtomicUsize>,
        seen_auth: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[tonic::async_trait]
    impl GNmi for Mock {
        async fn capabilities(&self, _: Request<CapabilityRequest>) -> Result<Response<CapabilityResponse>, Status> {
            Err(Status::unimplemented("-"))
        }
        async fn get(&self, _: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            Err(Status::unimplemented("-"))
        }
        async fn set(&self, _: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
            Err(Status::unimplemented("-"))
        }
        type SubscribeStream =
            std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<SubscribeResponse, Status>> + Send>>;
        async fn subscribe(
            &self,
            req: Request<Streaming<SubscribeRequest>>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let md = req.metadata();
            let get = |k: &str| md.get(k).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
            self.seen_auth.lock().unwrap().push((get("username"), get("password")));
            let mut inbound = req.into_inner();
            let first = inbound.message().await?.ok_or_else(|| Status::invalid_argument("нет запроса"))?;
            let Some(subscribe_request::Request::Subscribe(list)) = first.request else {
                return Err(Status::invalid_argument("ожидался Subscribe"));
            };
            let path = list.subscription[0].path.clone().unwrap();
            assert_eq!(path.elem[0].name, "interfaces");
            self.subs.fetch_add(1, Ordering::SeqCst);
            let items: Vec<Result<SubscribeResponse, Status>> = (0..3)
                .map(|i| {
                    Ok(SubscribeResponse {
                        response: Some(subscribe_response::Response::Update(Notification {
                            timestamp: 1000 + i,
                            ..Default::default()
                        })),
                        extension: vec![],
                    })
                })
                .collect();
            Ok(Response::new(Box::pin(tokio_stream::iter(items))))
        }
    }

    struct Collect(Mutex<Vec<RawGnmiUpdate>>);
    #[async_trait::async_trait]
    impl Sink for Collect {
        async fn publish(&self, u: RawGnmiUpdate) -> Result<(), String> {
            self.0.lock().unwrap().push(u);
            Ok(())
        }
    }

    fn task(addr: &str) -> GnmiSubscribeTask {
        GnmiSubscribeTask {
            device_id: "sw1".into(),
            target: addr.into(),
            paths: vec![GnmiPath { path: "/interfaces/interface/state/counters".into() }],
            mode: GnmiMode::Sample as i32,
            sample_interval_ns: 10_000_000_000,
            ..Default::default()
        }
    }

    #[test]
    fn request_validation() {
        let mut t = task("x:1");
        assert!(build_request(&t).is_ok());
        t.paths.clear();
        assert!(build_request(&t).is_err());
        let mut t = task("x:1");
        t.mode = 0;
        assert!(build_request(&t).is_err());
        let mut t = task("x:1");
        t.paths[0].path = "/a//b".into();
        assert!(build_request(&t).is_err());
    }

    #[tokio::test]
    async fn streams_updates_and_reconnects_with_new_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let subs = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mock = Mock { subs: subs.clone(), seen_auth: seen.clone() };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(GNmiServer::new(mock))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let sink = Arc::new(Collect(Mutex::new(Vec::new())));
        let (stop_tx, stop_rx) = watch::channel(false);
        let params = Params {
            task: task(&addr.to_string()),
            cred: Some(Arc::new(
                Credential::from_json(br#"{"version":"basic","username":"mon","password":"pw-123456"}"#).unwrap(),
            )),
            ca_pem: None,
            collector_id: "c1".into(),
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
        };
        let handle = tokio::spawn(run_forever(params, sink.clone(), stop_rx));

        // ждём две сессии (устройство закрывает поток после трёх обновлений)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while sink.0.lock().unwrap().len() < 6 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle).await.expect("остановка по отмене").unwrap();

        let got = sink.0.lock().unwrap();
        assert!(got.len() >= 6, "получено {}", got.len());
        assert!(subs.load(Ordering::SeqCst) >= 2, "после закрытия потока должно быть переподключение");
        let ids: std::collections::HashSet<_> = got.iter().map(|u| u.session_id.clone()).collect();
        assert!(ids.len() >= 2);
        // последовательность: 0,1,2 в каждой сессии
        let first_sess: Vec<_> = got.iter().filter(|u| u.session_id == got[0].session_id).map(|u| u.sequence).collect();
        assert_eq!(first_sess, [0, 1, 2]);
        // байты - валидный SubscribeResponse, ничего не потеряно
        let resp = SubscribeResponse::decode(got[0].subscribe_response.as_slice()).unwrap();
        assert!(matches!(resp.response, Some(subscribe_response::Response::Update(_))));
        // логин и пароль ушли в метаданных, но не попали в обновления
        assert_eq!(seen.lock().unwrap()[0], ("mon".to_owned(), "pw-123456".to_owned()));
        assert!(!format!("{:?}", got[0]).contains("pw-123456"));
    }
}
