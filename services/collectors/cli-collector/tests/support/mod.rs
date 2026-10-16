#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]
use cli_collector::{
    session::{timestamp, Policy},
    transport::KnownHosts,
};
use contracts::netmon::v1::CliTask;
use creds::{CredError, CredStore, Credential};
use russh::{
    server::{self, Session},
    Channel, ChannelId, Pty,
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub const USER: &str = "test-reader";
pub const PASSWORD: &str = "test-only-cli-password";
pub const ALLOW: &str = "^(show|display|get) [a-z0-9 .-]+$";
#[derive(Default)]
pub struct Stats {
    pub connections: AtomicUsize,
    pub active: AtomicUsize,
    pub shells: AtomicUsize,
    pub commands: AtomicUsize,
    pub authentications: AtomicUsize,
}
pub struct SshServer {
    pub target: SocketAddr,
    pub known_hosts: String,
    pub stats: Arc<Stats>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for SshServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn server() -> SshServer {
    server_with_auth_delay(Duration::ZERO).await
}
pub async fn server_with_auth_delay(auth_delay: Duration) -> SshServer {
    let key = ssh_key::PrivateKey::random(&mut rand::rng(), ssh_key::Algorithm::Ed25519).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let known_hosts = format!("[{}]:{} {}\n", target.ip(), target.port(), key.public_key().to_openssh().unwrap());
    let config = Arc::new(server::Config {
        keys: vec![key],
        auth_rejection_time: Duration::from_millis(1),
        auth_rejection_time_initial: Some(Duration::ZERO),
        inactivity_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    });
    let stats = Arc::new(Stats::default());
    let s = stats.clone();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (socket, _) = accepted.unwrap(); s.connections.fetch_add(1, Ordering::SeqCst); s.active.fetch_add(1, Ordering::SeqCst);
                    let handler = Device { stats: s.clone(), input: Vec::new(), auth_delay }; let config = config.clone();
                    connections.spawn(async move { if let Ok(session) = server::run_stream(config, socket, handler).await { let _ = session.await; } });
                }
            }
        }
    });
    SshServer { target, known_hosts, stats, task }
}
struct Device {
    stats: Arc<Stats>,
    input: Vec<u8>,
    auth_delay: Duration,
}
impl Drop for Device {
    fn drop(&mut self) {
        self.stats.active.fetch_sub(1, Ordering::SeqCst);
    }
}
impl server::Handler for Device {
    type Error = russh::Error;
    async fn auth_password(&mut self, user: &str, password: &str) -> Result<server::Auth, Self::Error> {
        self.stats.authentications.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.auth_delay).await;
        Ok(if user == USER && password == PASSWORD {
            server::Auth::Accept
        } else {
            server::Auth::Reject { proceed_with_methods: None, partial_success: false }
        })
    }
    async fn channel_open_session(
        &mut self,
        _: Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await;
        Ok(())
    }
    async fn pty_request(
        &mut self,
        ch: ChannelId,
        _: &str,
        _: u32,
        _: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(ch)?;
        Ok(())
    }
    async fn shell_request(&mut self, ch: ChannelId, session: &mut Session) -> Result<(), Self::Error> {
        self.stats.shells.fetch_add(1, Ordering::SeqCst);
        session.channel_success(ch)?;
        session.data(ch, b"test device\r\nrouter# ".to_vec())?;
        Ok(())
    }
    async fn data(&mut self, ch: ChannelId, bytes: &[u8], session: &mut Session) -> Result<(), Self::Error> {
        self.input.extend_from_slice(bytes);
        while let Some(end) = self.input.iter().position(|b| *b == b'\n') {
            let command: Vec<_> = self.input.drain(..=end).collect();
            let command = String::from_utf8_lossy(&command).trim().to_owned();
            self.stats.commands.fetch_add(1, Ordering::SeqCst);
            let handle = session.handle();
            tokio::spawn(async move {
                let chunks: Vec<Vec<u8>> = match command.as_str() {
                    "show version" => vec![
                        b"show version\r\n\x1b[3".to_vec(),
                        "2mОС device 1.0\x1b[0m\r\nrou".as_bytes().to_vec(),
                        b"ter# ".to_vec(),
                    ],
                    "show interfaces" => vec![b"show interfaces\r\neth0 up\r\nrouter# ".to_vec()],
                    "show legacy" => vec![b"\xcf\xf0\xe8\xe2\xe5\xf2\r\nrouter# ".to_vec()],
                    "show slow" => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        vec![b"slow\r\nrouter# ".to_vec()]
                    }
                    "show large" => vec![vec![b'x'; 8192]; 32],
                    "show partial" => vec![b"partial output without prompt".to_vec()],
                    "show closed" => {
                        let _ = handle.close(ch).await;
                        vec![]
                    }
                    _ => vec![b"unknown diagnostic\r\nrouter# ".to_vec()],
                };
                for chunk in chunks {
                    if handle.data(ch, chunk).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            });
        }
        Ok(())
    }
}
pub struct Store {
    pub password: &'static str,
    pub fail: bool,
}
#[async_trait::async_trait]
impl CredStore for Store {
    async fn get(&self, _: &str) -> Result<Arc<Credential>, CredError> {
        if self.fail {
            return Err(CredError::Backend(PASSWORD.into()));
        }
        Ok(Arc::new(Credential::Basic { username: USER.into(), password: self.password.into() }))
    }
}
pub fn store() -> Store {
    Store { password: PASSWORD, fail: false }
}
pub fn policy(s: &SshServer) -> Policy {
    Policy {
        collector_id: "test-collector".into(),
        hosts: Arc::new(KnownHosts::parse(&s.known_hosts).unwrap()),
        allowed: regex::Regex::new(ALLOW).unwrap(),
        prompt: "(?m)router# *$".into(),
        connect_timeout: Duration::from_secs(2),
        auth_timeout: Duration::from_secs(2),
        command_timeout: Duration::from_secs(2),
        task_timeout: Duration::from_secs(5),
        max_command_bytes: 16 * 1024,
        max_total_bytes: 32 * 1024,
    }
}
pub fn task(s: &SshServer) -> CliTask {
    let mut deadline = timestamp();
    deadline.seconds += 60;
    CliTask {
        task_id: "task-1".into(),
        device_id: "device-1".into(),
        target: s.target.to_string(),
        credential_ref: "env:test".into(),
        commands: vec!["show version".into(), "show interfaces".into()],
        deadline: Some(deadline),
        ..Default::default()
    }
}
