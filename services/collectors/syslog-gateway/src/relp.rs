use crate::{
    config::Config,
    error::{Error, Result},
    model::envelope,
    Intake,
};
use bytes::{Buf, BytesMut};
use contracts::netmon::v1::SyslogTransport;
use futures::{SinkExt, StreamExt};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_while_m_n},
    combinator::{map, map_res},
    sequence::tuple,
    IResult,
};
use std::{fmt, io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::Semaphore,
    task::JoinSet,
};
use tokio_util::{
    codec::{Decoder, Encoder, Framed},
    sync::CancellationToken,
};

const MAX_TXNR: u32 = 999_999_999;
const MAX_HEADER: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    Open,
    Syslog,
    Close,
    Rsp,
    Unknown,
}
impl Command {
    pub fn name(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Syslog => "syslog",
            Self::Close => "close",
            Self::Rsp => "rsp",
            Self::Unknown => "unknown",
        }
    }
}
pub struct Frame {
    pub txnr: u32,
    pub command: Command,
    pub payload: Vec<u8>,
}
impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("txnr", &self.txnr)
            .field("command", &self.command)
            .field("bytes", &self.payload.len())
            .finish()
    }
}
pub struct RelpCodec {
    max: usize,
}
impl RelpCodec {
    pub fn new(max: usize) -> Self {
        Self { max }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid or oversized RELP frame")
}

fn number(input: &[u8]) -> IResult<&[u8], u32> {
    map_res(take_while_m_n(1, 9, |c: u8| c.is_ascii_digit()), |digits: &[u8]| {
        std::str::from_utf8(digits).ok().and_then(|s| s.parse::<u32>().ok()).ok_or(())
    })(input)
}
fn header(input: &[u8]) -> IResult<&[u8], (u32, Command, u32, bool)> {
    let command = map(take_while_m_n(1, 32, |c: u8| c.is_ascii_alphabetic()), |token: &[u8]| match token {
        b"open" => Command::Open,
        b"syslog" => Command::Syslog,
        b"close" => Command::Close,
        b"rsp" => Command::Rsp,
        _ => Command::Unknown,
    });
    let (remaining, (txnr, _, command, _, len, delimiter)) =
        tuple((number, tag(&b" "[..]), command, tag(&b" "[..]), number, alt((tag(&b" "[..]), tag(&b"\n"[..])))))(
            input,
        )?;
    Ok((remaining, (txnr, command, len, delimiter == b" ")))
}
impl Decoder for RelpCodec {
    type Item = Frame;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame>> {
        let (rest, (txnr, command, length, space)) = match header(src.as_ref()) {
            Ok(parsed) => parsed,
            Err(nom::Err::Incomplete(_)) if src.len() <= MAX_HEADER => return Ok(None),
            Err(_) => return Err(invalid()),
        };
        let length = length as usize;
        let prefix = src.len() - rest.len();
        if txnr == 0 || length > self.max || (length > 0) != space {
            return Err(invalid());
        }
        if length == 0 {
            src.advance(prefix);
            return Ok(Some(Frame { txnr, command, payload: vec![] }));
        }
        let total = prefix + length + 1;
        if src.len() < total {
            return Ok(None);
        }
        if src[prefix + length] != b'\n' {
            return Err(invalid());
        }
        let payload = src[prefix..prefix + length].to_vec();
        src.advance(total);
        Ok(Some(Frame { txnr, command, payload }))
    }
    fn decode_eof(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame>> {
        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            None if src.is_empty() => Ok(None),
            None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete RELP frame")),
        }
    }
}
impl Encoder<Frame> for RelpCodec {
    type Error = io::Error;
    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> io::Result<()> {
        if frame.txnr == 0 || frame.txnr > MAX_TXNR || frame.command == Command::Unknown || frame.payload.len() > 65536
        {
            return Err(invalid());
        }
        dst.extend_from_slice(format!("{} {} {}", frame.txnr, frame.command.name(), frame.payload.len()).as_bytes());
        if !frame.payload.is_empty() {
            dst.extend_from_slice(b" ");
            dst.extend_from_slice(&frame.payload);
        }
        dst.extend_from_slice(b"\n");
        Ok(())
    }
}

fn offers(payload: &[u8]) -> Option<u8> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut version = None;
    let mut syslog = false;
    for line in text.lines().filter(|line| !line.is_empty()) {
        let (name, values) = line.split_once('=')?;
        if name == "relp_version" {
            if version.is_some() {
                return None;
            }
            version = if values.split(',').any(|v| v == "1") {
                Some(1)
            } else if values.split(',').any(|v| v == "0") {
                Some(0)
            } else {
                return None;
            };
        } else if name == "commands" {
            syslog |= values.split(',').any(|v| v == "syslog");
        }
    }
    version.filter(|_| syslog)
}

async fn response<S: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut Framed<S, RelpCodec>,
    txnr: u32,
    payload: Vec<u8>,
    cfg: &Config,
    stop: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        _ = stop.cancelled() => Err(Error::Task),
        result = tokio::time::timeout(cfg.idle_timeout, framed.send(Frame { txnr, command: Command::Rsp, payload })) => result.map_err(|_| Error::Task)?.map_err(Error::Io),
    }
}

pub(crate) async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    peer: SocketAddr,
    cfg: Arc<Config>,
    intake: Intake,
    stop: CancellationToken,
    transport: SyslogTransport,
) -> Result<()> {
    let mut framed = Framed::new(stream, RelpCodec::new(cfg.max_frame));
    let mut opened = false;
    let mut expected = 1;
    loop {
        let frame = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            result = tokio::time::timeout(cfg.idle_timeout, framed.next()) => match result {
                Ok(Some(Ok(frame))) => frame,
                Ok(None) | Err(_) => return Ok(()),
                Ok(Some(Err(_))) => { tracing::debug!(transport = transport as i32, source_ip = %peer.ip(), status = "relp_invalid_frame", "RELP connection closed"); return Ok(()); }
            },
        };
        tracing::debug!(transport = transport as i32, source_ip = %peer.ip(), site = %cfg.site, command = frame.command.name(), txnr = frame.txnr, "RELP command received");
        let txnr = frame.txnr;
        if txnr != expected {
            response(&mut framed, txnr, b"500 Invalid transaction number".to_vec(), &cfg, &stop).await?;
            return Ok(());
        }
        expected = if txnr == MAX_TXNR { 1 } else { txnr + 1 };
        match frame.command {
            Command::Open if !opened => {
                let Some(version) = offers(&frame.payload) else {
                    response(&mut framed, txnr, b"500 Incompatible offers".to_vec(), &cfg, &stop).await?;
                    return Ok(());
                };
                let accepted = format!(
                    "200 OK\nrelp_version={version}\nrelp_software=cronet-syslog-gateway,0.1.0\ncommands=syslog\n"
                );
                response(&mut framed, txnr, accepted.into_bytes(), &cfg, &stop).await?;
                opened = true;
            }
            Command::Syslog if opened => {
                let event = envelope(&cfg, peer, transport, frame.payload);
                let event_id = event.event_id.clone();
                let committed =
                    tokio::select! { _ = stop.cancelled() => return Ok(()), result = intake.durable(event) => result, };
                if committed.is_err() {
                    return Ok(());
                } // No success response on a failed local commit.
                response(&mut framed, txnr, b"200 OK".to_vec(), &cfg, &stop).await?;
                tracing::debug!(transport = transport as i32, source_ip = %peer.ip(), site = %cfg.site, txnr, %event_id, status = "relp_ack", "RELP success after durable local commit");
            }
            Command::Close if opened && frame.payload.is_empty() => {
                response(&mut framed, txnr, b"200 OK".to_vec(), &cfg, &stop).await?;
                let _closed = tokio::time::timeout(cfg.idle_timeout, framed.close()).await;
                return Ok(());
            }
            _ => {
                response(&mut framed, txnr, b"500 Invalid command or session state".to_vec(), &cfg, &stop).await?;
                return Ok(());
            }
        }
    }
}

pub async fn listen(listener: TcpListener, cfg: Arc<Config>, intake: Intake, stop: CancellationToken) -> Result<()> {
    let slots = Arc::new(Semaphore::new(cfg.max_connections));
    let mut connections = JoinSet::new();
    loop {
        let permit = tokio::select! { _ = stop.cancelled() => break, permit = slots.clone().acquire_owned() => permit.map_err(|_| Error::Task)?, };
        let (socket, peer) =
            tokio::select! { _ = stop.cancelled() => break, accepted = listener.accept() => accepted?, };
        let (cfg, intake, stop) = (cfg.clone(), intake.clone(), stop.clone());
        connections.spawn(async move {
            let _permit = permit;
            let _result = session(socket, peer, cfg, intake, stop, SyslogTransport::Relp).await;
        });
        while let Some(result) = connections.try_join_next() {
            result.map_err(|_| Error::Task)?;
        }
    }
    drop(intake);
    while let Some(result) = connections.join_next().await {
        result.map_err(|_| Error::Task)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn fragmented_binary_frame_and_pipelined_frames() {
        let wire = b"2 syslog 4 a\n\xffb\n3 close 0\n";
        let mut codec = RelpCodec::new(4);
        let mut src = BytesMut::new();
        for byte in &wire[..15] {
            src.extend_from_slice(&[*byte]);
            assert!(codec.decode(&mut src).unwrap().is_none());
        }
        src.extend_from_slice(&wire[15..]);
        let frame = codec.decode(&mut src).unwrap().unwrap();
        assert_eq!(frame.txnr, 2);
        assert_eq!(frame.payload, b"a\n\xffb");
        assert_eq!(codec.decode(&mut src).unwrap().unwrap().command, Command::Close);
        assert!(src.is_empty());
    }
    #[test]
    fn reject_bad_numbers_lengths_delimiters_and_truncated_frames() {
        for wire in [
            "0 syslog 1 x\n",
            "1000000000 syslog 1 x\n",
            "-1 syslog 1 x\n",
            "1 syslog -1 x\n",
            "1 syslog 999999999 ",
            "1 syslog 0 \n",
            "1 syslog 1\n",
            "1 syslog 1 x!",
            "1 syslog 5 abcde\n",
        ] {
            assert!(RelpCodec::new(4).decode(&mut BytesMut::from(wire)).is_err(), "accepted malformed header");
        }
        for wire in ["1 ", "1 syslog 4 ab", "1 syslog 4 abcd"] {
            assert!(RelpCodec::new(4).decode_eof(&mut BytesMut::from(wire)).is_err());
        }
    }
    #[test]
    fn response_wire_is_relp_and_frame_debug_hides_payload() {
        let frame = Frame { txnr: 2, command: Command::Rsp, payload: b"200 OK".to_vec() };
        assert!(!format!("{frame:?}").contains("200 OK"));
        let mut dst = BytesMut::new();
        RelpCodec::new(65536).encode(frame, &mut dst).unwrap();
        assert_eq!(&dst[..], b"2 rsp 6 200 OK\n");
    }
    #[test]
    fn open_negotiates_only_supported_versions_and_commands() {
        assert_eq!(offers(b"relp_version=0\ncommands=syslog\n"), Some(0));
        assert_eq!(offers(b"relp_version=1\ncommands=syslog\n"), Some(1));
        for input in [
            b"commands=syslog\n".as_slice(),
            b"relp_version=2\ncommands=syslog\n",
            b"relp_version=1\ncommands=other\n",
            b"relp_version=1\nrelp_version=0\ncommands=syslog\n",
        ] {
            assert!(offers(input).is_none());
        }
    }

    #[tokio::test]
    async fn success_response_waits_for_the_durable_commit_completion() {
        use crate::{config::MIN_SPOOL_BYTES, spool::Spool};
        use std::sync::atomic::AtomicU64;
        let cfg = Arc::new(Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let intake = Intake { tx, drops: Arc::new(AtomicU64::new(0)) };
        let stop = CancellationToken::new();
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(session(
            server,
            "127.0.0.1:42".parse().unwrap(),
            cfg,
            intake,
            stop.clone(),
            SyslogTransport::Relp,
        ));
        let mut client = Framed::new(client, RelpCodec::new(65536));
        client
            .send(Frame { txnr: 1, command: Command::Open, payload: b"relp_version=0\ncommands=syslog\n".to_vec() })
            .await
            .unwrap();
        assert!(client.next().await.unwrap().unwrap().payload.starts_with(b"200 OK"));
        client.send(Frame { txnr: 2, command: Command::Syslog, payload: b"durable".to_vec() }).await.unwrap();
        let pending = rx.recv().await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), client.next()).await.is_err());
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(&dir.path().join("spool.redb"), MIN_SPOOL_BYTES).unwrap();
        spool.insert(&pending.event).unwrap();
        pending.committed.unwrap().send(()).unwrap();
        let response = client.next().await.unwrap().unwrap();
        assert_eq!(response.txnr, 2);
        assert_eq!(response.payload, b"200 OK");
        assert_eq!(spool.stats().unwrap().pending, 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_durable_intake_closes_without_success_response() {
        use std::sync::atomic::AtomicU64;
        let cfg = Arc::new(Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap());
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let intake = Intake { tx, drops: Arc::new(AtomicU64::new(0)) };
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(session(
            server,
            "127.0.0.1:42".parse().unwrap(),
            cfg,
            intake,
            CancellationToken::new(),
            SyslogTransport::Relp,
        ));
        let mut client = Framed::new(client, RelpCodec::new(65536));
        client
            .send(Frame { txnr: 1, command: Command::Open, payload: b"relp_version=0\ncommands=syslog\n".to_vec() })
            .await
            .unwrap();
        assert!(client.next().await.unwrap().unwrap().payload.starts_with(b"200 OK"));
        client.send(Frame { txnr: 2, command: Command::Syslog, payload: b"uncommitted".to_vec() }).await.unwrap();
        assert!(client.next().await.is_none());
        task.await.unwrap().unwrap();
    }
}
