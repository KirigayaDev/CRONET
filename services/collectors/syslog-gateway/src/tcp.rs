use crate::{
    config::Config,
    error::{Error, Result},
    framing::SyslogCodec,
    model::envelope,
    Intake,
};
use contracts::netmon::v1::SyslogTransport;
use futures::StreamExt;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_util::{codec::FramedRead, sync::CancellationToken};

async fn connection(socket: TcpStream, cfg: Arc<Config>, intake: Intake, stop: CancellationToken) -> Result<()> {
    let peer = socket.peer_addr()?;
    let mut frames = FramedRead::new(socket, SyslogCodec::new(cfg.max_frame));
    loop {
        // A whole-frame deadline also bounds slowloris clients that trickle bytes.
        let frame = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            frame = tokio::time::timeout(cfg.idle_timeout, frames.next()) => match frame {
                Ok(Some(Ok(frame))) => frame,
                Ok(None) | Err(_) => return Ok(()),
                Ok(Some(Err(_))) => { tracing::debug!(transport = "tcp", source_ip = %peer.ip(), status = "invalid_frame", "TCP connection closed"); return Ok(()); }
            },
        };
        let event = envelope(&cfg, peer, SyslogTransport::Tcp, frame);
        // At most one frame per connection awaits a durable commit. No further read.
        tokio::select! { _ = stop.cancelled() => return Ok(()), result = intake.tcp(event) => result? }
    }
}

pub async fn listen(listener: TcpListener, cfg: Arc<Config>, intake: Intake, stop: CancellationToken) -> Result<()> {
    let slots = Arc::new(Semaphore::new(cfg.max_connections));
    let mut connections = JoinSet::new();
    loop {
        // Taking a permit before accept applies the kernel's bounded accept backlog.
        let permit = tokio::select! {
            _ = stop.cancelled() => break,
            permit = slots.clone().acquire_owned() => permit.map_err(|_| Error::Task)?,
        };
        let socket = tokio::select! {
            _ = stop.cancelled() => break,
            accepted = listener.accept() => accepted?.0,
        };
        let (cfg, intake, stop) = (cfg.clone(), intake.clone(), stop.clone());
        connections.spawn(async move {
            let _permit = permit;
            connection(socket, cfg, intake, stop).await
        });
        while let Some(result) = connections.try_join_next() {
            let _connection_result = result.map_err(|_| Error::Task)?;
        }
    }
    drop(intake);
    while let Some(result) = connections.join_next().await {
        let _connection_result = result.map_err(|_| Error::Task)?;
    }
    Ok(())
}
