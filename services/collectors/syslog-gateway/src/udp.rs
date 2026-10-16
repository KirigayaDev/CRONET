use crate::{config::Config, error::Result, model::envelope, Intake};
use contracts::netmon::v1::SyslogTransport;
use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

pub fn bind(addr: SocketAddr, recv_buffer: usize) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_recv_buffer_size(recv_buffer)?;
    socket.set_nonblocking(true)?;
    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.bind(&addr.into())?;
    Ok(UdpSocket::from_std(socket.into())?)
}

pub async fn listen(socket: UdpSocket, cfg: Arc<Config>, intake: Intake, stop: CancellationToken) -> Result<()> {
    // Capture a complete IP datagram even if the configured payload limit is lower.
    // This also avoids Windows WSAEMSGSIZE and detects truncation deterministically.
    let mut buffer = vec![0u8; 65536];
    loop {
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            packet = socket.recv_from(&mut buffer) => {
                let (size, peer) = packet?;
                intake.udp(envelope(&cfg, peer, SyslogTransport::Udp, buffer[..size].to_vec()));
            }
        }
    }
}
