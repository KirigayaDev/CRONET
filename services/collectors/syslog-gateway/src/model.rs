use crate::config::Config;
use contracts::netmon::v1::{RawSyslogEvent, SyslogTransport};
use std::{
    net::{IpAddr, SocketAddr},
    time::SystemTime,
};

pub fn envelope(cfg: &Config, peer: SocketAddr, transport: SyslogTransport, mut payload: Vec<u8>) -> RawSyslogEvent {
    let truncated = payload.len() > cfg.max_frame;
    payload.truncate(cfg.max_frame);
    let source_ip = match peer.ip() {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(|| ip.to_string(), |v4| v4.to_string()),
        IpAddr::V4(ip) => ip.to_string(),
    };
    RawSyslogEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        gateway_id: cfg.gateway_id.clone(),
        site: cfg.site.clone(),
        received_at: Some(SystemTime::now().into()),
        source_ip,
        source_port: u32::from(peer.port()),
        transport: transport as i32,
        payload,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn udp_envelope_preserves_binary_and_normalizes_ip() {
        let mut cfg = Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap();
        cfg.max_frame = 2;
        let one = envelope(&cfg, "[::ffff:127.0.0.1]:42".parse().unwrap(), SyslogTransport::Udp, vec![0xff, 0x00, 1]);
        let two = envelope(&cfg, "127.0.0.1:42".parse().unwrap(), SyslogTransport::Udp, vec![0xff, 0x00]);
        assert_eq!(one.payload, [0xff, 0x00]);
        assert!(one.truncated);
        assert!(!two.truncated);
        assert_eq!(one.source_ip, "127.0.0.1");
        assert_eq!(one.source_port, 42);
        assert_eq!(one.site, "test");
        assert!(one.received_at.is_some());
        assert_ne!(one.event_id, two.event_id);
    }
}
