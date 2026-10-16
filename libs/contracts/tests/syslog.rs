#![allow(clippy::unwrap_used)]

use contracts::{
    netmon::v1::{RawSyslogEvent, SyslogTransport},
    subjects,
};
use prost::Message;

#[test]
fn syslog_contract_preserves_bytes_and_metadata() {
    assert_eq!(SyslogTransport::Udp as i32, 1);
    assert_eq!(SyslogTransport::Tcp as i32, 2);
    assert_eq!(SyslogTransport::Tls as i32, 3);
    let event = RawSyslogEvent {
        event_id: "stable-id".into(),
        gateway_id: "gateway-1".into(),
        site: "site-a_1".into(),
        received_at: Some(prost_types::Timestamp { seconds: 123, nanos: 456 }),
        source_ip: "::1".into(),
        source_port: 5514,
        transport: SyslogTransport::Udp as i32,
        payload: vec![0xff, 0x00, b'\n'],
        truncated: true,
    };
    assert_eq!(RawSyslogEvent::decode(event.encode_to_vec().as_slice()).unwrap(), event);
    assert_eq!(subjects::syslog(&event.site), "raw.syslog.site-a_1");
    assert_eq!(subjects::RAW_SYSLOG_FILTER, "raw.syslog.>");
    assert_eq!(subjects::RAW_SYSLOG_STREAM, "RAW_SYSLOG");
}
