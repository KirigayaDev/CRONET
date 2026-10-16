use contracts::netmon::v1::LogEvent;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv6Addr};
use uuid::Uuid;

// Bound the wire envelope before prost allocates strings/maps, including unknown fields.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_MESSAGE_BYTES: usize = 240 * 1024;
pub const MAX_ATTRIBUTES: usize = 64;
pub const MAX_ATTRIBUTE_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
pub struct LogRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    pub event_time: i64,
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub device_id: Option<Uuid>,
    pub source_ip: Ipv6Addr,
    pub hostname: String,
    pub facility: u8,
    pub severity: u8,
    pub app_name: String,
    // RowBinary String is length + bytes. No UTF-8 conversion or escaping.
    #[serde(with = "serde_bytes")]
    pub message: Vec<u8>,
    pub collector_id: String,
    pub attributes: Vec<(String, String)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("{0}")]
pub struct Invalid(pub &'static str);

fn text(value: &str, limit: usize, empty: bool) -> bool {
    (empty || !value.trim().is_empty()) && value.len() <= limit && !value.chars().any(char::is_control)
}

pub fn decode(payload: &[u8], subject: &str) -> Result<LogRow, Invalid> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(Invalid("payload_too_large"));
    }
    let event = LogEvent::decode(payload).map_err(|_| Invalid("protobuf"))?;
    if !crate::config::site_token(&event.site) || subject != contracts::subjects::log(&event.site) {
        return Err(Invalid("site_routing"));
    }
    let event_id = Uuid::parse_str(&event.event_id).map_err(|_| Invalid("event_uuid"))?;
    if event_id.is_nil() {
        return Err(Invalid("event_uuid"));
    }
    let device_id = event.device_id.as_deref().map(Uuid::parse_str).transpose().map_err(|_| Invalid("device_uuid"))?;
    let source_ip = match event.source_ip.parse::<IpAddr>().map_err(|_| Invalid("source_ip"))? {
        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
        IpAddr::V6(ip) => ip,
    };
    let time = event.event_time.ok_or(Invalid("missing_timestamp"))?;
    if !(-2_208_988_800..10_413_792_000).contains(&time.seconds) || !(0..1_000_000_000).contains(&time.nanos) {
        return Err(Invalid("timestamp_range"));
    }
    if event.facility > 23 || event.severity > 7 {
        return Err(Invalid("syslog_codes"));
    }
    if !text(&event.hostname, 256, true) || !text(&event.app_name, 128, true) {
        return Err(Invalid("source_metadata"));
    }
    if !text(&event.collector_id, 128, false) {
        return Err(Invalid("collector_id"));
    }
    if event.message.len() > MAX_MESSAGE_BYTES {
        return Err(Invalid("message_too_large"));
    }
    let attribute_bytes: usize = event.attributes.iter().map(|(k, v)| k.len() + v.len()).sum();
    if event.attributes.len() > MAX_ATTRIBUTES
        || attribute_bytes > MAX_ATTRIBUTE_BYTES
        || event.attributes.iter().any(|(k, v)| !text(k, 128, false) || v.len() > 4096)
    {
        return Err(Invalid("attributes"));
    }
    let mut attributes: Vec<_> = event.attributes.into_iter().collect();
    attributes.sort_unstable();
    Ok(LogRow {
        event_id,
        event_time: time.seconds * 1_000 + i64::from(time.nanos / 1_000_000),
        device_id,
        source_ip,
        hostname: event.hostname,
        facility: event.facility as u8,
        severity: event.severity as u8,
        app_name: event.app_name,
        message: event.message,
        collector_id: event.collector_id,
        attributes,
    })
}

fn varint(mut n: usize) -> usize {
    let mut size = 1;
    while n >= 128 {
        n >>= 7;
        size += 1;
    }
    size
}

impl LogRow {
    /// Exact uncompressed RowBinary row size; ingested_at is supplied by ClickHouse.
    pub fn encoded_bytes(&self) -> usize {
        let bytes = |len| varint(len) + len;
        // event UUID + timestamp + nullable tag + optional UUID + IPv6 + two UInt8.
        43 + usize::from(self.device_id.is_some()) * 16
            + bytes(self.hostname.len())
            + bytes(self.app_name.len())
            + bytes(self.message.len())
            + bytes(self.collector_id.len())
            + varint(self.attributes.len())
            + self.attributes.iter().map(|(k, v)| bytes(k.len()) + bytes(v.len())).sum::<usize>()
    }
}
