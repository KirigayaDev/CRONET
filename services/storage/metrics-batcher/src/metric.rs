use contracts::netmon::v1::MetricPoint;
use prost::Message;
use serde::Serialize;
use uuid::Uuid;

pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_LABELS: usize = 64;
pub const MAX_LABEL_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize)]
pub struct MetricRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    pub event_time: i64,
    #[serde(with = "clickhouse::serde::uuid")]
    pub device_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub interface_id: Option<Uuid>,
    pub metric: String,
    pub value: f64,
    pub collector_id: String,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("{0}")]
pub struct Invalid(pub &'static str);

fn text(value: &str, limit: usize, empty: bool) -> bool {
    (empty || !value.trim().is_empty()) && value.len() <= limit && !value.chars().any(char::is_control)
}

pub fn decode(payload: &[u8], subject: &str) -> Result<MetricRow, Invalid> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(Invalid("payload_too_large"));
    }
    let point = MetricPoint::decode(payload).map_err(|_| Invalid("protobuf"))?;
    if !crate::config::site_token(&point.site) || subject != contracts::subjects::metric(&point.site) {
        return Err(Invalid("site_routing"));
    }
    let event_id = Uuid::parse_str(&point.event_id).map_err(|_| Invalid("event_uuid"))?;
    if event_id.is_nil() {
        return Err(Invalid("event_uuid"));
    }
    let device_id = Uuid::parse_str(&point.device_id).map_err(|_| Invalid("device_uuid"))?;
    let interface_id =
        point.interface_id.as_deref().map(Uuid::parse_str).transpose().map_err(|_| Invalid("interface_uuid"))?;
    let time = point.event_time.ok_or(Invalid("missing_timestamp"))?;
    if !(-2_208_988_800..10_413_792_000).contains(&time.seconds) || !(0..1_000_000_000).contains(&time.nanos) {
        return Err(Invalid("timestamp_range"));
    }
    if !text(&point.metric, 256, false) {
        return Err(Invalid("metric_name"));
    }
    if !point.value.is_finite() {
        return Err(Invalid("non_finite_value"));
    }
    if !text(&point.collector_id, 128, false) {
        return Err(Invalid("collector_id"));
    }
    let label_bytes: usize = point.labels.iter().map(|(k, v)| k.len() + v.len()).sum();
    if point.labels.len() > MAX_LABELS
        || label_bytes > MAX_LABEL_BYTES
        || point.labels.iter().any(|(k, v)| !text(k, 128, false) || !text(v, 1024, true))
    {
        return Err(Invalid("labels"));
    }
    let mut labels: Vec<_> = point.labels.into_iter().collect();
    labels.sort_unstable();
    Ok(MetricRow {
        event_id,
        event_time: time.seconds * 1_000 + i64::from(time.nanos / 1_000_000),
        device_id,
        interface_id,
        metric: point.metric,
        value: point.value,
        collector_id: point.collector_id,
        labels,
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

impl MetricRow {
    /// Exact uncompressed RowBinary row size; credentials/transport overhead excluded.
    pub fn encoded_bytes(&self) -> usize {
        let string = |s: &str| varint(s.len()) + s.len();
        49 + usize::from(self.interface_id.is_some()) * 16
            + string(&self.metric)
            + string(&self.collector_id)
            + varint(self.labels.len())
            + self.labels.iter().map(|(k, v)| string(k) + string(v)).sum::<usize>()
    }
}
