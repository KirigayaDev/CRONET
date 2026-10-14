use contracts::netmon::v1::{snmp_value::Kind, SnmpValue};
use snmp2::{Oid, Value};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OidError {
    #[error("пустой OID")]
    Empty,
    #[error("OID слишком длинный (максимум {0} элементов)")]
    TooLong(usize),
    #[error("некорректный элемент OID: {0:?}")]
    BadArc(String),
    #[error("OID не соответствует правилам ASN.1 (первые два элемента)")]
    Invalid,
}

/// Максимум элементов в OID (по RFC 2578 - 128).
const MAX_OID_ARCS: usize = 128;

/// "1.3.6.1.2.1.1.3.0" или ".1.3.6..." -> `Oid`.
pub fn parse_oid(s: &str) -> Result<Oid<'static>, OidError> {
    let s = s.trim().trim_start_matches('.');
    if s.is_empty() {
        return Err(OidError::Empty);
    }
    let mut arcs = Vec::new();
    for part in s.split('.') {
        let arc: u64 = part.parse().map_err(|_| OidError::BadArc(part.chars().take(24).collect()))?;
        arcs.push(arc);
        if arcs.len() > MAX_OID_ARCS {
            return Err(OidError::TooLong(MAX_OID_ARCS));
        }
    }
    Oid::from(&arcs).map_err(|_| OidError::Invalid)
}

pub fn oid_to_string(oid: &Oid<'_>) -> String {
    oid.to_id_string()
}

/// Элементы OID в числовом виде (для сравнения порядка; BER-байты сравнивать нельзя).
pub fn oid_arcs(oid: &Oid<'_>) -> Vec<u64> {
    oid.iter().map(Iterator::collect).unwrap_or_default()
}

/// Преобразование значения без потери типа. Всё, что не является скалярным
/// SNMP-типом (последовательности, PDU), помечается `unsupported`.
pub fn value_to_proto(v: &Value<'_>) -> SnmpValue {
    let kind = match v {
        Value::Integer(n) => Kind::Integer(*n),
        Value::Boolean(b) => Kind::Integer(i64::from(*b)),
        Value::OctetString(b) => Kind::OctetString(b.to_vec()),
        Value::ObjectIdentifier(o) => Kind::ObjectId(oid_to_string(o)),
        Value::IpAddress(a) => Kind::IpAddress(a.to_vec()),
        Value::Counter32(n) => Kind::Counter32(*n),
        Value::Unsigned32(n) => Kind::Unsigned32(*n),
        Value::Timeticks(n) => Kind::Timeticks(*n),
        Value::Opaque(b) => Kind::Opaque(b.to_vec()),
        Value::Counter64(n) => Kind::Counter64(*n),
        Value::Null => Kind::Null(true),
        Value::NoSuchObject => Kind::NoSuchObject(true),
        Value::NoSuchInstance => Kind::NoSuchInstance(true),
        Value::EndOfMibView => Kind::EndOfMibView(true),
        _ => Kind::Unsupported(true),
    };
    SnmpValue { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_oids() {
        assert_eq!(oid_to_string(&parse_oid("1.3.6.1.2.1.1.3.0").unwrap()), "1.3.6.1.2.1.1.3.0");
        assert_eq!(oid_to_string(&parse_oid(".1.3.6.1.2.1.2.2.1.8").unwrap()), "1.3.6.1.2.1.2.2.1.8");
    }

    #[test]
    fn rejects_bad_oids() {
        assert_eq!(parse_oid(""), Err(OidError::Empty));
        assert_eq!(parse_oid("."), Err(OidError::Empty));
        assert!(matches!(parse_oid("1.3.x"), Err(OidError::BadArc(_))));
        assert!(matches!(parse_oid("1..3"), Err(OidError::BadArc(_))));
        assert!(matches!(parse_oid("-1.3"), Err(OidError::BadArc(_))));
        assert_eq!(parse_oid("7.1"), Err(OidError::Invalid));
        let long = vec!["1"; 200].join(".");
        assert_eq!(parse_oid(&long), Err(OidError::TooLong(MAX_OID_ARCS)));
    }

    #[test]
    fn keeps_snmp_types() {
        let c = value_to_proto(&Value::Counter64(7));
        assert_eq!(c.kind, Some(Kind::Counter64(7)));
        let g = value_to_proto(&Value::Unsigned32(5));
        assert_eq!(g.kind, Some(Kind::Unsigned32(5)));
        let s = value_to_proto(&Value::OctetString(b"ge-0/0/1"));
        assert_eq!(s.kind, Some(Kind::OctetString(b"ge-0/0/1".to_vec())));
        let m = value_to_proto(&Value::NoSuchInstance);
        assert_eq!(m.kind, Some(Kind::NoSuchInstance(true)));
    }
}
