use crate::probe::ProbeError;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct Classification {
    pub vendor: Option<String>,
    pub device_type: String,
    pub model: Option<String>,
}
pub fn classify(sys_object_id: &str, sys_descr: Option<&str>) -> Result<Classification, ProbeError> {
    let oid = snmp_types::parse_oid(sys_object_id).map_err(|_| ProbeError::InvalidIdentity)?;
    let arcs = snmp_types::oid_arcs(&oid);
    let enterprise = arcs.strip_prefix(&[1, 3, 6, 1, 4, 1]).and_then(|rest| rest.first()).copied();
    let vendor = match enterprise {
        Some(9) => Some("Cisco"),
        Some(11) => Some("HPE"),
        Some(2011) => Some("Huawei"),
        Some(2636) => Some("Juniper"),
        Some(14988) => Some("MikroTik"),
        Some(12356) => Some("Fortinet"),
        Some(30065) => Some("Arista"),
        Some(41112) => Some("Ubiquiti"),
        _ => None,
    }
    .map(str::to_owned);
    let text = sys_descr.unwrap_or("").to_ascii_lowercase();
    let words: Vec<_> = text.split(|c: char| !c.is_ascii_alphanumeric()).filter(|s| !s.is_empty()).collect();
    let device_type = if words.contains(&"firewall") {
        "firewall"
    } else if words.contains(&"wireless") || words.contains(&"wifi") {
        "wireless"
    } else if words.contains(&"switch") {
        "switch"
    } else if words.contains(&"router") {
        "router"
    } else if words.contains(&"server") {
        "server"
    } else {
        "network_device"
    }
    .into();
    // No model is claimed without a maintained mapping for an exact product OID.
    Ok(Classification { vendor, device_type, model: None })
}
