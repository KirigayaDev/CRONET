use contracts::netmon::v1::{SnmpVersion, V1TrapInfo, VarBind};
use secrecy::{ExposeSecret, SecretSlice};
use snmp2::{MessageType, Pdu, Version};

use crate::convert::{oid_to_string, value_to_proto};

/// Ограничение на число varbind'ов в одном трапе: защита от «раздутых» пакетов.
pub const MAX_TRAP_VARBINDS: usize = 256;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("пустая датаграмма")]
    Empty,
    #[error("не удалось разобрать SNMP PDU")]
    Malformed,
    #[error("SNMPv3 в этой версии приёмника не поддерживается")]
    UnsupportedVersion,
    #[error("PDU не является трапом или inform")]
    NotATrap,
    #[error("слишком много varbind'ов (максимум {MAX_TRAP_VARBINDS})")]
    TooManyVarbinds,
}

/// Разобранный трап. Community хранится в `SecretSlice` (не попадает в `Debug`).
pub struct DecodedTrap {
    pub version: SnmpVersion,
    pub inform: bool,
    pub request_id: i32,
    pub community: SecretSlice<u8>,
    pub varbinds: Vec<VarBind>,
    pub v1: Option<V1TrapInfo>,
}

impl std::fmt::Debug for DecodedTrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedTrap")
            .field("version", &self.version)
            .field("inform", &self.inform)
            .field("request_id", &self.request_id)
            .field("community", &"<redacted>")
            .field("varbinds", &self.varbinds.len())
            .finish()
    }
}

impl DecodedTrap {
    /// Сравнение community за постоянное время (не зависит от места расхождения).
    pub fn community_matches(&self, candidate: &[u8]) -> bool {
        let own = self.community.expose_secret();
        let mut diff = own.len() ^ candidate.len();
        for i in 0..own.len().max(candidate.len()) {
            let a = own.get(i).copied().unwrap_or(0);
            let b = candidate.get(i).copied().unwrap_or(0);
            diff |= usize::from(a ^ b);
        }
        diff == 0
    }
}

/// Разбор датаграммы, пришедшей из сети. Никогда не паникует.
pub fn decode_trap(bytes: &[u8]) -> Result<DecodedTrap, DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::Empty);
    }
    let pdu = Pdu::from_bytes(bytes).map_err(|e| match e {
        snmp2::Error::UnsupportedVersion | snmp2::Error::AuthFailure(_) => DecodeError::UnsupportedVersion,
        _ => DecodeError::Malformed,
    })?;

    let version = match pdu.version() {
        Ok(Version::V2C) => SnmpVersion::V2c,
        // v1-трап передаётся как есть (с блоком `v1`), в v2c не преобразуется
        Ok(Version::V1) => SnmpVersion::V1,
        _ => return Err(DecodeError::UnsupportedVersion),
    };
    let inform = match pdu.message_type {
        MessageType::Trap | MessageType::TrapV1 => false,
        MessageType::InformRequest => true,
        _ => return Err(DecodeError::NotATrap),
    };

    let mut varbinds = Vec::new();
    for (oid, value) in pdu.varbinds.clone() {
        if varbinds.len() >= MAX_TRAP_VARBINDS {
            return Err(DecodeError::TooManyVarbinds);
        }
        varbinds.push(VarBind { oid: oid_to_string(&oid), value: Some(value_to_proto(&value)) });
    }

    let v1 = pdu.v1_trap_info.as_ref().map(|t| V1TrapInfo {
        enterprise: oid_to_string(&t.enterprise),
        agent_addr: t.agent_addr.to_string(),
        generic_trap: t.generic_trap,
        specific_trap: t.specific_trap,
        timestamp: t.timestamp,
    });

    Ok(DecodedTrap {
        version,
        inform,
        request_id: pdu.req_id,
        community: SecretSlice::from(pdu.community.to_vec()),
        varbinds,
        v1,
    })
}

/// Ответ на InformRequest: тот же PDU с типом Response и error-status 0.
pub fn build_inform_response(datagram: &[u8]) -> Result<Vec<u8>, DecodeError> {
    let mut pdu = Pdu::from_bytes(datagram).map_err(|_| DecodeError::Malformed)?;
    if pdu.message_type != MessageType::InformRequest {
        return Err(DecodeError::NotATrap);
    }
    pdu.message_type = MessageType::Response;
    pdu.error_status = 0;
    pdu.error_index = 0;
    pdu.to_bytes().map_err(|_| DecodeError::Malformed)
}

#[cfg(test)]
pub(crate) mod testutil {
    //! Минимальный BER-кодировщик для сборки тестовых датаграмм.

    pub fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        let n = content.len();
        if n < 128 {
            out.push(n as u8);
        } else if n < 256 {
            out.extend([0x81, n as u8]);
        } else {
            out.extend([0x82, (n >> 8) as u8, (n & 0xff) as u8]);
        }
        out.extend_from_slice(content);
        out
    }

    pub fn int(v: i64) -> Vec<u8> {
        let mut bytes = v.to_be_bytes().to_vec();
        while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0))
        {
            bytes.remove(0);
        }
        tlv(0x02, &bytes)
    }

    pub fn oid(dotted: &str) -> Vec<u8> {
        let arcs: Vec<u64> = dotted.split('.').map(|a| a.parse().unwrap()).collect();
        let mut body = vec![(arcs[0] * 40 + arcs[1]) as u8];
        for &a in &arcs[2..] {
            let mut tmp = vec![(a & 0x7f) as u8];
            let mut a = a >> 7;
            while a > 0 {
                tmp.push(((a & 0x7f) as u8) | 0x80);
                a >>= 7;
            }
            tmp.reverse();
            body.extend(tmp);
        }
        tlv(0x06, &body)
    }

    pub fn varbind(oid_s: &str, value: Vec<u8>) -> Vec<u8> {
        let mut c = oid(oid_s);
        c.extend(value);
        tlv(0x30, &c)
    }

    /// SNMPv2c: tag 0xA7 = SNMPv2-Trap, 0xA6 = InformRequest.
    pub fn v2c_message(community: &[u8], pdu_tag: u8, req_id: i64, varbinds: &[Vec<u8>]) -> Vec<u8> {
        let vb = tlv(0x30, &varbinds.concat());
        let mut pdu = int(req_id);
        pdu.extend(int(0));
        pdu.extend(int(0));
        pdu.extend(vb);
        let mut msg = int(1); // version: v2c
        msg.extend(tlv(0x04, community));
        msg.extend(tlv(pdu_tag, &pdu));
        tlv(0x30, &msg)
    }

    pub fn sample_link_down(community: &[u8]) -> Vec<u8> {
        v2c_message(
            community,
            0xA7,
            42,
            &[
                varbind("1.3.6.1.2.1.1.3.0", tlv(0x43, &[0x00, 0x98, 0x96, 0x80])), // sysUpTime
                varbind("1.3.6.1.6.3.1.1.4.1.0", oid("1.3.6.1.6.3.1.1.5.3")),       // linkDown
                varbind("1.3.6.1.2.1.2.2.1.1.12", int(12)),                         // ifIndex
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;
    use contracts::netmon::v1::snmp_value::Kind;
    use proptest::prelude::*;

    #[test]
    fn decodes_v2c_trap() {
        let t = decode_trap(&sample_link_down(b"public")).unwrap();
        assert_eq!(t.version, SnmpVersion::V2c);
        assert!(!t.inform);
        assert_eq!(t.request_id, 42);
        assert_eq!(t.varbinds.len(), 3);
        assert_eq!(t.varbinds[0].oid, "1.3.6.1.2.1.1.3.0");
        assert_eq!(t.varbinds[0].value.as_ref().unwrap().kind, Some(Kind::Timeticks(10_000_000)));
        assert_eq!(t.varbinds[1].value.as_ref().unwrap().kind, Some(Kind::ObjectId("1.3.6.1.6.3.1.1.5.3".into())));
        assert!(t.community_matches(b"public"));
        assert!(!t.community_matches(b"publiC"));
        assert!(!t.community_matches(b"public2"));
        assert!(!t.community_matches(b""));
    }

    #[test]
    fn decodes_v1_trap() {
        // SNMPv1 Trap-PDU (0xA4): enterprise, agent-addr, generic, specific, time-stamp, varbinds
        let mut body = oid("1.3.6.1.4.1.9");
        body.extend(tlv(0x40, &[10, 0, 0, 7]));
        body.extend(int(2)); // linkDown
        body.extend(int(0));
        body.extend(tlv(0x43, &[0x01, 0x00]));
        body.extend(tlv(0x30, &varbind("1.3.6.1.2.1.2.2.1.1.3", int(3))));
        let mut msg = int(0); // version v1
        msg.extend(tlv(0x04, b"public"));
        msg.extend(tlv(0xA4, &body));
        let pkt = tlv(0x30, &msg);

        let t = decode_trap(&pkt).unwrap();
        assert_eq!(t.version, SnmpVersion::V1);
        let v1 = t.v1.expect("v1 info");
        assert_eq!(v1.enterprise, "1.3.6.1.4.1.9");
        assert_eq!(v1.agent_addr, "10.0.0.7");
        assert_eq!(v1.generic_trap, 2);
        assert_eq!(v1.timestamp, 256);
        assert_eq!(t.varbinds.len(), 1);
    }

    #[test]
    fn debug_does_not_leak_community() {
        let t = decode_trap(&sample_link_down(b"top-secret-community")).unwrap();
        let s = format!("{t:?}");
        assert!(!s.contains("top-secret"), "{s}");
    }

    #[test]
    fn inform_gets_response() {
        let inform = v2c_message(b"public", 0xA6, 7, &[varbind("1.3.6.1.2.1.1.3.0", tlv(0x43, &[1]))]);
        let t = decode_trap(&inform).unwrap();
        assert!(t.inform);
        let resp = build_inform_response(&inform).unwrap();
        let parsed = Pdu::from_bytes(&resp).unwrap();
        assert_eq!(parsed.message_type, MessageType::Response);
        assert_eq!(parsed.req_id, 7);
        assert_eq!(parsed.varbinds.count(), 1);
    }

    #[test]
    fn rejects_non_traps_and_junk() {
        assert_eq!(decode_trap(&[]).unwrap_err(), DecodeError::Empty);
        assert!(decode_trap(&[0xff; 32]).is_err());
        // GET-запрос (0xA0) - не трап
        let get = v2c_message(b"public", 0xA0, 1, &[varbind("1.3.6.1.2.1.1.1.0", tlv(0x05, &[]))]);
        assert_eq!(decode_trap(&get).unwrap_err(), DecodeError::NotATrap);
    }

    #[test]
    fn rejects_too_many_varbinds() {
        let many: Vec<_> = (0..=MAX_TRAP_VARBINDS).map(|i| varbind(&format!("1.3.6.1.4.1.{i}"), int(1))).collect();
        let msg = v2c_message(b"public", 0xA7, 1, &many);
        assert_eq!(decode_trap(&msg).unwrap_err(), DecodeError::TooManyVarbinds);
    }

    proptest! {
        // Любые байты: ни паники, ни зависания.
        #[test]
        fn random_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let _ = decode_trap(&data);
            let _ = build_inform_response(&data);
        }

        // Мутации корректного пакета: ломаем байты и обрезаем.
        #[test]
        fn mutated_valid_packet_never_panics(
            idx in 0usize..64, byte in any::<u8>(), cut in 0usize..64
        ) {
            let mut pkt = sample_link_down(b"public");
            let i = idx % pkt.len();
            pkt[i] = byte;
            pkt.truncate(pkt.len() - (cut % pkt.len()));
            let _ = decode_trap(&pkt);
        }
    }
}
