//! Обработка одной датаграммы: чистая функция без сети и NATS.
//! Порядок проверок важен: дешёвые (сеть, размер, частота) идут до разбора.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant, SystemTime},
};

use contracts::netmon::v1::RawTrap;
use ipnet::IpNet;
use secrecy::{ExposeSecret, SecretSlice};
use snmp_types::{build_inform_response, decode_trap, DecodeError};

/// Максимальный размер датаграммы. Реальные трапы - сотни байт.
pub const MAX_DATAGRAM: usize = 8 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum DropReason {
    NotAllowedSource,
    RateLimited,
    TooLarge,
    Malformed,
    UnsupportedVersion,
    NotATrap,
    BadCommunity,
}

pub enum Outcome {
    Drop(DropReason),
    Accept { trap: Box<RawTrap>, reply: Option<Vec<u8>> },
}

/// Простое ограничение частоты по источнику: окно в одну секунду.
pub struct RateLimiter {
    limit: u32,
    windows: HashMap<IpAddr, (Instant, u32)>,
}

impl RateLimiter {
    pub fn new(limit_per_sec: u32) -> Self {
        Self { limit: limit_per_sec, windows: HashMap::new() }
    }

    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        // таблица не должна расти бесконтрольно при подмене адресов
        if self.windows.len() > 100_000 {
            self.windows.retain(|_, (t, _)| now.duration_since(*t) < Duration::from_secs(1));
            if self.windows.len() > 100_000 {
                return false;
            }
        }
        let e = self.windows.entry(ip).or_insert((now, 0));
        if now.duration_since(e.0) >= Duration::from_secs(1) {
            *e = (now, 0);
        }
        e.1 += 1;
        e.1 <= self.limit
    }
}

pub struct Filter {
    pub allowed_sources: Vec<IpNet>,
    pub communities: Vec<SecretSlice<u8>>,
    pub collector_id: String,
}

pub fn process(buf: &[u8], src: SocketAddr, now: SystemTime, filter: &Filter, limiter: &mut RateLimiter) -> Outcome {
    if !filter.allowed_sources.is_empty() && !filter.allowed_sources.iter().any(|n| n.contains(&src.ip())) {
        return Outcome::Drop(DropReason::NotAllowedSource);
    }
    if !limiter.allow(src.ip(), Instant::now()) {
        return Outcome::Drop(DropReason::RateLimited);
    }
    if buf.len() >= MAX_DATAGRAM {
        return Outcome::Drop(DropReason::TooLarge);
    }
    let decoded = match decode_trap(buf) {
        Ok(d) => d,
        Err(DecodeError::UnsupportedVersion) => return Outcome::Drop(DropReason::UnsupportedVersion),
        Err(DecodeError::NotATrap) => return Outcome::Drop(DropReason::NotATrap),
        Err(_) => return Outcome::Drop(DropReason::Malformed),
    };
    // проверяем все community без раннего выхода, чтобы время не выдавало, какая подошла
    let mut ok = false;
    for c in &filter.communities {
        ok |= decoded.community_matches(c.expose_secret());
    }
    if !ok {
        return Outcome::Drop(DropReason::BadCommunity);
    }

    let reply = if decoded.inform { build_inform_response(buf).ok() } else { None };
    let d = now.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let trap = RawTrap {
        trap_id: uuid::Uuid::new_v4().to_string(),
        collector_id: filter.collector_id.clone(),
        source_ip: src.ip().to_string(),
        source_port: u32::from(src.port()),
        received_at: Some(prost_types::Timestamp {
            seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nanos: d.subsec_nanos() as i32,
        }),
        snmp_version: decoded.version as i32,
        inform: decoded.inform,
        varbinds: decoded.varbinds,
        v1: decoded.v1,
    };
    Outcome::Accept { trap: Box::new(trap), reply }
}

#[cfg(test)]
mod tests {
    use super::*;

    // v2c SNMPv2-Trap (linkDown), community "public" / "wrong"
    const PUBLIC: &str = "305202010104067075626c6963a74502012a020100020100303a300e06082b06010201010300430201003017060a2b06010603010104010006092b0601060301010503300f060a2b06010201020201010c02010c";
    const WRONG: &str = "3051020101040577726f6e67a74502012a020100020100303a300e06082b06010201010300430201003017060a2b06010603010104010006092b0601060301010503300f060a2b06010201020201010c02010c";

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    fn filter() -> Filter {
        Filter {
            allowed_sources: vec!["10.0.0.0/8".parse().unwrap()],
            communities: vec![SecretSlice::from(b"public".to_vec())],
            collector_id: "t".into(),
        }
    }

    fn src(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn run(buf: &[u8], from: &str) -> Outcome {
        process(buf, src(from), SystemTime::now(), &filter(), &mut RateLimiter::new(100))
    }

    #[test]
    fn accepts_valid_trap_without_leaking_community() {
        match run(&hex(PUBLIC), "10.1.1.1:40000") {
            Outcome::Accept { trap, reply } => {
                assert_eq!(trap.source_ip, "10.1.1.1");
                assert_eq!(trap.varbinds.len(), 3);
                assert!(reply.is_none());
                assert!(!format!("{trap:?}").contains("public"));
            }
            Outcome::Drop(r) => panic!("{r:?}"),
        }
    }

    #[test]
    fn drops_in_the_right_order() {
        assert!(matches!(run(&hex(PUBLIC), "192.168.0.5:1"), Outcome::Drop(DropReason::NotAllowedSource)));
        assert!(matches!(run(&hex(WRONG), "10.1.1.1:1"), Outcome::Drop(DropReason::BadCommunity)));
        assert!(matches!(run(&[0xff; 40], "10.1.1.1:1"), Outcome::Drop(DropReason::Malformed)));
        assert!(matches!(run(&vec![0u8; MAX_DATAGRAM], "10.1.1.1:1"), Outcome::Drop(DropReason::TooLarge)));
    }

    #[test]
    fn rate_limits_per_source() {
        let mut lim = RateLimiter::new(3);
        let f = filter();
        let pkt = hex(PUBLIC);
        let mut accepted = 0;
        for _ in 0..10 {
            if matches!(process(&pkt, src("10.1.1.1:1"), SystemTime::now(), &f, &mut lim), Outcome::Accept { .. }) {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 3);
        // другой источник не страдает
        assert!(matches!(process(&pkt, src("10.1.1.2:1"), SystemTime::now(), &f, &mut lim), Outcome::Accept { .. }));
    }
}
