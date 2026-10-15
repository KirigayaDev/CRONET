use crate::{
    classify::classify,
    cmdb::{CmdbSink, SinkError},
    error::{Error, Result},
    probe::{ProbeError, ReachabilityProbe, SnmpProbe},
};
use futures::{stream, StreamExt};
use ipnet::IpNet;
use serde::Serialize;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const DEFAULT_MAX_HOSTS: u64 = 4096;
pub const ABSOLUTE_MAX_HOSTS: u64 = 65536;
pub struct ScanPlan {
    cidr: IpNet,
    start: u128,
    count: u64,
    concurrency: usize,
    timeout: Duration,
    icmp: bool,
    port: u16,
}
impl ScanPlan {
    pub fn new(
        cidr: &str,
        max_hosts: u64,
        concurrency: usize,
        timeout: Duration,
        icmp: bool,
        port: u16,
    ) -> Result<Self> {
        if !(1..=ABSOLUTE_MAX_HOSTS).contains(&max_hosts)
            || !(1..=512).contains(&concurrency)
            || timeout.is_zero()
            || timeout > Duration::from_secs(30)
            || port == 0
        {
            return Err(Error::Config);
        }
        let cidr: IpNet = cidr.parse().map_err(|_| Error::Config)?;
        let cidr = cidr.trunc();
        let bits = if cidr.addr().is_ipv4() { 32 } else { 128 } - u32::from(cidr.prefix_len());
        // Reject giant IPv6/IPv4 prefixes before shifts, iteration or allocation.
        if bits > 16 {
            return Err(Error::TooManyHosts);
        }
        let total = 1_u64 << bits;
        let skip = cidr.addr().is_ipv4() && cidr.prefix_len() < 31;
        let count = if skip { total - 2 } else { total };
        if count > max_hosts {
            return Err(Error::TooManyHosts);
        }
        let start = match cidr.network() {
            IpAddr::V4(ip) => u128::from(u32::from(ip)),
            IpAddr::V6(ip) => u128::from(ip),
        } + u128::from(skip);
        Ok(Self { cidr, start, count, concurrency, timeout, icmp, port })
    }
    pub fn targets(&self) -> impl Iterator<Item = IpAddr> + '_ {
        (0..self.count).map(|offset| {
            if self.cidr.addr().is_ipv4() {
                IpAddr::V4(Ipv4Addr::from((self.start + u128::from(offset)) as u32))
            } else {
                IpAddr::V6(Ipv6Addr::from(self.start + u128::from(offset)))
            }
        })
    }
    pub fn host_count(&self) -> u64 {
        self.count
    }
    pub fn is_ipv6(&self) -> bool {
        self.cidr.addr().is_ipv6()
    }
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStatus {
    Created,
    Updated,
    Timeout,
    AuthFailure,
    NoSnmp,
    InvalidIdentity,
    CmdbRejected,
    OtherFailure,
}
#[derive(Debug, Serialize)]
pub struct DiscoveryResult {
    pub target_ip: IpAddr,
    pub icmp_reachable: Option<bool>,
    pub snmp_reachable: bool,
    pub sys_object_id: Option<String>,
    pub sys_name: Option<String>,
    pub sys_descr: Option<String>,
    pub vendor: Option<String>,
    pub device_type: Option<String>,
    pub model: Option<String>,
    pub cmdb_device_id: Option<Uuid>,
    pub skipped_fields: Vec<String>,
    pub status: DiscoveryStatus,
    pub error_code: Option<&'static str>,
}
impl DiscoveryResult {
    fn empty(ip: IpAddr) -> Self {
        Self {
            target_ip: ip,
            icmp_reachable: None,
            snmp_reachable: false,
            sys_object_id: None,
            sys_name: None,
            sys_descr: None,
            vendor: None,
            device_type: None,
            model: None,
            cmdb_device_id: None,
            skipped_fields: Vec::new(),
            status: DiscoveryStatus::NoSnmp,
            error_code: None,
        }
    }
    fn failed(mut self, error: ProbeError) -> Self {
        self.snmp_reachable = matches!(error, ProbeError::InvalidIdentity | ProbeError::Protocol | ProbeError::Auth);
        self.status = match error {
            ProbeError::Timeout => DiscoveryStatus::Timeout,
            ProbeError::Auth => DiscoveryStatus::AuthFailure,
            ProbeError::NoSnmp => DiscoveryStatus::NoSnmp,
            ProbeError::InvalidIdentity => DiscoveryStatus::InvalidIdentity,
            ProbeError::Protocol => DiscoveryStatus::OtherFailure,
        };
        self.error_code = Some(error.code());
        self
    }
}
#[derive(Debug, Serialize, Default)]
pub struct ScanSummary {
    pub cidr: String,
    pub hosts_considered: u64,
    pub snmp_responders: u64,
    pub persisted: u64,
    pub updated: u64,
    pub timeouts: u64,
    pub auth_failures: u64,
    pub other_failures: u64,
    pub duration_ms: u64,
}
#[derive(Debug, Serialize)]
pub struct ScanReport {
    pub summary: ScanSummary,
    pub results: Vec<DiscoveryResult>,
}
pub async fn scan(
    plan: &ScanPlan,
    snmp: &dyn SnmpProbe,
    icmp: &dyn ReachabilityProbe,
    cmdb: &dyn CmdbSink,
) -> Result<ScanReport> {
    tokio::time::timeout(Duration::from_secs(5), cmdb.check_ready())
        .await
        .map_err(|_| Error::CmdbUnavailable)?
        .map_err(|e| e.fatal().unwrap_or(Error::CmdbUnavailable))?;
    let started = Instant::now();
    let mut pending = stream::iter(plan.targets())
        .map(|ip| async move {
            let mut result = DiscoveryResult::empty(ip);
            if plan.icmp {
                let hint_timeout = plan.timeout.min(Duration::from_millis(300));
                result.icmp_reachable = match tokio::time::timeout(hint_timeout, icmp.reachable(ip, hint_timeout)).await
                {
                    Ok(Ok(reachable)) => Some(reachable),
                    Ok(Err(_)) => None,
                    Err(_) => Some(false),
                };
            }
            let identity = match tokio::time::timeout(plan.timeout, snmp.probe(SocketAddr::new(ip, plan.port))).await {
                Err(_) => return Ok(result.failed(ProbeError::Timeout)),
                Ok(Err(error)) => return Ok(result.failed(error)),
                Ok(Ok(identity)) => identity,
            };
            result.snmp_reachable = true;
            let classification = match classify(&identity.sys_object_id, identity.sys_descr.as_deref()) {
                Ok(value) => value,
                Err(error) => return Ok(result.failed(error)),
            };
            result.sys_object_id = Some(identity.sys_object_id.clone());
            result.sys_name = identity.sys_name.clone();
            result.sys_descr = identity.sys_descr.clone();
            result.vendor = classification.vendor.clone();
            result.device_type = Some(classification.device_type.clone());
            result.model = classification.model.clone();
            let applied = tokio::time::timeout(Duration::from_secs(5), cmdb.apply(ip, &identity, &classification))
                .await
                .unwrap_or(Err(SinkError::Unavailable));
            match applied {
                Ok(applied) => {
                    result.cmdb_device_id = Some(applied.device_id);
                    result.skipped_fields = applied.skipped_fields;
                    result.status = if applied.created { DiscoveryStatus::Created } else { DiscoveryStatus::Updated };
                }
                Err(error) => {
                    if let Some(fatal) = error.fatal() {
                        return Err(fatal);
                    }
                    result.status = DiscoveryStatus::CmdbRejected;
                    result.error_code = Some("cmdb_rejected");
                }
            }
            Ok(result)
        })
        .buffer_unordered(plan.concurrency);
    let mut results = Vec::with_capacity(plan.count as usize);
    while let Some(result) = pending.next().await {
        results.push(result?);
    }
    results.sort_by_key(|r| r.target_ip);
    let mut summary = ScanSummary {
        cidr: plan.cidr.to_string(),
        hosts_considered: plan.count,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        ..Default::default()
    };
    for result in &results {
        summary.snmp_responders += u64::from(result.snmp_reachable);
        match result.status {
            DiscoveryStatus::Created => summary.persisted += 1,
            DiscoveryStatus::Updated => summary.updated += 1,
            DiscoveryStatus::Timeout => summary.timeouts += 1,
            DiscoveryStatus::AuthFailure => summary.auth_failures += 1,
            _ => summary.other_failures += 1,
        }
    }
    Ok(ScanReport { summary, results })
}
