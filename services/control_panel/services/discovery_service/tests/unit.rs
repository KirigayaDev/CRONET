#![cfg(test)]
use discovery_service::{
    classify::{classify, Classification},
    cmdb::{Applied, CmdbSink, SinkError},
    config::{Cli, SnmpVersion},
    error::Error,
    probe::{self, HintUnavailable, Identity, ProbeError, ReachabilityProbe, Snmp2Probe, SnmpProbe},
    scan::{self, DiscoveryStatus, ScanPlan},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;

fn plan(cidr: &str) -> ScanPlan {
    ScanPlan::new(cidr, 4096, 3, Duration::from_millis(30), false, 161).unwrap()
}
#[test]
fn ipv4_host_32() {
    let p = plan("192.0.2.1/32");
    assert_eq!(p.targets().collect::<Vec<_>>(), vec!["192.0.2.1".parse::<IpAddr>().unwrap()]);
}
#[test]
fn ipv4_point_to_point_31() {
    let p = plan("192.0.2.0/31");
    assert_eq!(p.host_count(), 2);
    assert_eq!(p.targets().last().unwrap().to_string(), "192.0.2.1");
}
#[test]
fn ipv4_24_excludes_network_and_broadcast() {
    let p = plan("192.0.2.17/24");
    assert_eq!(p.host_count(), 254);
    assert_eq!(p.targets().next().unwrap().to_string(), "192.0.2.1");
    assert_eq!(p.targets().last().unwrap().to_string(), "192.0.2.254");
}
#[test]
fn ipv6_small_network_includes_literal_addresses() {
    let p = plan("2001:db8::/126");
    assert!(p.is_ipv6());
    assert_eq!(p.host_count(), 4);
    assert_eq!(p.targets().last().unwrap().to_string(), "2001:db8::3");
}
#[test]
fn giant_networks_rejected_without_enumeration() {
    for cidr in ["0.0.0.0/0", "10.0.0.0/8", "::/0", "2001:db8::/64"] {
        assert!(matches!(ScanPlan::new(cidr, 65536, 64, Duration::from_secs(1), false, 161), Err(Error::TooManyHosts)));
    }
}
#[test]
fn configured_and_absolute_host_limits() {
    assert!(ScanPlan::new("10.0.0.0/24", 10, 1, Duration::from_secs(1), false, 161).is_err());
    assert_eq!(ScanPlan::new("::/112", 65536, 1, Duration::from_secs(1), false, 161).unwrap().host_count(), 65536);
    for max in [0, 65537] {
        assert!(ScanPlan::new("::1/128", max, 1, Duration::from_secs(1), false, 161).is_err());
    }
}
#[test]
fn invalid_bounds_and_dns_targets_rejected() {
    for n in [0, 513] {
        assert!(ScanPlan::new("127.0.0.1/32", 4096, n, Duration::from_secs(1), false, 161).is_err());
    }
    for t in [Duration::ZERO, Duration::from_secs(31)] {
        assert!(ScanPlan::new("::1/128", 4096, 1, t, false, 161).is_err());
    }
    assert!(ScanPlan::new("router.example/32", 4096, 1, Duration::from_secs(1), false, 161).is_err());
}
macro_rules! vendor_test {
    ($name:ident,$enterprise:expr,$vendor:expr) => {
        #[test]
        fn $name() {
            let c = classify(&format!("1.3.6.1.4.1.{}.1", $enterprise), None).unwrap();
            assert_eq!(c.vendor.as_deref(), Some($vendor));
            assert!(c.model.is_none());
        }
    };
}
vendor_test!(cisco, 9, "Cisco");
vendor_test!(hpe, 11, "HPE");
vendor_test!(huawei, 2011, "Huawei");
vendor_test!(juniper, 2636, "Juniper");
vendor_test!(mikrotik, 14988, "MikroTik");
vendor_test!(fortinet, 12356, "Fortinet");
vendor_test!(arista, 30065, "Arista");
vendor_test!(ubiquiti, 41112, "Ubiquiti");
#[test]
fn unknown_enterprise_not_guessed_from_description() {
    let c = classify("1.3.6.1.4.1.90.1", Some("Cisco router")).unwrap();
    assert!(c.vendor.is_none() && c.model.is_none());
    assert_eq!(c.device_type, "router");
}
#[test]
fn device_types_conservative_word_matching() {
    for (text, kind) in [
        ("Ethernet switch", "switch"),
        ("Firewall appliance", "firewall"),
        ("WiFi wireless", "wireless"),
        ("Compute server", "server"),
        ("Router", "router"),
        ("RouterOS", "network_device"),
        ("unknown", "network_device"),
    ] {
        assert_eq!(classify("1.3.6.1.4.1.9.1", Some(text)).unwrap().device_type, kind);
    }
}
#[test]
fn malformed_and_missing_object_ids_rejected() {
    for oid in ["", "1.3.x", "1.3.6.1.4.1.9evil"] {
        assert!(classify(oid, None).is_err());
    }
    assert!(probe::parse_identity(std::iter::empty()).is_err());
}
#[test]
fn snmp_parser_preserves_typed_oid_and_optional_text() {
    use snmp2::Value;
    let oid = snmp_types::parse_oid;
    let input = vec![
        (oid(probe::SYS_OBJECT_ID).unwrap(), Value::ObjectIdentifier(oid("1.3.6.1.4.1.9.1.1").unwrap())),
        (oid(probe::SYS_NAME).unwrap(), Value::OctetString(b" router-a ")),
        (oid(probe::SYS_DESCR).unwrap(), Value::OctetString(b"Cisco router")),
    ];
    let i = probe::parse_identity(input.into_iter()).unwrap();
    assert_eq!(i.sys_name.as_deref(), Some("router-a"));
    assert_eq!(i.sys_object_id, "1.3.6.1.4.1.9.1.1");
}
fn credential(json: &[u8]) -> Arc<creds::Credential> {
    Arc::new(creds::Credential::from_json(json).unwrap())
}
#[test]
fn credential_type_and_no_auth_policy_match_poller() {
    let c = credential(br#"{"version":"v3","username":"user"}"#);
    assert!(Snmp2Probe::new(c.clone(), SnmpVersion::V3, false).is_err());
    assert!(Snmp2Probe::new(c, SnmpVersion::V3, true).is_ok());
    let c = credential(br#"{"version":"basic","username":"user","password":"synthetic"}"#);
    assert!(Snmp2Probe::new(c, SnmpVersion::V2c, true).is_err());
}
#[test]
fn legacy_crypto_requires_explicit_policy_opt_in() {
    for auth in ["md5", "sha1"] {
        let json = format!(
            r#"{{"version":"v3","username":"user","auth_protocol":"{auth}","auth_password":"synthetic-auth"}}"#
        );
        let c = credential(json.as_bytes());
        assert!(Snmp2Probe::new(c.clone(), SnmpVersion::V3, false).is_err());
        assert!(Snmp2Probe::new(c, SnmpVersion::V3, true).is_ok());
    }
}
#[test]
fn modern_v3_auth_priv_allowed() {
    let c=credential(br#"{"version":"v3","username":"user","auth_protocol":"sha256","auth_password":"synthetic-auth","priv_protocol":"aes128","priv_password":"synthetic-priv"}"#);
    assert!(Snmp2Probe::new(c, SnmpVersion::V3, false).is_ok());
}
#[test]
fn secrets_do_not_appear_in_errors_or_probe_debug() {
    let secret = "D3_SYNTHETIC_SENTINEL";
    let c = credential(format!(r#"{{"version":"v2c","community":"{secret}"}}"#).as_bytes());
    let probe = Snmp2Probe::new(c, SnmpVersion::V2c, false).unwrap();
    assert!(!format!("{probe:?} {:?} {}", ProbeError::Auth, Error::Credential).contains(secret));
}
#[test]
fn cli_never_accepts_snmp_v1() {
    use clap::Parser;
    assert!(Cli::try_parse_from([
        "discovery-service",
        "scan",
        "--cidr",
        "127.0.0.1/32",
        "--credential-ref",
        "env:TEST",
        "--snmp-version",
        "v1"
    ])
    .is_err());
}

#[derive(Clone, Copy)]
enum Behavior {
    Ok,
    Auth,
    Hang,
    Malformed,
}
struct FakeSnmp {
    behavior: Behavior,
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
impl FakeSnmp {
    fn new(behavior: Behavior) -> Self {
        Self { behavior, calls: AtomicUsize::new(0), active: AtomicUsize::new(0), peak: AtomicUsize::new(0) }
    }
}
struct Lease<'a>(&'a AtomicUsize);
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl SnmpProbe for FakeSnmp {
    async fn probe(&self, _: SocketAddr) -> Result<Identity, ProbeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _lease = Lease(&self.active);
        self.peak.fetch_max(n, Ordering::SeqCst);
        tokio::time::sleep(if matches!(self.behavior, Behavior::Hang) {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(5)
        })
        .await;
        if matches!(self.behavior, Behavior::Auth) {
            return Err(ProbeError::Auth);
        }
        Ok(Identity {
            sys_object_id: if matches!(self.behavior, Behavior::Malformed) { "bad" } else { "1.3.6.1.4.1.9.1.1" }
                .into(),
            sys_name: Some("auto-router".into()),
            sys_descr: Some("Cisco router".into()),
        })
    }
}
enum Hint {
    Success,
    Timeout,
    Permission,
}
#[async_trait::async_trait]
impl ReachabilityProbe for Hint {
    async fn reachable(&self, _: IpAddr, _: Duration) -> Result<bool, HintUnavailable> {
        match self {
            Self::Success => Ok(true),
            Self::Permission => Err(HintUnavailable),
            Self::Timeout => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(false)
            }
        }
    }
}
struct FakeSink {
    calls: AtomicUsize,
    error: Option<SinkError>,
    ready: Option<SinkError>,
}
impl FakeSink {
    fn new() -> Self {
        Self { calls: AtomicUsize::new(0), error: None, ready: None }
    }
}
#[async_trait::async_trait]
impl CmdbSink for FakeSink {
    async fn check_ready(&self) -> Result<(), SinkError> {
        self.ready.map_or(Ok(()), Err)
    }
    async fn apply(&self, _: IpAddr, _: &Identity, _: &Classification) -> Result<Applied, SinkError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok(Applied { device_id: Uuid::new_v4(), created: true, skipped_fields: Vec::new() })
    }
}
#[tokio::test]
async fn concurrency_is_bounded_and_all_targets_considered() {
    let p = plan("192.0.2.0/28");
    let snmp = FakeSnmp::new(Behavior::Ok);
    let sink = FakeSink::new();
    let r = scan::scan(&p, &snmp, &Hint::Success, &sink).await.unwrap();
    assert_eq!(r.summary.hosts_considered, 14);
    assert_eq!(r.summary.persisted, 14);
    assert_eq!(snmp.calls.load(Ordering::SeqCst), 14);
    assert_eq!(snmp.peak.load(Ordering::SeqCst), 3);
    assert_eq!(snmp.active.load(Ordering::SeqCst), 0);
}
async fn hint_case(hint: Hint, expected: Option<bool>) {
    let p = ScanPlan::new("127.0.0.1/32", 1, 1, Duration::from_millis(30), true, 161).unwrap();
    let snmp = FakeSnmp::new(Behavior::Ok);
    let sink = FakeSink::new();
    let r = scan::scan(&p, &snmp, &hint, &sink).await.unwrap();
    assert_eq!(r.results[0].icmp_reachable, expected);
    assert_eq!(r.summary.persisted, 1);
    assert_eq!(snmp.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn icmp_success_still_uses_snmp() {
    hint_case(Hint::Success, Some(true)).await;
}
#[tokio::test]
async fn icmp_timeout_still_uses_snmp() {
    hint_case(Hint::Timeout, Some(false)).await;
}
#[tokio::test]
async fn icmp_permission_failure_still_uses_snmp() {
    hint_case(Hint::Permission, None).await;
}
#[tokio::test]
async fn snmp_timeout_has_no_canonical_write_and_is_nonfatal() {
    let s = FakeSnmp::new(Behavior::Hang);
    let sink = FakeSink::new();
    let r = scan::scan(&plan("127.0.0.1/32"), &s, &Hint::Success, &sink).await.unwrap();
    assert_eq!(r.summary.timeouts, 1);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
    assert_eq!(s.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn snmp_auth_failure_has_no_write_and_is_nonfatal() {
    let sink = FakeSink::new();
    let r = scan::scan(&plan("127.0.0.1/32"), &FakeSnmp::new(Behavior::Auth), &Hint::Success, &sink).await.unwrap();
    assert_eq!(r.summary.auth_failures, 1);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn malformed_identity_does_not_reach_cmdb() {
    let sink = FakeSink::new();
    let r =
        scan::scan(&plan("127.0.0.1/32"), &FakeSnmp::new(Behavior::Malformed), &Hint::Success, &sink).await.unwrap();
    assert_eq!(r.results[0].status, DiscoveryStatus::InvalidIdentity);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn per_host_cmdb_rejection_does_not_cancel_subnet() {
    let mut sink = FakeSink::new();
    sink.error = Some(SinkError::Rejected);
    let r = scan::scan(&plan("192.0.2.0/30"), &FakeSnmp::new(Behavior::Ok), &Hint::Success, &sink).await.unwrap();
    assert_eq!(r.summary.other_failures, 2);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn globally_missing_cmdb_or_auth_fails_before_probing() {
    for e in [SinkError::Unavailable, SinkError::Unauthorized, SinkError::Forbidden] {
        let mut sink = FakeSink::new();
        sink.ready = Some(e);
        let snmp = FakeSnmp::new(Behavior::Ok);
        assert!(scan::scan(&plan("127.0.0.1/32"), &snmp, &Hint::Success, &sink).await.is_err());
        assert_eq!(snmp.calls.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn binary_missing_token_fails_before_network_without_leaking_secrets() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_discovery-service"))
        .env_clear()
        .args(["scan", "--cidr", "127.0.0.1/32", "--credential-ref", "env:TEST", "--snmp-version", "v2c"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8(out.stderr).unwrap().contains("CMDB authentication"));
}
