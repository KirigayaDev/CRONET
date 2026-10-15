#![cfg(test)]
//! Opt-in real UDP + full CMDB test; only the disposable compose.discovery-test.yml project.
use creds::Credential;
use discovery_service::{
    classify::classify,
    config::SnmpVersion,
    probe::{Identity, Snmp2Probe, SnmpProbe},
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use sqlx::{postgres::PgConnectOptions, PgPool};
use std::{
    net::TcpListener,
    process::{Child, Command, Output, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

fn enabled() -> bool {
    std::env::var("DISCOVERY_E2E").as_deref() == Ok("1")
}
async fn probe(credential: Credential, version: SnmpVersion, address: &str) -> Identity {
    let probe = Snmp2Probe::new(Arc::new(credential), version, false).unwrap();
    tokio::time::timeout(Duration::from_secs(3), probe.probe(address.parse().unwrap())).await.unwrap().unwrap()
}
fn community(value: &str) -> Credential {
    Credential::V2c { community: value.to_owned().into() }
}

#[tokio::test]
async fn real_udp_v2c_parser_and_two_vendors() {
    if !enabled() {
        return;
    }
    for (name, vendor, hostname) in
        [("public", "Cisco", "discovered-router"), ("juniper", "Juniper", "juniper-fixture")]
    {
        let identity = probe(community(name), SnmpVersion::V2c, "127.0.0.1:1161").await;
        let result = classify(&identity.sys_object_id, identity.sys_descr.as_deref()).unwrap();
        assert_eq!(result.vendor.as_deref(), Some(vendor));
        assert_eq!(result.device_type, "router");
        assert_eq!(identity.sys_name.as_deref(), Some(hostname));
        assert_eq!(result.model, None);
    }
}
#[tokio::test]
async fn real_udp_v3_auth_priv() {
    if !enabled() {
        return;
    }
    let credential = Credential::from_json(br#"{"version":"v3","username":"mon","context":"public","auth_protocol":"sha256","auth_password":"authpass123","priv_protocol":"aes128","priv_password":"privpass123"}"#).unwrap();
    let identity = probe(credential, SnmpVersion::V3, "127.0.0.1:1161").await;
    assert_eq!(
        classify(&identity.sys_object_id, identity.sys_descr.as_deref()).unwrap().vendor.as_deref(),
        Some("Cisco")
    );
}
#[tokio::test]
async fn real_udp_ipv6_literal() {
    if !enabled() {
        return;
    }
    let identity = probe(community("public"), SnmpVersion::V2c, "[::1]:1161").await;
    assert_eq!(identity.sys_name.as_deref(), Some("discovered-router"));
}

// Direct SQL is confined to synthetic auth-user setup and read-only acceptance assertions.
// Discovery production code writes canonical inventory exclusively through the gRPC RPC.
async fn test_pool() -> PgPool {
    let url = std::env::var("CMDB_TEST_DATABASE_URL").expect("Use the disposable D3 Compose project");
    assert_eq!(PgConnectOptions::from_str(&url).unwrap().get_database(), Some("cronet_cmdb_test_schema"));
    let pool = PgPool::connect(&url).await.unwrap();
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await.unwrap();
    assert_eq!(db, "cronet_cmdb_test_schema", "Refusing a non-test DB");
    let head: String = sqlx::query_scalar("SELECT version_num FROM alembic_version").fetch_one(&pool).await.unwrap();
    assert_eq!(head, "8b6e2f4a9c10");
    pool
}
async fn token(pool: &PgPool, key: &EncodingKey, admin: bool) -> (Uuid, String) {
    let (id, version): (Uuid, Uuid) = sqlx::query_as("INSERT INTO users (username,email,password_hash,display_name,is_admin,is_super_admin) VALUES ($1,$2,'test-only-hash','Discovery E2E',$3,false) RETURNING uuid,token_version_uuid")
        .bind(Uuid::new_v4().simple().to_string()).bind(format!("{}@example.invalid", Uuid::new_v4())).bind(admin).fetch_one(pool).await.unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    let claims = json!({"iss":"Re:simple_videohosting_auth","aud":"Re:simple_videohosting_auth","sub":id,"token_version":version,"jti":Uuid::new_v4(),"iat":now-1.0,"nbf":now-1.0,"exp":now+600.0,"token_type":"access"});
    (id, encode(&Header::new(Algorithm::RS256), &claims, key).unwrap())
}
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn run(endpoint: &str, token: &str, community: &str, port: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_discovery-service"))
        .env_clear()
        .env("CMDB_GRPC_URL", endpoint)
        .env("CMDB_BEARER_TOKEN", token)
        .env("NETMON_CRED_BACKEND", "env")
        .env("NETMON_CRED_FIXTURE", json!({"version":"v2c","community":community}).to_string())
        .args([
            "scan",
            "--cidr",
            "127.0.0.1/32",
            "--credential-ref",
            "env:FIXTURE",
            "--snmp-version",
            "v2c",
            "--snmp-port",
            port,
            "--timeout-ms",
            "300",
            "--json-results",
            "--icmp",
        ])
        .output()
        .unwrap()
}
fn report(output: &Output, token: &str) -> Value {
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token), "JWT leaked in stderr");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token), "JWT leaked in stdout");
    assert!(output.status.success(), "Discovery failed: {}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test]
async fn full_cli_snmp_cmdb_and_manual_override_acceptance() {
    if !enabled() {
        return;
    }
    let pool = test_pool().await;
    let dir = tempfile::tempdir().unwrap();
    let private = dir.path().join("private.pem");
    let public = dir.path().join("public.pem");
    assert!(Command::new("openssl")
        .args(["genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out"])
        .arg(&private)
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    assert!(Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&private)
        .arg("-out")
        .arg(&public)
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    let key = EncodingKey::from_rsa_pem(&std::fs::read(&private).unwrap()).unwrap();
    let (admin_id, admin) = token(&pool, &key, true).await;
    let (_, reader) = token(&pool, &key, false).await;
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc = TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    let grpc_addr = grpc.local_addr().unwrap();
    drop((http, grpc));
    let redis = std::env::var("CMDB_TEST_REDIS_URL").unwrap();
    assert_eq!(redis, "redis://redis:6379", "Refusing a non-test Redis");
    let mut server = Server(
        Command::new(std::env::var("CMDB_TEST_BINARY").unwrap())
            .env_clear()
            .env("DATABASE_URL", std::env::var("CMDB_TEST_DATABASE_URL").unwrap())
            .env("REDIS_URL", redis)
            .env("JWT_PUBLIC_KEY_PATH", public)
            .env("CMDB_HTTP_ADDR", http_addr.to_string())
            .env("CMDB_GRPC_ADDR", grpc_addr.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    let http = format!("http://{http_addr}");
    let endpoint = format!("http://{grpc_addr}");
    let mut ready = false;
    for _ in 0..50 {
        if client.get(format!("{http}/health/ready")).send().await.is_ok_and(|r| r.status().is_success()) {
            ready = true;
            break;
        }
        assert!(server.0.try_wait().unwrap().is_none(), "CMDB exited during startup");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "CMDB did not become ready");
    let first = report(&run(&endpoint, &admin, "public", "1161"), &admin);
    assert_eq!(first["summary"]["persisted"], 1);
    let id: Uuid = first["results"][0]["cmdb_device_id"].as_str().unwrap().parse().unwrap();
    let (hostname, rack, start, height): (Option<String>, Option<Uuid>, Option<i32>, Option<i32>) =
        sqlx::query_as("SELECT hostname,rack_id,start_unit,height_u FROM devices WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hostname.as_deref(), Some("discovered-router"));
    assert_eq!((rack, start, height), (None, None, None));
    let (iface, ip): (Uuid, Uuid) = sqlx::query_as("SELECT i.id,p.id FROM interfaces i JOIN ip_addresses p ON p.interface_id=i.id WHERE i.device_id=$1 AND i.name='discovery-management' AND i.interface_type='discovery/logical' AND host(p.address)='127.0.0.1' AND p.is_management=true").bind(id).fetch_one(&pool).await.unwrap();
    let ids = vec![id, iface, ip];
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE entity_id=ANY($1) AND source='discovery' AND action='create' AND actor_type='user' AND actor_id=$2").bind(&ids).bind(admin_id).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 3);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM field_overrides WHERE entity_id=ANY($1)")
        .bind(&ids)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let path = format!("{http}/api/v1/cmdb/devices/{id}");
    let response =
        client.patch(&path).bearer_auth(&admin).json(&json!({"hostname":"manual-router"})).send().await.unwrap();
    assert!(response.status().is_success());
    let second = report(&run(&endpoint, &admin, "changed", "1161"), &admin);
    assert_eq!(second["summary"]["updated"], 1);
    assert_eq!(second["results"][0]["cmdb_device_id"], id.to_string());
    assert_eq!(second["results"][0]["sys_name"], "discovered-router-changed");
    assert!(second["results"][0]["skipped_fields"].as_array().unwrap().contains(&json!("hostname")));
    let stored: Value =
        client.get(&path).bearer_auth(&admin).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    assert_eq!(stored["hostname"], "manual-router");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ip_addresses p JOIN interfaces i ON i.id=p.interface_id WHERE host(p.address)='127.0.0.1'").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1, "Repeated scan duplicated the management endpoint");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM field_overrides WHERE entity_id=$1 AND field_name='hostname'")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let offline = report(&run(&endpoint, &admin, "public", "1162"), &admin);
    assert_eq!(offline["summary"]["snmp_responders"], 0);
    assert_eq!(offline["results"][0]["cmdb_device_id"], Value::Null);
    for token in [&reader, "invalid-fixture-token"] {
        let output = run(&endpoint, token, "public", "1161");
        assert!(!output.status.success(), "Unauthorized CLI scan must fail");
        assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
        assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    }
}
