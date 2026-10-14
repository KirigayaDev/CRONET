use axum::{
    body::Body,
    http::{Request as HttpRequest, StatusCode},
    response::IntoResponse,
    Router,
};
use cmdb_service::repository::Repository;
use cmdb_service::{
    api::{
        self,
        grpc::{self, pb},
        http::{ApiDoc, HttpError},
        ApiState,
    },
    auth::{self, AccessClaims, AuthError, Authenticator},
    config::Config,
    db,
    models::*,
    Error, Service,
};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use sqlx::{postgres::PgConnectOptions, PgPool};
use std::{
    process::{Command, Stdio},
    str::FromStr,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;
use utoipa::OpenApi;
use uuid::Uuid;

#[tokio::test]
async fn every_data_route_authenticates_and_every_write_requires_admin() {
    let f = fixture().await;
    let app = api::http::router(f.state);
    let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
    for (path, methods) in spec["paths"].as_object().unwrap() {
        if !path.starts_with("/api/") {
            continue;
        }
        let path = path
            .replace("{id}", &Uuid::new_v4().to_string())
            .replace("{device_id}", &Uuid::new_v4().to_string())
            .replace("{interface_id}", &Uuid::new_v4().to_string());
        for method in methods.as_object().unwrap().keys() {
            if !["get", "post", "patch", "delete"].contains(&method.as_str()) {
                continue;
            }
            let verb = method.to_uppercase();
            assert_eq!(request(&app, &verb, &path, None, None).await.0, StatusCode::UNAUTHORIZED, "{verb} {path}");
            if verb != "GET" {
                assert_eq!(
                    request(&app, &verb, &path, Some(&f.reader), None).await.0,
                    StatusCode::FORBIDDEN,
                    "{verb} {path}"
                );
            }
        }
    }
}

#[tokio::test]
async fn pagination_is_sql_bounded_for_all_six_entities() {
    let pool = test_pool().await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await.unwrap();
    let marker = Uuid::new_v4();
    let mut site = None;
    for i in 0..60 {
        let value = Service::create_site_in(
            &mut tx,
            &CreateSite { name: format!("zz-page-{marker}-{i:02}"), description: None },
        )
        .await
        .unwrap();
        site = Some(value);
    }
    let site = site.unwrap();
    let small = Pagination::new(2, 1).unwrap();
    let full = Repository::list_sites(&mut tx, Pagination::new(200, 0).unwrap()).await.unwrap();
    let subset = Repository::list_sites(&mut tx, small).await.unwrap();
    assert_eq!(subset.iter().map(|v| v.id).collect::<Vec<_>>(), full[1..3].iter().map(|v| v.id).collect::<Vec<_>>());
    assert_eq!(Repository::list_sites(&mut tx, Pagination::default()).await.unwrap().len(), 50);
    let mut room = None;
    for i in 0..3 {
        room = Some(
            Service::create_room_in(
                &mut tx,
                &CreateRoom { site_id: site.id, name: format!("Room {i}"), description: None },
            )
            .await
            .unwrap(),
        );
    }
    assert_eq!(Repository::list_rooms(&mut tx, Some(site.id), small).await.unwrap().len(), 2);
    let room = room.unwrap();
    for i in 0..3 {
        Service::create_rack_in(
            &mut tx,
            &CreateRack { room_id: room.id, name: format!("Rack {i}"), height_u: 42, description: None },
        )
        .await
        .unwrap();
    }
    assert_eq!(Repository::list_racks(&mut tx, Some(room.id), small).await.unwrap().len(), 2);
    let host = format!("page-{marker}");
    let mut device = None;
    for _ in 0..3 {
        device = Some(
            Service::create_device_in(&mut tx, &CreateDevice { hostname: Some(host.clone()), ..Default::default() })
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        Repository::list_devices(&mut tx, &DeviceFilter { hostname: Some(host), ..Default::default() }, small)
            .await
            .unwrap()
            .len(),
        2
    );
    let device = device.unwrap();
    let mut interface = None;
    for i in 1..=3 {
        interface = Some(
            Service::create_interface_in(
                &mut tx,
                &CreateInterface {
                    device_id: device.id,
                    name: format!("eth{i}"),
                    if_index: Some(i),
                    description: None,
                    interface_type: None,
                    mac_address: None,
                },
            )
            .await
            .unwrap(),
        );
    }
    assert_eq!(Repository::list_interfaces(&mut tx, device.id, small).await.unwrap().len(), 2);
    let interface = interface.unwrap();
    for i in 1..=3 {
        Service::create_ip_address_in(
            &mut tx,
            &CreateIpAddress {
                interface_id: interface.id,
                address: format!("192.0.2.{i}/24").parse().unwrap(),
                is_management: false,
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(Repository::list_ip_addresses(&mut tx, interface.id, small).await.unwrap().len(), 2);
    tx.rollback().await.unwrap();
    let invalid: Pagination = serde_json::from_value(json!({"limit":0,"offset":-1})).unwrap();
    assert!(matches!(Service::new(pool).list_sites_page(invalid).await, Err(Error::Validation(_))));
}

const CONTRACT: &str = "Re:simple_videohosting_auth";

struct Keys {
    _dir: tempfile::TempDir,
    public_path: std::path::PathBuf,
    public: Vec<u8>,
    private: EncodingKey,
}
fn generate_keys() -> Keys {
    let dir = tempfile::tempdir().unwrap();
    let private_path = dir.path().join("private.pem");
    let public_path = dir.path().join("public.pem");
    let output = Command::new("openssl")
        .args(["genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out"])
        .arg(&private_path)
        .output()
        .unwrap();
    assert!(output.status.success(), "Ephemeral test key generation failed");
    let output = Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&private_path)
        .arg("-out")
        .arg(&public_path)
        .output()
        .unwrap();
    assert!(output.status.success(), "Ephemeral test public key generation failed");
    let private = EncodingKey::from_rsa_pem(&std::fs::read(private_path).unwrap()).unwrap();
    Keys { _dir: dir, public: std::fs::read(&public_path).unwrap(), public_path, private }
}
fn keys() -> &'static Keys {
    static KEYS: OnceLock<Keys> = OnceLock::new();
    KEYS.get_or_init(generate_keys)
}
fn wrong_keys() -> &'static Keys {
    static KEYS: OnceLock<Keys> = OnceLock::new();
    KEYS.get_or_init(generate_keys)
}
fn sign(claims: &AccessClaims) -> String {
    encode(&Header::new(Algorithm::RS256), claims, &keys().private).unwrap()
}

async fn test_pool() -> PgPool {
    let url = std::env::var("CMDB_TEST_DATABASE_URL").expect("Use the isolated CMDB test project");
    assert_eq!(
        PgConnectOptions::from_str(&url).unwrap().get_database(),
        Some("cronet_cmdb_test_schema"),
        "Refusing a non-test DB"
    );
    let pool = db::connect(&Config::from_database_url(&url).unwrap()).await.unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await.unwrap();
    assert_eq!(name, "cronet_cmdb_test_schema");
    let head: String = sqlx::query_scalar("SELECT version_num FROM alembic_version").fetch_one(&pool).await.unwrap();
    assert_eq!(head, "8b6e2f4a9c10");
    pool
}
fn redis_url() -> String {
    let url = std::env::var("CMDB_TEST_REDIS_URL").expect("Use the isolated CMDB test Redis");
    assert!(url == "redis://redis:6379", "Refusing a non-test Redis");
    url
}
async fn user(pool: &PgPool, admin: bool, super_admin: bool) -> AccessClaims {
    let (sub, version): (Uuid, Uuid) = sqlx::query_as(
        "INSERT INTO users (username,email,password_hash,display_name,is_admin,is_super_admin)
        VALUES ($1,$2,'test-only-hash','Adapter test',$3,$4) RETURNING uuid,token_version_uuid",
    )
    .bind(Uuid::new_v4().simple().to_string())
    .bind(format!("{}@example.invalid", Uuid::new_v4()))
    .bind(admin)
    .bind(super_admin)
    .fetch_one(pool)
    .await
    .unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    AccessClaims {
        iss: CONTRACT.into(),
        sub,
        aud: CONTRACT.into(),
        exp: now + 600.0,
        nbf: now - 1.0,
        iat: now - 1.0,
        jti: Uuid::new_v4(),
        token_version: version,
        token_type: "access".into(),
    }
}
struct Fixture {
    pool: PgPool,
    state: ApiState,
    reader: String,
    admin: String,
    super_admin: String,
}
async fn fixture() -> Fixture {
    let pool = test_pool().await;
    let redis = auth::connect_redis(&redis_url()).await.unwrap();
    let auth = Authenticator::new(pool.clone(), redis, &keys().public, CONTRACT, CONTRACT).unwrap();
    let reader = sign(&user(&pool, false, false).await);
    let admin = sign(&user(&pool, true, false).await);
    let super_admin = sign(&user(&pool, false, true).await);
    Fixture { pool: pool.clone(), state: ApiState { service: Service::new(pool), auth }, reader, admin, super_admin }
}
async fn request(
    app: &Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = HttpRequest::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let body = if let Some(v) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(v.to_string())
    } else {
        Body::empty()
    };
    let response = app.clone().oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
    (status, json)
}
async fn location(service: &Service) -> Rack {
    let site =
        service.create_site(CreateSite { name: format!("API {}", Uuid::new_v4()), description: None }).await.unwrap();
    let room =
        service.create_room(CreateRoom { site_id: site.id, name: "Room A".into(), description: None }).await.unwrap();
    service
        .create_rack(CreateRack { room_id: room.id, name: "Rack A".into(), height_u: 42, description: None })
        .await
        .unwrap()
}
fn auth_request<T>(value: T, token: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[tokio::test]
async fn auth_accepts_fractional_python_timestamps_and_rejects_invalid_tokens() {
    let pool = test_pool().await;
    let redis = auth::connect_redis(&redis_url()).await.unwrap();
    let verifier = Authenticator::new(pool.clone(), redis, &keys().public, CONTRACT, CONTRACT).unwrap();
    let good = user(&pool, false, false).await;
    let principal = verifier.authenticate(Some(&format!("Bearer {}", sign(&good)))).await.unwrap();
    assert_eq!(principal.user_id, good.sub);
    for header in
        [None, Some(""), Some("Basic value"), Some("Bearer"), Some("Bearer malformed"), Some("Bearer one two")]
    {
        assert_eq!(verifier.authenticate(header).await, Err(AuthError::Unauthorized));
    }
    for case in
        ["expired", "future_nbf", "future_iat", "audience", "issuer", "refresh", "csrf", "version", "nonexistent_user"]
    {
        let mut value = good.clone();
        match case {
            "expired" => value.exp = value.iat - 120.0,
            "future_nbf" => value.nbf = value.exp - 1.0,
            "future_iat" => value.iat = value.exp - 1.0,
            "audience" => value.aud = "other".into(),
            "issuer" => value.iss = "other".into(),
            "refresh" => value.token_type = "refresh".into(),
            "csrf" => value.token_type = "csrf".into(),
            "version" => value.token_version = Uuid::new_v4(),
            "nonexistent_user" => value.sub = Uuid::new_v4(),
            _ => unreachable!(),
        }
        assert_eq!(
            verifier.authenticate(Some(&format!("Bearer {}", sign(&value)))).await,
            Err(AuthError::Unauthorized),
            "case {case}"
        );
    }
    let wrong = encode(&Header::new(Algorithm::RS256), &good, &wrong_keys().private).unwrap();
    assert_eq!(verifier.authenticate(Some(&format!("Bearer {wrong}"))).await, Err(AuthError::Unauthorized));
    let secret = Uuid::new_v4();
    let wrong_alg =
        encode(&Header::new(Algorithm::HS256), &good, &EncodingKey::from_secret(secret.as_bytes())).unwrap();
    assert_eq!(verifier.authenticate(Some(&format!("Bearer {wrong_alg}"))).await, Err(AuthError::Unauthorized));
    for field in ["sub", "jti", "token_version", "exp", "nbf", "iat"] {
        let mut value = serde_json::to_value(&good).unwrap();
        value[field] = json!("invalid");
        let token = encode(&Header::new(Algorithm::RS256), &value, &keys().private).unwrap();
        assert_eq!(verifier.authenticate(Some(&format!("Bearer {token}"))).await, Err(AuthError::Unauthorized));
        value.as_object_mut().unwrap().remove(field);
        let token = encode(&Header::new(Algorithm::RS256), &value, &keys().private).unwrap();
        assert_eq!(verifier.authenticate(Some(&format!("Bearer {token}"))).await, Err(AuthError::Unauthorized));
    }
}

#[tokio::test]
async fn auth_blacklist_and_live_token_version_are_enforced() {
    let pool = test_pool().await;
    let mut redis = auth::connect_redis(&redis_url()).await.unwrap();
    let verifier = Authenticator::new(pool.clone(), redis.clone(), &keys().public, CONTRACT, CONTRACT).unwrap();
    let claims = user(&pool, true, false).await;
    let token = format!("Bearer {}", sign(&claims));
    assert!(verifier.authenticate(Some(&token)).await.is_ok());
    redis::cmd("SET")
        .arg(format!("blacklisted_jwt_token:{}", claims.jti))
        .arg(1)
        .arg("EX")
        .arg(120)
        .query_async::<()>(&mut redis)
        .await
        .unwrap();
    assert_eq!(verifier.authenticate(Some(&token)).await, Err(AuthError::Unauthorized));
    let claims = user(&pool, true, false).await;
    let token = format!("Bearer {}", sign(&claims));
    sqlx::query("UPDATE users SET token_version_uuid=$1 WHERE uuid=$2")
        .bind(Uuid::new_v4())
        .bind(claims.sub)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(verifier.authenticate(Some(&token)).await, Err(AuthError::Unauthorized));
}

#[tokio::test]
async fn auth_rejects_invalid_and_private_keys() {
    let pool = test_pool().await;
    let redis = auth::connect_redis(&redis_url()).await.unwrap();
    assert!(Authenticator::new(pool.clone(), redis.clone(), b"invalid", CONTRACT, CONTRACT).is_err());
    let private_pem = std::fs::read(keys()._dir.path().join("private.pem")).unwrap();
    assert!(Authenticator::new(pool, redis, &private_pem, CONTRACT, CONTRACT).is_err());
    assert!(auth::load_public_key(std::path::Path::new("/tmp/nonexistent-cmdb-public-key.pem")).is_err());
}

#[tokio::test]
async fn http_read_and_write_authorization_uses_actual_user_flags() {
    let f = fixture().await;
    let app = api::http::router(f.state.clone());
    assert_eq!(request(&app, "GET", "/api/v1/cmdb/sites", Some(&f.reader), None).await.0, StatusCode::OK);
    for method in ["POST", "PATCH", "DELETE"] {
        let path = if method == "POST" {
            "/api/v1/cmdb/sites".into()
        } else {
            format!("/api/v1/cmdb/sites/{}", Uuid::new_v4())
        };
        assert_eq!(
            request(&app, method, &path, Some(&f.reader), Some(json!({"name":"forbidden"}))).await.0,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        request(&app, "POST", &format!("/api/v1/cmdb/devices/{}/decommission", Uuid::new_v4()), Some(&f.reader), None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    for token in [&f.admin, &f.super_admin] {
        let (status, site) = request(
            &app,
            "POST",
            "/api/v1/cmdb/sites",
            Some(token),
            Some(json!({"name":format!("admin {}",Uuid::new_v4())})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let path = format!("/api/v1/cmdb/sites/{}", site["id"].as_str().unwrap());
        assert_eq!(request(&app, "GET", &path, Some(token), None).await.0, StatusCode::OK);
        assert_eq!(
            request(&app, "PATCH", &path, Some(token), Some(json!({"description":"updated"}))).await.0,
            StatusCode::OK
        );
        assert_eq!(request(&app, "DELETE", &path, Some(token), None).await.0, StatusCode::NO_CONTENT);
    }
}

#[tokio::test]
async fn http_sites_pagination_validation_and_error_shapes() {
    let f = fixture().await;
    let app = api::http::router(f.state.clone());
    let name = format!("HTTP {}", Uuid::new_v4());
    let (status, site) =
        request(&app, "POST", "/api/v1/cmdb/sites", Some(&f.admin), Some(json!({"name":name,"description":"before"})))
            .await;
    assert_eq!(status, StatusCode::CREATED);
    let path = format!("/api/v1/cmdb/sites/{}", site["id"].as_str().unwrap());
    assert_eq!(request(&app, "GET", &path, Some(&f.reader), None).await.1["description"], "before");
    assert_eq!(
        request(&app, "PATCH", &path, Some(&f.admin), Some(json!({"description":null}))).await.1["description"],
        Value::Null
    );
    let (status, body) = request(&app, "GET", "/api/v1/cmdb/sites?limit=1&offset=0", Some(&f.reader), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 1);
    for query in ["limit=0", "limit=-1", "limit=201", "offset=-1", "limit=invalid"] {
        let (status, body) = request(&app, "GET", &format!("/api/v1/cmdb/sites?{query}"), Some(&f.reader), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "validation");
    }
    assert_eq!(
        request(&app, "GET", &format!("/api/v1/cmdb/sites/{}", Uuid::new_v4()), Some(&f.reader), None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "GET", "/api/v1/cmdb/sites/not-a-uuid", Some(&f.reader), None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "POST", "/api/v1/cmdb/sites", Some(&f.admin), Some(json!({"name":name.to_uppercase()}))).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(&app, "POST", "/api/v1/cmdb/sites", Some(&f.admin), Some(json!({"name":" \t "}))).await.0,
        StatusCode::BAD_REQUEST
    );
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/api/v1/cmdb/sites")
        .header("authorization", format!("Bearer {}", f.admin))
        .header("content-type", "application/json")
        .body(Body::from("invalid sensitive-payload-marker"))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&bytes).contains("sensitive-payload-marker"));
    let response = app
        .clone()
        .oneshot(HttpRequest::builder().uri("/api/v1/cmdb/sites").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["www-authenticate"], "Bearer");
    assert_eq!(
        request(&app, "GET", &format!("/api/v1/cmdb/sites?access_token={}", f.reader), None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn http_location_filters_and_full_device_network_flow() {
    let f = fixture().await;
    let app = api::http::router(f.state.clone());
    let (_, site) = request(
        &app,
        "POST",
        "/api/v1/cmdb/sites",
        Some(&f.admin),
        Some(json!({"name":format!("location {}",Uuid::new_v4())})),
    )
    .await;
    let (status, room) = request(
        &app,
        "POST",
        "/api/v1/cmdb/rooms",
        Some(&f.admin),
        Some(json!({"site_id":site["id"],"name":"Room A"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, rack) = request(
        &app,
        "POST",
        "/api/v1/cmdb/racks",
        Some(&f.admin),
        Some(json!({"room_id":room["id"],"name":"Rack A","height_u":42})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for (path, id) in [("rooms", &site["id"]), ("racks", &room["id"])] {
        let param = if path == "rooms" { "site_id" } else { "room_id" };
        assert_eq!(
            request(
                &app,
                "GET",
                &format!("/api/v1/cmdb/{path}?{param}={}", id.as_str().unwrap()),
                Some(&f.reader),
                None
            )
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
            1
        );
        assert!(request(
            &app,
            "GET",
            &format!("/api/v1/cmdb/{path}?{param}={}", Uuid::new_v4()),
            Some(&f.reader),
            None
        )
        .await
        .1
        .as_array()
        .unwrap()
        .is_empty());
    }
    let host = format!("host-{}", Uuid::new_v4());
    let (status, device) = request(
        &app,
        "POST",
        "/api/v1/cmdb/devices",
        Some(&f.admin),
        Some(json!({"rack_id":rack["id"],"start_unit":1,"height_u":2,"hostname":host,"serial":"exact-serial"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let dp = format!("/api/v1/cmdb/devices/{}", device["id"].as_str().unwrap());
    assert_eq!(request(&app, "GET", &dp, Some(&f.reader), None).await.0, StatusCode::OK);
    assert_eq!(
        request(&app, "PATCH", &dp, Some(&f.admin), Some(json!({"lifecycle_status":"maintenance"}))).await.1
            ["lifecycle_status"],
        "maintenance"
    );
    assert_eq!(
        request(
            &app,
            "GET",
            &format!(
                "/api/v1/cmdb/devices?hostname={host}&serial=exact-serial&lifecycle_status=maintenance&rack_id={}",
                rack["id"].as_str().unwrap()
            ),
            Some(&f.reader),
            None
        )
        .await
        .1
        .as_array()
        .unwrap()
        .len(),
        1
    );
    assert!(request(&app, "GET", &format!("/api/v1/cmdb/devices?hostname={}x", host), Some(&f.reader), None)
        .await
        .1
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        request(&app, "POST", &format!("{dp}/decommission"), Some(&f.admin), None).await.1["lifecycle_status"],
        "decommissioned"
    );
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/v1/cmdb/devices",
            Some(&f.admin),
            Some(json!({"rack_id":rack["id"],"start_unit":1,"height_u":1}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let (status, interface) = request(
        &app,
        "POST",
        "/api/v1/cmdb/interfaces",
        Some(&f.admin),
        Some(json!({"device_id":device["id"],"name":"eth0","if_index":1,"mac_address":"aa:bb:cc:dd:ee:ff"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let ipath = format!("/api/v1/cmdb/interfaces/{}", interface["id"].as_str().unwrap());
    assert_eq!(
        request(&app, "GET", &format!("{dp}/interfaces?limit=1"), Some(&f.reader), None)
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        request(&app, "GET", &ipath, Some(&f.reader), None).await.1["mac_address"].as_str().unwrap().to_lowercase(),
        "aa:bb:cc:dd:ee:ff"
    );
    assert_eq!(
        request(&app, "PATCH", &ipath, Some(&f.admin), Some(json!({"description":"uplink"}))).await.0,
        StatusCode::OK
    );
    for address in ["192.0.2.1/24", "2001:db8::1/64"] {
        let (status, ip) = request(
            &app,
            "POST",
            "/api/v1/cmdb/ip-addresses",
            Some(&f.admin),
            Some(json!({"interface_id":interface["id"],"address":address})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let path = format!("/api/v1/cmdb/ip-addresses/{}", ip["id"].as_str().unwrap());
        assert_eq!(request(&app, "GET", &path, Some(&f.reader), None).await.1["address"], address);
        assert_eq!(
            request(&app, "PATCH", &path, Some(&f.admin), Some(json!({"is_management":true}))).await.1["is_management"],
            true
        );
        assert_eq!(request(&app, "DELETE", &path, Some(&f.admin), None).await.0, StatusCode::NO_CONTENT);
    }
    assert!(request(&app, "GET", &format!("{ipath}/ip-addresses"), Some(&f.reader), None)
        .await
        .1
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(request(&app, "DELETE", &ipath, Some(&f.admin), None).await.0, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn openapi_documents_all_operations_security_and_patch_schemas() {
    let f = fixture().await;
    let app = api::http::router(f.state);
    let (status, spec) = request(&app, "GET", "/openapi.json", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(spec["openapi"].as_str().unwrap().starts_with("3."));
    let mut operations = 0;
    for (path, methods) in spec["paths"].as_object().unwrap() {
        for (method, op) in methods.as_object().unwrap() {
            if ["get", "post", "patch", "delete"].contains(&method.as_str()) {
                operations += 1;
                if path.starts_with("/api/") {
                    assert!(op["security"][0]["bearer_jwt"].is_array());
                    assert!(op["responses"]["401"].is_object());
                }
            }
        }
    }
    assert_eq!(operations, 45);
    assert_eq!(spec["components"]["securitySchemes"]["bearer_jwt"]["scheme"], "bearer");
    assert!(spec["components"]["schemas"]["UpdateDeviceDto"].is_object());
    assert!(spec["paths"]["/api/v1/cmdb/devices"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["name"] == "limit"));
    let parsed: utoipa::openapi::OpenApi = serde_json::from_value(spec).unwrap();
    assert_eq!(parsed.paths.paths.len(), ApiDoc::openapi().paths.paths.len());
}

#[tokio::test]
async fn postgres_failure_is_auth_unavailable_readiness_fails_liveness_survives() {
    let f = fixture().await;
    f.pool.close().await;
    let app = api::http::router(f.state.clone());
    assert_eq!(request(&app, "GET", "/health/live", None, None).await.0, StatusCode::OK);
    assert_eq!(request(&app, "GET", "/health/ready", None, None).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(request(&app, "GET", "/api/v1/cmdb/sites", Some(&f.reader), None).await.1["code"], "auth_unavailable");
}

#[tokio::test]
async fn redis_failure_is_auth_unavailable_and_never_authenticated() {
    let f = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let proxy = tokio::spawn(async move {
        let mut children = tokio::task::JoinSet::new();
        loop {
            let (mut input, _) = listener.accept().await.unwrap();
            children.spawn(async move {
                let mut target = tokio::net::TcpStream::connect("redis:6379").await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut input, &mut target).await;
            });
        }
    });
    let redis = auth::connect_redis(&format!("redis://{addr}")).await.unwrap();
    let verifier = Authenticator::new(f.pool.clone(), redis, &keys().public, CONTRACT, CONTRACT).unwrap();
    assert!(verifier.authenticate(Some(&format!("Bearer {}", f.reader))).await.is_ok());
    proxy.abort();
    let _ = proxy.await;
    assert_eq!(verifier.authenticate(Some(&format!("Bearer {}", f.reader))).await, Err(AuthError::Unavailable));
    let app = api::http::router(ApiState { service: f.state.service, auth: verifier });
    assert_eq!(request(&app, "GET", "/health/live", None, None).await.0, StatusCode::OK);
    assert_eq!(request(&app, "GET", "/health/ready", None, None).await.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn http_internal_errors_are_sanitized_and_grpc_mapping_is_consistent() {
    let error = Error::Database(sqlx::Error::Protocol("sensitive-driver-marker".into()));
    let response = HttpError::Core(error).into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&bytes).contains("sensitive-driver-marker"));
    assert_eq!(grpc::domain_status(Error::Validation("sensitive-payload".into())).code(), tonic::Code::InvalidArgument);
    for (code, expected) in [
        ("23505", tonic::Code::AlreadyExists),
        ("23503", tonic::Code::FailedPrecondition),
        ("23P01", tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(grpc::domain_status(Error::Conflict { sqlstate: code.into(), constraint: None }).code(), expected);
    }
    assert_eq!(grpc::domain_status(Error::Retryable { sqlstate: "40001".into() }).code(), tonic::Code::Unavailable);
    assert_eq!(grpc::auth_status(AuthError::Forbidden).code(), tonic::Code::PermissionDenied);
    assert_eq!(grpc::auth_status(AuthError::Unavailable).code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn grpc_real_server_all_rpcs_and_cross_transport_shared_service() {
    let f = fixture().await;
    let rack = location(&f.state.service).await;
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = grpc_listener.local_addr().unwrap();
    let (stop, receiver) = tokio::sync::oneshot::channel();
    let state = f.state.clone();
    let server = tokio::spawn(api::serve(state, http, grpc_listener, async {
        let _ = receiver.await;
    }));
    let mut client = pb::cmdb_client::CmdbClient::connect(format!("http://{addr}")).await.unwrap();
    let device = client
        .create_device(auth_request(
            pb::CreateDeviceRequest {
                rack_id: Some(rack.id.to_string()),
                start_unit: Some(1),
                height_u: Some(2),
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(device.created_at.is_some());
    assert_eq!(
        client
            .get_device(auth_request(pb::IdRequest { id: device.id.clone() }, &f.reader))
            .await
            .unwrap()
            .into_inner()
            .id,
        device.id
    );
    let updated = client
        .update_device(auth_request(
            pb::UpdateDeviceRequest {
                id: device.id.clone(),
                hostname: Some(pb::StringPatch { change: Some(pb::string_patch::Change::Value("grpc-host".into())) }),
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.hostname.as_deref(), Some("grpc-host"));
    let list = client
        .list_devices(auth_request(
            pb::ListDevicesRequest { rack_id: Some(rack.id.to_string()), limit: Some(1), ..Default::default() },
            &f.reader,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.devices.len(), 1);
    assert_eq!(
        client
            .get_rack(auth_request(pb::IdRequest { id: rack.id.to_string() }, &f.reader))
            .await
            .unwrap()
            .into_inner()
            .height_u,
        42
    );
    assert_eq!(
        client.get_device(pb::IdRequest { id: device.id.clone() }).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client.create_device(auth_request(pb::CreateDeviceRequest::default(), &f.reader)).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        client.get_device(auth_request(pb::IdRequest { id: "invalid".into() }, &f.reader)).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        client
            .get_device(auth_request(pb::IdRequest { id: Uuid::new_v4().to_string() }, &f.reader))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .create_device(auth_request(
                pb::CreateDeviceRequest {
                    rack_id: Some(rack.id.to_string()),
                    start_unit: Some(2),
                    height_u: Some(2),
                    ..Default::default()
                },
                &f.admin
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        client
            .list_devices(auth_request(pb::ListDevicesRequest { limit: Some(0), ..Default::default() }, &f.reader))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let interface = client
        .create_interface(auth_request(
            pb::CreateInterfaceRequest {
                device_id: device.id.clone(),
                name: "eth0".into(),
                mac_address: Some("aa:bb:cc:dd:ee:ff".into()),
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        client
            .list_interfaces(auth_request(
                pb::ListInterfacesRequest { device_id: device.id.clone(), ..Default::default() },
                &f.reader
            ))
            .await
            .unwrap()
            .into_inner()
            .interfaces
            .len(),
        1
    );
    assert_eq!(
        client
            .update_interface(auth_request(
                pb::UpdateInterfaceRequest {
                    id: interface.id.clone(),
                    name: Some("eth1".into()),
                    ..Default::default()
                },
                &f.admin
            ))
            .await
            .unwrap()
            .into_inner()
            .name,
        "eth1"
    );
    for address in ["192.0.2.1/24", "2001:db8::1/64"] {
        let ip = client
            .create_ip_address(auth_request(
                pb::CreateIpAddressRequest {
                    interface_id: interface.id.clone(),
                    address: address.into(),
                    is_management: false,
                },
                &f.admin,
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(
            client
                .update_ip_address(auth_request(
                    pb::UpdateIpAddressRequest { id: ip.id, is_management: Some(true), ..Default::default() },
                    &f.admin
                ))
                .await
                .unwrap()
                .into_inner()
                .is_management
        );
    }
    assert_eq!(
        client
            .list_ip_addresses(auth_request(
                pb::ListIpAddressesRequest { interface_id: interface.id.clone(), ..Default::default() },
                &f.reader
            ))
            .await
            .unwrap()
            .into_inner()
            .ip_addresses
            .len(),
        2
    );
    assert_eq!(
        client
            .decommission_device(auth_request(pb::IdRequest { id: device.id.clone() }, &f.admin))
            .await
            .unwrap()
            .into_inner()
            .lifecycle_status,
        pb::LifecycleStatus::Decommissioned as i32
    );
    let response = reqwest::Client::new()
        .get(format!("http://{http_addr}/api/v1/cmdb/devices/{}", device.id))
        .bearer_auth(&f.reader)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.json::<Value>().await.unwrap()["lifecycle_status"], "decommissioned");
    let detached = client
        .update_device(auth_request(
            pb::UpdateDeviceRequest {
                id: device.id.clone(),
                rack_id: Some(pb::StringPatch { change: Some(pb::string_patch::Change::Clear(0)) }),
                start_unit: Some(pb::Int32Patch { change: Some(pb::int32_patch::Change::Clear(0)) }),
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(detached.rack_id.is_none());
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn binary_starts_both_servers_and_rejects_bad_key_configuration() {
    let f = fixture().await;
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    drop(http);
    drop(grpc_listener);
    let mut child = Command::new(env!("CARGO_BIN_EXE_cmdb-service"))
        .env("DATABASE_URL", std::env::var("CMDB_TEST_DATABASE_URL").unwrap())
        .env("REDIS_URL", redis_url())
        .env("JWT_PUBLIC_KEY_PATH", &keys().public_path)
        .env("CMDB_HTTP_ADDR", http_addr.to_string())
        .env("CMDB_GRPC_ADDR", grpc_addr.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let http_client = reqwest::Client::builder().timeout(Duration::from_secs(1)).build().unwrap();
    let mut started = false;
    for _ in 0..100 {
        if let Ok(response) = http_client.get(format!("http://{http_addr}/health/live")).send().await {
            if response.status() == 200 {
                started = true;
                break;
            }
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !started {
        let _ = child.kill();
    }
    assert!(started, "HTTP binary startup failed");
    assert_eq!(http_client.get(format!("http://{http_addr}/health/ready")).send().await.unwrap().status(), 200);
    let mut client = pb::cmdb_client::CmdbClient::connect(format!("http://{grpc_addr}")).await.unwrap();
    assert!(client
        .list_devices(auth_request(pb::ListDevicesRequest { limit: Some(1), ..Default::default() }, &f.reader))
        .await
        .is_ok());
    child.kill().unwrap();
    let output = child.wait_with_output().unwrap();
    let logs = String::from_utf8(output.stdout).unwrap();
    assert!(!logs.contains(&f.reader));
    assert!(!logs.contains("BEGIN PUBLIC KEY"));
    let bad = Command::new(env!("CARGO_BIN_EXE_cmdb-service"))
        .env("DATABASE_URL", std::env::var("CMDB_TEST_DATABASE_URL").unwrap())
        .env("REDIS_URL", redis_url())
        .env("JWT_PUBLIC_KEY_PATH", "/tmp/nonexistent-cmdb-public-key.pem")
        .output()
        .unwrap();
    assert!(!bad.status.success());
}

#[path = "support/workflow_tests.rs"]
mod workflow_tests;
