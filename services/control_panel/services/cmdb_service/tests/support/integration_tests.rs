use super::*;

#[tokio::test]
async fn readiness_requires_d1_schema_even_when_postgres_and_redis_are_reachable() {
    let f = fixture().await;
    let migrated = api::http::router(f.state);
    assert_eq!(request(&migrated, "GET", "/health/ready", None, None).await.0, StatusCode::OK);

    // test_pool verifies the isolated parent DB before any database-level DDL.
    let name = format!("cmdb_ready_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}")).execute(&f.pool).await.unwrap();
    let options =
        PgConnectOptions::from_str(&std::env::var("CMDB_TEST_DATABASE_URL").unwrap()).unwrap().database(&name);
    let empty = PgPool::connect_with(options).await.unwrap();
    db::check_connection(&empty).await.unwrap();
    let auth = Authenticator::new(
        empty.clone(),
        auth::connect_redis(&redis_url()).await.unwrap(),
        &keys().public,
        CONTRACT,
        CONTRACT,
    )
    .unwrap();
    let app = api::http::router(ApiState { service: Service::new(empty.clone()), auth });
    let ready = request(&app, "GET", "/health/ready", None, None).await.0;
    let live = request(&app, "GET", "/health/live", None, None).await.0;
    empty.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)")).execute(&f.pool).await.unwrap();
    assert_eq!(ready, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(live, StatusCode::OK);
}

#[cfg(unix)]
#[tokio::test]
async fn binary_gracefully_exits_on_sigterm_and_sigint() {
    let _pool = test_pool().await;
    for signal in ["TERM", "INT"] {
        let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let http_addr = http.local_addr().unwrap();
        let grpc = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = grpc.local_addr().unwrap();
        drop(http);
        drop(grpc);
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
        let client = reqwest::Client::builder().timeout(Duration::from_secs(1)).build().unwrap();
        let mut started = false;
        for _ in 0..100 {
            if let Ok(response) = client.get(format!("http://{http_addr}/health/live")).send().await {
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
            let _ = child.wait();
            panic!("Signal test binary failed to start");
        }
        let sent = Command::new("sh")
            .args(["-c", "kill -\"$1\" \"$2\"", "cmdb-signal-test", signal, &child.id().to_string()])
            .status()
            .unwrap();
        assert!(sent.success());
        let mut exited = false;
        for _ in 0..200 {
            if child.try_wait().unwrap().is_some() {
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !exited {
            let _ = child.kill();
        }
        let output = child.wait_with_output().unwrap();
        assert!(exited && output.status.success(), "{signal} must cause a clean exit");
        assert!(String::from_utf8(output.stdout).unwrap().contains("CMDB shutdown requested"));
    }
}

#[tokio::test]
async fn shared_contract_client_mutates_and_reads_the_http_inventory() {
    use contracts::cronet::cmdb::v1 as shared;

    let f = fixture().await;
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc.local_addr().unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(api::serve(f.state, http, grpc, async {
        let _ = shutdown.await;
    }));
    let mut client = shared::cmdb_client::CmdbClient::connect(format!("http://{grpc_addr}")).await.unwrap();
    let hostname = format!("shared-client-{}", Uuid::new_v4());
    let created = client
        .create_device(auth_request(
            shared::CreateDeviceRequest {
                hostname: Some(hostname.clone()),
                lifecycle_status: shared::LifecycleStatus::Active as i32,
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    let read = client
        .get_device(auth_request(shared::IdRequest { id: created.id.clone() }, &f.reader))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.id, created.id);
    assert_eq!(read.hostname.as_deref(), Some(hostname.as_str()));
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(format!("http://{http_addr}/api/v1/cmdb/devices/{}", created.id))
        .bearer_auth(&f.reader)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.json::<Value>().await.unwrap()["hostname"], hostname);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
