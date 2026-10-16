use super::*;
use pb::cmdb_server::Cmdb;

fn observation() -> pb::ApplyDiscoveryObservationRequest {
    let ip = std::net::Ipv6Addr::from((0x20010db8_u128 << 96) | (Uuid::new_v4().as_u128() & ((1_u128 << 96) - 1)));
    pb::ApplyDiscoveryObservationRequest {
        target_ip: ip.to_string(),
        hostname: Some(format!("auto-{}", Uuid::new_v4())),
        vendor: Some("Cisco".into()),
        model: Some("Known fixture model".into()),
        ..Default::default()
    }
}
async fn apply(f: &Fixture, input: pb::ApplyDiscoveryObservationRequest) -> pb::ApplyDiscoveryObservationResponse {
    grpc::GrpcApi::new(f.state.clone())
        .apply_discovery_observation(auth_request(input, &f.admin))
        .await
        .unwrap()
        .into_inner()
}
async fn graph(f: &Fixture, id: &str) -> (Vec<Interface>, Vec<IpAddress>) {
    let interfaces = f.state.service.list_interfaces(id.parse().unwrap()).await.unwrap();
    let mut ips = Vec::new();
    for interface in &interfaces {
        ips.extend(f.state.service.list_ip_addresses(interface.id).await.unwrap());
    }
    (interfaces, ips)
}
async fn device_events(f: &Fixture, id: &str) -> Vec<cmdb_service::workflow::AuditEvent> {
    f.state
        .service
        .list_audit_events(
            &cmdb_service::workflow::AuditFilter { entity_id: Some(id.parse().unwrap()), ..Default::default() },
            Pagination::default(),
        )
        .await
        .unwrap()
}
async fn manual_device(f: &Fixture, serial: Option<String>) -> Device {
    f.state.service.create_device(CreateDevice { serial, ..Default::default() }).await.unwrap()
}
async fn endpoint(f: &Fixture, device: Uuid, address: &str, management: bool) {
    let interface = f
        .state
        .service
        .create_interface(CreateInterface {
            device_id: device,
            name: "real-port".into(),
            if_index: None,
            description: None,
            interface_type: None,
            mac_address: None,
        })
        .await
        .unwrap();
    f.state
        .service
        .create_ip_address(CreateIpAddress {
            interface_id: interface.id,
            address: address.parse().unwrap(),
            is_management: management,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn creates_unplaced_device_logical_management_graph_and_discovery_audit_without_overrides() {
    let f = fixture().await;
    let input = observation();
    let r = apply(&f, input.clone()).await;
    assert!(r.created);
    let d = r.device.unwrap();
    assert!(d.rack_id.is_none() && d.start_unit.is_none() && d.height_u.is_none());
    let (interfaces, ips) = graph(&f, &d.id).await;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(interfaces[0].name, "discovery-management");
    assert_eq!(interfaces[0].interface_type.as_deref(), Some("discovery/logical"));
    assert!(interfaces[0].if_index.is_none() && interfaces[0].mac_address.is_none());
    assert_eq!(ips.len(), 1);
    assert!(ips[0].is_management);
    assert_eq!(ips[0].address.ip().to_string(), input.target_ip);
    let events = device_events(&f, &d.id).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source, "discovery");
    assert_eq!(events[0].action, "create");
    assert_eq!(events[0].actor_type, "user");
    assert!(events[0].actor_id.is_some() && events[0].request_id.is_some());
    for id in [d.id.parse().unwrap(), interfaces[0].id, ips[0].id] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM field_overrides WHERE entity_id=$1")
            .bind(id)
            .fetch_one(&f.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let sources: Vec<String> = sqlx::query_scalar("SELECT source FROM audit_events WHERE entity_id=$1")
            .bind(id)
            .fetch_all(&f.pool)
            .await
            .unwrap();
        assert_eq!(sources, vec!["discovery"]);
    }
}

#[tokio::test]
async fn repeated_ip_updates_unlocked_name_without_duplicate_device_interface_or_ip() {
    let f = fixture().await;
    let mut input = observation();
    let first = apply(&f, input.clone()).await.device.unwrap();
    input.hostname = Some("updated by discovery".into());
    let second = apply(&f, input).await;
    assert!(!second.created);
    let d = second.device.unwrap();
    assert_eq!(d.id, first.id);
    assert_eq!(d.hostname.as_deref(), Some("updated by discovery"));
    let (interfaces, ips) = graph(&f, &d.id).await;
    assert_eq!((interfaces.len(), ips.len()), (1, 1));
    let events = device_events(&f, &d.id).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].action, "update");
    assert_eq!(events[0].before_data.as_ref().unwrap()["hostname"], first.hostname.unwrap());
}

#[tokio::test]
async fn identical_observation_is_noop_without_timestamp_or_audit_churn() {
    let f = fixture().await;
    let input = observation();
    let a = apply(&f, input.clone()).await.device.unwrap();
    let b = apply(&f, input).await.device.unwrap();
    assert_eq!(a.updated_at, b.updated_at);
    assert_eq!(device_events(&f, &a.id).await.len(), 1);
}

#[tokio::test]
async fn manual_http_hostname_override_is_preserved_and_reported() {
    let f = fixture().await;
    let mut input = observation();
    let id = apply(&f, input.clone()).await.device.unwrap().id;
    let app = api::http::router(f.state.clone());
    assert_eq!(
        request(
            &app,
            "PATCH",
            &format!("/api/v1/cmdb/devices/{id}"),
            Some(&f.admin),
            Some(json!({"hostname":"manual-name"}))
        )
        .await
        .0,
        StatusCode::OK
    );
    input.hostname = Some("new-auto-name".into());
    let r = apply(&f, input).await;
    assert_eq!(r.device.unwrap().hostname.as_deref(), Some("manual-name"));
    assert!(r.skipped_fields.contains(&"hostname".into()));
}

#[tokio::test]
async fn manual_vendor_override_is_preserved_while_unlocked_hostname_updates() {
    let f = fixture().await;
    let mut input = observation();
    let id = apply(&f, input.clone()).await.device.unwrap().id;
    let app = api::http::router(f.state.clone());
    assert_eq!(
        request(
            &app,
            "PATCH",
            &format!("/api/v1/cmdb/devices/{id}"),
            Some(&f.admin),
            Some(json!({"vendor":"Manual vendor"}))
        )
        .await
        .0,
        StatusCode::OK
    );
    input.hostname = Some("auto-new".into());
    let r = apply(&f, input).await;
    let d = r.device.unwrap();
    assert_eq!(d.vendor.as_deref(), Some("Manual vendor"));
    assert_eq!(d.hostname.as_deref(), Some("auto-new"));
    assert_eq!(r.skipped_fields, vec!["vendor"]);
}

#[tokio::test]
async fn absent_and_whitespace_observations_never_clear_known_values() {
    let f = fixture().await;
    let mut input = observation();
    let a = apply(&f, input.clone()).await.device.unwrap();
    input.model = None;
    input.hostname = Some(" \t ".into());
    input.vendor = None;
    let b = apply(&f, input).await.device.unwrap();
    assert_eq!(a.model, b.model);
    assert_eq!(a.hostname, b.hostname);
    assert_eq!(a.vendor, b.vendor);
}

#[tokio::test]
async fn discovery_preserves_existing_placement() {
    let f = fixture().await;
    let input = observation();
    let d = apply(&f, input.clone()).await.device.unwrap();
    let rack = location(&f.state.service).await;
    f.state
        .service
        .update_device(
            d.id.parse().unwrap(),
            UpdateDevice {
                rack_id: Patch::Set(rack.id),
                start_unit: Patch::Set(3),
                height_u: Patch::Set(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let after = apply(&f, input).await.device.unwrap();
    assert_eq!(after.rack_id, Some(rack.id.to_string()));
    assert_eq!(after.start_unit, Some(3));
    assert_eq!(after.height_u, Some(2));
}

#[tokio::test]
async fn discovery_preserves_maintenance_and_decommissioned_lifecycle() {
    let f = fixture().await;
    for status in [LifecycleStatus::Maintenance, LifecycleStatus::Decommissioned] {
        let input = observation();
        let d = apply(&f, input.clone()).await.device.unwrap();
        f.state
            .service
            .update_device(
                d.id.parse().unwrap(),
                UpdateDevice { lifecycle_status: Patch::Set(status), ..Default::default() },
            )
            .await
            .unwrap();
        let after = apply(&f, input).await.device.unwrap();
        assert_eq!(after.lifecycle_status, if status == LifecycleStatus::Maintenance { 2 } else { 3 });
    }
}

#[tokio::test]
async fn unique_serial_reuses_device_and_one_logical_interface_for_multiple_ips() {
    let f = fixture().await;
    let serial = Uuid::new_v4().to_string();
    let d = manual_device(&f, Some(serial.clone())).await;
    for _ in 0..2 {
        let mut input = observation();
        input.serial = Some(serial.clone());
        let r = apply(&f, input).await;
        assert!(!r.created);
        assert_eq!(r.device.unwrap().id, d.id.to_string());
    }
    let (interfaces, ips) = graph(&f, &d.id.to_string()).await;
    assert_eq!((interfaces.len(), ips.len()), (1, 2));
}

#[tokio::test]
async fn ambiguous_serial_conflicts_without_adding_endpoint() {
    let f = fixture().await;
    let serial = Uuid::new_v4().to_string();
    let a = manual_device(&f, Some(serial.clone())).await;
    let b = manual_device(&f, Some(serial.clone())).await;
    let mut input = observation();
    input.serial = Some(serial);
    let error = grpc::GrpcApi::new(f.state.clone())
        .apply_discovery_observation(auth_request(input, &f.admin))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(graph(&f, &a.id.to_string()).await.1.is_empty());
    assert!(graph(&f, &b.id.to_string()).await.1.is_empty());
}

#[tokio::test]
async fn existing_ip_with_prefix_reuses_device_and_preserves_existing_endpoint() {
    let f = fixture().await;
    let input = observation();
    let d = manual_device(&f, None).await;
    endpoint(&f, d.id, &format!("{}/64", input.target_ip), false).await;
    let r = apply(&f, input).await;
    assert!(!r.created);
    assert_eq!(r.device.unwrap().id, d.id.to_string());
    let (interfaces, ips) = graph(&f, &d.id.to_string()).await;
    assert_eq!((interfaces.len(), ips.len()), (1, 1));
    assert!(!ips[0].is_management);
}

#[tokio::test]
async fn conflicting_ip_relations_fail_without_reassignment_or_device_updates() {
    let f = fixture().await;
    let input = observation();
    let a = manual_device(&f, None).await;
    let b = manual_device(&f, None).await;
    endpoint(&f, a.id, &input.target_ip, true).await;
    endpoint(&f, b.id, &input.target_ip, true).await;
    let error = grpc::GrpcApi::new(f.state.clone())
        .apply_discovery_observation(auth_request(input, &f.admin))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(f.state.service.get_device(a.id).await.unwrap().hostname.is_none());
    assert_eq!(graph(&f, &b.id.to_string()).await.1.len(), 1);
}

#[tokio::test]
async fn ip_and_serial_pointing_at_different_devices_conflict() {
    let f = fixture().await;
    let mut input = observation();
    let a = manual_device(&f, None).await;
    let serial = Uuid::new_v4().to_string();
    let _b = manual_device(&f, Some(serial.clone())).await;
    endpoint(&f, a.id, &input.target_ip, true).await;
    input.serial = Some(serial);
    assert_eq!(
        grpc::GrpcApi::new(f.state.clone())
            .apply_discovery_observation(auth_request(input, &f.admin))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn concurrent_same_ip_creates_exactly_one_device_and_endpoint() {
    let f = fixture().await;
    let input = observation();
    let (a, b) = tokio::join!(apply(&f, input.clone()), apply(&f, input));
    assert_ne!(a.created, b.created);
    let a = a.device.unwrap();
    assert_eq!(a.id, b.device.unwrap().id);
    assert_eq!(graph(&f, &a.id).await.1.len(), 1);
}

#[tokio::test]
async fn concurrent_different_ips_with_same_serial_share_device() {
    let f = fixture().await;
    let serial = Uuid::new_v4().to_string();
    let mut a = observation();
    let mut b = observation();
    a.serial = Some(serial.clone());
    b.serial = Some(serial);
    let (a, b) = tokio::join!(apply(&f, a), apply(&f, b));
    assert_ne!(a.created, b.created);
    assert_eq!(a.device.unwrap().id, b.device.unwrap().id);
}

#[tokio::test]
async fn admin_only_discovery_rpc_accepts_admin_and_super_admin_but_not_reader() {
    let f = fixture().await;
    let api = grpc::GrpcApi::new(f.state.clone());
    assert_eq!(
        api.apply_discovery_observation(auth_request(observation(), &f.reader)).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert!(api.apply_discovery_observation(auth_request(observation(), &f.admin)).await.is_ok());
    assert!(api.apply_discovery_observation(auth_request(observation(), &f.super_admin)).await.is_ok());
}

#[tokio::test]
async fn discovery_requires_valid_jwt_and_literal_target_ip() {
    let f = fixture().await;
    let api = grpc::GrpcApi::new(f.state.clone());
    assert_eq!(
        api.apply_discovery_observation(tonic::Request::new(observation())).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    for target in ["", "router.example", "192.0.2.1/24"] {
        let mut input = observation();
        input.target_ip = target.into();
        assert_eq!(
            api.apply_discovery_observation(auth_request(input, &f.admin)).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[tokio::test]
async fn existing_manual_grpc_still_locks_fields_and_uses_grpc_audit_source() {
    let f = fixture().await;
    let api = grpc::GrpcApi::new(f.state.clone());
    let d = api
        .create_device(auth_request(
            pb::CreateDeviceRequest {
                hostname: Some("Manual".into()),
                vendor: Some("Vendor".into()),
                ..Default::default()
            },
            &f.admin,
        ))
        .await
        .unwrap()
        .into_inner();
    let fields: Vec<String> =
        sqlx::query_scalar("SELECT field_name FROM field_overrides WHERE entity_id=$1 ORDER BY field_name")
            .bind(Uuid::parse_str(&d.id).unwrap())
            .fetch_all(&f.pool)
            .await
            .unwrap();
    assert_eq!(fields, vec!["hostname", "vendor"]);
    assert_eq!(device_events(&f, &d.id).await[0].source, "grpc");
}

#[tokio::test]
async fn caller_request_id_is_used_for_entire_automatic_graph() {
    let f = fixture().await;
    let request_id = Uuid::new_v4();
    let mut req = auth_request(observation(), &f.admin);
    req.metadata_mut().insert("x-request-id", request_id.to_string().parse().unwrap());
    grpc::GrpcApi::new(f.state.clone()).apply_discovery_observation(req).await.unwrap();
    let events: Vec<(String, String)> =
        sqlx::query_as("SELECT entity_type,action FROM audit_events WHERE request_id=$1")
            .bind(request_id)
            .fetch_all(&f.pool)
            .await
            .unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|(_, a)| a == "create"));
}

async fn fail_audit(f: &Fixture, req: Uuid, ip_audit_only: bool) -> String {
    let name = format!("d3_fail_{}", Uuid::new_v4().simple());
    let condition = if ip_audit_only { "AND NEW.entity_type='ip_addresses'" } else { "" };
    sqlx::raw_sql(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.request_id='{req}'::uuid {condition} THEN RAISE EXCEPTION 'synthetic audit failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$; CREATE TRIGGER {name} BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION {name}()"))
        .execute(&f.pool).await.unwrap();
    name
}
async fn remove_failure(f: &Fixture, name: &str) {
    sqlx::raw_sql(&format!("DROP TRIGGER {name} ON audit_events; DROP FUNCTION {name}()"))
        .execute(&f.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn ip_audit_failure_rolls_back_device_interface_ip_and_all_audits() {
    let f = fixture().await;
    let req = Uuid::new_v4();
    let input = observation();
    let hostname = input.hostname.clone().unwrap();
    let name = fail_audit(&f, req, true).await;
    let mut request = auth_request(input, &f.admin);
    request.metadata_mut().insert("x-request-id", req.to_string().parse().unwrap());
    let result = grpc::GrpcApi::new(f.state.clone()).apply_discovery_observation(request).await;
    remove_failure(&f, &name).await;
    assert!(result.is_err());
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM devices WHERE hostname=$1")
        .bind(hostname)
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE request_id=$1")
        .bind(req)
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn update_audit_failure_rolls_back_observed_device_changes() {
    let f = fixture().await;
    let mut input = observation();
    let d = apply(&f, input.clone()).await.device.unwrap();
    let req = Uuid::new_v4();
    let name = fail_audit(&f, req, false).await;
    input.hostname = Some("must roll back".into());
    let mut request = auth_request(input, &f.admin);
    request.metadata_mut().insert("x-request-id", req.to_string().parse().unwrap());
    let result = grpc::GrpcApi::new(f.state.clone()).apply_discovery_observation(request).await;
    remove_failure(&f, &name).await;
    assert!(result.is_err());
    assert_eq!(f.state.service.get_device(d.id.parse().unwrap()).await.unwrap().hostname, d.hostname);
}
