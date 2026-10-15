use super::*;
use cmdb_service::{imports::*, workflow::*};

#[tokio::test]
async fn grpc_missing_or_invalid_request_id_is_generated_once() {
    use pb::cmdb_server::Cmdb;
    let (f, claims, service) = audited().await;
    let token = sign(&claims);
    let api = grpc::GrpcApi::new(f.state);
    for supplied in [None, Some("invalid-id")] {
        let mut req = auth_request(pb::CreateDeviceRequest::default(), &token);
        if let Some(id) = supplied {
            req.metadata_mut().insert("x-request-id", id.parse().unwrap());
        }
        let device = api.create_device(req).await.unwrap().into_inner();
        let events = events(&service, device.id.parse().unwrap()).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].request_id.unwrap().get_version_num(), 4);
    }
}

#[tokio::test]
async fn concurrent_placement_committing_during_confirm_is_caught_after_rack_lock() {
    let (f, _, service) = audited().await;
    let (rack, input) = placed_row(&service, &Uuid::new_v4().to_string(), 1).await;
    let batch = service.upload_devices("concurrent.csv", &csv(&[input])).await.unwrap();
    let mut competitor = f.pool.begin().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *competitor).await.unwrap();
    Service::create_device_in(
        &mut competitor,
        &CreateDevice { rack_id: Some(rack.id), start_unit: Some(1), height_u: Some(2), ..Default::default() },
    )
    .await
    .unwrap();
    let worker_service = service.clone();
    let id = batch.batch.id;
    let worker = tokio::spawn(async move { worker_service.confirm_import(id, true).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))")
                    .bind(pid)
                    .fetch_one(&f.pool)
                    .await
                    .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Confirm must serialize on the competing rack placement");
    competitor.commit().await.unwrap();
    assert!(matches!(worker.await.unwrap(), Err(Error::ImportValidation)));
    assert_eq!(service.get_import(id).await.unwrap().batch.status, "pending");
    assert!(service
        .list_audit_events(&AuditFilter { import_batch_id: Some(id), ..Default::default() }, Pagination::default())
        .await
        .unwrap()
        .is_empty());
}

async fn audited() -> (Fixture, AccessClaims, Service) {
    let f = fixture().await;
    let claims = user(&f.pool, true, false).await;
    let service = f.state.service.for_mutation(MutationContext::user(claims.sub, Source::Http, Uuid::new_v4()));
    (f, claims, service)
}
fn row(hostname: &str) -> [String; 11] {
    let mut row = std::array::from_fn(|_| String::new());
    row[5] = hostname.into();
    row
}
fn csv(rows: &[[String; 11]]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(COLUMNS).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}
async fn placed_row(service: &Service, hostname: &str, start: i32) -> (Rack, [String; 11]) {
    let rack = location(service).await;
    let room = service.get_room(rack.room_id).await.unwrap();
    let site = service.get_site(room.site_id).await.unwrap();
    let mut row = row(hostname);
    row[0] = format!("  {}  ", site.name.to_uppercase());
    row[1] = room.name.to_uppercase();
    row[2] = rack.name.clone();
    row[3] = start.to_string();
    row[4] = "2".into();
    (rack, row)
}
async fn events(service: &Service, id: Uuid) -> Vec<AuditEvent> {
    service
        .list_audit_events(&AuditFilter { entity_id: Some(id), ..Default::default() }, Pagination::new(200, 0).unwrap())
        .await
        .unwrap()
}
async fn multipart(app: &Router, token: Option<&str>, data: &[u8]) -> (StatusCode, Value) {
    let mut bytes=b"--cmdb-boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"inventory.csv\"\r\nContent-Type: text/csv\r\n\r\n".to_vec();
    bytes.extend_from_slice(data);
    bytes.extend_from_slice(b"\r\n--cmdb-boundary--\r\n");
    let mut builder = HttpRequest::builder()
        .method("POST")
        .uri("/api/v1/cmdb/imports/devices")
        .header("content-type", "multipart/form-data; boundary=cmdb-boundary");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = app.clone().oneshot(builder.body(Body::from(bytes)).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn http_audit_actor_request_id_before_after_and_delete_are_atomic() {
    let (f, claims, service) = audited().await;
    let app = api::http::router(f.state.clone());
    let token = sign(&claims);
    let id = Uuid::new_v4();
    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/api/v1/cmdb/sites")
                .header("authorization", format!("Bearer {token}"))
                .header("x-request-id", id.to_string())
                .header("content-type", "application/json")
                .body(Body::from(json!({"name":Uuid::new_v4().to_string()}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-request-id"], id.to_string());
    let body: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let site: Uuid = serde_json::from_value(body["id"].clone()).unwrap();
    let audit = events(&service, site).await;
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].actor_id, Some(claims.sub));
    assert_eq!(audit[0].request_id, Some(id));
    assert_eq!(audit[0].source, "http");
    assert_eq!(audit[0].action, "create");
    assert!(audit[0].before_data.is_none());
    assert_eq!(audit[0].after_data.as_ref().unwrap(), &body);
    assert!(audit[0].changed_at.is_some());
    let path = format!("/api/v1/cmdb/sites/{site}");
    assert_eq!(
        request(&app, "PATCH", &path, Some(&token), Some(json!({"description":"updated"}))).await.0,
        StatusCode::OK
    );
    let audit = events(&service, site).await;
    assert_eq!(audit[0].action, "update");
    assert_ne!(audit[0].before_data, audit[0].after_data);
    assert_eq!(request(&app, "DELETE", &path, Some(&token), None).await.0, StatusCode::NO_CONTENT);
    let audit = events(&service, site).await;
    assert_eq!(audit[0].action, "delete");
    assert!(audit[0].before_data.is_some());
    assert!(audit[0].after_data.is_none());
    let (_, read) = request(
        &app,
        "GET",
        &format!("/api/v1/cmdb/audit-events?entity_type=sites&entity_id={site}&actor_id={}&limit=2", claims.sub),
        Some(&f.reader),
        None,
    )
    .await;
    assert_eq!(read.as_array().unwrap().len(), 2);
    assert_eq!(
        request(&app, "GET", &format!("/api/v1/cmdb/audit-events/{}", audit[0].id), Some(&f.reader), None).await.0,
        StatusCode::OK
    );
    for supplied in [None, Some("invalid-request-id")] {
        let mut builder = HttpRequest::builder().uri("/health/live");
        if let Some(id) = supplied {
            builder = builder.header("x-request-id", id);
        }
        let response = app.clone().oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(Uuid::parse_str(response.headers()["x-request-id"].to_str().unwrap()).unwrap().get_version_num(), 4);
    }
}

#[tokio::test]
async fn every_grpc_mutation_has_audit_and_technical_overrides() {
    use pb::cmdb_server::Cmdb;
    let (f, claims, service) = audited().await;
    let token = sign(&claims);
    let api = grpc::GrpcApi::new(f.state.clone());
    let rid = Uuid::new_v4();
    fn req<T>(v: T, token: &str, id: Uuid) -> tonic::Request<T> {
        let mut req = auth_request(v, token);
        req.metadata_mut().insert("x-request-id", id.to_string().parse().unwrap());
        req
    }
    let device = api
        .create_device(req(
            pb::CreateDeviceRequest { hostname: Some(Uuid::new_v4().to_string()), ..Default::default() },
            &token,
            rid,
        ))
        .await
        .unwrap()
        .into_inner();
    api.update_device(req(
        pb::UpdateDeviceRequest {
            id: device.id.clone(),
            vendor: Some(pb::StringPatch { change: Some(pb::string_patch::Change::Value("manual".into())) }),
            ..Default::default()
        },
        &token,
        rid,
    ))
    .await
    .unwrap();
    api.decommission_device(req(pb::IdRequest { id: device.id.clone() }, &token, rid)).await.unwrap();
    let interface = api
        .create_interface(req(
            pb::CreateInterfaceRequest { device_id: device.id.clone(), name: "eth0".into(), ..Default::default() },
            &token,
            rid,
        ))
        .await
        .unwrap()
        .into_inner();
    api.update_interface(req(
        pb::UpdateInterfaceRequest {
            id: interface.id.clone(),
            description: Some(pb::StringPatch { change: Some(pb::string_patch::Change::Clear(0)) }),
            ..Default::default()
        },
        &token,
        rid,
    ))
    .await
    .unwrap();
    let ip = api
        .create_ip_address(req(
            pb::CreateIpAddressRequest {
                interface_id: interface.id.clone(),
                address: "192.0.2.7/24".into(),
                is_management: false,
            },
            &token,
            rid,
        ))
        .await
        .unwrap()
        .into_inner();
    api.update_ip_address(req(
        pb::UpdateIpAddressRequest { id: ip.id.clone(), address: Some("2001:db8::7/64".into()), ..Default::default() },
        &token,
        rid,
    ))
    .await
    .unwrap();
    let audit = service
        .list_audit_events(&AuditFilter { request_id: Some(rid), ..Default::default() }, Pagination::default())
        .await
        .unwrap();
    assert_eq!(audit.len(), 7);
    assert!(audit.iter().all(|e| e.source == "grpc" && e.actor_id == Some(claims.sub) && e.changed_at.is_some()));
    let d = events(&service, device.id.parse().unwrap()).await;
    assert_eq!(d[0].action, "decommission");
    assert_eq!(d[0].before_data.as_ref().unwrap()["lifecycle_status"], "active");
    assert_eq!(d[0].after_data.as_ref().unwrap()["lifecycle_status"], "decommissioned");
    assert!(service.is_field_overridden(EntityType::Devices, device.id.parse().unwrap(), "vendor").await.unwrap());
    assert!(service
        .is_field_overridden(EntityType::Interfaces, interface.id.parse().unwrap(), "description")
        .await
        .unwrap());
    assert!(service.is_field_overridden(EntityType::IpAddresses, ip.id.parse().unwrap(), "address").await.unwrap());
}

#[tokio::test]
async fn audit_and_override_failures_roll_back_the_other_writes() {
    let (f, claims, service) = audited().await;
    let name = Uuid::new_v4().to_string();
    let mut context = MutationContext::user(claims.sub, Source::CsvImport, Uuid::new_v4());
    context.import_batch_id = Some(Uuid::new_v4());
    assert!(matches!(
        service.for_mutation(context).create_site(CreateSite { name: name.clone(), description: None }).await,
        Err(Error::Conflict { .. })
    ));
    assert!(service.find_site_by_name(&name).await.unwrap().is_none());
    assert!(service
        .list_audit_events(
            &AuditFilter { request_id: Some(context.request_id), ..Default::default() },
            Pagination::default()
        )
        .await
        .unwrap()
        .is_empty());
    let context = MutationContext::user(Uuid::new_v4(), Source::Http, Uuid::new_v4());
    assert!(matches!(
        service
            .for_mutation(context)
            .create_device(CreateDevice { hostname: Some(name.clone()), ..Default::default() })
            .await,
        Err(Error::Conflict { .. })
    ));
    assert!(service
        .list_devices(&DeviceFilter { hostname: Some(name), ..Default::default() })
        .await
        .unwrap()
        .is_empty());
    assert!(service
        .list_audit_events(
            &AuditFilter { request_id: Some(context.request_id), ..Default::default() },
            Pagination::default()
        )
        .await
        .unwrap()
        .is_empty());
    f.pool.close().await;
}

#[tokio::test]
async fn real_serialization_retry_leaves_one_entity_and_one_audit() {
    let (f, claims, service) = audited().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let name = format!("cmdb_retry_{suffix}");
    let marker = Uuid::new_v4().to_string();
    sqlx::raw_sql(&format!(
        "CREATE SEQUENCE {name}; CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF EXISTS(SELECT 1 FROM devices WHERE id=NEW.entity_id AND hostname='{marker}') THEN IF nextval('{name}')=1 THEN
        RAISE EXCEPTION 'test retry' USING ERRCODE='40001'; END IF; END IF; RETURN NEW; END $$;
        CREATE TRIGGER {name} BEFORE INSERT ON field_overrides FOR EACH ROW EXECUTE FUNCTION {name}();"
    ))
    .execute(&f.pool)
    .await
    .unwrap();
    let rid = Uuid::new_v4();
    let service = service.for_mutation(MutationContext::user(claims.sub, Source::Http, rid));
    let device =
        service.create_device(CreateDevice { hostname: Some(marker.clone()), ..Default::default() }).await.unwrap();
    let attempts: i64 = sqlx::query_scalar(&format!("SELECT last_value FROM {name}")).fetch_one(&f.pool).await.unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(
        service.list_devices(&DeviceFilter { hostname: Some(marker), ..Default::default() }).await.unwrap().len(),
        1
    );
    let audit = events(&service, device.id).await;
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].request_id, Some(rid));
    assert_eq!(
        service
            .list_audit_events(&AuditFilter { request_id: Some(rid), ..Default::default() }, Pagination::default())
            .await
            .unwrap()
            .len(),
        1
    );
    sqlx::raw_sql(&format!("DROP TRIGGER {name} ON field_overrides; DROP FUNCTION {name}(); DROP SEQUENCE {name};"))
        .execute(&f.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn manual_override_allowlist_clear_and_last_actor_are_enforced() {
    let (f, claims, service) = audited().await;
    let rack = location(&service).await;
    let device = service
        .create_device(CreateDevice {
            rack_id: Some(rack.id),
            start_unit: Some(1),
            height_u: Some(1),
            hostname: Some("manual-host".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let locks =
        service.list_field_overrides(Some(EntityType::Devices), Some(device.id), Pagination::default()).await.unwrap();
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].field_name, "hostname");
    assert_eq!(locks[0].locked_by, claims.sub);
    assert!(!service.is_field_overridden(EntityType::Devices, device.id, "vendor").await.unwrap());
    service
        .update_device(device.id, UpdateDevice { vendor: Patch::Set("vendor".into()), ..Default::default() })
        .await
        .unwrap();
    service.update_device(device.id, UpdateDevice { vendor: Patch::Clear, ..Default::default() }).await.unwrap();
    assert!(service.get_device(device.id).await.unwrap().vendor.is_none());
    assert!(service.is_field_overridden(EntityType::Devices, device.id, "vendor").await.unwrap());
    for field in ["rack_id", "start_unit", "height_u", "lifecycle_status", "made_up_field"] {
        assert!(matches!(
            service.set_field_override(EntityType::Devices, device.id, field).await,
            Err(Error::Validation(_))
        ));
    }
    assert!(matches!(
        service.set_field_override(EntityType::Devices, Uuid::new_v4(), "hostname").await,
        Err(Error::NotFound { .. })
    ));
    let a = service.set_field_override(EntityType::Devices, device.id, "vendor").await.unwrap();
    let other = user(&f.pool, true, false).await;
    let b = service
        .for_mutation(MutationContext::user(other.sub, Source::Http, Uuid::new_v4()))
        .set_field_override(EntityType::Devices, device.id, "vendor")
        .await
        .unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(b.locked_by, other.sub);
    assert!(b.locked_at >= a.locked_at);
    service.clear_field_override(EntityType::Devices, device.id, "vendor").await.unwrap();
    assert!(!service.is_field_overridden(EntityType::Devices, device.id, "vendor").await.unwrap());
    let audit = events(&service, device.id).await;
    assert!(audit.iter().any(|e| e.action == "field_override_set"));
    assert_eq!(audit[0].action, "field_override_clear");
    let app = api::http::router(f.state.clone());
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/v1/cmdb/field-overrides",
            Some(&f.reader),
            Some(json!({"entity_type":"devices","entity_id":device.id,"field_name":"hostname"}))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let delete = format!("/api/v1/cmdb/field-overrides/devices/{}/hostname", device.id);
    assert_eq!(request(&app, "DELETE", &delete, Some(&f.reader), None).await.0, StatusCode::FORBIDDEN);
    assert_eq!(request(&app, "DELETE", &delete, Some(&f.admin), None).await.0, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn interface_delete_cleans_markers_and_keeps_snapshot() {
    let (_, _, service) = audited().await;
    let device = service.create_device(CreateDevice::default()).await.unwrap();
    let interface = service
        .create_interface(CreateInterface {
            device_id: device.id,
            name: "eth0".into(),
            if_index: Some(1),
            description: Some("manual".into()),
            interface_type: Some("ethernet".into()),
            mac_address: Some("02:00:00:00:00:01".parse().unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(
        service
            .list_field_overrides(Some(EntityType::Interfaces), Some(interface.id), Pagination::default())
            .await
            .unwrap()
            .len(),
        5
    );
    service.delete_interface(interface.id).await.unwrap();
    assert!(service
        .list_field_overrides(Some(EntityType::Interfaces), Some(interface.id), Pagination::default())
        .await
        .unwrap()
        .is_empty());
    let audit = events(&service, interface.id).await;
    assert_eq!(audit[0].action, "delete");
    assert_eq!(audit[0].before_data.as_ref().unwrap()["name"], "eth0");
    assert!(audit[0].after_data.is_none());
}

#[tokio::test]
async fn csv_validation_errors_cover_location_dimensions_lifecycle_and_bounds() {
    let (_, _, service) = audited().await;
    let (_, valid) = placed_row(&service, "csv-valid", 1).await;
    let mut cases = Vec::new();
    let mut value = valid.clone();
    value[2] = "missing-rack".into();
    cases.push((value, "not_found"));
    let mut value = valid.clone();
    value[1] = "wrong-room".into();
    cases.push((value, "not_found"));
    let mut value = valid.clone();
    value[0] = "missing-site".into();
    cases.push((value, "not_found"));
    let mut value = valid.clone();
    value[3] = "no-number".into();
    cases.push((value, "positive_integer"));
    let mut value = valid.clone();
    value[4] = "0".into();
    cases.push((value, "positive_integer"));
    let mut value = valid.clone();
    value[10] = "retired".into();
    cases.push((value, "invalid"));
    let mut value = valid.clone();
    value[3] = "2147483647".into();
    cases.push((value, "out_of_bounds"));
    let mut value = valid.clone();
    value[0] = String::new();
    cases.push((value, "incomplete"));
    let mut value = row("height-only");
    value[4] = "-1".into();
    cases.push((value, "positive_integer"));
    let batch = service
        .upload_devices("invalid.csv", &csv(&cases.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()))
        .await
        .unwrap();
    assert_eq!(batch.batch.status, "pending");
    assert_eq!(batch.invalid_rows, cases.len() as i64);
    let rows = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap();
    for (row, (_, code)) in rows.iter().zip(cases.iter()) {
        assert_eq!(row.status, "invalid");
        assert!(row.validation_errors.as_array().unwrap().iter().any(|e| e["code"] == *code));
    }
    assert!(matches!(service.confirm_import(batch.batch.id, true).await, Err(Error::ImportValidation)));
}

#[tokio::test]
async fn csv_detects_all_lifecycle_occupancy_and_intra_batch_overlaps() {
    let (_, _, service) = audited().await;
    let (rack, first) = placed_row(&service, "batch-a", 1).await;
    service
        .create_device(CreateDevice {
            rack_id: Some(rack.id),
            start_unit: Some(10),
            height_u: Some(2),
            lifecycle_status: LifecycleStatus::Decommissioned,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut second = first.clone();
    second[3] = "2".into();
    second[5] = "batch-b".into();
    second[10] = "maintenance".into();
    let mut occupied = first.clone();
    occupied[3] = "10".into();
    occupied[5] = "batch-c".into();
    let batch = service.upload_devices("overlap.csv", &csv(&[first, second, occupied])).await.unwrap();
    assert_eq!(batch.invalid_rows, 3);
    let rows = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap();
    assert!(rows[0].validation_errors.to_string().contains("batch_overlap"));
    assert!(rows[1].validation_errors.to_string().contains("batch_overlap"));
    assert!(rows[2].validation_errors.to_string().contains("occupied"));
    let fixed = service
        .edit_import_row(batch.batch.id, rows[1].id, ImportRowPatch { start_unit: Patch::Set(4), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(fixed.status, "valid");
    let rows = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap();
    assert_eq!(rows[0].status, "valid");
}

#[tokio::test]
async fn csv_warning_acceptance_import_audit_and_manual_fields_are_correct() {
    let (f, claims, service) = audited().await;
    let hostname = Uuid::new_v4().to_string();
    let serial = Uuid::new_v4().to_string();
    service
        .create_device(CreateDevice {
            hostname: Some(hostname.clone()),
            serial: Some(serial.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut first = row(&hostname);
    first[9] = serial;
    first[4] = "2".into();
    let second = row(&Uuid::new_v4().to_string());
    let batch = service.upload_devices("warnings.csv", &csv(&[first, second])).await.unwrap();
    assert_eq!(batch.batch.status, "validated");
    assert_eq!(batch.warning_rows, 1);
    let staged = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap();
    assert_eq!(staged[0].validation_warnings.as_array().unwrap().len(), 2);
    assert!(matches!(service.confirm_import(batch.batch.id, false).await, Err(Error::InvalidState(_))));
    assert!(service
        .list_audit_events(
            &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
            Pagination::default()
        )
        .await
        .unwrap()
        .is_empty());
    let result = service.confirm_import(batch.batch.id, true).await.unwrap();
    assert_eq!(result.batch.status, "confirmed");
    assert!(result.batch.confirmed_at.is_some());
    let audit = service
        .list_audit_events(
            &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
            Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(audit.len(), 2);
    assert!(audit.iter().all(|e| e.source == "csv_import"
        && e.action == "create"
        && e.actor_id == Some(claims.sub)
        && e.before_data.is_none()
        && e.changed_at.is_some()));
    for event in &audit {
        assert!(service.is_field_overridden(EntityType::Devices, event.entity_id, "hostname").await.unwrap());
        assert!(!service.is_field_overridden(EntityType::Devices, event.entity_id, "vendor").await.unwrap());
    }
    assert!(service
        .list_import_rows(batch.batch.id, Pagination::default())
        .await
        .unwrap()
        .iter()
        .all(|r| r.status == "imported"));
    assert!(matches!(service.confirm_import(batch.batch.id, true).await, Err(Error::InvalidState(_))));
    assert!(matches!(
        service.edit_import_row(batch.batch.id, staged[0].id, ImportRowPatch::default()).await,
        Err(Error::InvalidState(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events WHERE import_batch_id=$1")
            .bind(batch.batch.id)
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn row_edit_preserves_raw_data_clear_intent_and_revalidates_the_batch() {
    let (_, _, service) = audited().await;
    let corrected = format!("corrected-{}", Uuid::new_v4());
    let mut input = row("  old host  ");
    input[4] = "invalid".into();
    input[7] = "raw vendor".into();
    let batch = service.upload_devices("editable.csv", &csv(&[input])).await.unwrap();
    let before = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap().remove(0);
    assert_eq!(before.raw_data["hostname"], "  old host  ");
    assert_eq!(before.working_data["hostname"], "old host");
    let after = service
        .edit_import_row(
            batch.batch.id,
            before.id,
            ImportRowPatch {
                height_u: Patch::Set(3),
                vendor: Patch::Clear,
                hostname: Patch::Set(corrected.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(after.raw_data, before.raw_data);
    assert_eq!(after.status, "valid");
    assert_eq!(after.working_data["hostname"], corrected);
    assert_eq!(service.revalidate_import(batch.batch.id).await.unwrap().batch.status, "validated");
    service.confirm_import(batch.batch.id, false).await.unwrap();
    let audit = service
        .list_audit_events(
            &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
            Pagination::default(),
        )
        .await
        .unwrap();
    let device = service.get_device(audit[0].entity_id).await.unwrap();
    assert!(device.vendor.is_none());
    assert_eq!(device.height_u, Some(3));
    assert!(service.is_field_overridden(EntityType::Devices, device.id, "vendor").await.unwrap());
}

#[tokio::test]
async fn preview_to_confirm_inventory_race_revalidates_without_canonical_writes() {
    let (_, _, service) = audited().await;
    let (rack, input) = placed_row(&service, "race-import", 1).await;
    let batch = service.upload_devices("race.csv", &csv(&[input])).await.unwrap();
    assert_eq!(batch.batch.status, "validated");
    service
        .create_device(CreateDevice {
            rack_id: Some(rack.id),
            start_unit: Some(1),
            height_u: Some(1),
            lifecycle_status: LifecycleStatus::Maintenance,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(service.confirm_import(batch.batch.id, true).await, Err(Error::ImportValidation)));
    let updated = service.get_import(batch.batch.id).await.unwrap();
    assert_eq!(updated.batch.status, "pending");
    assert!(updated.batch.confirmed_at.is_none());
    let rows = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap();
    assert_eq!(rows[0].status, "invalid");
    assert!(rows[0].validation_errors.to_string().contains("occupied"));
    assert!(service
        .list_audit_events(
            &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
            Pagination::default()
        )
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failure_on_second_csv_device_rolls_back_devices_audit_and_overrides() {
    let (f, claims, service) = audited().await;
    let first = Uuid::new_v4().to_string();
    let second = Uuid::new_v4().to_string();
    let name = format!("cmdb_fail_{}", Uuid::new_v4().simple());
    let batch = service.upload_devices("rollback.csv", &csv(&[row(&first), row(&second)])).await.unwrap();
    sqlx::raw_sql(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.hostname='{second}' THEN RAISE EXCEPTION 'test row failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER {name} BEFORE INSERT ON devices FOR EACH ROW EXECUTE FUNCTION {name}();")).execute(&f.pool).await.unwrap();
    assert!(matches!(service.confirm_import(batch.batch.id, true).await, Err(Error::Validation(_))));
    for hostname in [first, second] {
        assert!(service
            .list_devices(&DeviceFilter { hostname: Some(hostname), ..Default::default() })
            .await
            .unwrap()
            .is_empty());
    }
    assert!(service
        .list_audit_events(
            &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
            Pagination::default()
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(service.get_import(batch.batch.id).await.unwrap().batch.status, "validated");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM field_overrides WHERE locked_by=$1")
            .bind(claims.sub)
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    assert!(service
        .list_import_rows(batch.batch.id, Pagination::default())
        .await
        .unwrap()
        .iter()
        .all(|r| r.status == "valid"));
    sqlx::raw_sql(&format!("DROP TRIGGER {name} ON devices; DROP FUNCTION {name}();")).execute(&f.pool).await.unwrap();
}

#[tokio::test]
async fn concurrent_confirm_imports_once_and_cancelled_batch_is_final() {
    let (_, _, service) = audited().await;
    let batch = service.upload_devices("once.csv", &csv(&[row("once-import")])).await.unwrap();
    let (a, b) =
        tokio::join!(service.confirm_import(batch.batch.id, true), service.confirm_import(batch.batch.id, true));
    assert!(matches!((a, b), (Ok(_), Err(Error::InvalidState(_))) | (Err(Error::InvalidState(_)), Ok(_))));
    assert_eq!(
        service
            .list_audit_events(
                &AuditFilter { import_batch_id: Some(batch.batch.id), ..Default::default() },
                Pagination::default()
            )
            .await
            .unwrap()
            .len(),
        1
    );
    let batch = service.upload_devices("cancel.csv", &csv(&[row("cancel-import")])).await.unwrap();
    let id = service.list_import_rows(batch.batch.id, Pagination::default()).await.unwrap()[0].id;
    assert_eq!(service.cancel_import(batch.batch.id).await.unwrap().batch.status, "cancelled");
    assert!(matches!(service.confirm_import(batch.batch.id, true).await, Err(Error::InvalidState(_))));
    assert!(matches!(
        service.edit_import_row(batch.batch.id, id, ImportRowPatch::default()).await,
        Err(Error::InvalidState(_))
    ));
    assert!(matches!(service.revalidate_import(batch.batch.id).await, Err(Error::InvalidState(_))));
}

#[tokio::test]
async fn http_csv_upload_preview_edit_confirm_and_cancel_endpoints_work() {
    let f = fixture().await;
    let app = api::http::router(f.state.clone());
    assert_eq!(multipart(&app, None, &csv(&[row("upload")])).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(multipart(&app, Some(&f.reader), &csv(&[row("upload")])).await.0, StatusCode::FORBIDDEN);
    let (status, batch) = multipart(&app, Some(&f.admin), &csv(&[row(&Uuid::new_v4().to_string())])).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = batch["id"].as_str().unwrap();
    let base = format!("/api/v1/cmdb/imports/{id}");
    assert_eq!(request(&app, "GET", &base, Some(&f.reader), None).await.0, StatusCode::OK);
    let (_, rows) = request(&app, "GET", &format!("{base}/rows"), Some(&f.reader), None).await;
    assert!(rows[0]["working_data"].get("_cleared_fields").is_none());
    let row_id = rows[0]["id"].as_str().unwrap();
    assert_eq!(
        request(
            &app,
            "PATCH",
            &format!("{base}/rows/{row_id}"),
            Some(&f.admin),
            Some(json!({"vendor":null,"height_u":2}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(request(&app, "POST", &format!("{base}/revalidate"), Some(&f.admin), None).await.0, StatusCode::OK);
    for bad in [json!({"vendor":"invalid\0text"}), json!({"raw_data":{"hostname":"changed"}})] {
        assert_eq!(
            request(&app, "PATCH", &format!("{base}/rows/{row_id}"), Some(&f.admin), Some(bad)).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, result) =
        request(&app, "POST", &format!("{base}/confirm"), Some(&f.admin), Some(json!({"accept_warnings":false}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["status"], "confirmed");
    assert_eq!(
        request(&app, "POST", &format!("{base}/confirm"), Some(&f.admin), Some(json!({"accept_warnings":true})))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (_, batch) = multipart(&app, Some(&f.super_admin), &csv(&[row("cancel via HTTP")])).await;
    assert_eq!(
        request(
            &app,
            "POST",
            &format!("/api/v1/cmdb/imports/{}/cancel", batch["id"].as_str().unwrap()),
            Some(&f.super_admin),
            None
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, "GET", &format!("{base}/rows?limit=0"), Some(&f.reader), None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "GET", "/api/v1/cmdb/audit-events?limit=201", Some(&f.reader), None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "GET", "/api/v1/cmdb/field-overrides?offset=-1", Some(&f.reader), None).await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn csv_size_headers_quoting_utf8_and_row_limits_fail_safely() {
    let f = fixture().await;
    let app = api::http::router(f.state.clone());
    let huge = vec![b'a'; MAX_FILE_BYTES + 1];
    assert!(parse_csv(&huge).is_err());
    assert_eq!(multipart(&app, Some(&f.admin), &huge).await.0, StatusCode::BAD_REQUEST);
    let mut too_many = COLUMNS.join(",").into_bytes();
    too_many.push(b'\n');
    for _ in 0..=MAX_ROWS {
        too_many.extend_from_slice(b",,,,,test,,,,,\n");
    }
    assert!(parse_csv(&too_many).is_err());
    let mut exact = COLUMNS.join(",").into_bytes();
    exact.push(b'\n');
    for _ in 0..MAX_ROWS {
        exact.extend_from_slice(b",,,,,test,,,,,\n");
    }
    assert_eq!(parse_csv(&exact).unwrap().len(), MAX_ROWS);
    for bytes in [
        b"hostname\nx\n".to_vec(),
        COLUMNS.join(",").replace("serial", "hostname").into_bytes(),
        vec![0xff],
        format!("{}\n\"unclosed", COLUMNS.join(",")).into_bytes(),
        format!("{}\n,,,,,bad\"quote,,,,,", COLUMNS.join(",")).into_bytes(),
        format!("{}\nshort,row", COLUMNS.join(",")).into_bytes(),
        format!("{}\n,,,,,nul\0value,,,,,", COLUMNS.join(",")).into_bytes(),
    ] {
        assert!(parse_csv(&bytes).is_err());
        assert_eq!(multipart(&app, Some(&f.admin), &bytes).await.0, StatusCode::BAD_REQUEST);
    }
    let parsed = parse_csv(&csv(&[row("quoted, host\nwith \"quotes\"")])).unwrap();
    assert_eq!(parsed[0].raw["hostname"], "quoted, host\nwith \"quotes\"");
}

#[tokio::test]
async fn openapi_describes_workflow_multipart_and_hides_internal_intent() {
    let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
    for name in [
        "AuditEvent",
        "FieldOverride",
        "ImportBatch",
        "BatchSummary",
        "ImportRow",
        "ImportRowPatch",
        "ValidationIssue",
        "ConfirmInput",
    ] {
        assert!(spec["components"]["schemas"][name].is_object(), "{name}");
    }
    assert!(spec["paths"]["/api/v1/cmdb/imports/devices"]["post"]["requestBody"]["content"]["multipart/form-data"]
        .is_object());
    assert_eq!(spec["components"]["schemas"]["CsvUpload"]["properties"]["file"]["format"], "binary");
    assert!(spec["components"]["schemas"]["ImportData"]["properties"].get("_cleared_fields").is_none());
    for (name, schema) in spec["components"]["schemas"].as_object().unwrap() {
        assert!(
            serde_json::from_value::<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>(schema.clone()).is_ok(),
            "Invalid schema {name}: {schema}"
        );
    }
    let _: utoipa::openapi::OpenApi = serde_json::from_value(spec).unwrap();
}
