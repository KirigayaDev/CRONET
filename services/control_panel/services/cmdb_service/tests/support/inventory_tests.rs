use super::*;
use chrono::{Duration as Delta, Utc};
use cmdb_service::{
    inventory::*,
    workflow::{MutationContext, Source},
};
use pb::cmdb_server::Cmdb;

fn component(index: i32, serial: &str) -> InventoryComponent {
    InventoryComponent {
        ent_physical_index: index,
        class: "module".into(),
        contained_in: Some(100_000),
        parent_rel_pos: Some(index),
        serial: Some(serial.into()),
        hardware_rev: Some("A".into()),
        ..Default::default()
    }
}
fn input(components: Vec<InventoryComponent>) -> SubmitInventory {
    SubmitInventory { collected_at: Utc::now(), components }
}
async fn setup() -> (Fixture, Service, Uuid) {
    let f = fixture().await;
    let claims = user(&f.pool, true, false).await;
    let service = f.state.service.for_mutation(MutationContext::user(claims.sub, Source::Http, Uuid::new_v4()));
    let device = service.create_device(CreateDevice::default()).await.unwrap().id;
    (f, service, device)
}
async fn count(pool: &PgPool, device: Uuid, table: &str) -> i64 {
    let sql = match table {
        "snapshots" => "SELECT count(*) FROM inventory_snapshots WHERE device_id=$1",
        _ => "SELECT count(*) FROM audit_events WHERE entity_id=$1 AND after_data ? 'inventory_snapshot'",
    };
    sqlx::query_scalar(sql).bind(device).fetch_one(pool).await.unwrap()
}

#[test]
fn hash_is_order_independent_and_optional_text_is_normalized() {
    let a = component(1, "001");
    let mut b = component(2, "002");
    b.name = Some("  hello  ".into());
    b.model = Some("   ".into());
    let (normalized, hash) = canonicalize(vec![b.clone(), a.clone()]).unwrap();
    assert_eq!(normalized[1].name.as_deref(), Some("hello"));
    assert_eq!(normalized[1].model, None);
    b.name = Some("hello".into());
    b.model = None;
    assert_eq!(hash, canonicalize(vec![a, b]).unwrap().1);
}
#[test]
fn invalid_indices_duplicates_and_limits_are_rejected() {
    for bad in [0, -1] {
        assert!(canonicalize(vec![component(bad, "a")]).is_err());
    }
    let mut a = component(1, "a");
    a.contained_in = Some(-1);
    assert!(canonicalize(vec![a]).is_err());
    let mut a = component(1, "a");
    a.parent_rel_pos = Some(-2);
    assert!(canonicalize(vec![a]).is_err());
    assert!(canonicalize(vec![component(1, "a"), component(1, "b")]).is_err());
    assert!(canonicalize((1..=MAX_COMPONENTS as i32 + 1).map(|i| component(i, "a")).collect()).is_err());
    let mut a = component(1, "a");
    a.description = Some("x".repeat(1025));
    assert!(canonicalize(vec![a]).is_err());
}
#[test]
fn mib_root_zero_and_unknown_position_are_accepted_with_index_fallback() {
    let mut a = component(1, "a");
    a.contained_in = Some(0);
    a.parent_rel_pos = Some(-1);
    let mut b = component(2, "b");
    b.contained_in = Some(0);
    b.parent_rel_pos = Some(-1);
    let (items, _) = canonicalize(vec![a, b]).unwrap();
    let diff = compare_components(&items, &items);
    assert_eq!(diff.unchanged.len(), 2);
}
#[test]
fn diff_detects_revision_added_removed_and_replacement() {
    let old = vec![component(1, "a"), component(2, "b"), component(3, "c")];
    let mut new = vec![component(1, "a"), component(2, "replacement"), component(4, "d")];
    new[0].hardware_rev = Some("B".into());
    let diff = compare_components(&old, &new);
    assert_eq!((diff.added.len(), diff.removed.len(), diff.changed.len(), diff.replaced.len()), (1, 1, 1, 1));
    assert_eq!(diff.changed[0].changed_fields, vec!["hardware_rev"]);
    assert!(diff.changed[0].text_diff.contains("hardware_rev"));
    assert_eq!(diff.replaced[0].old.serial.as_deref(), Some("b"));
}
#[test]
fn serial_move_and_swapped_slots_are_changes_never_replacements() {
    let old = vec![component(1, "a"), component(2, "b")];
    let new = vec![component(2, "a"), component(1, "b")];
    let diff = compare_components(&old, &new);
    assert_eq!(diff.changed.len(), 2);
    assert!(diff.replaced.is_empty());
}
#[test]
fn missing_serial_and_ambiguous_slots_never_claim_replacement() {
    let old = vec![component(1, "a")];
    let mut new = old.clone();
    new[0].serial = None;
    assert_eq!(compare_components(&old, &new).changed.len(), 1);
    let mut old = vec![component(1, "a"), component(2, "b")];
    old[1].parent_rel_pos = Some(1);
    let mut new = old.clone();
    new[0].serial = Some("c".into());
    new[1].serial = Some("d".into());
    let diff = compare_components(&old, &new);
    assert!(diff.replaced.is_empty());
    assert_eq!(diff.added.len(), 2);
    for c in &mut old {
        c.serial = None;
    }
    let diff = compare_components(&old, &old);
    assert_eq!(diff.unchanged.len(), 2);
    assert!(diff.added.is_empty() && diff.removed.is_empty());
}

#[test]
fn maximum_snapshot_and_multibyte_string_boundary_are_accepted() {
    let mut components: Vec<_> = (1..=MAX_COMPONENTS as i32).map(|i| component(i, "a")).collect();
    components[0].name = Some("я".repeat(255));
    assert_eq!(canonicalize(components).unwrap().0.len(), MAX_COMPONENTS);
}

#[tokio::test]
async fn inventory_grpc_wire_contract_roundtrips_all_fields() {
    let (f, _, device) = setup().await;
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = grpc.local_addr().unwrap();
    let (stop, done) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(api::serve(f.state, http, grpc, async {
        let _ = done.await;
    }));
    let mut client = pb::cmdb_client::CmdbClient::connect(format!("http://{addr}")).await.unwrap();
    let mut c = component(1, "0001");
    c.name = Some("module".into());
    c.description = Some("line1\nline2".into());
    c.vendor_type = Some("1.3.6.1.4.1.9".into());
    c.model = Some("M".into());
    c.firmware_rev = Some("F".into());
    c.software_rev = Some("S".into());
    let request = pb::SubmitInventorySnapshotRequest {
        device_id: device.to_string(),
        collected_at: Some(prost_types::Timestamp { seconds: Utc::now().timestamp(), nanos: 123456000 }),
        components: vec![c.clone().into()],
    };
    let created =
        client.submit_inventory_snapshot(auth_request(request, &f.admin)).await.unwrap().into_inner().snapshot.unwrap();
    let latest = client
        .get_latest_inventory(auth_request(pb::IdRequest { id: device.to_string() }, &f.reader))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(latest.id, created.id);
    assert_eq!(InventoryComponent::from(latest.components[0].clone()), c);
    assert_eq!(latest.collected_at.unwrap().nanos, 123456000);
    let diff = client
        .compare_inventory_snapshots(auth_request(
            pb::CompareInventorySnapshotsRequest {
                device_id: device.to_string(),
                previous_id: created.id.clone(),
                current_id: created.id,
            },
            &f.reader,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(diff.unchanged.len(), 1);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn create_bulk_128_components_persists_and_audits_metadata() {
    let (f, s, d) = setup().await;
    let result =
        s.submit_inventory(d, input((1..=128).map(|i| component(i, &format!("s{i}"))).collect())).await.unwrap();
    assert!(!result.unchanged);
    assert_eq!(result.snapshot.metadata.component_count, 128);
    assert_eq!(s.latest_inventory(d).await.unwrap().components.len(), 128);
    let event: Value = sqlx::query_scalar(
        "SELECT after_data FROM audit_events WHERE entity_id=$1 AND after_data ? 'inventory_snapshot'",
    )
    .bind(d)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(event["inventory_snapshot"]["id"], result.snapshot.metadata.id.to_string());
    assert!(event.to_string().len() < 512);
    assert!(!event.to_string().contains("components"));
}
#[tokio::test]
async fn identical_snapshot_deduplicates_without_extra_audit_despite_time_and_order() {
    let (f, s, d) = setup().await;
    let first = s.submit_inventory(d, input(vec![component(1, "a"), component(2, "b")])).await.unwrap();
    let second = s.submit_inventory(d, input(vec![component(2, "b"), component(1, "a")])).await.unwrap();
    assert!(second.unchanged);
    assert_eq!(first.snapshot.metadata.id, second.snapshot.metadata.id);
    assert_eq!(count(&f.pool, d, "snapshots").await, 1);
    assert_eq!(count(&f.pool, d, "audit").await, 1);
}
#[tokio::test]
async fn concurrent_identical_submissions_are_serialized_and_deduplicated() {
    let (f, s, d) = setup().await;
    let (a, b) = tokio::join!(
        s.submit_inventory(d, input(vec![component(1, "a")])),
        s.submit_inventory(d, input(vec![component(1, "a")]))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.unchanged, b.unchanged);
    assert_eq!(a.snapshot.metadata.id, b.snapshot.metadata.id);
    assert_eq!(count(&f.pool, d, "snapshots").await, 1);
    assert_eq!(count(&f.pool, d, "audit").await, 1);
}
#[tokio::test]
async fn latest_uses_collection_time_and_history_diff_is_device_scoped() {
    let (_, s, d) = setup().await;
    let first = s.submit_inventory(d, input(vec![component(1, "a")])).await.unwrap();
    let mut c = component(1, "a");
    c.hardware_rev = Some("B".into());
    let second = s.submit_inventory(d, input(vec![c])).await.unwrap();
    let mut late = input(vec![component(2, "late")]);
    late.collected_at = Utc::now() - Delta::days(1);
    s.submit_inventory(d, late).await.unwrap();
    assert_eq!(s.latest_inventory(d).await.unwrap().metadata.id, second.snapshot.metadata.id);
    let diff = s.compare_inventory(d, first.snapshot.metadata.id, second.snapshot.metadata.id).await.unwrap();
    assert_eq!(diff.changed.len(), 1);
    assert!(matches!(
        s.compare_inventory(Uuid::new_v4(), first.snapshot.metadata.id, second.snapshot.metadata.id).await,
        Err(Error::NotFound { .. })
    ));
}
#[tokio::test]
async fn nonexistent_device_duplicate_and_oversize_leave_no_snapshots() {
    let (f, s, d) = setup().await;
    assert!(matches!(s.submit_inventory(Uuid::new_v4(), input(vec![])).await, Err(Error::NotFound { .. })));
    assert!(s.submit_inventory(d, input(vec![component(1, "a"), component(1, "b")])).await.is_err());
    assert!(s.submit_inventory(d, input((1..=10001).map(|i| component(i, "a")).collect())).await.is_err());
    assert_eq!(count(&f.pool, d, "snapshots").await, 0);
    assert_eq!(count(&f.pool, d, "audit").await, 0);
}
#[tokio::test]
async fn failed_bulk_insert_rolls_back_snapshot_and_audit() {
    let (f, s, d) = setup().await;
    // A device-specific trigger injects a DB failure after the snapshot INSERT.
    // Unique trigger name and device condition isolate this from parallel tests.
    let suffix = Uuid::new_v4().simple().to_string();
    let function = format!("inventory_fail_{suffix}");
    let sql=format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM inventory_snapshots WHERE id=NEW.snapshot_id AND device_id='{d}') THEN RAISE EXCEPTION 'injected inventory failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} BEFORE INSERT ON inventory_components FOR EACH ROW EXECUTE FUNCTION {function}()");
    sqlx::raw_sql(&sql).execute(&f.pool).await.unwrap();
    let result = s.submit_inventory(d, input(vec![component(1, "a")])).await;
    sqlx::raw_sql(&format!("DROP TRIGGER {function} ON inventory_components; DROP FUNCTION {function}()"))
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(count(&f.pool, d, "snapshots").await, 0);
    assert_eq!(count(&f.pool, d, "audit").await, 0);
}
#[tokio::test]
async fn empty_snapshot_can_record_removal_of_all_components() {
    let (_, s, d) = setup().await;
    let a = s.submit_inventory(d, input(vec![component(1, "a")])).await.unwrap();
    let b = s.submit_inventory(d, input(vec![])).await.unwrap();
    assert_eq!(b.snapshot.metadata.component_count, 0);
    assert_eq!(s.compare_inventory(d, a.snapshot.metadata.id, b.snapshot.metadata.id).await.unwrap().removed.len(), 1);
}
#[tokio::test]
async fn inventory_http_read_write_auth_and_diff() {
    let (f, s, d) = setup().await;
    let app = api::http::router(f.state.clone());
    let path = format!("/api/v1/cmdb/devices/{d}/inventory");
    for route in [&path, &format!("{path}/diff?previous_id={d}&current_id={d}"), &format!("{path}/export?format=csv")] {
        assert_eq!(request(&app, "GET", route, None, None).await.0, StatusCode::UNAUTHORIZED);
    }
    let payload = serde_json::to_value(input(vec![component(1, "a")])).unwrap();
    assert_eq!(request(&app, "POST", &path, Some(&f.reader), Some(payload.clone())).await.0, StatusCode::FORBIDDEN);
    let (status, first) = request(&app, "POST", &path, Some(&f.admin), Some(payload)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(request(&app, "GET", &path, Some(&f.reader), None).await.1["components"].as_array().unwrap().len(), 1);
    let next = s.submit_inventory(d, input(vec![component(1, "b")])).await.unwrap();
    let q = format!(
        "{path}/diff?previous_id={}&current_id={}",
        first["snapshot"]["metadata"]["id"].as_str().unwrap(),
        next.snapshot.metadata.id
    );
    assert_eq!(request(&app, "GET", &q, Some(&f.reader), None).await.1["replaced"].as_array().unwrap().len(), 1);
}
#[tokio::test]
async fn inventory_grpc_auth_validation_latest_diff_and_dedup() {
    let (f, _, d) = setup().await;
    let api = grpc::GrpcApi::new(f.state);
    let req = pb::SubmitInventorySnapshotRequest {
        device_id: d.to_string(),
        collected_at: Some(prost_types::Timestamp { seconds: Utc::now().timestamp(), nanos: 0 }),
        components: vec![component(1, "a").into()],
    };
    assert_eq!(
        api.submit_inventory_snapshot(tonic::Request::new(req.clone())).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        api.submit_inventory_snapshot(auth_request(req.clone(), &f.reader)).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let first = api.submit_inventory_snapshot(auth_request(req.clone(), &f.admin)).await.unwrap().into_inner();
    assert!(!first.unchanged);
    assert!(api.submit_inventory_snapshot(auth_request(req.clone(), &f.admin)).await.unwrap().into_inner().unchanged);
    let id = first.snapshot.unwrap().id;
    assert_eq!(
        api.get_latest_inventory(auth_request(pb::IdRequest { id: d.to_string() }, &f.reader))
            .await
            .unwrap()
            .into_inner()
            .id,
        id
    );
    let diff = api
        .compare_inventory_snapshots(auth_request(
            pb::CompareInventorySnapshotsRequest { device_id: d.to_string(), previous_id: id.clone(), current_id: id },
            &f.reader,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(diff.unchanged.len(), 1);
    let mut bad = req;
    bad.collected_at.as_mut().unwrap().nanos = -1;
    assert_eq!(
        api.submit_inventory_snapshot(auth_request(bad, &f.admin)).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}
#[tokio::test]
async fn csv_and_xlsx_exports_are_valid_in_memory_and_preserve_text() {
    use std::io::Read;
    let (f, s, d) = setup().await;
    let mut c = component(1, "000123");
    c.name = Some("=1+1".into());
    c.description = Some("line1\nline2, \"quoted\"".into());
    s.submit_inventory(d, input(vec![c])).await.unwrap();
    let app = api::http::router(f.state);
    for format in ["csv", "xlsx"] {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/api/v1/cmdb/devices/{d}/inventory/export?format={format}"))
                    .header("authorization", format!("Bearer {}", f.reader))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers()["content-disposition"].to_str().unwrap().ends_with(format));
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        if format == "csv" {
            let mut reader = csv::Reader::from_reader(bytes.as_ref());
            assert_eq!(reader.headers().unwrap().len(), 12);
            let row = reader.records().next().unwrap().unwrap();
            assert_eq!(&row[2], "'=1+1");
            assert_eq!(&row[4], "000123");
            assert_eq!(&row[3], "line1\nline2, \"quoted\"");
        } else {
            let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
            let mut xml = String::new();
            zip.by_name("xl/sharedStrings.xml").unwrap().read_to_string(&mut xml).unwrap();
            assert!(xml.contains("000123") && xml.contains("=1+1"));
            let mut sheet = String::new();
            zip.by_name("xl/worksheets/sheet1.xml").unwrap().read_to_string(&mut sheet).unwrap();
            assert!(!sheet.contains("<f>"));
        }
    }
}
