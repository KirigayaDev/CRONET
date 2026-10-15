use super::*;
use pb::cmdb_server::Cmdb;

async fn target(service: &Service, id: Uuid) -> Option<cmdb_service::poll_targets::PollTarget> {
    let mut cursor = None;
    loop {
        let rows = service.list_poll_targets(20, cursor).await.unwrap();
        if rows.is_empty() {
            return None;
        }
        cursor = rows.last().map(|r| r.device_id);
        if let Some(row) = rows.into_iter().find(|r| r.device_id == id) {
            return Some(row);
        }
    }
}
async fn placed(service: &Service) -> Device {
    let rack = location(service).await;
    service
        .create_device(CreateDevice {
            rack_id: Some(rack.id),
            start_unit: Some(1),
            height_u: Some(1),
            hostname: Some("poll-target".into()),
            ..Default::default()
        })
        .await
        .unwrap()
}
async fn ip(service: &Service, device: Uuid, address: &str, management: bool) {
    let i = service
        .create_interface(CreateInterface {
            device_id: device,
            name: Uuid::new_v4().to_string(),
            if_index: None,
            description: None,
            interface_type: None,
            mac_address: None,
        })
        .await
        .unwrap();
    service
        .create_ip_address(CreateIpAddress {
            interface_id: i.id,
            address: address.parse().unwrap(),
            is_management: management,
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn poll_targets_reader_allowed_and_anonymous_rejected() {
    let f = fixture().await;
    let api = grpc::GrpcApi::new(f.state);
    assert_eq!(
        api.list_poll_targets(tonic::Request::new(pb::ListPollTargetsRequest::default())).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert!(api.list_poll_targets(auth_request(pb::ListPollTargetsRequest::default(), &f.reader)).await.is_ok());
}
#[tokio::test]
async fn poll_targets_include_site_and_all_management_ip_hosts_only() {
    let s = Service::new(test_pool().await);
    let d = placed(&s).await;
    ip(&s, d.id, "192.0.2.1/24", true).await;
    ip(&s, d.id, "2001:db8::1/64", true).await;
    ip(&s, d.id, "192.0.2.2/24", false).await;
    let row = target(&s, d.id).await.unwrap();
    assert_eq!(row.hostname.as_deref(), Some("poll-target"));
    assert!(row.site_id.is_some() && row.site_name.is_some());
    assert_eq!(row.management_ips.len(), 2);
    assert!(row.management_ips.contains(&"192.0.2.1".into()));
    assert!(row.management_ips.contains(&"2001:db8::1".into()));
}
#[tokio::test]
async fn poll_targets_exclude_maintenance_and_decommissioned() {
    let s = Service::new(test_pool().await);
    let d = placed(&s).await;
    s.update_device(
        d.id,
        UpdateDevice { lifecycle_status: Patch::Set(LifecycleStatus::Maintenance), ..Default::default() },
    )
    .await
    .unwrap();
    assert!(target(&s, d.id).await.is_none());
    s.decommission_device(d.id).await.unwrap();
    assert!(target(&s, d.id).await.is_none());
}
#[tokio::test]
async fn poll_targets_unplaced_and_zero_management_ip_are_explicit() {
    let s = Service::new(test_pool().await);
    let d = s.create_device(CreateDevice::default()).await.unwrap();
    let row = target(&s, d.id).await.unwrap();
    assert!(row.site_id.is_none() && row.site_name.is_none());
    assert!(row.management_ips.is_empty());
}
#[tokio::test]
async fn poll_targets_reflect_site_move_and_removal_of_placement() {
    let s = Service::new(test_pool().await);
    let d = placed(&s).await;
    let old = target(&s, d.id).await.unwrap().site_id;
    let rack = location(&s).await;
    s.update_device(d.id, UpdateDevice { rack_id: Patch::Set(rack.id), ..Default::default() }).await.unwrap();
    assert_ne!(target(&s, d.id).await.unwrap().site_id, old);
    s.update_device(d.id, UpdateDevice { rack_id: Patch::Clear, start_unit: Patch::Clear, ..Default::default() })
        .await
        .unwrap();
    assert!(target(&s, d.id).await.unwrap().site_id.is_none());
}
#[tokio::test]
async fn poll_targets_validate_limit_cursor_and_stable_keyset_pages() {
    let f = fixture().await;
    Service::new(f.pool.clone()).create_device(CreateDevice::default()).await.unwrap();
    let api = grpc::GrpcApi::new(f.state);
    for input in [
        pb::ListPollTargetsRequest { limit: Some(0), ..Default::default() },
        pb::ListPollTargetsRequest { limit: Some(201), ..Default::default() },
        pb::ListPollTargetsRequest { after_device_id: Some("bad".into()), ..Default::default() },
    ] {
        assert_eq!(
            api.list_poll_targets(auth_request(input, &f.reader)).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    let first = api
        .list_poll_targets(auth_request(pb::ListPollTargetsRequest { limit: Some(1), ..Default::default() }, &f.reader))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.targets.len(), 1);
    let cursor = first.next_after_device_id.unwrap();
    let second = api
        .list_poll_targets(auth_request(
            pb::ListPollTargetsRequest { limit: Some(1), after_device_id: Some(cursor.clone()) },
            &f.reader,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(second.targets.iter().all(|r| r.device_id > cursor));
}
#[tokio::test]
async fn poll_targets_projection_has_no_mutation_audit_or_ownership_side_effects() {
    let pool = test_pool().await;
    let s = Service::new(pool.clone());
    let d = placed(&s).await;
    let before = s.get_device(d.id).await.unwrap().updated_at;
    target(&s, d.id).await.unwrap();
    assert_eq!(s.get_device(d.id).await.unwrap().updated_at, before);
    for table in ["audit_events", "field_overrides"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE entity_id=$1"))
            .bind(d.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
}
