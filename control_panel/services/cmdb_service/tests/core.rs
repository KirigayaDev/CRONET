//! Requires the disposable DB created by tests/compose.core-test.yml.
use cmdb_service::{
    config::Config, db, models::*, repository::Repository, service::RetryPolicy, Error, Service,
};
use sqlx::{postgres::PgConnectOptions, PgPool};
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;

async fn core() -> (PgPool, Service) {
    let url = std::env::var("CMDB_TEST_DATABASE_URL")
        .expect("Run integration tests with the dedicated tests/compose.core-test.yml database");
    let options = PgConnectOptions::from_str(&url).expect("Invalid test database URL");
    assert_eq!(
        options.get_database(),
        Some("cronet_cmdb_test_schema"),
        "Refusing a non-test database"
    );
    let pool = db::connect(&Config::from_database_url(&url).unwrap())
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        name, "cronet_cmdb_test_schema",
        "Refusing a non-test database"
    );
    let head: String = sqlx::query_scalar("SELECT version_num FROM alembic_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        head, "8b6e2f4a9c10",
        "Apply the existing CMDB Alembic chain first"
    );
    (pool.clone(), Service::new(pool))
}

fn site_input() -> CreateSite {
    CreateSite {
        name: format!("Site {}", Uuid::new_v4()),
        description: Some("original".into()),
    }
}

async fn hierarchy(service: &Service) -> (Site, Room, Rack) {
    let site = service.create_site(site_input()).await.unwrap();
    let room = service
        .create_room(CreateRoom {
            site_id: site.id,
            name: "Room A".into(),
            description: None,
        })
        .await
        .unwrap();
    let rack = service
        .create_rack(CreateRack {
            room_id: room.id,
            name: "Rack A".into(),
            height_u: 42,
            description: None,
        })
        .await
        .unwrap();
    (site, room, rack)
}

fn placed(rack: Uuid, start: i32, status: LifecycleStatus) -> CreateDevice {
    CreateDevice {
        rack_id: Some(rack),
        start_unit: Some(start),
        height_u: Some(2),
        lifecycle_status: status,
        ..Default::default()
    }
}

fn interface_input(device: Uuid) -> CreateInterface {
    CreateInterface {
        device_id: device,
        if_index: Some(1),
        name: "eth0".into(),
        description: Some("uplink".into()),
        interface_type: Some("ethernet".into()),
        mac_address: Some("aa:bb:cc:dd:ee:ff".parse().unwrap()),
    }
}

fn assert_conflict(error: Error, expected_code: &str) {
    match error {
        Error::Conflict {
            sqlstate,
            constraint,
        } => {
            assert_eq!(sqlstate, expected_code);
            assert!(constraint.is_some());
        }
        other => panic!("Expected constraint conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn pool_and_minimal_binary_startup() {
    let (pool, _) = core().await;
    db::check_connection(&pool).await.unwrap();
    let missing = std::process::Command::new(env!("CARGO_BIN_EXE_cmdb-service"))
        .env_remove("DATABASE_URL")
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8(missing.stderr)
        .unwrap()
        .contains("DATABASE_URL is required"));
    let ready = std::process::Command::new(env!("CARGO_BIN_EXE_cmdb-service"))
        .env_remove("REDIS_URL")
        .env(
            "DATABASE_URL",
            std::env::var("CMDB_TEST_DATABASE_URL").unwrap(),
        )
        .output()
        .unwrap();
    assert!(!ready.status.success());
    assert!(String::from_utf8(ready.stderr)
        .unwrap()
        .contains("REDIS_URL is required"));
}

#[tokio::test]
async fn site_crud_and_patch_null_semantics() {
    let (_, service) = core().await;
    let site = service.create_site(site_input()).await.unwrap();
    assert_eq!(
        service
            .get_site(site.id)
            .await
            .unwrap()
            .description
            .as_deref(),
        Some("original")
    );
    assert!(service
        .list_sites_page(Pagination::new(200, 0).unwrap())
        .await
        .unwrap()
        .iter()
        .any(|row| row.id == site.id));
    let updated = service
        .update_site(
            site.id,
            UpdateSite {
                name: Patch::Set(format!("Renamed {}", site.id)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.description.as_deref(), Some("original"));
    let cleared = service
        .update_site(
            site.id,
            UpdateSite {
                description: Patch::Clear,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(cleared.description.is_none());
    assert!(cleared.updated_at >= site.updated_at);
    service.delete_site(site.id).await.unwrap();
    assert!(matches!(
        service.get_site(site.id).await,
        Err(Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn room_and_rack_relations_crud_and_parent_filters() {
    let (_, service) = core().await;
    let (site, room, rack) = hierarchy(&service).await;
    let other = service.create_site(site_input()).await.unwrap();
    assert_eq!(service.get_room(room.id).await.unwrap().site_id, site.id);
    assert_eq!(service.get_rack(rack.id).await.unwrap().room_id, room.id);
    assert_eq!(service.list_rooms(Some(site.id)).await.unwrap().len(), 1);
    assert!(service.list_rooms(Some(other.id)).await.unwrap().is_empty());
    assert_eq!(service.list_racks(Some(room.id)).await.unwrap().len(), 1);
    assert!(service
        .list_racks(Some(Uuid::new_v4()))
        .await
        .unwrap()
        .is_empty());
    assert!(service
        .list_rooms(None)
        .await
        .unwrap()
        .iter()
        .any(|row| row.id == room.id));
    assert!(service
        .list_racks(None)
        .await
        .unwrap()
        .iter()
        .any(|row| row.id == rack.id));
    let moved = service
        .update_room(
            room.id,
            UpdateRoom {
                site_id: Patch::Set(other.id),
                name: Patch::Set("Room B".into()),
                description: Patch::Set("moved".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(moved.site_id, other.id);
    let changed = service
        .update_rack(
            rack.id,
            UpdateRack {
                height_u: Patch::Set(48),
                name: Patch::Set("Rack B".into()),
                description: Patch::Set("new".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(changed.height_u, 48);
    assert_eq!(
        service
            .find_room_by_name(other.id, " room  b ")
            .await
            .unwrap()
            .unwrap()
            .id,
        room.id
    );
    assert_eq!(
        service
            .find_rack_by_name(room.id, " RACK\tB ")
            .await
            .unwrap()
            .unwrap()
            .id,
        rack.id
    );
    assert_conflict(service.delete_room(room.id).await.unwrap_err(), "23503");
    service.delete_rack(rack.id).await.unwrap();
    service.delete_room(room.id).await.unwrap();
    service.delete_site(site.id).await.unwrap();
    service.delete_site(other.id).await.unwrap();
}

#[tokio::test]
async fn normalized_lookup_conflicts_and_parameterized_names() {
    let (_, service) = core().await;
    let input = site_input();
    let site = service.create_site(input.clone()).await.unwrap();
    let equivalent = format!(" \t{}  ", input.name.to_uppercase().replace(' ', "  "));
    assert_eq!(
        service
            .find_site_by_name(&equivalent)
            .await
            .unwrap()
            .unwrap()
            .id,
        site.id
    );
    assert_conflict(
        service
            .create_site(CreateSite {
                name: equivalent,
                description: None,
            })
            .await
            .unwrap_err(),
        "23505",
    );
    let room = service
        .create_room(CreateRoom {
            site_id: site.id,
            name: "Room A".into(),
            description: None,
        })
        .await
        .unwrap();
    assert_conflict(
        service
            .create_room(CreateRoom {
                site_id: site.id,
                name: " room\tA ".into(),
                description: None,
            })
            .await
            .unwrap_err(),
        "23505",
    );
    service
        .create_rack(CreateRack {
            room_id: room.id,
            name: "Rack A".into(),
            height_u: 42,
            description: None,
        })
        .await
        .unwrap();
    assert_conflict(
        service
            .create_rack(CreateRack {
                room_id: room.id,
                name: " RACK  A ".into(),
                height_u: 42,
                description: None,
            })
            .await
            .unwrap_err(),
        "23505",
    );
    let quoted = format!("Site '{}'; SELECT 1 --", Uuid::new_v4());
    let value = service
        .create_site(CreateSite {
            name: quoted.clone(),
            description: None,
        })
        .await
        .unwrap();
    assert_eq!(
        service
            .find_site_by_name(&quoted)
            .await
            .unwrap()
            .unwrap()
            .id,
        value.id
    );
    assert!(service
        .find_site_by_name("absent ' OR true --")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn obvious_validation_and_database_checks_are_domain_errors() {
    let (pool, service) = core().await;
    assert!(matches!(
        service
            .create_site(CreateSite {
                name: " \t\n".into(),
                description: None
            })
            .await,
        Err(Error::Validation(_))
    ));
    let (site, _, rack) = hierarchy(&service).await;
    assert!(matches!(
        service
            .update_site(
                site.id,
                UpdateSite {
                    name: Patch::Clear,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        service
            .create_device(placed(rack.id, 42, LifecycleStatus::Active))
            .await,
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        service
            .create_device(CreateDevice {
                rack_id: Some(rack.id),
                ..Default::default()
            })
            .await,
        Err(Error::Validation(_))
    ));
    let mut conn = pool.acquire().await.unwrap();
    let error = Repository::create_site(
        &mut conn,
        &CreateSite {
            name: "   ".into(),
            description: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::Validation(_))); // Actual PostgreSQL 23514.
    let invalid = sqlx::query("SELECT $1::inet")
        .bind("not-an-ip")
        .execute(&mut *conn)
        .await
        .unwrap_err();
    assert!(matches!(Error::from(invalid), Error::Validation(_))); // 22P02.
    let internal = sqlx::query("SELECT * FROM cmdb_missing_test_table")
        .execute(&mut *conn)
        .await
        .unwrap_err();
    let error = Error::from(internal);
    assert!(matches!(error, Error::Database(_)));
    assert_eq!(error.to_string(), "Database operation failed");
}

#[tokio::test]
async fn foreign_key_and_not_found_are_distinct() {
    let (_, service) = core().await;
    let missing = Uuid::new_v4();
    assert_conflict(
        service
            .create_room(CreateRoom {
                site_id: missing,
                name: "room".into(),
                description: None,
            })
            .await
            .unwrap_err(),
        "23503",
    );
    assert_conflict(
        service
            .create_interface(interface_input(missing))
            .await
            .unwrap_err(),
        "23503",
    );
    assert!(matches!(
        service.get_site(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_room(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_rack(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_device(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_interface(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_ip_address(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.delete_site(missing).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.update_site(missing, UpdateSite::default()).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.decommission_device(missing).await,
        Err(Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn device_placement_filters_updates_and_decommission_keep_occupancy() {
    let (_, service) = core().await;
    let (_, _, rack) = hierarchy(&service).await;
    let unique = Uuid::new_v4().to_string();
    let mut input = placed(rack.id, 1, LifecycleStatus::Active);
    input.hostname = Some(format!("host-{unique}"));
    input.serial = Some(format!("serial-{unique}"));
    let device = service.create_device(input.clone()).await.unwrap();
    assert_eq!(
        service.get_device(device.id).await.unwrap().rack_id,
        Some(rack.id)
    );
    for filter in [
        DeviceFilter {
            rack_id: Some(rack.id),
            ..Default::default()
        },
        DeviceFilter {
            hostname: input.hostname.clone(),
            ..Default::default()
        },
        DeviceFilter {
            serial: input.serial.clone(),
            ..Default::default()
        },
        DeviceFilter {
            rack_id: Some(rack.id),
            lifecycle_status: Some(LifecycleStatus::Active),
            ..Default::default()
        },
    ] {
        assert_eq!(
            service
                .list_devices(&filter)
                .await
                .unwrap()
                .iter()
                .map(|d| d.id)
                .collect::<Vec<_>>(),
            [device.id]
        );
    }
    assert!(service
        .list_devices(&DeviceFilter {
            hostname: Some("missing ' OR true --".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .is_empty());
    let updated = service
        .update_device(
            device.id,
            UpdateDevice {
                device_type: Patch::Set("switch".into()),
                vendor: Patch::Set("vendor".into()),
                model: Patch::Set("model".into()),
                hostname: Patch::Clear,
                serial: Patch::Set(format!("new-{unique}")),
                lifecycle_status: Patch::Set(LifecycleStatus::Maintenance),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(updated.hostname.is_none());
    assert_eq!(updated.device_type.as_deref(), Some("switch"));
    assert_eq!(updated.vendor.as_deref(), Some("vendor"));
    assert_eq!(updated.model.as_deref(), Some("model"));
    assert_conflict(
        service
            .create_device(placed(rack.id, 1, LifecycleStatus::Active))
            .await
            .unwrap_err(),
        "23P01",
    );
    let decommissioned = service.decommission_device(device.id).await.unwrap();
    assert_eq!(
        decommissioned.lifecycle_status,
        LifecycleStatus::Decommissioned
    );
    assert_eq!(decommissioned.rack_id, Some(rack.id));
    assert_conflict(
        service
            .create_device(placed(rack.id, 1, LifecycleStatus::Maintenance))
            .await
            .unwrap_err(),
        "23P01",
    );
    assert!(matches!(
        service
            .update_rack(
                rack.id,
                UpdateRack {
                    height_u: Patch::Set(1),
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Validation(_))
    ));
    let detached = service
        .update_device(
            device.id,
            UpdateDevice {
                rack_id: Patch::Clear,
                start_unit: Patch::Clear,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(detached.rack_id.is_none() && detached.start_unit.is_none());
    service
        .create_device(placed(rack.id, 1, LifecycleStatus::Decommissioned))
        .await
        .unwrap();
}

#[tokio::test]
async fn interface_mac_and_ip_v4_v6_crud_preserve_native_values() {
    let (_, service) = core().await;
    let device = service
        .create_device(CreateDevice::default())
        .await
        .unwrap();
    let interface = service
        .create_interface(interface_input(device.id))
        .await
        .unwrap();
    assert_eq!(
        service
            .get_interface(interface.id)
            .await
            .unwrap()
            .mac_address,
        interface.mac_address
    );
    assert_eq!(service.list_interfaces(device.id).await.unwrap().len(), 1);
    let updated = service
        .update_interface(
            interface.id,
            UpdateInterface {
                name: Patch::Set("eth1".into()),
                if_index: Patch::Set(2),
                interface_type: Patch::Set("fiber".into()),
                description: Patch::Clear,
                mac_address: Patch::Set("02:00:00:00:00:01".parse().unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.if_index, Some(2));
    assert!(updated.description.is_none());
    assert_eq!(
        updated.mac_address.unwrap().to_string().to_lowercase(),
        "02:00:00:00:00:01"
    );
    let mut ids = Vec::new();
    for address in ["192.0.2.1/24", "2001:db8::1/64"] {
        let input = CreateIpAddress {
            interface_id: interface.id,
            address: address.parse().unwrap(),
            is_management: false,
        };
        let ip = service.create_ip_address(input.clone()).await.unwrap();
        assert_eq!(ip.address, input.address);
        assert_eq!(
            service.get_ip_address(ip.id).await.unwrap().address,
            input.address
        );
        assert_conflict(service.create_ip_address(input).await.unwrap_err(), "23505");
        let changed = service
            .update_ip_address(
                ip.id,
                UpdateIpAddress {
                    is_management: Patch::Set(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(changed.is_management);
        ids.push(ip.id);
    }
    assert_eq!(
        service.list_ip_addresses(interface.id).await.unwrap().len(),
        2
    );
    assert_conflict(
        service.delete_interface(interface.id).await.unwrap_err(),
        "23503",
    );
    for id in ids {
        service.delete_ip_address(id).await.unwrap();
    }
    service.delete_interface(interface.id).await.unwrap();
    assert!(matches!(
        service.get_interface(interface.id).await,
        Err(Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn service_and_repository_share_caller_transaction_and_roll_back_atomically() {
    let (pool, service) = core().await;
    let mut tx = pool.begin().await.unwrap();
    let site = Service::create_site_in(&mut tx, &site_input())
        .await
        .unwrap();
    let room = Service::create_room_in(
        &mut tx,
        &CreateRoom {
            site_id: site.id,
            name: "atomic room".into(),
            description: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        Repository::get_room(&mut tx, room.id)
            .await
            .unwrap()
            .unwrap()
            .site_id,
        site.id
    );
    sqlx::query(
        "INSERT INTO audit_events (actor_type, source, action, entity_type, entity_id, after_data)
        VALUES ('system', 'test', 'create', 'sites', $1, '{}'::jsonb)",
    )
    .bind(site.id)
    .execute(&mut *tx)
    .await
    .unwrap();
    let failed = Service::create_room_in(
        &mut tx,
        &CreateRoom {
            site_id: Uuid::new_v4(),
            name: "bad parent".into(),
            description: None,
        },
    )
    .await
    .unwrap_err();
    assert_conflict(failed, "23503");
    tx.rollback().await.unwrap();
    assert!(matches!(
        service.get_site(site.id).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        service.get_room(room.id).await,
        Err(Error::NotFound { .. })
    ));
    let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE entity_id = $1")
        .bind(site.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(audits, 0);
}

#[tokio::test]
async fn field_override_read_helper_sees_same_transaction() {
    let (pool, service) = core().await;
    let mut tx = pool.begin().await.unwrap();
    let device = Service::create_device_in(&mut tx, &CreateDevice::default())
        .await
        .unwrap();
    let user: Uuid = sqlx::query_scalar(
        "INSERT INTO users (username, email, password_hash, display_name)
        VALUES ($1, $2, 'test-only-hash', 'CMDB core test') RETURNING uuid",
    )
    .bind(Uuid::new_v4().simple().to_string())
    .bind(format!("{}@example.invalid", Uuid::new_v4()))
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO field_overrides (entity_type, entity_id, field_name, locked_by)
        VALUES ('devices', $1, 'hostname', $2)",
    )
    .bind(device.id)
    .bind(user)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(
        Service::is_field_overridden_in(&mut tx, EntityType::Devices, device.id, "hostname")
            .await
            .unwrap()
    );
    assert!(
        !Service::is_field_overridden_in(&mut tx, EntityType::Devices, device.id, "serial")
            .await
            .unwrap()
    );
    tx.rollback().await.unwrap();
    assert!(!service
        .is_field_overridden(EntityType::Devices, device.id, "hostname")
        .await
        .unwrap());
}

#[tokio::test]
async fn serialization_and_deadlock_errors_retry_whole_transaction_without_partial_writes() {
    let (pool, _) = core().await;
    let service = Service::with_retry_policy(pool, RetryPolicy::new(3, Duration::ZERO).unwrap());
    for code in ["40001", "40P01"] {
        let input = site_input();
        let attempts = Arc::new(AtomicUsize::new(0));
        let site = service.retryable_transaction(|tx| {
            let attempts = attempts.clone();
            let input = input.clone();
            Box::pin(async move {
                let site = Service::create_site_in(tx, &input).await?;
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Server-reported SQLSTATE; aborted writes must be rolled back.
                    let sql = if code == "40001" {
                        "DO $$ BEGIN RAISE EXCEPTION 'retry test' USING ERRCODE = '40001'; END $$"
                    } else { "DO $$ BEGIN RAISE EXCEPTION 'retry test' USING ERRCODE = '40P01'; END $$" };
                    sqlx::query(sql).execute(&mut **tx).await?;
                }
                Ok(site)
            })
        }).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            service
                .find_site_by_name(&input.name)
                .await
                .unwrap()
                .unwrap()
                .id,
            site.id
        );
    }
}

#[tokio::test]
async fn retry_budget_is_enforced_and_conflicts_are_not_retried() {
    let (pool, _) = core().await;
    let service = Service::with_retry_policy(pool, RetryPolicy::new(3, Duration::ZERO).unwrap());
    let input = site_input();
    let attempts = Arc::new(AtomicUsize::new(0));
    let error = service
        .retryable_transaction::<(), _>(|tx| {
            let attempts = attempts.clone();
            let input = input.clone();
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Service::create_site_in(tx, &input).await?;
                sqlx::query(
                    "DO $$ BEGIN RAISE EXCEPTION 'retry test' USING ERRCODE = '40001'; END $$",
                )
                .execute(&mut **tx)
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert!(error.is_retryable());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(service
        .find_site_by_name(&input.name)
        .await
        .unwrap()
        .is_none());
    service.create_site(input.clone()).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let error = service
        .retryable_transaction(|tx| {
            let count = count.clone();
            let input = input.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Service::create_site_in(tx, &input).await
            })
        })
        .await
        .unwrap_err();
    assert_conflict(error, "23505");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
