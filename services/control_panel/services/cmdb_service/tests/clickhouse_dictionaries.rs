//! Real PostgreSQL + ClickHouse 25.8 acceptance. Run via run_clickhouse_integration.py.
use clickhouse::Client;
use cmdb_service::{models::*, Service};
use serde::Deserialize;
use sqlx::{postgres::PgConnectOptions, PgPool};
use std::time::Duration;
use uuid::Uuid;

const DDL: &str = include_str!("../../../clickhouse/cmdb/dictionaries.sql");

#[derive(Debug, Deserialize, clickhouse::Row)]
struct DeviceLookup {
    hostname: Option<String>,
    vendor: Option<String>,
    model: Option<String>,
    site_name: Option<String>,
    rack_name: Option<String>,
    lifecycle_status: String,
    management_ip: Option<String>,
    start_unit: Option<i32>,
}
#[derive(Debug, Deserialize, clickhouse::Row)]
struct InterfaceLookup {
    name: String,
    mac_address: Option<String>,
    device_id: String,
}
async fn device_lookup(ch: &Client, id: Uuid) -> Result<DeviceLookup, clickhouse::error::Error> {
    ch.query("WITH tuple(?) AS k SELECT dictGet('cmdb_devices_dict','hostname',k),dictGet('cmdb_devices_dict','vendor',k),dictGet('cmdb_devices_dict','model',k),dictGet('cmdb_devices_dict','site_name',k),dictGet('cmdb_devices_dict','rack_name',k),dictGet('cmdb_devices_dict','lifecycle_status',k),dictGet('cmdb_devices_dict','management_ip',k),dictGet('cmdb_devices_dict','start_unit',k)")
        .bind(id.to_string()).fetch_one().await
}
async fn reload(ch: &Client, name: &str) -> Result<(), clickhouse::error::Error> {
    assert!(["cmdb_devices_dict", "cmdb_interfaces_dict", "cmdb_ips_dict"].contains(&name));
    ch.query(&format!("SYSTEM RELOAD DICTIONARY {name}")).execute().await
}

#[tokio::test]
#[ignore = "Requires disposable PostgreSQL and ClickHouse; run tests/run_clickhouse_integration.py"]
async fn real_dictionaries_lookups_refresh_nulls_ip_collisions_and_read_only_login() {
    let env = |name| std::env::var(name).expect("Use the disposable D5 runner");
    assert_eq!(env("D5_TEST_CLICKHOUSE_URL"), "http://clickhouse:8123");
    let options = PgConnectOptions::new()
        .host("postgres")
        .database("cronet_cmdb_test_schema")
        .username("cmdb_test")
        .password(&env("D5_TEST_ADMIN_PASSWORD"));
    let pool = PgPool::connect_with(options.clone()).await.unwrap();
    let db: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await.unwrap();
    assert_eq!(db, "cronet_cmdb_test_schema");
    let head: String = sqlx::query_scalar("SELECT version_num FROM alembic_version").fetch_one(&pool).await.unwrap();
    assert_eq!(head, "d5c1a2b3e001");
    let username = env("D5_TEST_READER_USER");
    let password = env("D5_TEST_READER_PASSWORD");
    assert!(username.starts_with("d5_reader_") && username.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    assert_eq!(password.len(), 64);
    assert!(password.bytes().all(|b| b.is_ascii_hexdigit()));
    // Identifiers and password are generated, bounded ASCII; never print either.
    sqlx::raw_sql(&format!("CREATE ROLE {username} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}' IN ROLE cronet_clickhouse_reader"))
        .execute(&pool).await.unwrap();
    let reader = PgPool::connect_with(options.username(&username).password(&password)).await.unwrap();
    for view in ["cmdb_clickhouse_devices", "cmdb_clickhouse_interfaces", "cmdb_clickhouse_ips"] {
        sqlx::query(&format!("SELECT * FROM {view}")).fetch_all(&reader).await.unwrap();
    }
    for query in [
        "INSERT INTO devices(hostname) VALUES ('forbidden')",
        "UPDATE devices SET hostname='forbidden'",
        "DELETE FROM devices",
        "SELECT * FROM devices",
        "SELECT * FROM users",
        "TRUNCATE devices",
    ] {
        let error = sqlx::query(query).execute(&reader).await.unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
    }
    let service = Service::new(pool.clone());
    let site =
        service.create_site(CreateSite { name: format!("D5 {}", Uuid::new_v4()), description: None }).await.unwrap();
    let room =
        service.create_room(CreateRoom { site_id: site.id, name: "Room".into(), description: None }).await.unwrap();
    let rack = service
        .create_rack(CreateRack { room_id: room.id, name: "Rack A".into(), height_u: 42, description: None })
        .await
        .unwrap();
    let rack2 = service
        .create_rack(CreateRack { room_id: room.id, name: "Rack B".into(), height_u: 42, description: None })
        .await
        .unwrap();
    let device = service
        .create_device(CreateDevice {
            rack_id: Some(rack.id),
            start_unit: Some(1),
            height_u: Some(2),
            hostname: Some("d5-original".into()),
            vendor: Some("vendor-a".into()),
            model: Some("model-a".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let interface = service
        .create_interface(CreateInterface {
            device_id: device.id,
            if_index: Some(1),
            name: "eth0".into(),
            description: Some("uplink".into()),
            interface_type: Some("ethernet".into()),
            mac_address: Some("aa:bb:cc:dd:ee:ff".parse().unwrap()),
        })
        .await
        .unwrap();
    for address in ["2001:db8::1/64", "192.0.2.20/24", "192.0.2.3/24"] {
        service
            .create_ip_address(CreateIpAddress {
                interface_id: interface.id,
                address: address.parse().unwrap(),
                is_management: true,
            })
            .await
            .unwrap();
    }
    let unplaced = service
        .create_device(CreateDevice {
            height_u: Some(2),
            lifecycle_status: LifecycleStatus::Decommissioned,
            ..Default::default()
        })
        .await
        .unwrap();
    let ch = Client::default()
        .with_url(env("D5_TEST_CLICKHOUSE_URL"))
        .with_user("d5_test")
        .with_password(env("D5_TEST_CLICKHOUSE_PASSWORD"))
        .with_database("cronet_cmdb_test_dictionaries");
    let version: String = ch.query("SELECT version()").fetch_one().await.unwrap();
    assert!(version.starts_with("25.8."));
    let ddl = DDL.lines().filter(|line| !line.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n");
    for sql in ddl.split(';').filter(|sql| !sql.trim().is_empty()) {
        ch.query(sql).execute().await.unwrap();
    }
    let first = device_lookup(&ch, device.id).await.unwrap();
    assert_eq!(
        (first.hostname.as_deref(), first.vendor.as_deref(), first.model.as_deref()),
        (Some("d5-original"), Some("vendor-a"), Some("model-a"))
    );
    assert_eq!(first.site_name.as_deref(), Some(site.name.as_str()));
    assert_eq!(first.rack_name.as_deref(), Some("Rack A"));
    assert_eq!(first.management_ip.as_deref(), Some("192.0.2.3"));
    assert_eq!(first.start_unit, Some(1));
    let empty = device_lookup(&ch, unplaced.id).await.unwrap();
    assert_eq!(empty.lifecycle_status, "decommissioned");
    assert!(
        empty.hostname.is_none()
            && empty.site_name.is_none()
            && empty.rack_name.is_none()
            && empty.start_unit.is_none()
    );
    let values:InterfaceLookup=ch.query("WITH tuple(?) AS k SELECT dictGet('cmdb_interfaces_dict','name',k),dictGet('cmdb_interfaces_dict','mac_address',k),dictGet('cmdb_interfaces_dict','device_id',k)")
        .bind(interface.id.to_string()).fetch_one().await.unwrap();
    assert_eq!(
        (values.name, values.mac_address, values.device_id),
        ("eth0".into(), Some("aa:bb:cc:dd:ee:ff".into()), device.id.to_string())
    );
    for address in ["192.0.2.3", "2001:db8::1"] {
        let found:(String,String,u8,u64)=ch.query("WITH tuple(?) AS k SELECT dictGet('cmdb_ips_dict','device_id',k),dictGet('cmdb_ips_dict','interface_id',k),dictGet('cmdb_ips_dict','is_management',k),dictGet('cmdb_ips_dict','address_count',k)").bind(address).fetch_one().await.unwrap();
        assert_eq!(found, (device.id.to_string(), interface.id.to_string(), 1, 1));
    }
    let missing: u8 = ch
        .query("SELECT dictHas('cmdb_devices_dict',tuple(?))")
        .bind(Uuid::new_v4().to_string())
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(missing, 0);
    let uuid_before: String = ch
        .query("SELECT toString(uuid) FROM system.tables WHERE database=currentDatabase() AND name='cmdb_devices_dict'")
        .fetch_one()
        .await
        .unwrap();
    service
        .update_device(
            device.id,
            UpdateDevice {
                hostname: Patch::Set("d5-refreshed".into()),
                vendor: Patch::Set("vendor-b".into()),
                model: Patch::Set("model-b".into()),
                rack_id: Patch::Set(rack2.id),
                start_unit: Patch::Set(5),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    reload(&ch, "cmdb_devices_dict").await.unwrap();
    let refreshed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let d = device_lookup(&ch, device.id).await.unwrap();
            if d.hostname.as_deref() == Some("d5-refreshed") {
                break d;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        (refreshed.vendor.as_deref(), refreshed.model.as_deref(), refreshed.rack_name.as_deref(), refreshed.start_unit),
        (Some("vendor-b"), Some("model-b"), Some("Rack B"), Some(5))
    );
    let uuid_after: String = ch
        .query("SELECT toString(uuid) FROM system.tables WHERE database=currentDatabase() AND name='cmdb_devices_dict'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(uuid_before, uuid_after);
    // Duplicate address: active device outranks decommissioned; ambiguity stays visible.
    let other = service
        .create_interface(CreateInterface {
            device_id: unplaced.id,
            if_index: None,
            name: "eth0".into(),
            description: None,
            interface_type: None,
            mac_address: None,
        })
        .await
        .unwrap();
    service
        .create_ip_address(CreateIpAddress {
            interface_id: other.id,
            address: "2001:db8::1/128".parse().unwrap(),
            is_management: true,
        })
        .await
        .unwrap();
    reload(&ch, "cmdb_ips_dict").await.unwrap();
    let collision:(String,u64)=ch.query("SELECT dictGet('cmdb_ips_dict','device_id',tuple('2001:db8::1')),dictGet('cmdb_ips_dict','address_count',tuple('2001:db8::1'))").fetch_one().await.unwrap();
    assert_eq!(collision, (device.id.to_string(), 2));
    for name in ["cmdb_devices_dict", "cmdb_interfaces_dict", "cmdb_ips_dict"] {
        ch.query(&format!("DROP DICTIONARY {name}")).execute().await.unwrap();
    }
    reader.close().await;
    sqlx::query(&format!("DROP ROLE {username}")).execute(&pool).await.unwrap();
}
