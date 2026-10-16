-- Run in the destination metrics database after installing named-collection.xml.
-- String keys use COMPLEX_KEY_HASHED; call dictGet with tuple(canonical_string).
CREATE DICTIONARY IF NOT EXISTS cmdb_devices_dict
(
    device_id String,
    hostname Nullable(String) DEFAULT NULL,
    device_type Nullable(String) DEFAULT NULL,
    vendor Nullable(String) DEFAULT NULL,
    model Nullable(String) DEFAULT NULL,
    serial Nullable(String) DEFAULT NULL,
    lifecycle_status String DEFAULT '',
    site_id Nullable(String) DEFAULT NULL,
    site_name Nullable(String) DEFAULT NULL,
    room_id Nullable(String) DEFAULT NULL,
    room_name Nullable(String) DEFAULT NULL,
    rack_id Nullable(String) DEFAULT NULL,
    rack_name Nullable(String) DEFAULT NULL,
    start_unit Nullable(Int32) DEFAULT NULL,
    height_u Nullable(Int32) DEFAULT NULL,
    management_ip Nullable(String) DEFAULT NULL
)
PRIMARY KEY device_id
SOURCE(POSTGRESQL(NAME cronet_cmdb_postgres TABLE 'cmdb_clickhouse_devices'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 120);

CREATE DICTIONARY IF NOT EXISTS cmdb_interfaces_dict
(
    interface_id String,
    device_id String DEFAULT '',
    if_index Nullable(Int32) DEFAULT NULL,
    name String DEFAULT '',
    description Nullable(String) DEFAULT NULL,
    interface_type Nullable(String) DEFAULT NULL,
    mac_address Nullable(String) DEFAULT NULL,
    hostname Nullable(String) DEFAULT NULL,
    site_id Nullable(String) DEFAULT NULL,
    site_name Nullable(String) DEFAULT NULL
)
PRIMARY KEY interface_id
SOURCE(POSTGRESQL(NAME cronet_cmdb_postgres TABLE 'cmdb_clickhouse_interfaces'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 120);

CREATE DICTIONARY IF NOT EXISTS cmdb_ips_dict
(
    address String,
    ip_id String DEFAULT '',
    interface_id String DEFAULT '',
    device_id String DEFAULT '',
    is_management UInt8 DEFAULT 0,
    address_cidr String DEFAULT '',
    prefix_length Int32 DEFAULT 0,
    hostname Nullable(String) DEFAULT NULL,
    site_id Nullable(String) DEFAULT NULL,
    site_name Nullable(String) DEFAULT NULL,
    address_count UInt64 DEFAULT 0
)
PRIMARY KEY address
SOURCE(POSTGRESQL(NAME cronet_cmdb_postgres TABLE 'cmdb_clickhouse_ips'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 120);
