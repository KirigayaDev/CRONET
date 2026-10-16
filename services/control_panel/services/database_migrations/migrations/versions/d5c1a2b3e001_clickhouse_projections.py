"""Read-only CMDB projections for ClickHouse dictionaries.

Revision ID: d5c1a2b3e001
Revises: d4c1a2b3e001
"""
from alembic import op

revision = 'd5c1a2b3e001'
down_revision = 'd4c1a2b3e001'
branch_labels = None
depends_on = None


def upgrade():
    # A cluster-wide name collision must be reviewed, never adopted or modified.
    op.execute("""
        CREATE ROLE cronet_clickhouse_reader NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')
                AND has_table_privilege('cronet_clickhouse_reader',c.oid,
                    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            ) THEN
                RAISE EXCEPTION 'ClickHouse reader inherits existing PUBLIC relation privileges; review grants first';
            END IF;
        END $$;

        CREATE VIEW public.cmdb_clickhouse_devices WITH (security_barrier=true) AS
        SELECT d.id::text AS device_id, d.hostname, d.device_type, d.vendor,
            d.model, d.serial, d.lifecycle_status,
            s.id::text AS site_id, s.name AS site_name,
            ro.id::text AS room_id, ro.name AS room_name,
            ra.id::text AS rack_id, ra.name AS rack_name,
            CASE WHEN ra.id IS NOT NULL THEN d.start_unit END AS start_unit,
            CASE WHEN ra.id IS NOT NULL THEN d.height_u END AS height_u,
            management.address AS management_ip
        FROM public.devices d
        LEFT JOIN public.racks ra ON ra.id=d.rack_id
        LEFT JOIN public.rooms ro ON ro.id=ra.room_id
        LEFT JOIN public.sites s ON s.id=ro.site_id
        LEFT JOIN LATERAL (
            SELECT host(ip.address) AS address
            FROM public.interfaces i JOIN public.ip_addresses ip ON ip.interface_id=i.id
            WHERE i.device_id=d.id AND ip.is_management
            ORDER BY family(ip.address),ip.address,i.id,ip.id LIMIT 1
        ) management ON true;

        CREATE VIEW public.cmdb_clickhouse_interfaces WITH (security_barrier=true) AS
        SELECT i.id::text AS interface_id, i.device_id::text AS device_id,
            i.if_index, i.name, i.description, i.interface_type,
            i.mac_address::text AS mac_address, d.hostname,
            d.site_id, d.site_name
        FROM public.interfaces i
        JOIN public.cmdb_clickhouse_devices d ON d.device_id=i.device_id::text;

        CREATE VIEW public.cmdb_clickhouse_ips WITH (security_barrier=true) AS
        SELECT DISTINCT ON (host(ip.address))
            ip.id::text AS ip_id, ip.interface_id::text AS interface_id,
            i.device_id::text AS device_id, host(ip.address) AS address,
            ip.is_management::integer AS is_management,
            ip.address::text AS address_cidr, masklen(ip.address) AS prefix_length,
            d.hostname, d.site_id, d.site_name,
            count(*) OVER (PARTITION BY host(ip.address)) AS address_count
        FROM public.ip_addresses ip
        JOIN public.interfaces i ON i.id=ip.interface_id
        JOIN public.cmdb_clickhouse_devices d ON d.device_id=i.device_id::text
        ORDER BY host(ip.address),
            CASE d.lifecycle_status WHEN 'active' THEN 0 WHEN 'maintenance' THEN 1 ELSE 2 END,
            ip.is_management DESC, i.device_id, i.id, ip.id;

        GRANT USAGE ON SCHEMA public TO cronet_clickhouse_reader;
        GRANT SELECT ON public.cmdb_clickhouse_devices,
            public.cmdb_clickhouse_interfaces,public.cmdb_clickhouse_ips
            TO cronet_clickhouse_reader;
    """)


def downgrade():
    # No DROP OWNED / CASCADE: unexpected external dependencies fail atomically.
    op.execute("""
        DROP VIEW public.cmdb_clickhouse_ips;
        DROP VIEW public.cmdb_clickhouse_interfaces;
        DROP VIEW public.cmdb_clickhouse_devices;
        REVOKE USAGE ON SCHEMA public FROM cronet_clickhouse_reader;
        DROP ROLE cronet_clickhouse_reader;
    """)
