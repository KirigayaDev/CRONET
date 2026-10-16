"""add initial CMDB schema

Revision ID: 8b6e2f4a9c10
Revises: 72ca3097da29
Create Date: 2026-10-09

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '8b6e2f4a9c10'
down_revision: Union[str, Sequence[str], None] = '72ca3097da29'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Keep the display name; uniqueness ignores case and whitespace runs.
_NORMALIZED_NAME = "lower(btrim(regexp_replace(name, '[[:space:]]+', ' ', 'g')))"


def _id_column() -> sa.Column:
    return sa.Column('id', postgresql.UUID(as_uuid=True),
                     server_default=sa.text('uuid_generate_v4()'), nullable=False)


def _timestamps() -> list[sa.Column]:
    return [
        sa.Column('created_at', sa.TIMESTAMP(timezone=True),
                  server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True),
                  server_default=sa.text('now()'), nullable=False),
    ]


def upgrade() -> None:
    """Add CMDB without changing the existing users schema."""
    # UUID equality in the GiST exclusion constraint needs btree_gist.
    op.execute('CREATE EXTENSION IF NOT EXISTS btree_gist')

    op.create_table(
        'sites', _id_column(),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_sites'),
        sa.CheckConstraint("name ~ '[^[:space:]]'", name='ck_sites_name_not_blank'),
    )
    op.create_index('uq_sites_name_normalized', 'sites',
                    [sa.text(_NORMALIZED_NAME)], unique=True)
    op.create_table(
        'rooms', _id_column(),
        sa.Column('site_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_rooms'),
        sa.ForeignKeyConstraint(['site_id'], ['sites.id'], name='fk_rooms_site_id'),
        sa.CheckConstraint("name ~ '[^[:space:]]'", name='ck_rooms_name_not_blank'),
    )
    op.create_index('uq_rooms_site_id_name_normalized', 'rooms',
                    ['site_id', sa.text(_NORMALIZED_NAME)], unique=True)
    op.create_table(
        'racks', _id_column(),
        sa.Column('room_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('height_u', sa.Integer(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_racks'),
        sa.ForeignKeyConstraint(['room_id'], ['rooms.id'], name='fk_racks_room_id'),
        sa.CheckConstraint("name ~ '[^[:space:]]'", name='ck_racks_name_not_blank'),
        sa.CheckConstraint('height_u > 0', name='ck_racks_height_u_positive'),
    )
    op.create_index('uq_racks_room_id_name_normalized', 'racks',
                    ['room_id', sa.text(_NORMALIZED_NAME)], unique=True)
    op.create_table(
        'devices', _id_column(),
        sa.Column('rack_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('start_unit', sa.Integer(), nullable=True),
        sa.Column('height_u', sa.Integer(), nullable=True),
        sa.Column('hostname', sa.Text(), nullable=True),
        sa.Column('device_type', sa.Text(), nullable=True),
        sa.Column('vendor', sa.Text(), nullable=True),
        sa.Column('model', sa.Text(), nullable=True),
        sa.Column('serial', sa.Text(), nullable=True),
        sa.Column('lifecycle_status', sa.Text(), nullable=False,
                  server_default=sa.text("'active'")),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_devices'),
        sa.ForeignKeyConstraint(['rack_id'], ['racks.id'], name='fk_devices_rack_id'),
        sa.CheckConstraint(
            '(rack_id IS NULL AND start_unit IS NULL) OR '
            '(rack_id IS NOT NULL AND start_unit IS NOT NULL AND height_u IS NOT NULL)',
            name='ck_devices_placement_complete'),
        sa.CheckConstraint('start_unit > 0', name='ck_devices_start_unit_positive'),
        sa.CheckConstraint('height_u > 0', name='ck_devices_height_u_positive'),
        sa.CheckConstraint(
            "lifecycle_status IN ('active', 'maintenance', 'decommissioned')",
            name='ck_devices_lifecycle_status'),
    )
    op.create_index('ix_devices_rack_id', 'devices', ['rack_id'])
    op.create_index('ix_devices_hostname', 'devices', ['hostname'])
    op.create_index('ix_devices_serial', 'devices', ['serial'])
    # Half-open ranges allow adjacent devices. Bigint avoids integer overflow.
    op.execute("""
        ALTER TABLE devices ADD CONSTRAINT ex_devices_rack_units
        EXCLUDE USING gist (
            rack_id WITH =,
            int8range(start_unit::bigint,
                      start_unit::bigint + height_u::bigint, '[)') WITH &&
        ) WHERE (rack_id IS NOT NULL)
    """)
    op.create_table(
        'interfaces', _id_column(),
        sa.Column('device_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('if_index', sa.Integer(), nullable=True),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('interface_type', sa.Text(), nullable=True),
        sa.Column('mac_address', postgresql.MACADDR(), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_interfaces'),
        sa.ForeignKeyConstraint(['device_id'], ['devices.id'],
                                name='fk_interfaces_device_id'),
        sa.CheckConstraint("name ~ '[^[:space:]]'", name='ck_interfaces_name_not_blank'),
        sa.CheckConstraint('if_index > 0', name='ck_interfaces_if_index_positive'),
        # PostgreSQL's default NULL-distinct semantics allow unknown if_index.
        sa.UniqueConstraint('device_id', 'if_index',
                            name='uq_interfaces_device_id_if_index'),
    )
    op.create_table(
        'ip_addresses', _id_column(),
        sa.Column('interface_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('address', postgresql.INET(), nullable=False),
        sa.Column('is_management', sa.Boolean(), nullable=False,
                  server_default=sa.text('false')),
        *_timestamps(),
        sa.PrimaryKeyConstraint('id', name='pk_ip_addresses'),
        sa.ForeignKeyConstraint(['interface_id'], ['interfaces.id'],
                                name='fk_ip_addresses_interface_id'),
        sa.UniqueConstraint('interface_id', 'address',
                            name='uq_ip_addresses_interface_id_address'),
    )
    op.create_table(
        'field_overrides', _id_column(),
        sa.Column('entity_type', sa.Text(), nullable=False),
        sa.Column('entity_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('field_name', sa.Text(), nullable=False),
        sa.Column('locked_by', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('locked_at', sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id', name='pk_field_overrides'),
        sa.ForeignKeyConstraint(['locked_by'], ['users.uuid'],
                                name='fk_field_overrides_locked_by'),
        sa.UniqueConstraint('entity_type', 'entity_id', 'field_name',
                            name='uq_field_overrides_entity_field'),
        sa.CheckConstraint(
            "entity_type IN ('sites', 'rooms', 'racks', 'devices', "
            "'interfaces', 'ip_addresses')", name='ck_field_overrides_entity_type'),
        sa.CheckConstraint("length(btrim(field_name)) > 0",
                           name='ck_field_overrides_field_name'),
    )
    op.create_index('ix_field_overrides_locked_by', 'field_overrides', ['locked_by'])
    op.create_table(
        'import_batches', _id_column(),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('filename', sa.Text(), nullable=False),
        sa.Column('status', sa.Text(), nullable=False,
                  server_default=sa.text("'pending'")),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text('now()')),
        sa.Column('confirmed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_import_batches'),
        sa.ForeignKeyConstraint(['created_by'], ['users.uuid'],
                                name='fk_import_batches_created_by'),
        sa.CheckConstraint("status IN ('pending', 'validated', 'confirmed', 'cancelled')",
                           name='ck_import_batches_status'),
        sa.CheckConstraint("(status = 'confirmed') = (confirmed_at IS NOT NULL)",
                           name='ck_import_batches_confirmed_at'),
    )
    op.create_index('ix_import_batches_created_by', 'import_batches', ['created_by'])
    op.create_table(
        'import_rows', _id_column(),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(), nullable=False),
        sa.Column('working_data', postgresql.JSONB(), nullable=False),
        sa.Column('validation_errors', postgresql.JSONB(), nullable=False,
                  server_default=sa.text("'[]'::jsonb")),
        sa.Column('validation_warnings', postgresql.JSONB(), nullable=False,
                  server_default=sa.text("'[]'::jsonb")),
        sa.Column('status', sa.Text(), nullable=False,
                  server_default=sa.text("'pending'")),
        sa.PrimaryKeyConstraint('id', name='pk_import_rows'),
        sa.ForeignKeyConstraint(['batch_id'], ['import_batches.id'],
                                name='fk_import_rows_batch_id'),
        sa.UniqueConstraint('batch_id', 'row_number', name='uq_import_rows_batch_row'),
        sa.CheckConstraint('row_number > 0', name='ck_import_rows_row_number_positive'),
        sa.CheckConstraint("status IN ('pending', 'valid', 'invalid', 'imported')",
                           name='ck_import_rows_status'),
        sa.CheckConstraint("jsonb_typeof(raw_data) = 'object'",
                           name='ck_import_rows_raw_data_object'),
        sa.CheckConstraint("jsonb_typeof(working_data) = 'object'",
                           name='ck_import_rows_working_data_object'),
        sa.CheckConstraint("jsonb_typeof(validation_errors) = 'array'",
                           name='ck_import_rows_validation_errors_array'),
        sa.CheckConstraint("jsonb_typeof(validation_warnings) = 'array'",
                           name='ck_import_rows_validation_warnings_array'),
    )
    op.create_table(
        'audit_events', _id_column(),
        sa.Column('actor_type', sa.Text(), nullable=False),
        # Polymorphic actor/entity references deliberately survive entity deletion.
        sa.Column('actor_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('source', sa.Text(), nullable=False),
        sa.Column('action', sa.Text(), nullable=False),
        sa.Column('entity_type', sa.Text(), nullable=False),
        sa.Column('entity_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('before_data', postgresql.JSONB(), nullable=True),
        sa.Column('after_data', postgresql.JSONB(), nullable=True),
        sa.Column('request_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('import_batch_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('detected_at', sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text('now()')),
        sa.Column('changed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_audit_events'),
        sa.ForeignKeyConstraint(['import_batch_id'], ['import_batches.id'],
                                name='fk_audit_events_import_batch_id'),
    )
    op.create_index('ix_audit_events_entity', 'audit_events',
                    ['entity_type', 'entity_id', 'detected_at'])
    op.create_index('ix_audit_events_import_batch_id', 'audit_events', ['import_batch_id'])
    op.create_index('ix_audit_events_request_id', 'audit_events', ['request_id'])

    op.execute("""
        CREATE FUNCTION public.cmdb_set_updated_at() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            IF NEW IS DISTINCT FROM OLD THEN
                NEW.updated_at := statement_timestamp();
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    for table in ('sites', 'rooms', 'racks', 'devices', 'interfaces', 'ip_addresses'):
        op.execute(f'CREATE TRIGGER trg_{table}_updated_at BEFORE UPDATE ON {table} '
                   'FOR EACH ROW EXECUTE FUNCTION public.cmdb_set_updated_at()')

    op.execute("""
        CREATE FUNCTION public.cmdb_check_device_rack_bounds() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        DECLARE rack_height integer;
        BEGIN
            IF NEW.rack_id IS NULL THEN
                RETURN NEW;
            END IF;
            -- A real MVCC write serializes placement with rack resizing, including
            -- REPEATABLE READ. A row lock alone cannot invalidate a stale snapshot.
            -- The no-op keeps updated_at unchanged and stores no duplicated height.
            UPDATE public.racks SET height_u = height_u WHERE id = NEW.rack_id
                RETURNING height_u INTO rack_height;
            IF FOUND AND NEW.start_unit::bigint + NEW.height_u::bigint - 1 > rack_height THEN
                RAISE EXCEPTION 'Device placement exceeds rack height'
                    USING ERRCODE = '23514', CONSTRAINT = 'ck_devices_rack_bounds';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER trg_devices_rack_bounds
        BEFORE INSERT OR UPDATE OF rack_id, start_unit, height_u ON devices
        FOR EACH ROW EXECUTE FUNCTION public.cmdb_check_device_rack_bounds()
    """)
    op.execute("""
        CREATE FUNCTION public.cmdb_check_rack_resize() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            IF NEW.height_u IS DISTINCT FROM OLD.height_u AND EXISTS (
                SELECT 1 FROM public.devices WHERE rack_id = OLD.id
                AND start_unit::bigint + height_u::bigint - 1 > NEW.height_u
            ) THEN
                RAISE EXCEPTION 'Rack height would exclude an existing device'
                    USING ERRCODE = '23514', CONSTRAINT = 'ck_racks_device_bounds';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER trg_racks_device_bounds BEFORE UPDATE OF height_u ON racks
        FOR EACH ROW EXECUTE FUNCTION public.cmdb_check_rack_resize()
    """)
    op.execute("""
        CREATE FUNCTION public.cmdb_reject_audit_mutation() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            RAISE EXCEPTION 'audit_events is append-only' USING ERRCODE = '55000';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER trg_audit_events_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON audit_events
        FOR EACH STATEMENT EXECUTE FUNCTION public.cmdb_reject_audit_mutation()
    """)


def downgrade() -> None:
    """Remove only CMDB objects; retain potentially shared extensions."""
    for table in ('audit_events', 'import_rows', 'import_batches', 'field_overrides',
                  'ip_addresses', 'interfaces', 'devices', 'racks', 'rooms', 'sites'):
        op.drop_table(table)
    for function in ('cmdb_reject_audit_mutation', 'cmdb_check_rack_resize',
                     'cmdb_check_device_rack_bounds', 'cmdb_set_updated_at'):
        op.execute(f'DROP FUNCTION public.{function}()')
    # Do not drop btree_gist: it may predate CMDB or serve other schemas.
