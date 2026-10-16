"""Device physical inventory history.

Revision ID: d4c1a2b3e001
Revises: 8b6e2f4a9c10
"""
from alembic import op

revision = 'd4c1a2b3e001'
down_revision = '8b6e2f4a9c10'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE inventory_snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            device_id UUID NOT NULL REFERENCES devices(id) ON DELETE RESTRICT,
            collected_at TIMESTAMPTZ NOT NULL CHECK (isfinite(collected_at)),
            content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
            component_count INTEGER NOT NULL CHECK (component_count BETWEEN 0 AND 10000),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        CREATE INDEX ix_inventory_snapshots_device_collected
            ON inventory_snapshots(device_id, collected_at DESC, created_at DESC, id DESC);
        CREATE TABLE inventory_components (
            snapshot_id UUID NOT NULL REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
            ent_physical_index INTEGER NOT NULL CHECK (ent_physical_index > 0),
            contained_in INTEGER CHECK (contained_in >= 0 AND contained_in <> ent_physical_index),
            parent_rel_pos INTEGER CHECK (parent_rel_pos >= -1),
            class TEXT NOT NULL CHECK (class <> '' AND class = btrim(class) AND length(class) <= 64),
            name TEXT CHECK (length(name) <= 255),
            description TEXT CHECK (length(description) <= 1024),
            vendor_type TEXT CHECK (length(vendor_type) <= 255),
            serial TEXT CHECK (length(serial) <= 255),
            model TEXT CHECK (length(model) <= 255),
            hardware_rev TEXT CHECK (length(hardware_rev) <= 255),
            firmware_rev TEXT CHECK (length(firmware_rev) <= 255),
            software_rev TEXT CHECK (length(software_rev) <= 255),
            PRIMARY KEY (snapshot_id, ent_physical_index)
        );
    """)
    # The composite primary key also indexes snapshot_id as its leading column.


def downgrade():
    op.execute('DROP TABLE inventory_components; DROP TABLE inventory_snapshots;')
