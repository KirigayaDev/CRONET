"""Integration checks; run only with compose.cmdb-test.yml on an ephemeral DB."""
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from uuid import UUID, uuid4

import psycopg2


PREVIOUS_HEAD = '72ca3097da29'
CMDB_HEAD = '8b6e2f4a9c10'
TABLES = {
    'sites', 'rooms', 'racks', 'devices', 'interfaces', 'ip_addresses',
    'field_overrides', 'audit_events', 'import_batches', 'import_rows',
}
MIGRATIONS_ROOT = Path(__file__).resolve().parents[1]


def connect():
    # This suite intentionally performs DDL/downgrade. Fail closed on any other DB.
    if os.environ.get('POSTGRES_DB') != 'cronet_cmdb_test_schema':
        raise RuntimeError('CMDB tests require the dedicated ephemeral test database')
    return psycopg2.connect(
        host='postgres', dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'],
        options='-c statement_timeout=10000 -c lock_timeout=5000',
    )


def alembic(*arguments):
    return subprocess.run(
        [sys.executable, '-B', '-m', 'alembic', *arguments],
        cwd=MIGRATIONS_ROOT, check=True, capture_output=True, text=True,
    ).stdout.strip()


def scalar(connection, sql, parameters=()):
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchone()[0]


def fingerprint(connection, tables):
    """Compare catalog definitions, not volatile object IDs."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name, column_name, data_type, udt_name, is_nullable,
                   column_default, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
        """, (list(tables),))
        columns = cursor.fetchall()
        cursor.execute("""
            SELECT c.relname, con.conname, pg_get_constraintdef(con.oid)
            FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = ANY(%s)
            ORDER BY c.relname, con.conname
        """, (list(tables),))
        constraints = cursor.fetchall()
        cursor.execute("""
            SELECT tablename, indexname, indexdef FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = ANY(%s)
            ORDER BY tablename, indexname
        """, (list(tables),))
        indexes = cursor.fetchall()
        cursor.execute("""
            SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid)
            FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND NOT t.tgisinternal
            AND c.relname = ANY(%s) ORDER BY c.relname, t.tgname
        """, (list(tables),))
        triggers = cursor.fetchall()
    return columns, constraints, indexes, triggers


class CmdbMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection = connect()
        try:
            existing = scalar(connection, """
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            if existing:
                raise RuntimeError('Test database must be empty; refusing to reuse it')
            connection.rollback()
            if alembic('heads') != f'{CMDB_HEAD} (head)':
                raise AssertionError('Expected exactly one CMDB migration head')
            history = alembic('history')
            if f'{PREVIOUS_HEAD} -> {CMDB_HEAD}' not in history:
                raise AssertionError('CMDB must extend the existing migration chain')
            print(f'\nMigration chain: {history}', flush=True)
            alembic('upgrade', PREVIOUS_HEAD)
            cls.user_schema = fingerprint(connection, {'users'})
            cls.user_id = scalar(connection, """
                INSERT INTO users (username, email, password_hash, display_name)
                VALUES ('cmdb_test', 'cmdb-test@example.invalid',
                        'test-only-hash', 'CMDB test') RETURNING uuid
            """)
            cls.user_data = scalar(connection,
                                   'SELECT row_to_json(users)::text FROM users WHERE uuid = %s',
                                   (cls.user_id,))
            connection.commit()
            alembic('upgrade', 'head')
            if scalar(connection, 'SELECT version_num FROM alembic_version') != CMDB_HEAD:
                raise AssertionError('Upgrade did not reach CMDB head')
        finally:
            connection.close()

    def setUp(self):
        self.connection = connect()

    def tearDown(self):
        self.connection.rollback()
        self.connection.close()

    def reject(self, sql, parameters=(), code='23514'):
        with self.connection.cursor() as cursor:
            cursor.execute('SAVEPOINT cmdb_expected_error')
            try:
                with self.assertRaises(psycopg2.Error) as raised:
                    cursor.execute(sql, parameters)
                self.assertEqual(raised.exception.pgcode, code)
            finally:
                cursor.execute('ROLLBACK TO SAVEPOINT cmdb_expected_error')
                cursor.execute('RELEASE SAVEPOINT cmdb_expected_error')

    def location(self, height=42):
        site = scalar(self.connection,
                      'INSERT INTO sites (name) VALUES (%s) RETURNING id',
                      (f'test site {uuid4()}',))
        room = scalar(self.connection,
                      "INSERT INTO rooms (site_id, name) VALUES (%s, 'test room') RETURNING id",
                      (site,))
        rack = scalar(self.connection, """
            INSERT INTO racks (room_id, name, height_u)
            VALUES (%s, 'test rack', %s) RETURNING id
        """, (room, height))
        return site, room, rack

    def device(self, rack=None, start=None, height=None, status='active'):
        return scalar(self.connection, """
            INSERT INTO devices (rack_id, start_unit, height_u, lifecycle_status)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (rack, start, height, status))

    def interface(self, device, if_index=None):
        return scalar(self.connection, """
            INSERT INTO interfaces (device_id, name, if_index, mac_address)
            VALUES (%s, 'eth0', %s, 'aa:bb:cc:dd:ee:ff') RETURNING id
        """, (device, if_index))

    def batch(self):
        return scalar(self.connection, """
            INSERT INTO import_batches (created_by, filename)
            VALUES (%s, 'inventory.csv') RETURNING id
        """, (self.user_id,))

    def test_tables_types_and_normalization(self):
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            self.assertEqual({row[0] for row in cursor}, TABLES | {'users', 'alembic_version'})
            cursor.execute("""
                SELECT table_name, column_name, udt_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ANY(%s)
            """, (list(TABLES),))
            types = {(table, column): kind for table, column, kind in cursor}
        for table in TABLES:
            self.assertEqual(types[table, 'id'], 'uuid')
        for table in ('sites', 'rooms', 'racks', 'devices', 'interfaces', 'ip_addresses'):
            self.assertEqual(types[table, 'created_at'], 'timestamptz')
            self.assertEqual(types[table, 'updated_at'], 'timestamptz')
        self.assertEqual(types['interfaces', 'mac_address'], 'macaddr')
        self.assertEqual(types['ip_addresses', 'address'], 'inet')
        self.assertEqual(types['audit_events', 'before_data'], 'jsonb')
        for column in ('site_id', 'room_id', 'address', 'mac_address'):
            self.assertNotIn(('devices', column), types)
        self.assertNotIn(('racks', 'site_id'), types)
        self.assertNotIn(('field_overrides', 'value'), types)
        device = self.device(height=2)
        self.assertEqual(UUID(device).version, 4)

    def test_existing_users_unchanged(self):
        self.assertEqual(fingerprint(self.connection, {'users'}), self.user_schema)
        self.assertEqual(scalar(self.connection,
                               'SELECT row_to_json(users)::text FROM users WHERE uuid = %s',
                               (self.user_id,)), self.user_data)

    def test_hierarchy_uniqueness_and_foreign_keys(self):
        site, room, rack = self.location()
        self.reject("INSERT INTO rooms (site_id, name) VALUES (%s, 'test room')",
                    (site,), '23505')
        self.reject("INSERT INTO racks (room_id, name, height_u) VALUES (%s, 'test rack', 42)",
                    (room,), '23505')
        other_site, other_room, _ = self.location()
        self.assertNotEqual(site, other_site)
        self.assertNotEqual(room, other_room)
        self.reject("INSERT INTO rooms (site_id, name) VALUES (%s, 'missing')",
                    (str(uuid4()),), '23503')
        self.reject('DELETE FROM sites WHERE id = %s', (site,), '23503')
        self.reject('DELETE FROM rooms WHERE id = %s', (room,), '23503')
        device = self.device(rack, 1, 1)
        self.reject('DELETE FROM racks WHERE id = %s', (rack,), '23503')
        self.interface(device)
        self.reject('DELETE FROM devices WHERE id = %s', (device,), '23503')

    def test_site_names_are_logically_unique(self):
        name = 'Site A'
        site = scalar(self.connection,
                      'INSERT INTO sites (name) VALUES (%s) RETURNING id', (name,))
        for duplicate in (name, 'site a', 'SITE A', '  Site A  ', '\tSite A\r\n',
                          'Site  A', 'Site\tA'):
            with self.subTest(duplicate=duplicate):
                self.reject('INSERT INTO sites (name) VALUES (%s)', (duplicate,), '23505')
        self.assertEqual(scalar(self.connection, 'SELECT name FROM sites WHERE id = %s',
                                (site,)), name)

    def test_room_names_are_logically_unique_within_site(self):
        site, _, _ = self.location()
        for duplicate in ('test room', 'TEST ROOM', '  test room  ', '\ttest room\n',
                          'test  room', 'test\troom'):
            with self.subTest(duplicate=duplicate):
                self.reject('INSERT INTO rooms (site_id, name) VALUES (%s, %s)',
                            (site, duplicate), '23505')
        other_site = scalar(self.connection,
                            "INSERT INTO sites (name) VALUES ('other site') RETURNING id")
        scalar(self.connection, 'INSERT INTO rooms (site_id, name) VALUES (%s, %s) RETURNING id',
               (other_site, ' TEST ROOM '))

    def test_rack_names_are_logically_unique_within_room(self):
        site, room, _ = self.location()
        for duplicate in ('test rack', 'TEST RACK', '  test rack  ', '\ttest rack\n',
                          'test  rack', 'test\track'):
            with self.subTest(duplicate=duplicate):
                self.reject('INSERT INTO racks (room_id, name, height_u) VALUES (%s, %s, 42)',
                            (room, duplicate), '23505')
        other_room = scalar(self.connection, """
            INSERT INTO rooms (site_id, name) VALUES (%s, 'other room') RETURNING id
        """, (site,))
        scalar(self.connection, """
            INSERT INTO racks (room_id, name, height_u) VALUES (%s, %s, 42) RETURNING id
        """, (other_room, ' TEST RACK '))

    def test_required_names_reject_empty_and_whitespace(self):
        site, room, _ = self.location()
        device = self.device()
        statements = (
            ('sites', 'INSERT INTO sites (name) VALUES (%s)', ()),
            ('rooms', 'INSERT INTO rooms (site_id, name) VALUES (%s, %s)', (site,)),
            ('racks', 'INSERT INTO racks (room_id, height_u, name) VALUES (%s, 42, %s)', (room,)),
            ('interfaces', 'INSERT INTO interfaces (device_id, name) VALUES (%s, %s)', (device,)),
        )
        for table, sql, parameters in statements:
            for blank in ('', ' ', '   ', '\t', '\r\n', ' \t\n\v\f\r '):
                with self.subTest(table=table, blank=blank):
                    self.reject(sql, (*parameters, blank))

    def test_name_updates_enforce_uniqueness_and_nonblank(self):
        site, room, rack = self.location()
        other_site = scalar(self.connection,
                            "INSERT INTO sites (name) VALUES ('other site') RETURNING id")
        site_name = scalar(self.connection, 'SELECT name FROM sites WHERE id = %s', (site,))
        self.reject('UPDATE sites SET name = %s WHERE id = %s',
                    (f'  {site_name.upper()}  ', other_site), '23505')
        other_room = scalar(self.connection, """
            INSERT INTO rooms (site_id, name) VALUES (%s, 'other room') RETURNING id
        """, (site,))
        self.reject("UPDATE rooms SET name = ' TEST  ROOM ' WHERE id = %s", (other_room,), '23505')
        other_rack = scalar(self.connection, """
            INSERT INTO racks (room_id, name, height_u)
            VALUES (%s, 'other rack', 42) RETURNING id
        """, (room,))
        self.reject("UPDATE racks SET name = ' TEST  RACK ' WHERE id = %s", (other_rack,), '23505')
        interface = self.interface(self.device())
        for table, entity in (('sites', site), ('rooms', room), ('racks', rack),
                              ('interfaces', interface)):
            with self.subTest(table=table):
                self.reject(f'UPDATE {table} SET name = %s WHERE id = %s', ('\t \n', entity))

    def test_normalization_preserves_original_display_name(self):
        name = '  Display\t  Name  '
        site = scalar(self.connection, 'INSERT INTO sites (name) VALUES (%s) RETURNING id', (name,))
        self.assertEqual(scalar(self.connection, 'SELECT name FROM sites WHERE id = %s', (site,)), name)
        self.reject("INSERT INTO sites (name) VALUES ('display name')", code='23505')

    def test_placement_completeness_positive_dimensions_and_lifecycle(self):
        _, room, rack = self.location()
        self.device()
        self.device(height=2)
        for values in ((rack, None, 1), (rack, 1, None), (None, 1, 1),
                       (rack, 0, 1), (rack, 1, 0), (None, None, -1)):
            self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,%s,%s)',
                        values)
        self.reject("INSERT INTO racks (room_id, name, height_u) VALUES (%s, 'zero', 0)",
                    (room,))
        self.reject("INSERT INTO devices (lifecycle_status) VALUES ('unknown')")
        self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,1,1)',
                    (str(uuid4()),), '23503')

    def test_rack_bounds_and_resize(self):
        _, _, rack = self.location()
        device = self.device(rack, 41, 2)
        self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,42,2)',
                    (rack,))
        self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,2147483647,2)',
                    (rack,))
        self.reject('UPDATE devices SET height_u = 3 WHERE id = %s', (device,))
        self.reject('UPDATE racks SET height_u = 41 WHERE id = %s', (rack,))
        with self.connection.cursor() as cursor:
            cursor.execute('UPDATE racks SET height_u = 48 WHERE id = %s', (rack,))
            cursor.execute('UPDATE devices SET height_u = 8 WHERE id = %s', (device,))
        self.reject('UPDATE racks SET height_u = 47 WHERE id = %s', (rack,))

    def test_active_overlap_adjacency_and_reactivation(self):
        _, _, rack = self.location()
        self.device(rack, 1, 2)
        self.device(rack, 3, 1)
        self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,2,2)',
                    (rack,), '23P01')
        maintenance = self.device(rack, 4, 2, 'maintenance')
        self.device(rack, 6, 2, 'decommissioned')
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE devices SET lifecycle_status = 'active' WHERE id = %s",
                           (maintenance,))
        _, _, other_rack = self.location()
        self.device(other_rack, 1, 2)
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO devices (hostname, serial) VALUES ('same', 'same'), ('same', 'same')")

    def test_maintenance_placement_blocks_every_lifecycle(self):
        _, _, rack = self.location()
        device = self.device(rack, 1, 2)
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE devices SET lifecycle_status = 'maintenance' WHERE id = %s", (device,))
        for status in ('active', 'maintenance', 'decommissioned'):
            with self.subTest(status=status):
                self.reject("""
                    INSERT INTO devices (rack_id, start_unit, height_u, lifecycle_status)
                    VALUES (%s, 2, 2, %s)
                """, (rack, status), '23P01')

    def test_decommissioned_placement_blocks_every_lifecycle(self):
        _, _, rack = self.location()
        device = self.device(rack, 1, 2, 'maintenance')
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE devices SET lifecycle_status = 'decommissioned' WHERE id = %s", (device,))
        for status in ('active', 'maintenance', 'decommissioned'):
            with self.subTest(status=status):
                self.reject("""
                    INSERT INTO devices (rack_id, start_unit, height_u, lifecycle_status)
                    VALUES (%s, 2, 2, %s)
                """, (rack, status), '23P01')

    def test_removing_placement_releases_units(self):
        for status in ('active', 'maintenance', 'decommissioned'):
            with self.subTest(status=status):
                _, _, rack = self.location()
                device = self.device(rack, 1, 2, status)
                self.reject('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,1,2)',
                            (rack,), '23P01')
                self.reject('UPDATE devices SET rack_id = NULL WHERE id = %s', (device,))
                with self.connection.cursor() as cursor:
                    cursor.execute('UPDATE devices SET rack_id = NULL, start_unit = NULL WHERE id = %s',
                                   (device,))
                self.device(rack, 1, 2)
                self.assertEqual(scalar(self.connection,
                                       'SELECT lifecycle_status FROM devices WHERE id = %s', (device,)), status)
                self.assertTrue(scalar(self.connection,
                                       'SELECT rack_id IS NULL AND start_unit IS NULL FROM devices WHERE id = %s',
                                       (device,)))
                self.reject('UPDATE devices SET rack_id = %s, start_unit = 1 WHERE id = %s',
                            (rack, device), '23P01')

    def test_interfaces_and_network_identity_are_locally_unique(self):
        first_device, second_device = self.device(), self.device()
        first = self.interface(first_device, 1)
        second = self.interface(second_device, 1)
        self.interface(first_device)
        self.interface(first_device)
        self.reject("INSERT INTO interfaces (device_id, name, if_index) VALUES (%s, 'eth1', 1)",
                    (first_device,), '23505')
        self.reject("INSERT INTO interfaces (device_id, name, if_index) VALUES (%s, 'bad', 0)",
                    (first_device,))
        with self.connection.cursor() as cursor:
            for interface in (first, second):
                cursor.execute('INSERT INTO ip_addresses (interface_id, address) VALUES (%s,%s)',
                               (interface, '192.0.2.1/24'))
                cursor.execute('INSERT INTO ip_addresses (interface_id, address) VALUES (%s,%s)',
                               (interface, '2001:db8::1/64'))
        self.reject('INSERT INTO ip_addresses (interface_id, address) VALUES (%s,%s)',
                    (first, '192.0.2.1/24'), '23505')
        self.reject('INSERT INTO ip_addresses (interface_id, address) VALUES (%s,%s)',
                    (first, 'invalid-ip'), '22P02')
        self.reject("INSERT INTO interfaces (device_id, name, mac_address) VALUES (%s, 'bad', 'invalid-mac')",
                    (first_device,), '22P02')
        self.reject('DELETE FROM interfaces WHERE id = %s', (first,), '23503')

    def test_overrides_reference_users_without_duplicating_values(self):
        device = self.device()
        parameters = (device, self.user_id)
        sql = """
            INSERT INTO field_overrides (entity_type, entity_id, field_name, locked_by)
            VALUES ('devices', %s, 'hostname', %s)
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, parameters)
        self.reject(sql, parameters, '23505')
        self.reject(sql, (str(uuid4()), str(uuid4())), '23503')
        self.reject("""
            INSERT INTO field_overrides (entity_type, entity_id, field_name, locked_by)
            VALUES ('devices', %s, ' ', %s)
        """, parameters)

    def test_import_staging_shapes_statuses_and_confirmation(self):
        devices_before = scalar(self.connection, 'SELECT count(*) FROM devices')
        batch = self.batch()
        sql = """
            INSERT INTO import_rows (batch_id, row_number, raw_data, working_data)
            VALUES (%s, %s, %s::jsonb, %s::jsonb) RETURNING id
        """
        row = scalar(self.connection, sql, (batch, 1, '{"hostname":"raw"}', '{"hostname":"edited"}'))
        self.reject(sql, (batch, 1, '{}', '{}'), '23505')
        self.reject(sql, (batch, 0, '{}', '{}'))
        self.reject(sql, (batch, 2, '[]', '{}'))
        self.reject(sql, (batch, 2, '{}', 'null'))
        self.reject("UPDATE import_rows SET validation_errors = '{}' WHERE id = %s", (row,))
        self.reject("UPDATE import_rows SET validation_warnings = '{}' WHERE id = %s", (row,))
        self.reject("UPDATE import_rows SET status = 'unknown' WHERE id = %s", (row,))
        self.reject("UPDATE import_batches SET status = 'confirmed' WHERE id = %s", (batch,))
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE import_batches SET status = 'confirmed', confirmed_at = now() WHERE id = %s",
                           (batch,))
        self.assertEqual(scalar(self.connection, 'SELECT count(*) FROM devices'), devices_before)
        self.assertEqual(scalar(self.connection, 'SELECT validation_warnings FROM import_rows WHERE id = %s',
                                (row,)), [])

    def test_audit_is_append_only_and_participates_in_entity_transaction(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SAVEPOINT cmdb_transaction')
        device = self.device()
        event = scalar(self.connection, """
            INSERT INTO audit_events (actor_type, actor_id, source, action,
                                      entity_type, entity_id, after_data, request_id)
            VALUES ('user', %s, 'api', 'create', 'devices', %s, '{}', %s) RETURNING id
        """, (self.user_id, device, str(uuid4())))
        self.reject("UPDATE audit_events SET action = 'changed' WHERE id = %s", (event,), '55000')
        self.reject('DELETE FROM audit_events WHERE id = %s', (event,), '55000')
        self.reject('TRUNCATE audit_events', code='55000')
        with self.connection.cursor() as cursor:
            cursor.execute('ROLLBACK TO SAVEPOINT cmdb_transaction')
            cursor.execute('RELEASE SAVEPOINT cmdb_transaction')
        self.assertEqual(scalar(self.connection, 'SELECT count(*) FROM devices WHERE id = %s', (device,)), 0)
        self.assertEqual(scalar(self.connection, 'SELECT count(*) FROM audit_events WHERE id = %s', (event,)), 0)

    def test_updated_at_changes_without_touching_rack_on_placement(self):
        site, _, rack = self.location()
        rack_time = scalar(self.connection, 'SELECT updated_at FROM racks WHERE id = %s', (rack,))
        self.device(rack, 1, 1)
        self.assertEqual(scalar(self.connection, 'SELECT updated_at FROM racks WHERE id = %s', (rack,)), rack_time)
        before = scalar(self.connection, 'SELECT updated_at FROM sites WHERE id = %s', (site,))
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE sites SET description = 'changed' WHERE id = %s", (site,))
        self.assertGreater(scalar(self.connection, 'SELECT updated_at FROM sites WHERE id = %s', (site,)), before)

    def blocked_write(self, sql, parameters, release, expected_code):
        """Require an observed DB lock, then release the competing transaction."""
        peer = connect()
        peer_pid = peer.get_backend_pid()
        result = []

        def writer():
            try:
                with peer.cursor() as cursor:
                    cursor.execute(sql, parameters)
                peer.commit()
                result.append('committed')
            except psycopg2.Error as error:
                peer.rollback()
                result.append(error.pgcode)
            finally:
                peer.close()

        worker = threading.Thread(target=writer, daemon=True)
        observer = connect()
        observer.autocommit = True
        worker.start()
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                waiting = scalar(observer,
                                 'SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s',
                                 (peer_pid,))
                if waiting == 'Lock':
                    break
                if not worker.is_alive():
                    self.fail(f'Concurrent writer did not wait: {result}')
                time.sleep(0.02)
            else:
                self.fail('Concurrent writer never acquired a pending database lock')
            release()
            worker.join(timeout=10)
            self.assertFalse(worker.is_alive(), 'Concurrent writer did not finish')
            self.assertEqual(result, [expected_code])
        finally:
            observer.close()
            self.connection.rollback()
            worker.join(timeout=10)

    def test_concurrent_active_overlap_is_rejected_after_commit(self):
        for status in ('active', 'maintenance', 'decommissioned'):
            with self.subTest(status=status):
                _, _, rack = self.location()
                self.connection.commit()
                self.device(rack, 1, 2, status)
                self.blocked_write("""
                    INSERT INTO devices (rack_id, start_unit, height_u, lifecycle_status)
                    VALUES (%s, 2, 2, 'maintenance')
                """, (rack,), self.connection.commit, '23P01')

    def test_concurrent_normalized_site_name_is_rejected_after_commit(self):
        name = f'concurrent site {uuid4()}'
        scalar(self.connection, 'INSERT INTO sites (name) VALUES (%s) RETURNING id', (name,))
        self.blocked_write('INSERT INTO sites (name) VALUES (%s)',
                           (f'  {name.upper()}  ',), self.connection.commit, '23505')

    def test_concurrent_placement_succeeds_after_competitor_rollback(self):
        _, _, rack = self.location()
        self.connection.commit()
        self.device(rack, 1, 2)
        self.blocked_write('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,2,2)',
                           (rack,), self.connection.rollback, 'committed')

    def test_concurrent_rack_shrink_rechecks_committed_placement(self):
        _, _, rack = self.location()
        self.connection.commit()
        self.device(rack, 40, 3)
        self.blocked_write('UPDATE racks SET height_u = 39 WHERE id = %s',
                           (rack,), self.connection.commit, '23514')

    def test_concurrent_placement_rechecks_committed_rack_shrink(self):
        _, _, rack = self.location()
        self.connection.commit()
        with self.connection.cursor() as cursor:
            cursor.execute('UPDATE racks SET height_u = 39 WHERE id = %s', (rack,))
        self.blocked_write('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,40,3)',
                           (rack,), self.connection.commit, '23514')

    def test_repeatable_read_and_serializable_reject_stale_rack_snapshot(self):
        for isolation in ('REPEATABLE READ', 'SERIALIZABLE'):
            with self.subTest(isolation=isolation):
                _, _, rack = self.location()
                self.connection.commit()
                self.connection.set_session(isolation_level=isolation)
                scalar(self.connection, 'SELECT height_u FROM racks WHERE id = %s', (rack,))
                peer = connect()
                try:
                    with peer.cursor() as cursor:
                        cursor.execute('INSERT INTO devices (rack_id, start_unit, height_u) VALUES (%s,40,3)',
                                       (rack,))
                    peer.commit()
                finally:
                    peer.close()
                self.reject('UPDATE racks SET height_u = 39 WHERE id = %s', (rack,), '40001')
                self.connection.rollback()
                self.connection.set_session(isolation_level='READ COMMITTED')

    def test_zzz_downgrade_upgrade_restores_schema_and_preserves_users(self):
        before = fingerprint(self.connection, TABLES)
        self.connection.rollback()
        alembic('downgrade', PREVIOUS_HEAD)
        self.assertEqual(scalar(self.connection, 'SELECT version_num FROM alembic_version'), PREVIOUS_HEAD)
        self.assertEqual(scalar(self.connection, """
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(%s)
        """, (list(TABLES),)), 0)
        self.assertEqual(scalar(self.connection, """
            SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname LIKE 'cmdb_%%'
        """), 0)
        self.assertEqual(fingerprint(self.connection, {'users'}), self.user_schema)
        self.assertEqual(scalar(self.connection,
                               'SELECT row_to_json(users)::text FROM users WHERE uuid = %s',
                               (self.user_id,)), self.user_data)
        self.connection.rollback()
        alembic('upgrade', 'head')
        self.assertEqual(fingerprint(self.connection, TABLES), before)
        self.assertEqual(scalar(self.connection, 'SELECT version_num FROM alembic_version'), CMDB_HEAD)
        print('\nUpgrade/downgrade/upgrade and users preservation: OK', flush=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
