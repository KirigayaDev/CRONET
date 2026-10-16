"""Disposable auth-user fixture only. Canonical inventory is created through CMDB HTTP."""
import json
import pathlib
import uuid

import psycopg2

with psycopg2.connect(host="postgres", user="cmdb_test", dbname="cronet_cmdb_test_schema") as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(),version_num FROM alembic_version")
        assert cursor.fetchone() == ("cronet_cmdb_test_schema", "8b6e2f4a9c10"), "Refusing a non-test database"
        identities = {}
        for role, admin in [("admin", True), ("reader", False)]:
            marker = uuid.uuid4().hex
            cursor.execute(
                "INSERT INTO users (username,email,password_hash,display_name,is_admin,is_super_admin) "
                "VALUES (%s,%s,'test-only-hash','Scheduler E2E',%s,false) RETURNING uuid,token_version_uuid",
                (marker, marker + "@example.invalid", admin),
            )
            user_id, version = cursor.fetchone()
            identities[role] = {"id": str(user_id), "version": str(version)}
pathlib.Path("/test-auth/identities.json").write_text(json.dumps(identities), encoding="utf-8")
print("Synthetic Scheduler auth fixture ready")
