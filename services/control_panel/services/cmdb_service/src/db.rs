use crate::{config::Config, Result};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;

pub async fn connect(config: &Config) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(config.database_url())
        .await?)
}

pub async fn check_connection(pool: &PgPool) -> Result<()> {
    sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool).await?;
    Ok(())
}

/// Check the relations used by D1, without tying runtime health to an Alembic head.
pub async fn schema_ready(pool: &PgPool) -> Result<bool> {
    const TABLES: &[&str] = &[
        "sites",
        "rooms",
        "racks",
        "devices",
        "interfaces",
        "ip_addresses",
        "field_overrides",
        "audit_events",
        "import_batches",
        "import_rows",
    ];
    Ok(sqlx::query_scalar(
        "SELECT bool_and(EXISTS (
            SELECT 1 FROM pg_catalog.pg_class
            WHERE oid = pg_catalog.to_regclass(required_table.name)
              AND relkind IN ('r', 'p')
         )) FROM unnest($1::text[]) AS required_table(name)",
    )
    .bind(TABLES)
    .fetch_one(pool)
    .await?)
}
