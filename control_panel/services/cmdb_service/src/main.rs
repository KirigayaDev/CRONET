use cmdb_service::{config::Config, db, Result};
use std::process::ExitCode;

async fn run() -> Result<()> {
    let config = Config::from_env()?;
    let pool = db::connect(&config).await?;
    db::check_connection(&pool).await?;
    println!("CMDB core ready: PostgreSQL connection verified");
    pool.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("CMDB startup failed: {error}");
            ExitCode::FAILURE
        }
    }
}
