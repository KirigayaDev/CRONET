use cmdb_service::{
    api::{self, ApiState},
    auth,
    config::Config,
    db, Error, Result, Service,
};
use std::process::ExitCode;

async fn run() -> Result<()> {
    let config = Config::from_env()?;
    let public_key = auth::load_public_key(config.public_key_path()?)
        .map_err(|_| Error::Internal("JWT public key could not be loaded"))?;
    let pool = db::connect(&config).await?;
    db::check_connection(&pool).await?;
    let redis = auth::connect_redis(config.redis_url()?)
        .await
        .map_err(|_| Error::Internal("Redis authentication dependency unavailable"))?;
    let authenticator =
        auth::Authenticator::new(pool.clone(), redis, &public_key, &config.jwt_issuer, &config.jwt_audience)
            .map_err(|_| Error::Internal("JWT public key is invalid"))?;
    let state = ApiState { service: Service::new(pool.clone()), auth: authenticator };
    let http =
        tokio::net::TcpListener::bind(config.http_addr).await.map_err(|_| Error::Internal("HTTP bind failed"))?;
    let grpc =
        tokio::net::TcpListener::bind(config.grpc_addr).await.map_err(|_| Error::Internal("gRPC bind failed"))?;
    tracing::info!(http=%http.local_addr().map_err(|_|Error::Internal("HTTP address unavailable"))?,grpc=%grpc.local_addr().map_err(|_|Error::Internal("gRPC address unavailable"))?,"CMDB servers ready");
    let result = api::serve(state, http, grpc, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    pool.close().await;
    result
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt().with_env_filter("cmdb_service=info,tower_http=info").with_target(false).init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("CMDB startup failed: {error}");
            ExitCode::FAILURE
        }
    }
}
