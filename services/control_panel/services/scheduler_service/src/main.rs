use scheduler_service::{config::Config, runtime};
use std::process::ExitCode;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt().json().with_writer(std::io::stderr).with_env_filter("scheduler_service=info").init();
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(error=%error,"Scheduler startup failed");
            return ExitCode::FAILURE;
        }
    };
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        wait_signal().await;
        signal.cancel();
    });
    match runtime::run(config, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error=%error,"Scheduler stopped with error");
            ExitCode::FAILURE
        }
    }
}
async fn wait_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}};
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
