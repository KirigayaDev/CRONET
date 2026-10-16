use logs_batcher::{config::Config, worker};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error);
            std::process::exit(1);
        }
    };
    let (stop, receiver) = watch::channel(false);
    tokio::spawn(async move {
        signal().await;
        let _ = stop.send(true);
    });
    if let Err(error) = worker::run(config, receiver).await {
        tracing::error!(%error, "logs-batcher stopped");
        std::process::exit(1);
    }
}

async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
