use syslog_gateway::{config::Config, error::Result};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

async fn shutdown() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = term.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Dependencies may include connection URLs in their own diagnostics. Keep
    // third-party logging off even if RUST_LOG requests a broad level.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("syslog_gateway=info"))
        .add_directive("async_nats=off".parse().map_err(|_| syslog_gateway::error::Error::Task)?);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json().with_writer(std::io::stdout).with_filter(
            tracing_subscriber::filter::filter_fn(|metadata| metadata.target().starts_with("syslog_gateway")),
        ))
        .init();
    let cfg = Config::from_env()?;
    let stop = CancellationToken::new();
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        let result = shutdown().await;
        signal_stop.cancel();
        result
    });
    let result = syslog_gateway::run(cfg, stop).await;
    signal.abort();
    result
}
