pub mod dto;
pub mod grpc;
pub mod http;
pub mod workflow;

#[derive(Clone)]
pub struct ApiState {
    pub service: crate::Service,
    pub auth: crate::auth::Authenticator,
}

/// If either server exits unexpectedly, stop its peer and return an error.
pub async fn serve(
    state: ApiState,
    http_listener: tokio::net::TcpListener,
    grpc_listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> crate::Result<()> {
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let http_stop = receiver.clone();
    let http = axum::serve(http_listener, http::router(state.clone()))
        .with_graceful_shutdown(wait_shutdown(http_stop))
        .into_future();
    let grpc = tonic::transport::Server::builder()
        .add_service(grpc::pb::cmdb_server::CmdbServer::new(grpc::GrpcApi::new(state)))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(grpc_listener),
            wait_shutdown(receiver),
        );
    tokio::pin!(http, grpc, shutdown);
    let result = tokio::select! {
        _ = &mut shutdown => { Ok(()) },
        _ = &mut http => { Err(crate::Error::Internal("HTTP server stopped unexpectedly")) },
        _ = &mut grpc => { Err(crate::Error::Internal("gRPC server stopped unexpectedly")) },
    };
    let _ = stop.send(true);
    // Completed futures must not be polled again after a server failure.
    if result.is_ok() {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let _ = tokio::join!(&mut http, &mut grpc);
        })
        .await;
    }
    result
}

async fn wait_shutdown(mut receiver: tokio::sync::watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

use std::future::IntoFuture;
