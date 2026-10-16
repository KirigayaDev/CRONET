pub mod config;
pub mod error;
pub mod framing;
pub mod model;
pub mod publisher;
pub mod spool;
pub mod tcp;
pub mod tls;
pub mod udp;

use crate::{
    config::Config,
    error::{Error, Result},
    spool::Spool,
};
use contracts::netmon::v1::RawSyslogEvent;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Notify},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub struct Ingress {
    event: RawSyslogEvent,
    committed: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct Intake {
    tx: mpsc::Sender<Ingress>,
    drops: Arc<AtomicU64>,
}
impl Intake {
    pub fn udp(&self, event: RawSyslogEvent) {
        if let Err(error) = self.tx.try_send(Ingress { event, committed: None }) {
            self.drop_udp(&error.into_inner().event, "ingress_queue_full_or_closed");
        }
    }
    pub async fn tcp(&self, event: RawSyslogEvent) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Ingress { event, committed: Some(tx) }).await.map_err(|_| Error::Task)?;
        rx.await.map_err(|_| Error::Task)
    }
    fn drop_udp(&self, event: &RawSyslogEvent, reason: &'static str) {
        drop_udp(&self.drops, event, reason);
    }
}

fn drop_udp(drops: &AtomicU64, event: &RawSyslogEvent, reason: &'static str) {
    let dropped = drops.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    // Logarithmic sampling remains bounded under a sustained UDP flood.
    if dropped.is_power_of_two() {
        tracing::warn!(transport = "udp", source_ip = %event.source_ip, bytes = event.payload.len(), site = %event.site,
                dropped_total = dropped, reason, "UDP dropped before durable acceptance");
    }
}

pub(crate) async fn blocking<T: Send + 'static>(
    spool: Arc<Spool>,
    op: impl FnOnce(&Spool) -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || op(&spool)).await.map_err(|_| Error::Task)?
}

async fn writer(
    mut rx: mpsc::Receiver<Ingress>,
    spool: Arc<Spool>,
    drops: Arc<AtomicU64>,
    wake: Arc<Notify>,
    stop: CancellationToken,
) -> Result<()> {
    while let Some(item) = rx.recv().await {
        let event = Arc::new(item.event);
        loop {
            let stored = event.clone();
            match blocking(spool.clone(), move |spool| spool.insert(&stored)).await {
                Ok(_) => {
                    let stats = blocking(spool.clone(), Spool::stats).await?;
                    tracing::debug!(transport = event.transport, source_ip = %event.source_ip, bytes = event.payload.len(),
                        event_id = %event.event_id, site = %event.site, spool_depth = stats.pending, spool_bytes = stats.used_bytes,
                        status = "spooled", "durable local commit");
                    if let Some(ack) = item.committed {
                        let _ = ack.send(());
                    }
                    wake.notify_one();
                    break;
                }
                Err(Error::Full) if item.committed.is_none() => {
                    drop_udp(&drops, &event, "spool_full");
                    break;
                }
                Err(Error::Full) => {
                    if stop.is_cancelled() {
                        tracing::warn!(transport = "tcp", source_ip = %event.source_ip, event_id = %event.event_id,
                            status = "shutdown_before_commit", "spool full; pending TCP frame was not durably accepted");
                        break;
                    }
                    tokio::select! { _ = stop.cancelled() => {}, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

/// Opens the spool before binding listeners. All accepted events survive shutdown
/// without requiring NATS to be available; remaining spool records replay next boot.
pub async fn run(cfg: Config, stop: CancellationToken) -> Result<()> {
    let path = cfg.spool_path.clone();
    let max = cfg.spool_max_bytes;
    let spool = Arc::new(tokio::task::spawn_blocking(move || Spool::open(&path, max)).await.map_err(|_| Error::Task)??);
    let stats = blocking(spool.clone(), Spool::stats).await?;
    let tls_manager = if let Some(settings) = cfg.tls.clone() {
        Some(Arc::new(
            tokio::task::spawn_blocking(move || tls::ReloadingTls::new(settings)).await.map_err(|_| Error::Task)??,
        ))
    } else {
        None
    };
    let tls_listener =
        if let Some(addr) = cfg.tls_addr { Some(tokio::net::TcpListener::bind(addr).await?) } else { None };
    let tls_addr = tls_listener
        .as_ref()
        .map(|listener| listener.local_addr())
        .transpose()?
        .map_or_else(|| "disabled".into(), |addr| addr.to_string());
    let udp = udp::bind(cfg.udp_addr, cfg.udp_rcvbuf)?;
    let tcp = tokio::net::TcpListener::bind(cfg.tcp_addr).await?;
    let udp_addr = udp.local_addr()?;
    let tcp_addr = tcp.local_addr()?;
    let (tx, rx) = mpsc::channel(cfg.channel_capacity);
    let intake = Intake { tx, drops: Arc::new(AtomicU64::new(0)) };
    let wake = Arc::new(Notify::new());
    let publisher_stop = CancellationToken::new();
    let cfg = Arc::new(cfg);
    let mut tasks = JoinSet::new();
    tasks.spawn(udp::listen(udp, cfg.clone(), intake.clone(), stop.clone()));
    tasks.spawn(tcp::listen(tcp, cfg.clone(), intake.clone(), stop.clone()));
    if let (Some(listener), Some(tls)) = (tls_listener, tls_manager) {
        tasks.spawn(tls.clone().watch(stop.clone()));
        tasks.spawn(tls::listen(listener, cfg.clone(), tls, intake.clone(), stop.clone()));
    }
    tasks.spawn(publisher::run(cfg.clone(), spool.clone(), wake.clone(), publisher_stop.clone()));
    // Move rx to a separate task; pass drops separately to avoid a sender cycle.
    let drops = intake.drops.clone();
    let writer_stop = stop.clone();
    let writer_spool = spool.clone();
    let writer_wake = wake.clone();
    let writer_handle = tokio::spawn(writer(rx, writer_spool, drops, writer_wake, writer_stop));
    drop(intake);
    tracing::info!(site = %cfg.site, gateway_id = %cfg.gateway_id, %udp_addr, %tcp_addr, %tls_addr,
        spool_depth = stats.pending, spool_bytes = stats.used_bytes, quarantined = stats.quarantined,
        status = "ready", "syslog listeners ready");

    let mut writer_handle = writer_handle;
    let mut writer_finished = false;
    let result = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        result = &mut writer_handle => { writer_finished = true; match result { Ok(Err(error)) => Err(error), _ if stop.is_cancelled() => Ok(()), _ => Err(Error::Task) } },
        result = tasks.join_next() => match result { Some(Ok(Err(error))) => Err(error), _ if stop.is_cancelled() => Ok(()), _ => Err(Error::Task) },
    };
    stop.cancel();
    // Stop ingress, drain the bounded queue to disk, then stop publication.
    // Publisher remains active during the drain so TCP can make room when possible.
    let shutdown = async {
        if !writer_finished {
            (&mut writer_handle).await.map_err(|_| Error::Task)??;
        }
        publisher_stop.cancel();
        while let Some(result) = tasks.join_next().await {
            result.map_err(|_| Error::Task)??;
        }
        Ok::<_, Error>(())
    };
    match tokio::time::timeout(cfg.shutdown_timeout, shutdown).await {
        Ok(ended) => ended?,
        Err(_) => {
            publisher_stop.cancel();
            writer_handle.abort();
            tasks.abort_all();
            return Err(Error::Task);
        }
    }
    let stats = blocking(spool, Spool::stats).await?;
    tracing::info!(
        spool_depth = stats.pending,
        spool_bytes = stats.used_bytes,
        status = "stopped",
        "pending records retained for replay"
    );
    result
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use contracts::netmon::v1::SyslogTransport;

    #[tokio::test]
    async fn bounded_ingress_udp_drops_tcp_waits_for_commit() {
        let cfg = Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap();
        let event = crate::model::envelope(&cfg, "127.0.0.1:1".parse().unwrap(), SyslogTransport::Udp, vec![]);
        let (tx, mut rx) = mpsc::channel(1);
        let intake = Intake { tx, drops: Arc::new(AtomicU64::new(0)) };
        intake.udp(event.clone());
        intake.udp(event.clone());
        assert_eq!(intake.drops.load(Ordering::Relaxed), 1);
        let sender = intake.clone();
        let task = tokio::spawn(async move { sender.tcp(event).await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        rx.recv().await.unwrap();
        let pending = rx.recv().await.unwrap();
        assert!(!task.is_finished());
        pending.committed.unwrap().send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn full_spool_drops_udp_and_backpressures_tcp_until_ack_deletion() {
        let cfg = Config::from_map([("NETMON_SITE".into(), "test".into())].into()).unwrap();
        let event =
            crate::model::envelope(&cfg, "127.0.0.1:1".parse().unwrap(), SyslogTransport::Tcp, vec![0xff; 65536]);
        let dir = tempfile::tempdir().unwrap();
        let spool = Arc::new(Spool::open(&dir.path().join("spool.redb"), crate::config::MIN_SPOOL_BYTES).unwrap());
        let mut first = None;
        loop {
            match spool.insert(&event) {
                Ok(key) => {
                    first.get_or_insert(key);
                }
                Err(error) => {
                    assert!(matches!(error, Error::Full));
                    break;
                }
            }
        }
        let (tx, rx) = mpsc::channel(1);
        let drops = Arc::new(AtomicU64::new(0));
        let intake = Intake { tx, drops: drops.clone() };
        let stop = CancellationToken::new();
        let writer = tokio::spawn(writer(rx, spool.clone(), drops.clone(), Arc::new(Notify::new()), stop.clone()));
        intake.udp(event.clone());
        tokio::time::timeout(Duration::from_secs(2), async {
            while drops.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let tcp = intake.clone();
        let pending = tokio::spawn(async move { tcp.tcp(event).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!pending.is_finished());
        spool.delete(first.unwrap()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), pending).await.unwrap().unwrap().unwrap();
        drop(intake);
        stop.cancel();
        writer.await.unwrap().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
