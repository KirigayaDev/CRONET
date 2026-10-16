use crate::{
    blocking,
    config::Config,
    error::{Error, Result},
    spool::Spool,
};
use bus::{Bus, BusConfig};
use contracts::subjects;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn backoff(attempt: u32) -> Duration {
    let ceiling = 250u64.saturating_mul(1u64 << attempt.min(7)).min(30_000);
    let random = uuid::Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(ceiling / 2 + random % (ceiling / 2 + 1))
}

pub async fn run(cfg: Arc<Config>, spool: Arc<Spool>, wake: Arc<Notify>, stop: CancellationToken) -> Result<()> {
    // The existing bus owns reconnection and Nats-Msg-Id/ACK handling.
    let bus_config =
        BusConfig { url: cfg.nats_url.clone(), creds_file: cfg.nats_creds.clone(), name: cfg.gateway_id.clone() };
    let bus = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        bus = Bus::connect(&bus_config) => bus.map_err(|_| Error::NatsConfig)?,
    };
    let mut ready = !cfg.bootstrap_streams;
    let mut attempts = 0u32;
    let mut newest = false;
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        let mut failed = false;
        if !ready {
            ready = matches!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    bus.ensure_streams(cfg.stream_replicas, &cfg.stream_limits)
                )
                .await,
                Ok(Ok(()))
            );
            failed = !ready;
        }
        if ready {
            let limit = cfg.replay_batch;
            let entries = blocking(spool.clone(), move |spool| spool.batch(limit, newest)).await?;
            newest = !newest; // Alternate FIFO backlog and newest arrivals.
            if entries.is_empty() {
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = wake.notified() => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                continue;
            }
            for entry in entries {
                let event = &entry.event;
                let published = tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    result = tokio::time::timeout(Duration::from_secs(5), bus.publish_proto(subjects::syslog(&event.site), &event.event_id, event)) => matches!(result, Ok(Ok(()))),
                };
                if !published {
                    failed = true;
                    break;
                }
                let key = entry.key;
                blocking(spool.clone(), move |spool| spool.delete(key)).await?;
                tracing::debug!(transport = event.transport, source_ip = %event.source_ip, bytes = event.payload.len(),
                    event_id = %event.event_id, site = %event.site, status = "published", "JetStream ACK; spool entry removed");
                attempts = 0;
            }
        }
        if failed {
            let delay = backoff(attempts);
            attempts = attempts.saturating_add(1);
            tracing::warn!(site = %cfg.site, attempt = attempts, backoff_ms = delay.as_millis() as u64,
                status = "retry", "JetStream unavailable; records remain durable locally");
            tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(delay) => {} }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backoff_is_bounded_with_jitter() {
        for attempt in 0..100 {
            let delay = backoff(attempt);
            assert!(delay >= Duration::from_millis(125));
            assert!(delay <= Duration::from_secs(30));
        }
    }
}
