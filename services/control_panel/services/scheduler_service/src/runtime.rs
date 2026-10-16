use crate::{
    cmdb::{normalize, CmdbSource, TonicCmdbSource},
    config::{Config, Schedule, MAX_ENTRIES},
    coordinator::{Coordinator, NatsCoordination, Outcome},
    error::{Error, Result},
    membership::{bucket, validate_tasks_stream, Membership, Ownership},
    model::{Spec, Target},
    publisher::JetStreamPublisher,
    schedule::{ScheduleQueue, Work},
};
use bus::{Bus, BusConfig};
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;

pub fn specs(targets: &[Target], schedules: &[Schedule], ring: &Ownership, instance: &str) -> Result<Vec<Spec>> {
    let mut by_site: HashMap<&str, Vec<&Schedule>> = HashMap::new();
    for schedule in schedules.iter().filter(|s| s.enabled) {
        by_site.entry(&schedule.site).or_default().push(schedule);
    }
    let mut out = Vec::new();
    for target in targets {
        for schedule in by_site.get(target.site.as_str()).into_iter().flatten() {
            let spec = Spec { schedule: (*schedule).clone(), target: target.clone() };
            if ring.owner(&spec.key()) == Some(instance) {
                if out.len() >= MAX_ENTRIES {
                    return Err(Error::Config);
                }
                out.push(spec);
            }
        }
    }
    Ok(out)
}
async fn inventory_loop(
    source: Arc<dyn CmdbSource>,
    sender: watch::Sender<Vec<Target>>,
    period: Duration,
    shutdown: CancellationToken,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let result = tokio::select! {
            biased;
            _=shutdown.cancelled()=>return,
            result=tokio::time::timeout(Duration::from_secs(30),source.fetch())=>result,
        };
        let delay = match result {
            Ok(Ok(rows)) => match normalize(rows) {
                Ok((targets, diagnostics)) => {
                    tracing::info!(
                        cmdb_targets = targets.len(),
                        unplaced = diagnostics.unplaced,
                        missing_management_ip = diagnostics.missing_ip,
                        ambiguous_management_ip = diagnostics.ambiguous_ip,
                        invalid_site = diagnostics.invalid_site,
                        "CMDB inventory refreshed"
                    );
                    if diagnostics.ambiguous_ip > 0 {
                        tracing::warn!(
                            ambiguous = diagnostics.ambiguous_ip,
                            "Devices with multiple management IPs skipped"
                        );
                    }
                    sender.send_replace(targets);
                    backoff = Duration::from_secs(1);
                    period
                }
                Err(_) => {
                    tracing::warn!("Invalid CMDB snapshot; retaining last inventory");
                    backoff
                }
            },
            _ => {
                tracing::warn!("CMDB resync unavailable; retaining last inventory");
                backoff
            }
        };
        let spread = rand::random::<u64>() % (delay.as_millis() as u64 / 4 + 1);
        let delay = (delay + Duration::from_millis(spread)).min(Duration::from_secs(3600));
        tokio::select! {_=shutdown.cancelled()=>return,_=tokio::time::sleep(delay)=>{}}
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(60));
    }
}
pub async fn run(config: Config, shutdown: CancellationToken) -> Result<()> {
    let bus_config = BusConfig {
        url: config.nats_url.clone(),
        creds_file: config.nats_creds.clone(),
        name: format!("scheduler-{}", config.instance_id),
    };
    let (bus, mut member, store) = loop {
        let startup = async {
            let bus = Bus::connect(&bus_config).await.map_err(|_| Error::Coordination)?;
            if config.bootstrap {
                bus.ensure_streams(config.replicas, &config.stream_limits).await.map_err(|_| Error::Coordination)?;
            }
            validate_tasks_stream(&bus).await?;
            let store = NatsCoordination::connect(
                &bus,
                &config.namespace,
                config.claim_ttl,
                config.definition.fired_ttl(),
                config.replicas,
            )
            .await?;
            let membership =
                bucket(&bus, &format!("{}_MEMBERS", config.namespace), config.member_ttl, config.replicas, false)
                    .await?;
            let member = Membership::join(membership, config.instance_id.clone()).await?;
            Ok::<_, Error>((bus, member, store))
        };
        let result = tokio::select! {biased;_=shutdown.cancelled()=>return Ok(()),result=tokio::time::timeout(Duration::from_secs(10),startup)=>result};
        if let Ok(Ok(state)) = result {
            break state;
        }
        tracing::warn!(instance_id=%config.instance_id,"NATS startup unavailable or incompatible; retrying");
        tokio::select! {_=shutdown.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(2))=>{}}
    };
    let coordinator = Arc::new(Coordinator {
        instance: config.instance_id.clone(),
        store: Arc::new(store),
        publisher: Arc::new(JetStreamPublisher(bus)),
    });
    let (inventory_tx, mut inventory_rx) = watch::channel(Vec::new());
    let source = Arc::new(TonicCmdbSource { endpoint: config.cmdb_url, token: config.token });
    let inventory_task = tokio::spawn(inventory_loop(source, inventory_tx, config.resync, shutdown.clone()));
    let mut queue = ScheduleQueue::default();
    let mut inventory = Vec::new();
    let mut ring = Arc::new(Ownership::new(Vec::new()));
    let mut in_flight: FuturesUnordered<BoxFuture<'static, (Work, Outcome)>> = FuturesUnordered::new();
    let mut heartbeat = tokio::time::interval(config.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_warning = Instant::now() - Duration::from_secs(10);
    let mut expired_occurrences = 0_u64;
    loop {
        tokio::select! {
            biased;
            _=shutdown.cancelled()=>break,
            _=heartbeat.tick()=>{
                // Slow/failing membership work is bounded and never trusts an old ownership view.
                let members=tokio::select! {_=shutdown.cancelled()=>break,result=tokio::time::timeout(config.heartbeat.min(Duration::from_secs(4)),member.heartbeat())=>result};
                let ids=match members {Ok(Ok(ids))=>ids,_=>{tracing::warn!("Membership unavailable; pausing publication");Vec::new()}};
                if ids!=ring.members {
                    ring=Arc::new(Ownership::new(ids));
                    match specs(&inventory,&config.definition.schedules,&ring,&config.instance_id) {
                        Ok(specs)=>queue.reconcile(specs,SystemTime::now()),
                        Err(_)=>{queue.clear();tracing::warn!("Owned schedule count exceeds safety bound");}
                    }
                }
                tracing::info!(instance_id=%config.instance_id,active_members=ring.members.len(),owned_keys=queue.len(),scheduled_entries=queue.len(),expired_occurrences,"Scheduler membership refreshed");
            },
            changed=inventory_rx.changed()=>{
                if changed.is_err() {break;}
                inventory=inventory_rx.borrow_and_update().clone();
                match specs(&inventory,&config.definition.schedules,&ring,&config.instance_id) {
                    Ok(specs)=>queue.reconcile(specs,SystemTime::now()),
                    Err(_)=>tracing::warn!("Inventory schedule count exceeds safety bound; retaining queue"),
                }
            },
            Some(work)=queue.next(),if !queue.is_empty() && in_flight.len()<32=>{
                let coordinator=coordinator.clone();let ring=ring.clone();
                in_flight.push(Box::pin(async move {
                    let outcome=tokio::select! {biased;_=work.cancel.cancelled()=>Outcome::Cancelled,result=coordinator.fire(&ring,&work.occurrence)=>result};
                    (work,outcome)
                }));
            },
            Some((work,outcome))=in_flight.next(),if !in_flight.is_empty()=>{
                let outcome=if outcome==Outcome::Retry && work.occurrence.expired(SystemTime::now()) {Outcome::Expired} else {outcome};
                if outcome==Outcome::Expired {
                    expired_occurrences=expired_occurrences.saturating_add(1);
                    if last_warning.elapsed()>Duration::from_secs(5) {
                        tracing::warn!(task_id=%work.occurrence.task_id,expired_occurrences,"Occurrence exceeded lateness/deadline; skipping without historical backfill");last_warning=Instant::now();
                    }
                }
                if outcome==Outcome::Done {
                    tracing::info!(task_id=%work.occurrence.task_id,device_id=%work.occurrence.spec.target.device_id,schedule_id=%work.occurrence.spec.schedule.id,site=%work.occurrence.spec.target.site,"Occurrence acknowledged or already fired");
                } else if outcome==Outcome::Retry && last_warning.elapsed()>Duration::from_secs(5) {
                    tracing::warn!("Occurrence not acknowledged; bounded retry retained");last_warning=Instant::now();
                }
                queue.complete(work,outcome,SystemTime::now());
            },
        }
    }
    shutdown.cancel();
    queue.clear();
    drop(in_flight);
    let _ = tokio::time::timeout(Duration::from_secs(2), member.leave()).await;
    inventory_task.abort();
    let _ = inventory_task.await;
    tracing::info!(instance_id=%config.instance_id,"Scheduler stopped");
    Ok(())
}
