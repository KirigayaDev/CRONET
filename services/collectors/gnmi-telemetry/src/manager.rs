use std::{collections::HashMap, sync::Arc, time::Duration};

use async_nats::jetstream::kv::{Operation, Store};
use bus::Bus;
use contracts::{netmon::v1::GnmiSubscribeTask, subjects};
use creds::CredStore;
use futures::StreamExt;
use prand::seq::SliceRandom;
use prost::Message;
use rand as prand;
use tokio::{sync::watch, task::JoinHandle};

use crate::session::{self, Params, Sink};

pub struct Settings {
    pub site: String,
    pub collector_id: String,
    pub max_devices: usize,
    pub lease_ttl: Duration,
    pub reconcile_every: Duration,
    pub ca_pem: Option<Vec<u8>>,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

struct Held {
    revision: u64,
    task_bytes: Vec<u8>,
    cancel: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

pub struct Manager {
    pub bus: Bus,
    pub creds: Arc<dyn CredStore>,
    pub sink: Arc<dyn Sink>,
    pub settings: Settings,
}

impl Manager {
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), String> {
        let desired_kv = self.bus.kv(subjects::KV_GNMI_DESIRED, Duration::ZERO, 1).await.map_err(|e| e.to_string())?;
        let leases_kv =
            self.bus.kv(subjects::KV_GNMI_LEASES, self.settings.lease_ttl, 1).await.map_err(|e| e.to_string())?;
        let mut watcher = desired_kv.watch_with_history(">").await.map_err(|e| e.to_string())?;

        let mut desired: HashMap<String, Vec<u8>> = HashMap::new();
        let mut held: HashMap<String, Held> = HashMap::new();
        let mut tick = tokio::time::interval(self.settings.reconcile_every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                entry = watcher.next() => match entry {
                    Some(Ok(e)) => { tracing::debug!(key = %e.key, op = ?e.operation, "изменение желаемого состояния"); match e.operation {
                        Operation::Put => { desired.insert(e.key, e.value.to_vec()); }
                        Operation::Delete | Operation::Purge => { desired.remove(&e.key); }
                    } },
                    Some(Err(e)) => tracing::warn!(error = %e, "ошибка наблюдения за gnmi-desired"),
                    None => return Err("наблюдение за gnmi-desired завершилось".into()),
                },
                _ = tick.tick() => self.reconcile(&desired, &mut held, &leases_kv).await,
            }
        }

        for (device, h) in held.drain() {
            let _ = h.cancel.send(true);
            let _ = tokio::time::timeout(Duration::from_secs(5), h.handle).await;
            let _ = leases_kv.delete(&device).await;
        }
        Ok(())
    }

    async fn reconcile(&self, desired: &HashMap<String, Vec<u8>>, held: &mut HashMap<String, Held>, leases: &Store) {
        let me = self.settings.collector_id.clone();
        tracing::debug!(desired = desired.len(), held = held.len(), "сверка");

        let devices: Vec<String> = held.keys().cloned().collect();
        for device in devices {
            let Some(h) = held.get_mut(&device) else { continue };
            match desired.get(&device) {
                None => {
                    Self::stop(held.remove(&device), leases, &device, true).await;
                    tracing::info!(%device, "устройство убрано из желаемого состояния, подписка остановлена");
                    continue;
                }
                Some(bytes) if *bytes != h.task_bytes => {
                    tracing::info!(%device, "задача изменилась, перезапуск подписки");
                    let old = held.remove(&device);
                    let rev = old.as_ref().map(|o| o.revision);
                    Self::stop(old, leases, &device, false).await;
                    if let Some(rev) = rev {
                        if let Some(nh) = self.start(&device, bytes, rev).await {
                            held.insert(device.clone(), nh);
                        }
                    }
                    continue;
                }
                _ => {}
            }
            match leases.update(&device, me.clone().into(), h.revision).await {
                Ok(rev) => h.revision = rev,
                Err(e) => {
                    tracing::warn!(%device, error = %e, "lease потерян, подписка останавливается");
                    Self::stop(held.remove(&device), leases, &device, false).await;
                }
            }
        }

        let mut free: Vec<&String> = desired.keys().filter(|d| !held.contains_key(*d)).collect();
        free.shuffle(&mut prand::rng());
        for device in free {
            if held.len() >= self.settings.max_devices {
                break;
            }
            let bytes = &desired[device];
            let Ok(task) = GnmiSubscribeTask::decode(bytes.as_slice()) else {
                tracing::warn!(%device, "нечитаемая задача в gnmi-desired");
                continue;
            };
            if !task.site.is_empty() && task.site != self.settings.site {
                continue;
            }
            match leases.create(device, me.clone().into()).await {
                Ok(rev) => {
                    if let Some(h) = self.start(device, bytes, rev).await {
                        tracing::info!(%device, "lease захвачен, подписка запущена");
                        held.insert(device.clone(), h);
                    } else {
                        let _ = leases.delete(device).await;
                    }
                }
                Err(e) if e.kind() == async_nats::jetstream::kv::CreateErrorKind::AlreadyExists => {} // занято другим инстансом
                Err(e) => tracing::warn!(%device, error = %e, "не удалось захватить lease"),
            }
        }
    }

    async fn stop(h: Option<Held>, leases: &Store, device: &str, release: bool) {
        if let Some(h) = h {
            let _ = h.cancel.send(true);
            let _ = tokio::time::timeout(Duration::from_secs(5), h.handle).await;
            if release {
                let _ = leases.delete(device).await;
            }
        }
    }

    async fn start(&self, device: &str, bytes: &[u8], revision: u64) -> Option<Held> {
        let task = GnmiSubscribeTask::decode(bytes).ok()?;
        let cred = if task.credential_ref.is_empty() {
            None
        } else {
            match self.creds.get(&task.credential_ref).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(%device, error = %e, "секрет недоступен, устройство пропущено до следующего цикла");
                    return None;
                }
            }
        };
        let (tx, rx) = watch::channel(false);
        let params = Params {
            task,
            cred,
            ca_pem: self.settings.ca_pem.clone(),
            collector_id: self.settings.collector_id.clone(),
            backoff_min: self.settings.backoff_min,
            backoff_max: self.settings.backoff_max,
        };
        let handle = tokio::spawn(session::run_forever(params, self.sink.clone(), rx));
        Some(Held { revision, task_bytes: bytes.to_vec(), cancel: tx, handle })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bus::{BusConfig, StreamLimits};
    use contracts::netmon::v1::{GnmiMode, GnmiPath};
    use creds::EnvCredStore;
    use std::collections::{HashMap, HashSet};

    struct NullSink;
    #[async_trait::async_trait]
    impl Sink for NullSink {
        async fn publish(&self, _: contracts::netmon::v1::RawGnmiUpdate) -> Result<(), String> {
            Ok(())
        }
    }

    async fn manager(id: &str, max: usize) -> Manager {
        let bus = Bus::connect(&BusConfig { url: "nats://127.0.0.1:4222".into(), creds_file: None, name: id.into() })
            .await
            .unwrap();
        Manager {
            bus,
            creds: Arc::new(EnvCredStore),
            sink: Arc::new(NullSink),
            settings: Settings {
                site: "e2e".into(),
                collector_id: id.into(),
                max_devices: max,
                lease_ttl: Duration::from_secs(3),
                reconcile_every: Duration::from_millis(500),
                ca_pem: None,
                backoff_min: Duration::from_secs(30),
                backoff_max: Duration::from_secs(30),
            },
        }
    }

    async fn owners(leases: &Store, devices: &[String]) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for d in devices {
            if let Ok(Some(v)) = leases.get(d.clone()).await {
                out.insert(d.clone(), String::from_utf8_lossy(&v).into_owned());
            }
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn devices_are_split_between_instances_and_taken_over_on_failure() {
        if std::env::var("NETMON_E2E").is_err() {
            eprintln!("NETMON_E2E не задан: тест пропущен");
            return;
        }
        let _ = tracing_subscriber::fmt().with_test_writer().with_env_filter("gnmi_telemetry=debug").try_init();
        let a = manager("collector-a", 4).await;
        let b = manager("collector-b", 6).await;
        a.bus.ensure_streams(1, &StreamLimits::dev()).await.unwrap();
        let desired = a.bus.kv(subjects::KV_GNMI_DESIRED, Duration::ZERO, 1).await.unwrap();
        let leases = a.bus.kv(subjects::KV_GNMI_LEASES, Duration::from_secs(3), 1).await.unwrap();

        let devices: Vec<String> = (0..6).map(|i| format!("sw{i}")).collect();
        for d in &devices {
            let t = GnmiSubscribeTask {
                device_id: d.clone(),
                target: "127.0.0.1:1".into(),
                paths: vec![GnmiPath { path: "/interfaces".into() }],
                mode: GnmiMode::Sample as i32,
                site: "e2e".into(),
                ..Default::default()
            };
            desired.put(d.clone(), t.encode_to_vec().into()).await.unwrap();
        }

        let (stop_a, rx_a) = watch::channel(false);
        let (stop_b, rx_b) = watch::channel(false);
        let ha = tokio::spawn(a.run(rx_a));
        let hb = tokio::spawn(b.run(rx_b));

        tokio::time::sleep(Duration::from_secs(4)).await;
        if ha.is_finished() {
            panic!("менеджер A завершился: {:?}", ha.await);
        }
        let o = owners(&leases, &devices).await;
        assert_eq!(o.len(), 6, "не все устройства захвачены: {o:?}");
        let by_a = o.values().filter(|v| *v == "collector-a").count();
        assert!(by_a <= 4, "превышен max_devices у collector-a: {by_a}");
        eprintln!("распределение: a={by_a}, b={}", 6 - by_a);

        ha.abort();
        let _ = ha.await;
        tokio::time::sleep(Duration::from_secs(8)).await;
        let o = owners(&leases, &devices).await;
        let holders: HashSet<_> = o.values().cloned().collect();
        assert_eq!(o.len(), 6, "после падения A остались неподхваченные устройства: {o:?}");
        assert_eq!(holders, HashSet::from(["collector-b".to_owned()]), "все устройства должны перейти к B");

        desired.delete("sw0").await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            leases.get("sw0".to_owned()).await.unwrap().is_none(),
            "lease удалённого устройства должен быть освобождён"
        );

        let _ = stop_b.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), hb).await;
        drop(stop_a);
    }
}
