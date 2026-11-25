use ae_node::{
    configuration::{
        Configuration,
        resolution::{Context, Directories},
    },
    runtime,
};
use ae_sync::{observe::statsd, runtime::workers};
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::UdpSocket;
pub struct Metrics {
    receiver: UdpSocket,
    log: Arc<Mutex<Vec<u8>>>,
}
struct Log(Arc<Mutex<Vec<u8>>>);
impl Write for Log {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Metrics {
    pub fn assert_registry_joined(client: &statsd::Client) {
        let registry = client.registry().unwrap();
        let info = registry.info();
        assert!(info.joined && !info.running);
        assert!(info.managers.iter().all(|m| m.joined && !m.running));
        assert_eq!(client.store().unwrap().retained_bytes(), 0);
        assert_eq!(
            client.store().unwrap().histogram_usage(),
            ae_sync::observe::registry::histogram::probe::Usage::default()
        );
        assert_eq!(
            registry.find_entries(&vec![]),
            Err(ae_sync::observe::registry::Error::Closed)
        );
        assert_eq!(
            client.store().unwrap().find_entries(&vec![]),
            Err(ae_sync::observe::registry::Error::Closed)
        );
    }
    pub async fn new() -> Self {
        Self {
            receiver: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            log: Default::default(),
        }
    }
    pub fn configure(&self, p: &mut runtime::Prepared, s: &mut runtime::Settings) {
        let config=Configuration::from_value(serde_json::json!({"metrics":{"host":"127.0.0.1","port":self.receiver.local_addr().unwrap().port(),"reconnect_interval":17,"rules":[{"name":"ae.**","actions":"none"},{"name":"ae.epoch.aecore.chain.height","datapoints":"value","actions":"send"}]}})).unwrap();
        let ctx = Context::new(Directories {
            home: "release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        p.configure_metrics(&config.resolve(&ctx).unwrap(), s)
            .unwrap();
        p.configure_metrics_log(Some(Box::new(Log(self.log.clone()))))
            .unwrap();
    }
    async fn receive(&self) -> (Vec<u8>, std::net::SocketAddr) {
        let mut b = [0; 1024];
        let (n, from) =
            tokio::time::timeout(Duration::from_secs(12), self.receiver.recv_from(&mut b))
                .await
                .unwrap()
                .unwrap();
        (b[..n].to_vec(), from)
    }
    pub async fn verify(&self, h: &runtime::Handle, normal: bool) -> statsd::Client {
        let client = h.metrics().unwrap();
        assert!(client.info().ready && client.info().connected);
        assert_eq!(client.info().open_attempts, 1);
        let reporter = client.reporter().unwrap();
        let first = reporter.reporters().unwrap().await.unwrap().remove(0);
        assert!(first.alive && first.enabled);
        assert_eq!(
            first.intervals[0].interval.period,
            ae_sync::observe::reporter::Period::Milliseconds(10_000)
        );
        let registry = client.registry().unwrap();
        let histogram = vec![ae_sync::routing::Part::atom("application-histogram")];
        registry
            .create_histogram(
                &histogram,
                &ae_sync::observe::registry::HistogramOptions::default(),
            )
            .unwrap()
            .await
            .unwrap();
        registry
            .update(
                &histogram,
                &ae_sync::observe::registry::Value::Integer(17.into()),
            )
            .unwrap();
        let scalar = vec![ae_sync::routing::Part::atom("application-counter")];
        registry
            .create(&scalar, ae_sync::observe::registry::Kind::Counter, true)
            .unwrap()
            .await
            .unwrap();
        registry
            .update(
                &scalar,
                &ae_sync::observe::registry::Value::Integer(9.into()),
            )
            .unwrap();
        use ae_sync::observe::registry::{Alias, Manager, Reading, Value};
        let point = ae_sync::routing::Part::atom("value");
        let points = ae_sync::observe::reporter::Points::One(point.clone());
        let alias = Alias::Atom("application-counter".into());
        registry
            .add_alias(&alias, &scalar, &point)
            .unwrap()
            .await
            .unwrap();
        registry.set_cache(&scalar, 60_000).unwrap();
        assert_eq!(
            registry.get_value(&scalar, &points).unwrap(),
            Reading::Values(vec![(point.clone(), Value::Integer(9.into()))])
        );
        registry.barrier(Manager::Cache).unwrap().await.unwrap();
        let cached = registry.cache_info(&scalar, &point).unwrap().unwrap();
        for manager in [Manager::Admin, Manager::Cache, Manager::Alias] {
            assert_eq!(registry.restart(manager, 1).unwrap().await, Ok(2));
        }
        let after = registry.cache_info(&scalar, &point).unwrap().unwrap();
        assert_eq!(cached.sequence, after.sequence);
        assert_ne!(cached.timer, after.timer);
        assert_eq!(
            registry.alias_value(&alias).unwrap(),
            Reading::Values(vec![(point, Value::Integer(9.into()))])
        );
        assert!(
            registry
                .info()
                .managers
                .iter()
                .all(|m| m.running && m.previous_joined)
        );
        let unchanged = reporter.reporters().unwrap().await.unwrap().remove(0);
        assert_eq!(
            registry
                .get_value(
                    &histogram,
                    &ae_sync::observe::reporter::Points::One(ae_sync::routing::Part::atom("mean"))
                )
                .unwrap(),
            Reading::Values(vec![(
                ae_sync::routing::Part::atom("mean"),
                Value::Integer(17.into())
            )])
        );
        assert_eq!(client.store().unwrap().histogram_usage().workers, 1);
        assert_eq!(unchanged.generation, first.generation);
        assert_eq!(
            client.supervisor().unwrap().status().supervisor_generation,
            1
        );
        if !normal {
            assert!(h.connections().is_none());
            client
                .report(
                    ae_sync::observe::metrics::Metric {
                        name: "application.value",
                        value: ae_sync::observe::metrics::Number::Integer(42),
                    }
                    .report("", Default::default())
                    .unwrap(),
                )
                .unwrap();
            assert_eq!(self.receive().await.0, b"application.value:42|g");
            tokio::time::timeout(Duration::from_secs(3), async {
                while !self
                    .log
                    .lock()
                    .unwrap()
                    .ends_with(b"application.value:42|g\n")
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            return client;
        }
        let mut connections = h.connections().unwrap();
        let mut generation = connections.current().unwrap();
        let mut immediate = [0; 1024];
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                self.receiver.recv(&mut immediate)
            )
            .await
            .is_err()
        );
        let (line, from) = self.receive().await;
        assert!(
            std::str::from_utf8(&line)
                .unwrap()
                .starts_with("ae.epoch.aecore.chain.height.value:")
        );
        let workers = if let Some(pow) = h.pow() {
            pow.workers().unwrap()
        } else {
            (*h.hyperchain().unwrap().workers().unwrap()).clone()
        };
        let key = ae_sync::observe::metrics::Metric {
            name: "ae.epoch.aecore.chain.height.value",
            value: ae_sync::observe::metrics::Number::Integer(0),
        }
        .routing()
        .1;
        let old = workers.bindings().routing.unwrap().current().unwrap();
        assert_eq!(
            old.destinations(key.clone()).unwrap().await.unwrap(),
            Some(vec!["send".into()])
        );
        workers
            .restart(workers::Child::Routing, workers.status().routing_generation)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(!old.info().running && !old.info().accepting);
        let new = workers.bindings().routing.unwrap().current().unwrap();
        assert_eq!(
            new.destinations(key).unwrap().await.unwrap(),
            Some(vec!["send".into()])
        );
        assert_eq!(connections.status().generation, generation.number);
        generation.peer.shutdown();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(g) = connections.current()
                    && g.number > generation.number
                {
                    break;
                }
                connections.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        while !generation.observer.status().joined {
            generation.observer.changed().await.unwrap();
        }
        let next_reporter = reporter.reporters().unwrap().await.unwrap().remove(0);
        assert_eq!(next_reporter.generation, first.generation);
        assert!(Arc::ptr_eq(
            &client.store().unwrap(),
            &h.metrics().unwrap().store().unwrap()
        ));
        assert_eq!(
            registry
                .get_value(
                    &scalar,
                    &ae_sync::observe::reporter::Points::One(ae_sync::routing::Part::atom("value"))
                )
                .unwrap(),
            ae_sync::observe::registry::Reading::Values(vec![(
                ae_sync::routing::Part::atom("value"),
                ae_sync::observe::registry::Value::Integer(9.into())
            )])
        );
        let (next, source) = self.receive().await;
        assert!(
            std::str::from_utf8(&next)
                .unwrap()
                .starts_with("ae.epoch.aecore.chain.height.value:")
        );
        assert_eq!(source, from);
        assert!(client.same_owner(&h.metrics().unwrap()));
        assert_eq!(client.info().open_attempts, 1);
        assert!(client.info().running);
        client
    }
    pub async fn replace_logger(&self, h: &runtime::Handle) {
        let mut ready = h.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.status().ready {
                ready.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let stable = h.metrics().unwrap();
        let supervisor = stable.supervisor().unwrap();
        let old = supervisor.current().unwrap();
        let reporter = stable.reporter().unwrap();
        let before = reporter.reporters().unwrap().await.unwrap().remove(0);
        let registry = stable.registry().unwrap();
        let scalar = vec![ae_sync::routing::Part::atom("logger-counter")];
        registry
            .create(&scalar, ae_sync::observe::registry::Kind::Counter, true)
            .unwrap()
            .await
            .unwrap();
        registry
            .update(
                &scalar,
                &ae_sync::observe::registry::Value::Integer(12.into()),
            )
            .unwrap();
        let connection = h.connections().map(|c| c.status().generation);
        let ticket = supervisor.restart(old.number).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), ticket.wait())
                .await
                .unwrap(),
            Ok(old.number + 1)
        );
        assert!(old.client.info().joined && !old.client.info().running);
        assert!(old.client.input().unwrap().info().joined);
        assert_eq!(
            supervisor.status().phase,
            statsd::supervision::Phase::Running
        );
        assert!(stable.same_owner(&h.metrics().unwrap()));
        assert_eq!(h.connections().map(|c| c.status().generation), connection);
        assert_eq!(supervisor.status().supervisor_generation, 1);
        assert_eq!(stable.info().open_attempts, 2);
        let after = reporter.reporters().unwrap().await.unwrap().remove(0);
        assert_eq!(after.generation, before.generation);
        assert!(after.alive);
        assert_eq!(
            registry
                .get_value(
                    &scalar,
                    &ae_sync::observe::reporter::Points::One(ae_sync::routing::Part::atom("value"))
                )
                .unwrap(),
            ae_sync::observe::registry::Reading::Values(vec![(
                ae_sync::routing::Part::atom("value"),
                ae_sync::observe::registry::Value::Integer(12.into())
            )])
        );

        assert!(h.status().ready && h.status().failure.is_none());
        let marker = format!("logger-generation.value:{}|g", old.number + 1).into_bytes();
        stable
            .report(
                ae_sync::observe::metrics::Metric {
                    name: "logger-generation.value",
                    value: ae_sync::observe::metrics::Number::Integer((old.number + 1).into()),
                }
                .report("", Default::default())
                .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.receive().await.0 == marker {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
}
