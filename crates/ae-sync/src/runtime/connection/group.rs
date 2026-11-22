use super::*;
use std::{collections::HashMap, future::Future, pin::Pin};
use tokio::task::{Id, JoinSet};

type Run = Pin<Box<dyn Future<Output = Failure> + Send>>;
pub(super) struct Group<O: MapOrder + Send + Sync + 'static> {
    cache: Option<admission::CacheOwner>,
    importer: Option<chain::Importer<O>>,
    pending: Vec<(Child, Run)>,
    tasks: JoinSet<Failure>,
    ids: HashMap<Id, Child>,
    stop: Vec<Box<dyn Fn() + Send>>,
    network: Option<network::Handle>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Group<O> {
    pub fn new() -> Self {
        Self {
            cache: None,
            importer: None,
            pending: vec![],
            tasks: JoinSet::new(),
            ids: HashMap::new(),
            stop: vec![],
            network: None,
        }
    }
    fn add(
        &mut self,
        child: Child,
        stop: impl Fn() + Send + 'static,
        run: impl Future<Output = Failure> + Send + 'static,
    ) {
        self.stop.push(Box::new(stop));
        self.pending.push((child, Box::pin(run)));
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare<F>(
        &mut self,
        inputs: &Inputs<O>,
        dependencies: &super::super::workers::Bindings,
        config: &Config,
        outputs: &mut F,
        cache: Option<&mut admission::CacheController>,
        number: u64,
    ) -> Result<Generation, Failure>
    where
        F: FnMut() -> Result<observe::Outputs, observe::Error>,
    {
        if let Some(cache) = cache {
            let pool = dependencies
                .admission
                .snapshot()
                .value
                .ok_or_else(|| Failure::new(Child::GossipCache, "admission unavailable"))?;
            if !cache.uses_cache(&pool.client.gossip_cache()) {
                return Err(Failure::new(Child::GossipCache, "foreign cache lookup"));
            }
            self.cache = Some(
                cache
                    .start()
                    .map_err(|e| Failure::new(Child::GossipCache, e))?,
            );
        }
        let db = &inputs.database;
        let network_profile = inputs.execution.network().clone();
        let consensus = inputs.execution.consensus();
        self.importer = Some(
            chain::Importer::new(
                db.clone(),
                inputs.execution.clone(),
                inputs.rules.clone(),
                config.importer.clone(),
            )
            .map_err(|e| Failure::new(Child::Importer, e))?,
        );
        let mut network_config = config.network.clone();
        if let Some(analytics) = dependencies.analytics.as_ref().and_then(|a| a.current()) {
            crate::analytics::configure_network(
                &mut network_config.registry.policy,
                analytics.enabled(),
            );
        }
        let (network, scheduled, incoming, service) = network::create_bound(
            db.clone(),
            dependencies.admission.clone(),
            inputs.identity.clone(),
            network_profile.clone(),
            network_config,
        )
        .await
        .map_err(|e| Failure::new(Child::Network, e))?;
        self.network = Some(network.clone());
        let handle = network.clone();
        self.add(Child::Network, move || handle.shutdown(), async move {
            Failure::new(Child::Network, service.run().await.status.failure)
        });
        let (gossip, incoming, service) = gossip::create_with_votes(
            db.clone(),
            dependencies.admission.clone(),
            network.peers(),
            incoming,
            network_profile,
            consensus,
            config.gossip,
            dependencies.votes.clone(),
        )
        .map_err(|e| Failure::new(Child::Gossip, e))?;
        let handle = gossip.clone();
        self.add(Child::Gossip, move || handle.shutdown(), async move {
            Failure::new(Child::Gossip, service.run().await.last_error)
        });
        let mut peer_config = config.peer.clone();
        peer_config.port_mapping = dependencies.upnp_listener.clone();
        let (peer, incoming, service) = peer::create_bound(
            db.clone(),
            network.clone(),
            scheduled,
            incoming,
            peer_config,
            dependencies.analytics.clone(),
            dependencies.capabilities.clone(),
        )
        .map_err(|e| Failure::new(Child::Peer, e))?;
        let handle = peer.clone();
        self.add(Child::Peer, move || handle.shutdown(), async move {
            Failure::new(Child::Peer, service.run().await.last_error)
        });
        let (sync, incoming, service) = sync::create_bound(
            self.importer.as_ref().expect("prepared importer").shared(),
            network.clone(),
            peer.clone(),
            incoming,
            config.sync.clone(),
            dependencies.resilience.clone(),
        )
        .map_err(|e| Failure::new(Child::Sync, e))?;
        let handle = sync.clone();
        self.add(Child::Sync, move || handle.shutdown(), async move {
            Failure::new(Child::Sync, service.run().await.last_error)
        });
        // Native event subscriptions start at the live tail on each new owner.
        // An old owner's completed broadcasts are never replayed on restart.
        let (propagation, service) = conductor::create(
            db.clone(),
            network.clone(),
            sync.clone(),
            None,
            config.propagation.clone(),
        )
        .map_err(|e| Failure::new(Child::Propagation, e))?;
        let handle = propagation.clone();
        self.add(Child::Propagation, move || handle.shutdown(), async move {
            Failure::new(Child::Propagation, service.run().await.last_error)
        });
        let maintenance = if let Some(gc) = &dependencies.gc {
            let (handle, service) =
                maintenance::create_bound(db, sync.clone(), gc.clone(), config.maintenance)
                    .map_err(|e| Failure::new(Child::Maintenance, e))?;
            let stop = handle.clone();
            self.add(Child::Maintenance, move || stop.shutdown(), async move {
                Failure::new(Child::Maintenance, service.run().await.last_error)
            });
            Some(handle)
        } else {
            None
        };
        let outputs = outputs().map_err(|e| Failure::new(Child::Observer, e))?;
        let (observer, service) = observe::create_bound(
            observe::Sources {
                database: db.clone(),
                network: network.clone(),
                peer: peer.clone(),
                gossip: gossip.clone(),
                sync: sync.clone(),
                admission: dependencies
                    .admission
                    .snapshot()
                    .value
                    .ok_or_else(|| Failure::new(Child::Observer, "admission unavailable"))?
                    .client,
            },
            incoming,
            config.observer.clone(),
            outputs,
            dependencies.admission.clone(),
            dependencies.routing.clone(),
        )
        .map_err(|e| Failure::new(Child::Observer, e))?;
        let handle = observer.clone();
        self.add(Child::Observer, move || handle.shutdown(), async move {
            Failure::new(Child::Observer, service.run().await.last_error)
        });
        Ok(Generation {
            number,
            cache: self.cache.as_ref().map(admission::CacheOwner::handle),
            network,
            gossip,
            peer,
            sync,
            propagation,
            maintenance,
            observer,
        })
    }
    pub fn launch(&mut self) {
        // All channels and consumers are already constructed. Network runs
        // last, so no inbound listener is exposed during partial construction.
        for (child, run) in self.pending.drain(..).rev() {
            let handle = self.tasks.spawn(run);
            self.ids.insert(handle.id(), child);
        }
    }
    pub async fn next(&mut self) -> Failure {
        match self
            .tasks
            .join_next_with_id()
            .await
            .expect("active children")
        {
            Ok((id, result)) => {
                self.ids.remove(&id);
                result
            }
            Err(error) => {
                let child = self.ids.remove(&error.id()).expect("owned child");
                Failure::new(child, error)
            }
        }
    }
    pub fn recovery_required(&self) -> bool {
        self.importer
            .as_ref()
            .is_some_and(|i| i.recovery_required())
    }
    pub fn cache_failure(&self) -> Option<Failure> {
        self.cache
            .as_ref()
            .filter(|cache| !cache.is_running())
            .map(|_| Failure::new(Child::GossipCache, "cache stopped"))
    }
    pub fn network_failure(&self) -> Option<Failure> {
        self.network
            .as_ref()?
            .status()
            .failure
            .map(|error| Failure::new(Child::Network, error))
    }
    pub async fn shutdown(&mut self) {
        // Network encapsulates both the listener and transports. Stop accepting
        // first, then signal consumers in reverse construction order; keep all
        // tasks driven together while their cross-service work drains.
        if let Some(network) = &self.network {
            network.shutdown();
        }
        for stop in self.stop.iter().rev() {
            stop();
        }
        if let Some(importer) = &self.importer {
            importer.stop();
        }
        // Also join the services constructed before an initial-start failure.
        self.launch();
        while !self.tasks.is_empty() {
            self.next().await;
        }
        if let Some(importer) = &self.importer {
            importer.join().await;
        }
        self.cache.take();
    }
}
impl<O: MapOrder + Send + Sync + 'static> Drop for Group<O> {
    fn drop(&mut self) {
        if let Some(network) = &self.network {
            network.shutdown();
        }
        for stop in self.stop.iter().rev() {
            stop();
        }
        if let Some(importer) = &self.importer {
            importer.stop();
        }
        // JoinSet aborts remaining tasks on abrupt owner cancellation. That
        // path deliberately leaves the public joined flag false.
    }
}
