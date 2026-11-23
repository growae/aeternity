use super::*;

pub struct Recoverable {
    pub runtime: Runtime,
    pub epochs: Epochs,
    pub factory: Factory,
    pub signing: ae_hyperchain::signing::Keys,
}
type Spawn = dyn FnMut(&Arc<Source>, Arc<()>) -> Result<Runtime, Error> + Send;
pub struct Factory {
    source: Weak<Source>,
    epoch: Weak<()>,
    hub: pool::Hub,
    capacity: runtime::Capacity,
    spawn: Box<Spawn>,
}
impl Factory {
    pub fn capacity(&self) -> &runtime::Capacity {
        &self.capacity
    }
    pub fn hub(&self) -> &pool::Hub {
        &self.hub
    }
    // The independent epoch feed is the authority for this factory. It may
    // outlive any number of joined runtimes; this weak token cannot extend
    // the feed's lifetime, reset its cursor or create another subscription.
    pub fn spawn(&mut self) -> Result<Runtime, Error> {
        let _epoch = self.epoch.upgrade().ok_or(Error::Closed)?;
        let source = self.source.upgrade().ok_or(Error::Closed)?;
        if self.hub.is_closed() {
            return Err(Error::Closed);
        }
        let lease = {
            let epochs = source
                .epochs_live
                .lock()
                .map_err(|_| Error::Worker("poisoned epoch owner".into()))?;
            if !epochs.ptr_eq(&self.epoch) {
                return Err(Error::Closed);
            }
            let mut live = source
                .live
                .lock()
                .map_err(|_| Error::Worker("poisoned EOE source owner".into()))?;
            if live.upgrade().is_some() {
                return Err(Error::AlreadyRunning);
            }
            let lease = Arc::new(());
            *live = Arc::downgrade(&lease);
            lease
        };
        (self.spawn)(&source, lease)
    }
}
pub(super) struct Recipe {
    config: Config,
    keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>,
    services: Services,
    capacity: runtime::Capacity,
}
impl Recipe {
    pub(super) fn new(
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        services: Services,
    ) -> Result<Self, Error> {
        let capacity =
            runtime::Capacity::new(config.runtime).map_err(|e| Error::Worker(e.to_string()))?;
        Ok(Self {
            config,
            keys: Arc::new(keys),
            services,
            capacity,
        })
    }
    pub(super) fn spawn(
        &self,
        source: &Arc<Source>,
        lease: Arc<()>,
        order: impl MapOrder + Send + 'static,
    ) -> Result<(Runtime, ae_hyperchain::signing::Keys, events::Cursor), Error> {
        let initial = source.capture().map_err(Error::State)?;
        let cursor = initial.metadata.cursor.clone();
        let protocol = initial.tip.protocol;
        drop(initial);
        // Only immutable keys and service bindings survive replacement.
        // Host state, machines, mailboxes and shutdown flags start fresh.
        let mut host = local_host::LocalHost::with_shared_keys(
            self.config.host.clone(),
            Tip {
                trees: None,
                protocol,
                consensus: ConsensusType::Pos,
            },
            self.keys.clone(),
        )
        .with_selected_source(Arc::new(Live {
            source: source.clone(),
            _lease: lease,
        }));
        if let Some(hub) = &self.services.pool {
            host = host
                .with_independent_pool(hub.clone())
                .map_err(Error::StateHost)?;
        }
        host.set_parent_cache(self.services.parent.clone());
        if let Some(route) = &self.services.route {
            host.set_parent_route(route.clone());
        }
        let signing = host.signer();
        let mut coordinator = coordinator::Coordinator::new(
            host,
            self.config.block_time.clone(),
            self.config.coordinator,
        );
        coordinator.set_live_gossip_cache(self.services.gossip.clone());
        let runtime = Runtime::spawn_with_capacity(
            coordinator,
            order,
            self.config.runtime,
            self.capacity.clone(),
        )
        .map_err(|e| Error::Worker(e.to_string()))?;
        Ok((runtime, signing, cursor))
    }
}
impl Source {
    pub fn connect_recoverable(
        self: &Arc<Self>,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Clone + Send + 'static,
        hub: pool::Hub,
    ) -> Result<Recoverable, Error> {
        self.connect_recoverable_with(
            config,
            keys,
            order,
            Services {
                pool: Some(hub),
                ..Default::default()
            },
        )
    }
    pub(super) fn connect_recoverable_with(
        self: &Arc<Self>,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Clone + Send + 'static,
        services: Services,
    ) -> Result<Recoverable, Error> {
        config.epochs.validate()?;
        if config.host.network != self.network {
            return Err(Error::Identity);
        }
        let hub = services.pool.clone().ok_or(Error::Configuration)?;
        let limits = config.epochs;
        let recipe = Recipe::new(config, keys, services)?;
        let (epoch, lease) = self.reserve(true)?;
        let epoch = epoch.expect("independent epoch lease");
        let (runtime, signing, cursor) = recipe.spawn(self, lease, order.clone())?;
        let factory = Factory {
            source: Arc::downgrade(self),
            epoch: Arc::downgrade(&epoch),
            hub: hub.clone(),
            capacity: recipe.capacity.clone(),
            spawn: Box::new(move |source, lease| {
                recipe
                    .spawn(source, lease, order.clone())
                    .map(|(runtime, _, _)| runtime)
            }),
        };
        let epochs = self.epochs(cursor, Target::Independent { hub, _lease: epoch }, limits);
        Ok(Recoverable {
            runtime,
            epochs,
            factory,
            signing,
        })
    }
}
