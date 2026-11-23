use super::*;
use crate::chain::{Importer, hyperchain::conductor::Conductor};
use ae_vm_fate::maps::MapOrder;
use std::sync::Weak;

// Only independent services are configured here. Admission, vote and initial
// resilience settings belong to the conductor. Its policy parent must exist
// before importer construction; this supervisor adopts the initialized child.
#[derive(Debug, Clone)]
pub struct Config {
    pub upnp: Option<upnp::Config>,
    pub routing: Option<routing::Setup>,
    pub analytics: Option<analytics::Config>,
    pub pool_gc: Option<pool_gc::Config>,
    pub capabilities: Option<capabilities::Config>,
    pub keys: Option<crate::keys::Config>,
    pub gc: Option<GarbageCollection>,
    pub poll: Duration,
}
impl Default for Config {
    fn default() -> Self {
        let config = super::Config::default();
        Self {
            upnp: config.upnp,
            routing: config.routing,
            analytics: config.analytics,
            pool_gc: config.pool_gc,
            capabilities: config.capabilities,
            keys: config.keys,
            gc: config.gc,
            poll: config.poll,
        }
    }
}
pub struct Handle<O> {
    handle: super::Handle,
    importer: Weak<Importer<O>>,
}
impl<O> Clone for Handle<O> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            importer: self.importer.clone(),
        }
    }
}
impl<O> std::ops::Deref for Handle<O> {
    type Target = super::Handle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
impl<O> Handle<O> {
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.handle.changed().await
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Handle<O> {
    pub(crate) fn for_producer(&self, producer: &Conductor<O>) -> Option<Bindings> {
        (self.importer.ptr_eq(&Arc::downgrade(&producer.importer()))
            && !*self.handle.stopped.borrow()
            && !self.handle.commands.is_closed())
        .then(|| self.bindings())
    }
}
pub(super) struct External {
    pub pool: pool::worker::Client,
    pub votes: votes::Binding,
    pub group: super::super::restart::workers::Group,
    pub resilience: Option<resilience::Source>,
    pub resilience_worker: Option<resilience::Worker>,
    check: Box<dyn Fn() -> Result<(), Error> + Send + Sync>,
}
impl External {
    pub fn healthy(&self) -> Result<(), Error> {
        (self.check)()?;
        if !self.pool.info().accepting {
            return Err(Error::Dependency(Child::Admission));
        }
        if self.votes.is_closed() {
            return Err(Error::Dependency(Child::Votes));
        }
        Ok(())
    }
}
// The returned owner supervises optional children with the existing one-for-one
// budget. Ending it closes its published bindings but never stops the producer.
pub fn create<O: MapOrder + Clone + Send + Sync + 'static>(
    producer: &mut Conductor<O>,
    config: Config,
) -> Result<(Handle<O>, Service), Error> {
    let importer = producer.importer();
    importer.check_open().map_err(Error::Producer)?;
    let pool = producer.pool();
    if (config.pool_gc.is_some() && pool.origins_interval().is_some())
        || config.poll.is_zero()
        || config.poll > Duration::from_secs(1)
    {
        return Err(Error::Configuration);
    }
    let source = Arc::downgrade(&importer);
    let cache = pool.gossip_cache();
    let (resilience, resilience_config, resilience_worker) = match producer.optional_resilience() {
        Some((source, config, worker)) => (Some(source), Some(config), worker),
        None => (None, None, None),
    };
    let external = External {
        pool,
        votes: producer.vote_binding(),
        group: producer.worker_group(),
        resilience,
        resilience_worker,
        check: Box::new(move || {
            source
                .upgrade()
                .ok_or(Error::Producer(crate::chain::Error::Closed))?
                .check_open()
                .map_err(Error::Producer)
        }),
    };
    let config = super::Config {
        upnp: config.upnp,
        routing: config.routing,
        analytics: config.analytics,
        pool_gc: config.pool_gc,
        resilience: resilience_config,
        capabilities: config.capabilities,
        keys: config.keys,
        gc: config.gc,
        poll: config.poll,
        pool: pool::Config {
            consensus: ConsensusType::Pos,
            ..Default::default()
        },
        ..Default::default()
    };
    let (handle, service) = create_inner(
        importer.database().clone(),
        importer.network().clone(),
        config,
        cache,
        Some(external),
    )?;
    Ok((
        Handle {
            handle,
            importer: Arc::downgrade(&importer),
        },
        service,
    ))
}
