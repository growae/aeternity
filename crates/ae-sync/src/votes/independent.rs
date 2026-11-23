use super::*;
use ae_hyperchain::eoe::pool;
use ae_mempool::gossip::Transactional;

pub struct Relay {
    hub: pool::Hub,
    input: pool::Publications,
    publications: Publications,
    _claim: publications::Claim,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Published,
    Backpressured,
}
impl Relay {
    pub fn new(hub: pool::Hub, publications: Publications) -> Result<Self, Error> {
        publications.validate_independent(hub.network())?;
        let claim = publications.claim()?;
        let input = hub.publications().map_err(map_error)?;
        Ok(Self {
            hub,
            input,
            publications,
            _claim: claim,
        })
    }
    pub fn hub(&self) -> &pool::Hub {
        &self.hub
    }
    pub fn publications(&self) -> &Publications {
        &self.publications
    }
    pub fn pending(&self) -> Option<pool::Publication> {
        self.input.peek()
    }
    // Explicit transfer keeps the Hub byte credit alive in the returned value.
    pub fn take_pending(&mut self) -> Option<pool::Publication> {
        self.input.acknowledge()
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
        let Some(publication) = self.input.peek() else {
            return Ok(Progress::Idle);
        };
        let bytes = publication
            .signed()
            .encode()
            .len()
            .checked_add(128)
            .ok_or(Fault::Busy)?;
        if bytes > self.publications.maximum_event_bytes() {
            return Err(policy::Error::ResourceLimit.into());
        }
        let reservation = match self.publications.reserve(bytes) {
            Ok(r) => r,
            Err(Error::Worker(Fault::Busy)) => return Ok(Progress::Backpressured),
            Err(e) => return Err(e),
        };
        let publication = self.input.acknowledge().expect("reserved publication");
        reservation.send(Arc::new(publication.signed().clone()), publication.event());
        Ok(Progress::Published)
    }
}
pub(crate) fn map_error(error: pool::Error) -> Error {
    match error {
        pool::Error::Admission(e) => Error::Pool(e),
        pool::Error::ResourceLimit => Error::Pool(policy::Error::ResourceLimit),
        pool::Error::Busy => Fault::Busy.into(),
        pool::Error::Closed => Error::AuthorityClosed,
        pool::Error::Configuration => Fault::Configuration.into(),
    }
}
fn native_rejection(error: &Error) -> bool {
    match error {
        Error::Pool(e) => !ae_hyperchain::eoe::local_host::Error::Pool(e.clone()).is_operational(),
        _ => false,
    }
}
pub(super) struct Independent {
    owner: pool::Owner,
    hub: pool::Hub,
}
impl Independent {
    pub(super) fn client(&self) -> pool::Client {
        self.owner.client()
    }
    pub(super) fn new_epoch(&self, epoch: Unsigned) -> Result<(), Error> {
        self.hub.new_epoch(epoch).map_err(map_error)
    }
}
impl State {
    pub(super) fn independent_admit(
        &self,
        signed: Arc<Signed>,
        hash: ae_types::Hash256,
        event: policy::Event,
    ) -> Result<policy::PushOutcome, Error> {
        let Pool::Independent(pool) = &self.pool else {
            unreachable!("independent pool")
        };
        let client = pool.client();
        let mut result = None;
        self.cache
            .as_ref()
            .expect("independent cache")
            .transact(Box::new(|check| {
                let admitted = (|| {
                    if check(hash) {
                        return Ok(policy::PushOutcome::Cached);
                    }
                    // The selected protocol is captured only on a cache miss.
                    let protocol = self.selected_protocol()?;
                    client
                        .admit(pool::Admission {
                            signed: Arc::unwrap_or_clone(signed),
                            event,
                            protocol,
                            consensus: ConsensusType::Pos,
                        })
                        .map_err(map_error)
                })();
                let commit = admitted.as_ref().map_or_else(native_rejection, |_| true);
                result = Some(admitted);
                commit
            }));
        result.expect("cache transaction completed")
    }
}
impl Worker {
    // The actor owns only this pool generation. The relay and EOE subscription
    // remain independently alive when the actor stops or is replaced.
    pub fn spawn_independent(
        database: Arc<Database>,
        cache: GossipCache,
        config: Config,
        relay: &Relay,
        limits: policy::Limits,
    ) -> Result<Self, Error> {
        config.validate()?;
        let publications = relay.publications.clone();
        let hub = relay.hub.clone();
        let network = hub.network().clone();
        if !publications.uses_database(&database) || hub.is_closed() {
            return Err(Fault::Configuration.into());
        }
        let view = database.view().map_err(|_| Error::Storage)?;
        if !view.identity().matches(&network) || view.identity().consensus != ConsensusType::Pos {
            return Err(Fault::Configuration.into());
        }
        let decoding = view.config().clone();
        drop(view);
        let replies = publications.replies(config)?;
        let weak = Arc::downgrade(&database);
        let inner = owned::Worker::spawn("ae-hc-vote-pool", config.queue, move || {
            let owner = hub.start_pool(limits).map_err(map_error)?;
            Ok(State {
                database,
                network,
                cache: Some(cache),
                config,
                pool: Pool::Independent(Independent { owner, hub }),
                decoding,
                replies,
                _claim: None,
                _eoe_claim: None,
            })
        })?;
        let client = Client {
            inner: inner.client(),
            database: weak,
            publications,
            config,
            shared: true,
        };
        Ok(Self {
            inner,
            client,
            eoe_stop: None,
        })
    }
}
