// One composing owner for committed epochs, received votes, local EOE output
// and producer calls. The node drives step and consumes every returned output.
use super::*;
use crate::votes;
use ae_hyperchain::{eoe, parent_block::ParentBlock, parent_cache::runtime as parent};
use ae_storage::pool::worker::GossipCache;

pub mod candidate;
pub mod producer;
mod requests;
use requests::Requests;
pub use requests::{Completion, Request, RequestError};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub reply_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 16,
            reply_bytes: 64 << 20,
        }
    }
}
pub struct Config {
    pub eoe: super::Config,
    pub votes: votes::Config,
    pub bridge: votes::eoe::Limits,
    pub calls: Limits,
}
pub struct Inputs {
    pub source: Arc<Source>,
    pub database: Arc<Database>,
    pub publications: votes::Publications,
    pub gossip: GossipCache,
    pub parent: Option<parent::Client>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Backpressured,
    ResourceLimit,
    Source(super::Error),
    State(local_host::SelectedError),
    TopUnavailable,
    Votes(votes::Error),
    ReceiptLost(u64),
    UnexpectedReply(u64),
    UnexpectedRequest(u64),
    CancelRejected(u64, coordinator::Error),
    WorkerPanicked,
    Producer(super::super::delivery::Error),
    ProductionSource(crate::chain::Error),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Advanced,
    Backpressured,
    ProducerReady,
    // ParentBlock has already been broadcast internally. Observing it here
    // must not send it back through NotifyParent. Supervision outputs likewise
    // need an explicit node policy; draining them is not successful recovery.
    Output(runtime::Output),
}
pub struct Shutdown {
    // These calls had no routed final result. Their execution may be unknown.
    pub abandoned_requests: Vec<u64>,
    pub retained_output: Option<runtime::Output>,
    pub retained_vote: Option<runtime::Output>,
    pub epoch_cursor: events::Cursor,
    pub errors: Vec<Error>,
    pub producer: Option<producer::Pending>,
}
pub struct Owner {
    source: Arc<Source>,
    parent: Option<parent::Client>,
    output: Option<votes::eoe::Owner<StateReader, StateReader>>,
    votes: Option<votes::Worker>,
    epochs: Epochs,
    requests: Requests,
    pending: Option<runtime::Output>,
    failure: Option<Error>,
    signing: ae_hyperchain::signing::Keys,
    producer_binding: repo::hyperchain::producer::Binding,
    production: Option<producer::Pending>,
}
impl Owner {
    pub fn start(
        inputs: Inputs,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Send + 'static,
    ) -> Result<Self, Error> {
        if !inputs
            .source
            .database
            .ptr_eq(&Arc::downgrade(&inputs.database))
            || !inputs.publications.uses_database(&inputs.database)
            || inputs
                .parent
                .as_ref()
                .is_some_and(parent::Client::is_closed)
        {
            return Err(Error::Configuration);
        }
        let requests = Requests::new(config.calls)?;
        let producer_binding = repo::hyperchain::producer::Binding {
            network: config.eoe.host.network.clone(),
            election: config.eoe.host.election_contract,
            parent_start: config.eoe.host.parent_start_height.clone(),
            parent_length: config.eoe.host.parent_epoch_length.clone(),
            block_time: config.eoe.block_time.clone(),
        };
        // Install both shared services before starting the runtime. No input
        // can race startup with an offline gossip cache or absent parent cache.
        let (Connected { runtime, epochs }, signing) = inputs
            .source
            .connect_with(
                config.eoe,
                keys,
                order,
                Some(Arc::new(inputs.gossip)),
                inputs.parent.clone(),
            )
            .map_err(Error::Source)?;
        let output = votes::eoe::Owner::new(runtime, inputs.publications)
            .map_err(|e| Error::Votes(e.error.clone()))?;
        let votes =
            votes::Worker::attach_eoe(inputs.database, config.votes, &output, config.bridge)
                .map_err(Error::Votes)?;
        Ok(Self {
            source: inputs.source,
            parent: inputs.parent,
            output: Some(output),
            votes: Some(votes),
            epochs,
            requests,
            pending: None,
            failure: None,
            signing,
            producer_binding,
            production: None,
        })
    }
    fn client(&self) -> Client {
        self.output.as_ref().expect("live owner").client()
    }
    fn ready(&self) -> Result<(), Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        if self.client().is_closed() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    pub fn votes(&self) -> votes::Client {
        self.votes.as_ref().expect("live vote bridge").client()
    }
    pub fn epoch_cursor(&self) -> &events::Cursor {
        self.epochs.cursor()
    }
    pub fn acknowledged_epochs(&self) -> u64 {
        self.epochs.acknowledged()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn retained_requests(&self) -> usize {
        self.requests.retained()
    }
    pub fn retained_reply_bytes(&self) -> usize {
        self.requests.retained_bytes()
    }
    pub fn pending_requests(&self) -> usize {
        self.requests.pending()
    }
    // This synchronous inspection runs through the importer's execution owner
    // and its real snapshot. Drive step afterward to deliver accepted parent
    // and negotiation effects before exposing the decision/signing selection.
    pub fn prepare_producer<O: MapOrder + Clone>(
        &mut self,
        source: &super::super::scoped::Producer<O>,
        input: producer::Input,
    ) -> Result<(), Error> {
        self.ready()?;
        if self.production.is_some() {
            return Err(Error::Backpressured);
        }
        let database = self.source.database.upgrade().ok_or(Error::Closed)?;
        if !source.uses_database(&database) {
            return Err(Error::Configuration);
        }
        let parent = self.parent.clone().ok_or(Error::Configuration)?;
        let client = self.client();
        let captured = parent.snapshot().cache;
        let prepared = source
            .prepare(
                ae_storage::hyperchain::ProducerRequest {
                    binding: self.producer_binding.clone(),
                    input,
                    keys: self.signing.clone(),
                    parent: captured,
                },
                || {
                    if parent.is_closed() || client.is_closed() {
                        Err(crate::chain::Error::Closed)
                    } else {
                        Ok(())
                    }
                },
            )
            .map_err(Error::ProductionSource)?;
        self.production = Some(producer::Pending::new(prepared, parent, client));
        Ok(())
    }
    pub fn production(&self) -> Option<&producer::Pending> {
        self.production.as_ref()
    }
    pub fn take_production(&mut self) -> Option<producer::Decision> {
        if self.failure.is_some()
            || self.client().is_closed()
            || !self
                .production
                .as_ref()
                .is_some_and(producer::Pending::ready)
        {
            return None;
        }
        let (signing, decision) = self.production.take().expect("ready producer").finish();
        self.signing = signing;
        Some(decision)
    }
    pub fn signing(&self) -> &ae_hyperchain::signing::Keys {
        &self.signing
    }
    // Capture the explicit caller view at invocation, as vote_results/0 does.
    // Pending callbacks still use LocalHost's then-current selected source.
    pub fn finalize(&mut self) -> Result<Request, Error> {
        self.ready()?;
        let lease = self.requests.reserve()?;
        let state = self.source.capture().map_err(Error::State)?;
        let view = state.tip.trees.ok_or(Error::TopUnavailable)?;
        let client = self.client();
        self.requests.submit(lease, state.metadata, |request| {
            client.call(runtime::Command::Finalize { request, view })
        })
    }
    // hc_apply_vote's retry must see the candidate's actual overlay, including
    // earlier successful transactions. Pending callbacks still independently
    // consult LocalHost's selected source, exactly as ordinary finalize does.
    pub fn finalize_candidate<O>(
        &mut self,
        candidate: &ae_storage::hyperchain::candidate::Prepared<O>,
    ) -> Result<Request, Error> {
        use ae_storage::hyperchain::candidate::Candidate;
        self.ready()?;
        let database = self.source.database.upgrade().ok_or(Error::Closed)?;
        let view = database
            .view()
            .map_err(|e| Error::State(state_error(snapshots::Error::Repository(e))))?;
        if repo::Read::source_id(view.source()) != candidate.metadata.source_id {
            return Err(Error::Configuration);
        }
        let view = match candidate
            .outcome
            .as_ref()
            .map_err(|_| Error::Configuration)?
        {
            Candidate::Micro(prepared) => prepared.candidate.trees().clone(),
            Candidate::Key(prepared) => prepared.candidate.trees().clone(),
        };
        let lease = self.requests.reserve()?;
        let client = self.client();
        self.requests
            .submit(lease, candidate.metadata.clone(), |request| {
                client.call(runtime::Command::Finalize { request, view })
            })
    }
    // Private command receipts belong to the caller. Dropping one does not
    // cancel accepted work or authorize replay into another service instance.
    fn call(
        &self,
        command: runtime::Command<StateReader, StateReader>,
    ) -> Result<runtime::Call, Error> {
        self.ready()?;
        self.client().call(command).map_err(submit_error)
    }
    pub fn negotiate(&self, input: eoe::Negotiation) -> Result<runtime::Call, Error> {
        self.call(runtime::Command::Negotiate(input))
    }
    pub fn entropy(&self, epoch: Unsigned) -> Result<runtime::Call, Error> {
        self.call(runtime::Command::Entropy(epoch))
    }
    pub fn notify_parent(
        &self,
        epoch: Unsigned,
        block: ParentBlock,
    ) -> Result<runtime::Call, Error> {
        self.call(runtime::Command::NotifyParent { epoch, block })
    }
    pub fn cache_parent(&self, block: ParentBlock) -> Result<runtime::Call, Error> {
        self.call(runtime::Command::CacheParent(block))
    }
    pub fn configure_runtime(&self, config: coordinator::Config) -> Result<runtime::Call, Error> {
        self.client()
            .call(runtime::Command::Configure(config))
            .map_err(submit_error)
    }
    pub fn retry_runtime(&self) -> Result<runtime::Call, Error> {
        self.client()
            .call(runtime::Command::Retry)
            .map_err(submit_error)
    }
    pub fn parent_delivery<O: MapOrder + Clone>(
        &self,
        source: super::super::scoped::Accepted<
            O,
            ae_chain::repository::hyperchain::inputs::Effects,
        >,
    ) -> Result<super::super::delivery::Delivery<O, StateReader, StateReader>, Error> {
        self.ready()?;
        let database = self.source.database.upgrade().ok_or(Error::Closed)?;
        if !source.uses_database(&database) {
            return Err(Error::Configuration);
        }
        let cache = self.parent.clone().ok_or(Error::Configuration)?;
        Ok(super::super::delivery::Delivery::new(
            source,
            cache,
            self.client(),
        ))
    }
    // Resolve a known rejected cancellation/epoch command explicitly. Unknown
    // receipts, lost sources and closed authorities cannot be rebound/retried.
    pub fn retry(&mut self, limits: super::Limits) -> Result<(), Error> {
        match &self.failure {
            None | Some(Error::CancelRejected(_, _)) => (),
            Some(Error::Source(super::Error::ResourceLimit | super::Error::Rejected(_))) => (),
            Some(Error::Producer(error)) if producer::retryable(error) => (),
            Some(e) => return Err(e.clone()),
        }
        self.epochs.retry(limits).map_err(Error::Source)?;
        if let Some(production) = &mut self.production {
            production.retry().map_err(Error::Producer)?;
        }
        self.failure = None;
        Ok(())
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance();
        if let Err(e) = &result {
            if !matches!(
                e,
                Error::CancelRejected(_, _)
                    | Error::Source(super::Error::ResourceLimit | super::Error::Rejected(_))
            ) && !matches!(e,Error::Producer(error) if producer::retryable(error))
            {
                self.client().request_stop();
            }
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self) -> Result<Progress, Error> {
        let votes = self.votes.as_ref().expect("live vote bridge").info();
        if let Some(error) = votes.failure {
            return Err(Error::Votes(error));
        }
        if !votes.accepting {
            return Err(Error::Votes(votes::Fault::Stopped.into()));
        }
        let client = self.client();
        let calls = self.requests.poll(&client)?;
        let epoch = self.epochs.step().map_err(Error::Source)?;
        let producing = self
            .production
            .as_mut()
            .map(producer::Pending::step)
            .transpose()
            .map_err(Error::Producer)?
            .unwrap_or(false);
        let output = self.output.as_mut().expect("live owner");
        if self.pending.is_none() {
            match output.route().map_err(Error::Votes)? {
                votes::eoe::Routed::Idle => (),
                votes::eoe::Routed::Published => return Ok(Progress::Advanced),
                votes::eoe::Routed::Backpressured => return Ok(Progress::Backpressured),
                votes::eoe::Routed::Output(o) => self.pending = Some(o),
            }
        }
        if self.pending.is_some() {
            return match self.requests.route(&mut self.pending)? {
                requests::Routed::Completed => Ok(Progress::Advanced),
                requests::Routed::Backpressured => Ok(Progress::Backpressured),
                requests::Routed::Other => Ok(Progress::Output(
                    self.pending.take().expect("pending output"),
                )),
            };
        }
        Ok(
            if self
                .production
                .as_ref()
                .is_some_and(producer::Pending::ready)
            {
                Progress::ProducerReady
            } else if calls
                || producing
                || matches!(
                    epoch,
                    super::Progress::Loaded
                        | super::Progress::Submitted
                        | super::Progress::Acknowledged
                )
            {
                Progress::Advanced
            } else {
                Progress::Idle
            },
        )
    }
    // Explicit abort: no claim to drain every accepted publication. The report
    // transfers locally retained output and identifies unresolved calls. All
    // workers are joined before returning, even if a vote bridge failed.
    pub fn shutdown(mut self) -> Shutdown {
        self.finish()
    }
    fn finish(&mut self) -> Shutdown {
        let mut errors = self.failure.clone().into_iter().collect::<Vec<_>>();
        if let Some(output) = &self.output {
            output.client().request_stop();
        }
        if let Some(votes) = self.votes.take()
            && let Err(error) = votes.shutdown()
        {
            errors.push(Error::Votes(error));
        }
        let retained_vote = self
            .output
            .as_mut()
            .and_then(votes::eoe::Owner::take_pending);
        if let Some(output) = self.output.take()
            && output.shutdown().is_err()
        {
            errors.push(Error::WorkerPanicked);
        }
        Shutdown {
            abandoned_requests: self.requests.abandon(),
            retained_output: self.pending.take(),
            retained_vote,
            epoch_cursor: self.epochs.cursor().clone(),
            errors,
            producer: self.production.take(),
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if self.output.is_some() || self.votes.is_some() {
            let _ = self.finish();
        }
    }
}
fn submit_error(error: runtime::SubmitError<StateReader, StateReader>) -> Error {
    match error {
        runtime::SubmitError::Full(_) => Error::Backpressured,
        runtime::SubmitError::ResourceLimit(_) => Error::ResourceLimit,
        runtime::SubmitError::Disconnected(_) => Error::Closed,
    }
}
