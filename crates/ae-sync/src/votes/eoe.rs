use super::*;
use ae_hyperchain::eoe::{coordinator, local_host, runtime};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub receipt_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            receipt_timeout: Duration::from_secs(5),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptError {
    Closed,
    TimedOut,
    Interrupted,
    Unexpected,
}
pub(super) struct Stop(Arc<AtomicBool>);
impl Stop {
    pub fn request(&self) {
        self.0.store(true, Ordering::Release);
    }
}
impl Drop for Stop {
    fn drop(&mut self) {
        self.request();
    }
}
pub(super) struct Lease(Arc<AtomicBool>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// This owner is the sole EOE output consumer and holds the publication claim
// independently of the request bridge. Stopping/replacing a bridge leaves
// its accepted publications in this owner or the bounded EOE output queues.
pub struct Owner<A, C> {
    runtime: Option<runtime::Runtime<A, C>>,
    client: runtime::Client<A, C>,
    route: runtime::Route<A, C>,
    retained: std::collections::VecDeque<runtime::Output>,
    retired: Option<Retirement>,
    publications: Publications,
    _claim: Option<publications::Claim>,
    relay: Option<independent::Relay>,
    pool_first: bool,
    attached: Arc<AtomicBool>,
    routed: AtomicBool,
    pending: Option<runtime::Output>,
}
#[derive(Debug)]
pub struct Retirement {
    pub info: runtime::Info,
    pub state: Option<coordinator::Snapshot>,
    pub inputs: Option<ae_hyperchain::eoe::pool::RetiredInbox>,
    pub result: Result<(), String>,
    pub queued: Vec<runtime::Submission>,
    pub interrupted: Option<runtime::Submission>,
}
pub struct StartError<A, C> {
    pub error: Error,
    pub runtime: runtime::Runtime<A, C>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Routed {
    Idle,
    Published,
    Backpressured,
    // Finalizations, parent publications, restart/failure messages and legacy
    // acknowledgements remain owned by the caller; none are discarded.
    Output(runtime::Output),
}
impl<A, C> Owner<A, C> {
    pub fn new(
        runtime: runtime::Runtime<A, C>,
        publications: Publications,
    ) -> Result<Self, Box<StartError<A, C>>> {
        let client = runtime.client();
        let (claim, relay) = if let Some(hub) = client.independent_pool() {
            match independent::Relay::new(hub.clone(), publications.clone()) {
                Ok(relay) => (None, Some(relay)),
                Err(error) => return Err(Box::new(StartError { error, runtime })),
            }
        } else {
            match publications.claim() {
                Ok(claim) => (Some(claim), None),
                Err(error) => return Err(Box::new(StartError { error, runtime })),
            }
        };
        Ok(Self {
            runtime: Some(runtime),
            route: runtime::Route::new(client.clone()),
            client,
            retained: Default::default(),
            retired: None,
            publications,
            _claim: claim,
            relay,
            pool_first: true,
            attached: Arc::new(AtomicBool::new(false)),
            routed: AtomicBool::new(false),
            pending: None,
        })
    }
    pub fn relay(&self) -> Option<&independent::Relay> {
        self.relay.as_ref()
    }
    pub fn pending_publication(&self) -> Option<ae_hyperchain::eoe::pool::Publication> {
        self.relay.as_ref().and_then(independent::Relay::pending)
    }
    pub fn take_pending_publication(&mut self) -> Option<ae_hyperchain::eoe::pool::Publication> {
        self.relay
            .as_mut()
            .and_then(independent::Relay::take_pending)
    }
    pub fn client(&self) -> runtime::Client<A, C> {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.runtime
            .as_ref()
            .is_none_or(runtime::Runtime::is_finished)
    }
    pub fn replacement_pending(&self) -> bool {
        self.client.replacement_pending()
    }
    // Retain the publication claim while joining the old generation. All
    // outputs, including the current backpressured vote, drain through route.
    pub fn begin_retirement(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        self.route.replace(None);
        let runtime::Retired {
            info,
            state,
            inputs,
            result,
            outputs,
            queued,
            interrupted,
        } = runtime.retire();
        self.retained.extend(outputs);
        self.retired = Some(Retirement {
            info,
            state,
            inputs,
            result,
            queued,
            interrupted,
        });
    }
    pub fn retirement(&self) -> Option<&Retirement> {
        self.retired.as_ref()
    }
    pub fn retained_outputs(&self) -> usize {
        self.retained.len() + usize::from(self.pending.is_some())
    }
    // A receipt is transferred only after every old output has been routed or
    // explicitly taken by the owner. A replacement cannot bypass that order.
    pub fn take_retirement(&mut self) -> Option<Retirement> {
        if self.retained_outputs() == 0 {
            self.retired.take()
        } else {
            None
        }
    }
    pub fn replace_runtime(
        &mut self,
        runtime: runtime::Runtime<A, C>,
    ) -> Result<(), Box<StartError<A, C>>> {
        let next = runtime.client();
        let same_pool = match (self.client.independent_pool(), next.independent_pool()) {
            (Some(a), Some(b)) => a.same_hub(b),
            (None, None) => true,
            _ => false,
        };
        let error = if self.client.network() != next.network() || !same_pool {
            Some(Fault::Configuration.into())
        } else if self.runtime.is_some()
            || self.retired.is_some()
            || self.retained_outputs() != 0
            || (self.attached.load(Ordering::Acquire) && !self.routed.load(Ordering::Acquire))
        {
            Some(Fault::Busy.into())
        } else {
            None
        };
        if let Some(error) = error {
            return Err(Box::new(StartError { error, runtime }));
        }
        self.client = runtime.client();
        self.route.replace(Some(self.client.clone()));
        self.runtime = Some(runtime);
        Ok(())
    }
    pub fn pending(&self) -> Option<&runtime::Output> {
        self.pending.as_ref()
    }
    // Explicit handoff for a permanently undersized/closed publication sink.
    // The caller takes responsibility for routing this output exactly once.
    pub fn take_pending(&mut self) -> Option<runtime::Output> {
        self.pending.take()
    }
    // Call from the composing service's output loop. Pressure retains the
    // exact output at the head and propagates through the bounded runtime.
    pub fn route(&mut self) -> Result<Routed, Error> {
        if self.relay.is_none() {
            return self.route_runtime();
        }
        let pool_first = self.pool_first;
        self.pool_first = !self.pool_first;
        let mut pressure = false;
        if pool_first {
            match self.relay.as_mut().expect("independent relay").step()? {
                independent::Progress::Published => return Ok(Routed::Published),
                independent::Progress::Backpressured => pressure = true,
                independent::Progress::Idle => (),
            }
        }
        match self.route_runtime()? {
            Routed::Idle => (),
            other => return Ok(other),
        }
        if !pool_first {
            match self.relay.as_mut().expect("independent relay").step()? {
                independent::Progress::Published => return Ok(Routed::Published),
                independent::Progress::Backpressured => pressure = true,
                independent::Progress::Idle => (),
            }
        }
        Ok(if pressure {
            Routed::Backpressured
        } else {
            Routed::Idle
        })
    }
    fn route_runtime(&mut self) -> Result<Routed, Error> {
        if self.pending.is_none() {
            self.pending = if let Some(runtime) = &self.runtime {
                match runtime.recv_timeout(Duration::ZERO) {
                    Ok(output) => Some(output),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Ok(Routed::Idle),
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(Error::AuthorityClosed);
                    }
                }
            } else {
                self.retained.pop_front()
            };
            if self.pending.is_none() {
                return Ok(Routed::Idle);
            }
        }
        let Some(runtime::Output::Service(coordinator::ServiceOutput::Publication(
            local_host::Output::Vote { signed, .. },
        ))) = &self.pending
        else {
            return Ok(Routed::Output(self.pending.take().expect("pending output")));
        };
        let bytes = signed.encode().len().checked_add(128).ok_or(Fault::Busy)?;
        if bytes > self.publications.maximum_event_bytes() {
            return Err(policy::Error::ResourceLimit.into());
        }
        let reservation = match self.publications.reserve(bytes) {
            Ok(reservation) => reservation,
            Err(Error::Worker(Fault::Busy)) => return Ok(Routed::Backpressured),
            Err(error) => return Err(error),
        };
        let Some(runtime::Output::Service(coordinator::ServiceOutput::Publication(
            local_host::Output::Vote { signed, event },
        ))) = self.pending.take()
        else {
            unreachable!("reserved vote")
        };
        reservation.send(Arc::new(*signed), event);
        Ok(Routed::Published)
    }
    // Explicit shutdown, like Runtime::shutdown, abandons undrained output.
    // Keep this owner alive when stopping a bridge to preserve accepted work.
    pub fn shutdown(self) -> thread::Result<()> {
        self.route.replace(None);
        match self.runtime {
            Some(runtime) => runtime.shutdown(),
            None => match self.retired.and_then(|retired| retired.result.err()) {
                Some(error) => Err(Box::new(error)),
                None => Ok(()),
            },
        }
    }
}

enum Request {
    Admit(Arc<Signed>, policy::Event),
    Epoch(Unsigned),
    Query(runtime::VoteQuery),
}
trait Endpoint: Send + Sync {
    fn submit(&self, request: Request) -> Result<(runtime::Call, bool), Error>;
}
impl<A, C> Endpoint for runtime::Client<A, C>
where
    Self: Send + Sync,
{
    fn submit(&self, request: Request) -> Result<(runtime::Call, bool), Error> {
        let command = match request {
            Request::Admit(signed, event) => runtime::Command::Admit {
                signed: Box::new(Arc::unwrap_or_clone(signed)),
                event,
            },
            Request::Epoch(epoch) => runtime::Command::NewEpoch(epoch),
            Request::Query(query) => runtime::Command::Votes(query),
        };
        self.call(command)
            .map(|call| (call, false))
            .map_err(|error| match error {
                runtime::SubmitError::Full(_) => Fault::Busy.into(),
                runtime::SubmitError::Disconnected(_) => Error::AuthorityClosed,
                runtime::SubmitError::ResourceLimit(_) => policy::Error::ResourceLimit.into(),
            })
    }
}
impl<A, C> Endpoint for runtime::Route<A, C>
where
    runtime::Client<A, C>: Send + Sync,
{
    fn submit(&self, request: Request) -> Result<(runtime::Call, bool), Error> {
        let client = self.current().ok_or(Fault::Busy)?;
        match Endpoint::submit(&client, request) {
            Ok((call, _)) => Ok((call, true)),
            Err(Error::AuthorityClosed) if client.replacement_pending() => Err(Fault::Busy.into()),
            Err(error) => Err(error),
        }
    }
}
pub(super) struct Authority {
    endpoint: Box<dyn Endpoint>,
    stop: Arc<AtomicBool>,
    limits: Limits,
}
impl Authority {
    fn call(&self, request: Request) -> Result<runtime::Applied, Error> {
        if self.stop.load(Ordering::Acquire) {
            return Err(Fault::Stopped.into());
        }
        let (mut call, supervised) = self.endpoint.submit(request)?;
        let started = Instant::now();
        loop {
            match call.try_reply() {
                Ok(Some(reply)) => {
                    return reply.map_err(|e| match e {
                        coordinator::Error::Host(local_host::Error::Pool(e)) => Error::Pool(e),
                        e => Error::Authority(e),
                    });
                }
                Err(_) => return Err(unknown(&call, supervised, ReceiptError::Closed)),
                Ok(None) => (),
            }
            // Once submitted, shutdown or a lost/timed-out receipt has an
            // unknown outcome. Only a known replaceable generation may leave
            // a supervised bridge usable. Never auto-resubmit the command.
            if self.stop.load(Ordering::Acquire) {
                return Err(unknown(&call, supervised, ReceiptError::Interrupted));
            }
            if started.elapsed() >= self.limits.receipt_timeout {
                return Err(unknown(&call, supervised, ReceiptError::TimedOut));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn admit(
        &self,
        signed: Arc<Signed>,
        event: policy::Event,
    ) -> Result<policy::PushOutcome, Error> {
        match self.call(Request::Admit(signed, event))? {
            runtime::Applied::Admission(outcome) => Ok(outcome),
            _ => Err(Error::UnknownReceipt(ReceiptError::Unexpected)),
        }
    }
    pub fn advance_epoch(&self, epoch: Unsigned) -> Result<(), Error> {
        match self.call(Request::Epoch(epoch))? {
            runtime::Applied::Unit => Ok(()),
            _ => Err(Error::UnknownReceipt(ReceiptError::Unexpected)),
        }
    }
    pub fn query(
        &self,
        epoch: Unsigned,
        kind: Option<Unsigned>,
        maximum: usize,
    ) -> Result<runtime::VoteSnapshot, Error> {
        match self.call(Request::Query(runtime::VoteQuery {
            epoch,
            kind,
            maximum,
        }))? {
            runtime::Applied::Votes(snapshot) => Ok(snapshot),
            _ => Err(Error::UnknownReceipt(ReceiptError::Unexpected)),
        }
    }
}
fn unknown(call: &runtime::Call, supervised: bool, error: ReceiptError) -> Error {
    if supervised && call.replacement_pending() {
        Error::RetiredReceipt(error)
    } else {
        Error::UnknownReceipt(error)
    }
}
impl Worker {
    // Explicit composition: selected tip/protocol and epochs belong to this
    // EOE instance. No independent pool or admission protocol is constructed.
    pub fn attach_eoe<A: 'static, C: 'static>(
        database: Arc<Database>,
        config: Config,
        owner: &Owner<A, C>,
        limits: Limits,
    ) -> Result<Self, Error>
    where
        runtime::Client<A, C>: Send + Sync,
    {
        Self::attach_eoe_inner(database, config, owner, limits, false)
    }
    // Keep one vote service/client while the composing supervisor replaces
    // EOE generations. Calls already accepted retain their original receipt.
    pub fn attach_supervised_eoe<A: 'static, C: 'static>(
        database: Arc<Database>,
        config: Config,
        owner: &Owner<A, C>,
        limits: Limits,
    ) -> Result<Self, Error>
    where
        runtime::Client<A, C>: Send + Sync,
    {
        Self::attach_eoe_inner(database, config, owner, limits, true)
    }
    fn attach_eoe_inner<A: 'static, C: 'static>(
        database: Arc<Database>,
        config: Config,
        owner: &Owner<A, C>,
        limits: Limits,
        supervised: bool,
    ) -> Result<Self, Error>
    where
        runtime::Client<A, C>: Send + Sync,
    {
        config.validate()?;
        if limits.receipt_timeout.is_zero() || limits.receipt_timeout > Duration::from_secs(60) {
            return Err(Fault::Configuration.into());
        }
        let endpoint = owner.client();
        // Independent pools have their own actor and admission receipts.
        if endpoint.independent_pool().is_some() {
            return Err(Fault::Configuration.into());
        }
        let publications = owner.publications.clone();
        if !publications.uses_database(&database) {
            return Err(Fault::Configuration.into());
        }
        if endpoint.is_closed() {
            return Err(Error::AuthorityClosed);
        }
        let network = endpoint.network().clone();
        let view = database.view().map_err(|_| Error::Storage)?;
        if !view.identity().matches(&network) {
            return Err(Fault::Configuration.into());
        }
        let decoding = view.config().clone();
        drop(view);
        let replies = publications.replies(config)?;
        owner
            .attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Fault::Busy)?;
        let lease = Lease(owner.attached.clone());
        owner.routed.store(supervised, Ordering::Release);
        let stop = Arc::new(AtomicBool::new(false));
        let authority = Authority {
            endpoint: if supervised {
                Box::new(owner.route.clone())
            } else {
                Box::new(endpoint)
            },
            stop: stop.clone(),
            limits,
        };
        let weak = Arc::downgrade(&database);
        let inner = owned::Worker::spawn("ae-hc-eoe-votes", config.queue, move || {
            Ok(State {
                database,
                network,
                cache: None,
                config,
                pool: Pool::Shared(authority),
                decoding,
                replies,
                _claim: None,
                _eoe_claim: Some(lease),
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
            eoe_stop: Some(Stop(stop)),
            inner,
            client,
        })
    }
}
