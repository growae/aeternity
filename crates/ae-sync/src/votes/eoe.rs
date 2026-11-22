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
    runtime: runtime::Runtime<A, C>,
    publications: Publications,
    _claim: publications::Claim,
    attached: Arc<AtomicBool>,
    pending: Option<runtime::Output>,
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
        let claim = match publications.claim() {
            Ok(claim) => claim,
            Err(error) => return Err(Box::new(StartError { error, runtime })),
        };
        Ok(Self {
            runtime,
            publications,
            _claim: claim,
            attached: Arc::new(AtomicBool::new(false)),
            pending: None,
        })
    }
    pub fn client(&self) -> runtime::Client<A, C> {
        self.runtime.client()
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
        if self.pending.is_none() {
            self.pending = match self.runtime.recv_timeout(Duration::ZERO) {
                Ok(output) => Some(output),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Ok(Routed::Idle),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(Error::AuthorityClosed);
                }
            };
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
        self.runtime.shutdown()
    }
}

enum Request {
    Admit(Arc<Signed>, policy::Event),
    Epoch(Unsigned),
    Query(runtime::VoteQuery),
}
trait Endpoint: Send + Sync {
    fn submit(&self, request: Request) -> Result<runtime::Call, Error>;
}
impl<A, C> Endpoint for runtime::Client<A, C>
where
    Self: Send + Sync,
{
    fn submit(&self, request: Request) -> Result<runtime::Call, Error> {
        let command = match request {
            Request::Admit(signed, event) => runtime::Command::Admit {
                signed: Box::new(Arc::unwrap_or_clone(signed)),
                event,
            },
            Request::Epoch(epoch) => runtime::Command::NewEpoch(epoch),
            Request::Query(query) => runtime::Command::Votes(query),
        };
        self.call(command).map_err(|error| match error {
            runtime::SubmitError::Full(_) => Fault::Busy.into(),
            runtime::SubmitError::Disconnected(_) => Error::AuthorityClosed,
            runtime::SubmitError::ResourceLimit(_) => policy::Error::ResourceLimit.into(),
        })
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
        let mut call = self.endpoint.submit(request)?;
        let started = Instant::now();
        loop {
            match call.try_reply() {
                Ok(Some(reply)) => {
                    return reply.map_err(|e| match e {
                        coordinator::Error::Host(local_host::Error::Pool(e)) => Error::Pool(e),
                        e => Error::Authority(e),
                    });
                }
                Err(_) => return Err(Error::UnknownReceipt(ReceiptError::Closed)),
                Ok(None) => (),
            }
            // Once submitted, shutdown or a lost/timed-out receipt has an
            // unknown outcome. Poison this bridge; never auto-resubmit it.
            if self.stop.load(Ordering::Acquire) {
                return Err(Error::UnknownReceipt(ReceiptError::Interrupted));
            }
            if started.elapsed() >= self.limits.receipt_timeout {
                return Err(Error::UnknownReceipt(ReceiptError::TimedOut));
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
        config.validate()?;
        if limits.receipt_timeout.is_zero() || limits.receipt_timeout > Duration::from_secs(60) {
            return Err(Fault::Configuration.into());
        }
        let endpoint = owner.client();
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
        let stop = Arc::new(AtomicBool::new(false));
        let authority = Authority {
            endpoint: Box::new(endpoint),
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
