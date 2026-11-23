use super::*;
use ae_hyperchain::eoe::Reply;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender, TryRecvError},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    Rejected(coordinator::Error),
    Failed(coordinator::CallFailure),
    // No routed completion before owner shutdown/loss. The request may have
    // executed; neither a dropped receiver nor this result permits replay.
    Unknown,
}
#[derive(Debug)]
pub(super) struct Lease {
    cancelled: AtomicBool,
    abandoned: AtomicBool,
    _slot: OwnedSemaphorePermit,
}
#[derive(Debug)]
struct ResultData {
    replies: Box<[Reply<local_host::Finalized>; 2]>,
    _bytes: OwnedSemaphorePermit,
    _request: Arc<Lease>,
}
// Clones and unread replies keep their byte/count reservation after shutdown.
#[derive(Debug, Clone)]
pub struct Completion(Arc<ResultData>);
impl Completion {
    pub fn replies(&self) -> &[Reply<local_host::Finalized>; 2] {
        &self.0.replies
    }
}
pub struct Request {
    id: u64,
    captured: snapshots::Metadata,
    receiver: Receiver<Result<Completion, RequestError>>,
    lease: Arc<Lease>,
}
impl Request {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn captured(&self) -> &snapshots::Metadata {
        &self.captured
    }
    pub fn cancel(&self) {
        self.lease.cancelled.store(true, Ordering::Release);
    }
    // Read at most one completion. Drive the owner concurrently; this method
    // does not drive or block the output/epoch event loop.
    pub fn try_reply(&mut self) -> Result<Option<Completion>, RequestError> {
        match self.receiver.try_recv() {
            Ok(result) => result.map(Some),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(RequestError::Unknown),
        }
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.lease.abandoned.store(true, Ordering::Release);
        self.cancel();
    }
}
enum Receipt {
    Submit(runtime::Call),
    Cancel(runtime::Call),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptKind {
    Submit,
    Cancel,
}
#[derive(Debug)]
pub struct RetiredRequest {
    pub id: u64,
    pub receipt: Option<(ReceiptKind, ReceiptObservation)>,
    pub outcome: RequestError,
}
pub type ReceiptObservation =
    Result<Option<Result<runtime::Applied, coordinator::Error>>, runtime::CallError>;
struct Pending {
    lease: Arc<Lease>,
    reply: SyncSender<Result<Completion, RequestError>>,
    receipt: Option<Receipt>,
    cancel_sent: bool,
}
pub(super) struct Requests {
    limits: Limits,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    sequence: u64,
    pending: BTreeMap<u64, Pending>,
}
pub(super) enum Routed {
    Completed,
    Backpressured,
    Other,
}
impl Requests {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        if !(1..=4096).contains(&limits.requests) || !(1..=1 << 30).contains(&limits.reply_bytes) {
            return Err(Error::Configuration);
        }
        Ok(Self {
            limits,
            slots: Arc::new(Semaphore::new(limits.requests)),
            bytes: Arc::new(Semaphore::new(limits.reply_bytes)),
            sequence: 0,
            pending: BTreeMap::new(),
        })
    }
    pub fn retained(&self) -> usize {
        self.limits.requests - self.slots.available_permits()
    }
    pub fn retained_bytes(&self) -> usize {
        self.limits.reply_bytes - self.bytes.available_permits()
    }
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
    pub fn reserve(&self) -> Result<Arc<Lease>, Error> {
        Ok(Arc::new(Lease {
            cancelled: AtomicBool::new(false),
            abandoned: AtomicBool::new(false),
            _slot: self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Backpressured)?,
        }))
    }
    pub fn submit(
        &mut self,
        lease: Arc<Lease>,
        captured: snapshots::Metadata,
        submit: impl FnOnce(
            u64,
        )
            -> Result<runtime::Call, runtime::SubmitError<StateReader, StateReader>>,
    ) -> Result<Request, Error> {
        // Never recycle request IDs, even for definitely rejected submissions.
        self.sequence = self.sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
        let id = self.sequence;
        let call = submit(id).map_err(submit_error)?;
        let (reply, receiver) = mpsc::sync_channel(1);
        self.pending.insert(
            id,
            Pending {
                lease: lease.clone(),
                reply,
                receipt: Some(Receipt::Submit(call)),
                cancel_sent: false,
            },
        );
        Ok(Request {
            id,
            captured,
            receiver,
            lease,
        })
    }
    pub fn poll(&mut self, client: &Client) -> Result<bool, Error> {
        let mut advanced = false;
        let ids = self.pending.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let pending = self.pending.get_mut(&id).expect("owned pending call");
            if let Some(receipt) = &mut pending.receipt {
                let (Receipt::Submit(call) | Receipt::Cancel(call)) = receipt;
                let reply = call.try_reply().map_err(|_| Error::ReceiptLost(id))?;
                let Some(reply) = reply else {
                    if client.is_closed() {
                        return Err(Error::ReceiptLost(id));
                    }
                    continue;
                };
                let cancelling = matches!(pending.receipt, Some(Receipt::Cancel(_)));
                pending.receipt = None;
                advanced = true;
                match reply {
                    Ok(runtime::Applied::Unit) => (),
                    Ok(_) => return Err(Error::UnexpectedReply(id)),
                    Err(coordinator::Error::Stopped) if cancelling => return Err(Error::Closed),
                    Err(error) if cancelling => {
                        pending.cancel_sent = false;
                        return Err(Error::CancelRejected(id, error));
                    }
                    Err(error) => {
                        let _ = pending.reply.try_send(Err(RequestError::Rejected(error)));
                        self.pending.remove(&id);
                        continue;
                    }
                }
            }
            if pending.lease.cancelled.load(Ordering::Acquire) && !pending.cancel_sent {
                match client.call(runtime::Command::CancelRequest(id)) {
                    Ok(call) => {
                        pending.receipt = Some(Receipt::Cancel(call));
                        pending.cancel_sent = true;
                        advanced = true;
                    }
                    Err(runtime::SubmitError::Full(_)) => (),
                    Err(e) => return Err(submit_error(e)),
                }
            }
        }
        Ok(advanced)
    }
    pub fn route(&mut self, output: &mut Option<runtime::Output>) -> Result<Routed, Error> {
        let Some(runtime::Output::Service(service)) = output.as_ref() else {
            return Ok(Routed::Other);
        };
        let id = match service {
            coordinator::ServiceOutput::Finalized { request, .. }
            | coordinator::ServiceOutput::CallFailed { request, .. } => *request,
            _ => return Ok(Routed::Other),
        };
        let pending = self.pending.get(&id).ok_or(Error::UnexpectedRequest(id))?;
        if pending.lease.abandoned.load(Ordering::Acquire) {
            output.take();
            self.pending.remove(&id);
            return Ok(Routed::Completed);
        }
        let bytes = if let coordinator::ServiceOutput::Finalized { replies, .. } = service {
            let size = replies
                .iter()
                .fold(std::mem::size_of::<ResultData>(), |size, reply| {
                    size.saturating_add(match reply {
                        Reply::Finalized(local_host::Finalized::Signed(signed)) => {
                            signed.encode().len()
                        }
                        _ => 0,
                    })
                });
            if size > self.limits.reply_bytes {
                return Err(Error::ResourceLimit);
            }
            match self.bytes.clone().try_acquire_many_owned(size as u32) {
                Ok(bytes) => Some(bytes),
                Err(_) => return Ok(Routed::Backpressured),
            }
        } else {
            None
        };
        let Some(runtime::Output::Service(service)) = output.take() else {
            unreachable!();
        };
        let result = match service {
            coordinator::ServiceOutput::Finalized { replies, .. } => {
                Ok(Completion(Arc::new(ResultData {
                    replies,
                    _bytes: bytes.expect("reserved final result"),
                    _request: pending.lease.clone(),
                })))
            }
            coordinator::ServiceOutput::CallFailed { failure, .. } => {
                Err(RequestError::Failed(failure))
            }
            _ => unreachable!(),
        };
        let _ = pending.reply.try_send(result);
        self.pending.remove(&id);
        Ok(Routed::Completed)
    }
    pub fn abandon(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.pending)
            .into_iter()
            .map(|(id, p)| {
                let _ = p.reply.try_send(Err(RequestError::Unknown));
                id
            })
            .collect()
    }
    // The old runtime is joined and every retained output has been routed.
    // A known rejected submission stays known; all other unfinished calls
    // remain unknown. Neither result authorizes replay after replacement.
    pub fn retire(&mut self) -> Vec<RetiredRequest> {
        std::mem::take(&mut self.pending)
            .into_iter()
            .map(|(id, mut pending)| {
                let receipt = pending.receipt.as_mut().map(|receipt| match receipt {
                    Receipt::Submit(call) => (ReceiptKind::Submit, call.try_reply()),
                    Receipt::Cancel(call) => (ReceiptKind::Cancel, call.try_reply()),
                });
                let outcome = match &receipt {
                    Some((ReceiptKind::Submit, Ok(Some(Err(error))))) => {
                        RequestError::Rejected(error.clone())
                    }
                    _ => RequestError::Unknown,
                };
                let _ = pending.reply.try_send(Err(outcome.clone()));
                RetiredRequest {
                    id,
                    receipt,
                    outcome,
                }
            })
            .collect()
    }
}
