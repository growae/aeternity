// aehttpc_aeternity/aehttpc_doge pin_to_pc, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC notice in repository LICENSE.
use super::{
    Chain, Client, Error, Node, Pending,
    transport::{self, Transport},
};
use ae_hyperchain::{pin, signing::Keys};
use ae_types::{AccountPublicKey, Unsigned};
use std::{
    sync::{
        Arc, Mutex,
        atomic::AtomicBool,
        mpsc::{self, SyncSender, TrySendError},
    },
    time::Instant,
};
mod callbacks;
pub(crate) mod journal;
pub use journal::Config as Journal;

#[derive(Clone)]
pub struct Signing {
    pub network: Vec<u8>,
    pub keys: Keys,
}
#[derive(Debug, Clone)]
pub struct Post {
    pub pin: pin::Parent,
    pub account: AccountPublicKey,
    pub amount: Unsigned,
    pub fee: Unsigned,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Local(Error),
    Unsupported,
    MissingKey,
    ParentResponse,
    NoSuitableUtxo,
    Journal(ae_storage::parent_posts::Failure),
    Recovered,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub chain: Chain,
    // Exact signed bytes (AE) or wallet-signed hexadecimal string (Doge).
    // Retain these for reconciliation; no service retry is performed.
    pub transaction: Arc<[u8]>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    NotSubmitted(Failure),
    Submitted {
        hash: Vec<u8>,
        submission: Submission,
    },
    // The final broadcast operation began, but no usable hash was returned.
    // This includes rejection responses: the service makes no inference about
    // chain acceptance from an HTTP status or a JSON-RPC error.
    Unknown {
        submission: Submission,
        reason: Failure,
    },
}
#[derive(Clone, Default)]
struct Progress(Arc<Mutex<Option<Submission>>>);
impl Progress {
    fn begin(&self, submission: Submission) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = Some(submission);
    }
    fn failure(&self, reason: Failure) -> Outcome {
        match self.0.lock().unwrap_or_else(|p| p.into_inner()).clone() {
            Some(submission) => Outcome::Unknown { submission, reason },
            None => Outcome::NotSubmitted(reason),
        }
    }
}
pub(super) struct Job {
    post: Post,
    payload: Vec<u8>,
    reply: SyncSender<Outcome>,
    cancel: Arc<AtomicBool>,
    deadline: Instant,
    progress: Progress,
}
// Results survive service shutdown and deadline expiry. Cancellation only asks
// the worker to stop; it cannot undo a broadcast. Poll through to its receipt.
pub struct Request {
    result: mpsc::Receiver<Outcome>,
    cancel: Pending,
    progress: Progress,
    done: bool,
}
impl Request {
    pub fn cancel(&self) {
        self.cancel
            .0
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn try_result(&mut self) -> Option<Outcome> {
        if self.done {
            return None;
        }
        let result = match self.result.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return None,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.progress.failure(Failure::Local(Error::Closed))
            }
        };
        self.done = true;
        Some(result)
    }
}
impl Client {
    pub fn post(&self, post: Post) -> Result<Request, Error> {
        self.check()?;
        if !self.has_nodes {
            return Err(Error::NoParents);
        }
        for n in [&post.amount, &post.fee] {
            if n.as_bytes().len() > self.config.maximum_number_digits
                || num_bigint::BigUint::from_bytes_be(n.as_bytes())
                    .to_string()
                    .len()
                    > self.config.maximum_number_digits
            {
                return Err(Error::ResourceLimit);
            }
        }
        let payload = pin::encode_parent(&post.pin, self.config.maximum_payload_bytes)
            .map_err(|_| Error::ResourceLimit)?;
        let deadline = Instant::now()
            .checked_add(self.config.lookup_timeout)
            .ok_or(Error::ResourceLimit)?;
        let (reply, result) = mpsc::sync_channel(1);
        let cancel = Pending(Arc::new(AtomicBool::new(false)));
        let progress = Progress::default();
        self.requests
            .try_send(super::Job::Post(Job {
                post,
                payload,
                reply,
                cancel: cancel.0.clone(),
                deadline,
                progress: progress.clone(),
            }))
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::Busy,
                TrySendError::Disconnected(_) => Error::Closed,
            })?;
        Ok(Request {
            result,
            cancel,
            progress,
            done: false,
        })
    }
}
pub(super) fn run(
    runtime: &tokio::runtime::Runtime,
    transport: &Transport,
    node: &Node,
    signing: Option<&Signing>,
    stop: &AtomicBool,
    job: Job,
    journal: Option<&mut journal::Owner>,
) {
    let mut journal = journal.map(|j| j.call(node, &job.post, &job.payload));
    let result = runtime.block_on(async {
        tokio::select! {
            biased;
            error = transport::cancelled(stop, &job.cancel, job.deadline) => Err(Failure::Local(error)),
            result = async {
                if let Some(call) = &journal
                    && let Some(record) = call.existing()? {
                    job.progress.begin(journal::submission(&record));
                    return record.response.ok_or(Failure::Recovered);
                }
                callbacks::post(transport, node, signing, &job.post, &job.payload, &job.progress, journal.as_mut()).await
            } => result,
        }
    });
    let result = match result {
        Ok(hash) => Outcome::Submitted {
            hash,
            submission: job
                .progress
                .0
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .expect("broadcast precedes hash"),
        },
        Err(failure) => job.progress.failure(failure),
    };
    let _ = job.reply.try_send(result);
}

pub(super) fn unavailable(job: Job) {
    let _ = job
        .reply
        .try_send(Outcome::NotSubmitted(Failure::Local(Error::NoParents)));
}
