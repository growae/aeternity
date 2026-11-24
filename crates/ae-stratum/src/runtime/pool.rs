// Busy is definitely unaccepted. Once submit returns a Submission, dropping
// its future does not cancel acceptance: its id must be reconciled on failure.
use crate::{ingress::Id, share::Effect};
use std::{future::Future, pin::Pin};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Busy,
    Closed,
    Limit,
    Invalid,
    Rejected,
    Unknown,
    Receipt,
}
pub struct Submission {
    pub id: Id,
    pub completion: Pin<Box<dyn Future<Output = Result<Id, Error>> + Send>>,
}
pub trait Sink: Send + Sync {
    fn submit(&self, effects: &[Effect]) -> Result<Submission, Error>;
    fn closed(&self) -> tokio::sync::watch::Receiver<bool>;
}
