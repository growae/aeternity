use super::*;
use std::sync::Weak;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    Closed,
    Busy,
    Stale { requested: u64, current: u64 },
    Retiring,
    NotBlockProducer,
    Terminal,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    Applied,
    Advanced(Progress),
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub controls: usize,
    pub driver: usize,
}
pub(super) struct Slot {
    result: Mutex<Option<Result<Reply, RequestError>>>,
    wake: Condvar,
    owner: Weak<Shared>,
    capacity: Arc<Mutex<Usage>>,
    driver: bool,
    pub sequence: u64,
    pub generation: u64,
}
impl Slot {
    pub fn new(owner: &Arc<Shared>, driver: bool, sequence: u64, generation: u64) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            wake: Condvar::new(),
            owner: Arc::downgrade(owner),
            capacity: owner.capacity.clone(),
            driver,
            sequence,
            generation,
        })
    }
    pub fn complete(&self, result: Result<Reply, RequestError>) {
        let mut slot = lock(&self.result);
        if slot.is_none() {
            *slot = Some(result);
            self.wake.notify_all();
        }
    }
    fn abandon(&self, executing: bool) {
        let mut slot = lock(&self.result);
        if slot.is_none() {
            if executing && let Some(owner) = self.owner.upgrade() {
                // Close admission before publishing an unknown accepted result.
                let mut control = lock(&owner.control);
                control.stop = true;
                if !self.driver {
                    control.applied += 1;
                    control.rejected += 1;
                }
                owner.wake.notify_all();
            }
            *slot = Some(Err(if executing {
                RequestError::Unknown
            } else {
                RequestError::Closed
            }));
            self.wake.notify_all();
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        let mut usage = lock(&self.capacity);
        if self.driver {
            usage.driver -= 1;
        } else {
            usage.controls -= 1;
        }
    }
}
#[derive(Clone)]
pub struct Ticket(pub(super) Arc<Slot>);
impl std::fmt::Debug for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlTicket")
            .field("sequence", &self.sequence())
            .field("generation", &self.generation())
            .finish()
    }
}
impl Ticket {
    pub fn sequence(&self) -> u64 {
        self.0.sequence
    }
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn try_result(&self) -> Option<Result<Reply, RequestError>> {
        *lock(&self.0.result)
    }
    // Timeout/drop does not cancel or resubmit accepted work.
    pub fn wait_timeout(&self, duration: Duration) -> Option<Result<Reply, RequestError>> {
        let (result, _) = self
            .0
            .wake
            .wait_timeout_while(lock(&self.0.result), duration, |r| r.is_none())
            .unwrap_or_else(|e| e.into_inner());
        *result
    }
}
#[derive(Debug)]
pub enum Observed {
    Pending(Ticket),
    Known {
        sequence: u64,
        generation: u64,
        result: Result<Reply, RequestError>,
    },
}
impl From<Ticket> for Observed {
    fn from(ticket: Ticket) -> Self {
        let mut result = Self::Pending(ticket);
        result.poll();
        result
    }
}
impl Observed {
    pub fn poll(&mut self) {
        if let Self::Pending(ticket) = self
            && let Some(result) = ticket.try_result()
        {
            *self = Self::Known {
                sequence: ticket.sequence(),
                generation: ticket.generation(),
                result,
            };
        }
    }
}
pub(super) struct Job {
    pub generation: u64,
    pub command: Command,
    pub reply: Arc<Slot>,
    pub executing: bool,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.reply.abandon(self.executing);
    }
}
