use super::*;
use std::sync::atomic::AtomicUsize;

pub(super) struct Calls {
    pub maximum: usize,
    pub used: AtomicUsize,
}
// A restart does not mint fresh credits while callers still hold accepted
// calls, unread results or cloned vote snapshots from an earlier runtime.
#[derive(Clone)]
pub struct Capacity {
    pub(super) calls: Arc<Calls>,
    pub(super) votes: Arc<votes::Budget>,
    vote_bytes: usize,
}
impl Capacity {
    pub fn new(config: Config) -> io::Result<Self> {
        validate(config)?;
        Ok(Self {
            calls: Arc::new(Calls {
                maximum: config.command_capacity,
                used: AtomicUsize::new(0),
            }),
            votes: Arc::new(votes::Budget::new(config.maximum_vote_reply_bytes)),
            vote_bytes: config.maximum_vote_reply_bytes,
        })
    }
    pub fn outstanding(&self) -> usize {
        self.calls.used.load(Ordering::Acquire)
    }
    pub fn retained_vote_bytes(&self) -> usize {
        self.votes.used()
    }
    pub(super) fn matches(&self, config: Config) -> bool {
        self.calls.maximum == config.command_capacity
            && self.vote_bytes == config.maximum_vote_reply_bytes
    }
}
pub(super) fn validate(config: Config) -> io::Result<()> {
    if config.command_capacity == 0
        || config.output_capacity == 0
        || config.batch_steps == 0
        || config.maximum_command_bytes == 0
        || config.maximum_vote_reply_bytes == 0
        || config.maximum_vote_reply_bytes > 1 << 30
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "EOE runtime limits must be positive and bounded",
        ));
    }
    Ok(())
}
