use crate::{
    Error, integer,
    nonce::Part,
    proof::Proof,
    queue::Queue,
    share::{self, Share},
    target,
};
use ae_types::Hash256;
use num_bigint::BigInt;
use std::collections::VecDeque;

pub const MAX_JOB_SHARE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RETAINED_SHARE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Work {
    hash_text: String,
    hash: Hash256,
    pub version: u32,
    pub block_target: BigInt,
}
impl Work {
    pub fn new(hash_text: String, version: u32, block_target: BigInt) -> Result<Self, Error> {
        if hash_text.len() != 64 {
            return Err(Error::Hash);
        }
        let mut hash = [0; 32];
        hex::decode_to_slice(&hash_text, &mut hash).map_err(|_| Error::Hash)?;
        integer(&block_target)?;
        Ok(Self {
            hash_text,
            hash: Hash256(hash),
            version,
            block_target,
        })
    }
    pub fn hash(&self) -> Hash256 {
        self.hash
    }
    pub fn hash_text(&self) -> &str {
        &self.hash_text
    }
    pub fn id(&self) -> Result<String, Error> {
        // The original uses a single iodata byte, not a u32 encoding.
        let version = u8::try_from(self.version).map_err(|_| Error::Version)?;
        let mut input = self.hash_text.as_bytes().to_vec();
        input.extend_from_slice(target::to_hex(&self.block_target)?.as_bytes());
        input.push(version);
        Ok(hex::encode(&ae_crypto::sha256(&input).0[..8]))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub shares: usize,
    pub errors: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            shares: 4096,
            errors: 4096,
            bytes: MAX_JOB_SHARE_BYTES,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.shares > 65536 || self.errors > 65536 {
            return Err(Error::Limit("share capacity"));
        }
        if self.bytes > MAX_JOB_SHARE_BYTES {
            return Err(Error::Limit("job share bytes"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    id: String,
    work: Work,
    share_target: BigInt,
    desired_solve_time: BigInt,
    maximum_solve_time: BigInt,
    created: BigInt,
    shares: VecDeque<Share>,
    errors: VecDeque<Share>,
    limits: Limits,
    retained_bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timing {
    pub desired: BigInt,
    pub maximum: BigInt,
    pub created: BigInt,
}
impl Job {
    pub fn new(
        id: String,
        work: Work,
        share_target: BigInt,
        timing: Timing,
        limits: Limits,
    ) -> Result<Self, Error> {
        if id.len() > 1024 {
            return Err(Error::Limit("job id"));
        }
        limits.validate()?;
        for value in [
            &work.block_target,
            &share_target,
            &timing.desired,
            &timing.maximum,
            &timing.created,
        ] {
            integer(value)?;
        }
        Ok(Self {
            id,
            work,
            share_target,
            desired_solve_time: timing.desired,
            maximum_solve_time: timing.maximum,
            created: timing.created,
            shares: VecDeque::new(),
            errors: VecDeque::new(),
            limits,
            retained_bytes: 0,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn work(&self) -> &Work {
        &self.work
    }
    pub fn share_target(&self) -> BigInt {
        self.share_target.clone()
    }
    pub fn desired_solve_time(&self) -> &BigInt {
        &self.desired_solve_time
    }
    pub fn maximum_solve_time(&self) -> &BigInt {
        &self.maximum_solve_time
    }
    pub fn created(&self) -> &BigInt {
        &self.created
    }
    pub fn shares(&self) -> &VecDeque<Share> {
        &self.shares
    }
    pub fn errors(&self) -> &VecDeque<Share> {
        &self.errors
    }
    pub fn is_submitted(&self) -> bool {
        !self.shares.is_empty()
    }
    pub fn solve_time(&self) -> BigInt {
        // Native insertion order, not minimum timestamp: clocks can regress.
        self.shares.back().map_or_else(
            || self.maximum_solve_time.clone(),
            |s| &s.created - &self.created,
        )
    }
    pub fn contains(&self, nonce: Part, proof: &Proof) -> bool {
        self.shares.iter().any(|s| s.duplicates(nonce, proof))
    }
    pub fn add_share(&mut self, share: Share) -> Result<(), Error> {
        share.validate()?;
        if self.shares.len() == self.limits.shares {
            return Err(Error::Limit("shares"));
        }
        self.reserve_share(&share)?;
        self.shares.push_front(share);
        Ok(())
    }
    pub fn add_error(&mut self, share: Share) -> Result<(), Error> {
        share.validate()?;
        if self.errors.len() == self.limits.errors {
            return Err(Error::Limit("error shares"));
        }
        self.reserve_share(&share)?;
        self.errors.push_front(share);
        Ok(())
    }
    fn retained(&self) -> usize {
        self.shares.len() + self.errors.len()
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    fn reserve_share(&mut self, share: &Share) -> Result<(), Error> {
        let next = self.retained_bytes + share.retained_bytes();
        if next > self.limits.bytes {
            return Err(Error::Limit("job share bytes"));
        }
        self.retained_bytes = next;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jobs {
    queue: Queue<String, Job>,
    maximum_retained: usize,
    maximum_bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub share: Share,
    pub decision: share::Decision,
    pub job_found: bool,
    pub response: share::Response,
    // Ordered descriptors only. The owner executes and durably accounts for
    // these after this state transition; this layer performs no side effects.
    pub effects: Vec<share::Effect>,
}
impl Jobs {
    pub fn new(capacity: usize, maximum_retained: usize) -> Result<Self, Error> {
        Self::with_limits(capacity, maximum_retained, MAX_RETAINED_SHARE_BYTES)
    }
    pub fn with_limits(
        capacity: usize,
        maximum_retained: usize,
        maximum_bytes: usize,
    ) -> Result<Self, Error> {
        if maximum_retained > 65536 {
            return Err(Error::Limit("retained shares"));
        }
        if maximum_bytes > MAX_RETAINED_SHARE_BYTES {
            return Err(Error::Limit("retained share bytes"));
        }
        Ok(Self {
            queue: Queue::new(capacity)?,
            maximum_retained,
            maximum_bytes,
        })
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Job> {
        self.queue.iter().map(|(_, j)| j)
    }
    pub fn find(&self, id: &str) -> Option<&Job> {
        self.queue
            .iter()
            .find(|(key, _)| key == id)
            .map(|(_, job)| job)
    }
    pub fn front(&self) -> Option<&Job> {
        self.queue.front().map(|(_, j)| j)
    }
    pub fn rear(&self) -> Option<&Job> {
        self.queue.rear().map(|(_, j)| j)
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn add(&mut self, job: Job) -> Result<Option<Job>, Error> {
        let retained: usize = self.iter().map(Job::retained).sum();
        let evicted = if self.queue.is_full() {
            self.front().map_or(0, Job::retained)
        } else {
            0
        };
        if retained - evicted + job.retained() > self.maximum_retained {
            return Err(Error::Limit("retained shares"));
        }
        let bytes: usize = self.iter().map(Job::retained_bytes).sum();
        let evicted_bytes = if self.queue.is_full() {
            self.front().map_or(0, Job::retained_bytes)
        } else {
            0
        };
        if bytes - evicted_bytes + job.retained_bytes() > self.maximum_bytes {
            return Err(Error::Limit("retained share bytes"));
        }
        Ok(self.queue.push(job.id.clone(), job).map(|(_, j)| j))
    }
    pub fn replace(&mut self, id: &str, job: Job) -> Result<(), Error> {
        if id.len() > 1024 {
            return Err(Error::Limit("job id"));
        }
        // Keep the queue key separate from the job's own ID, like native keyreplace.
        let retained: usize = self
            .queue
            .iter()
            .map(|(key, j)| {
                if key == id {
                    job.retained()
                } else {
                    j.retained()
                }
            })
            .sum();
        if retained > self.maximum_retained {
            return Err(Error::Limit("retained shares"));
        }
        let bytes: usize = self
            .queue
            .iter()
            .map(|(key, j)| {
                if key == id {
                    job.retained_bytes()
                } else {
                    j.retained_bytes()
                }
            })
            .sum();
        if bytes > self.maximum_bytes {
            return Err(Error::Limit("retained share bytes"));
        }
        self.queue.replace(&id.to_owned(), job);
        Ok(())
    }
    pub fn next_target(&self, desired: &BigInt, maximum: &BigInt) -> Result<BigInt, Error> {
        if self.len() < 3 {
            return Err(Error::NotEnoughJobs);
        }
        target::recalculate(
            &self
                .iter()
                .map(|j| (j.share_target(), j.solve_time()))
                .collect::<Vec<_>>(),
            desired,
            maximum,
        )
    }
    pub fn submit(
        &mut self,
        id: &str,
        mut submitted: Share,
        context: &share::Context<'_>,
    ) -> Result<Receipt, Error> {
        let job = self.find(id);
        let decision = share::classify(job, &submitted, context)?;
        submitted.validity = Some(decision.validity);
        let job_found = job.is_some();
        let mut effects = Vec::new();
        if let Some(job) = job {
            let mut next = job.clone();
            if decision.validity.accepted() {
                next.add_share(submitted.clone())?;
                effects.push(share::Effect::SubmitShare {
                    account: submitted.user.account.clone(),
                    target: job.share_target(),
                    hash: job.work().hash(),
                });
                if decision.validity == share::Validity::ValidBlock {
                    effects.push(share::Effect::SubmitSolution {
                        hash: job.work().hash(),
                        nonce: decision.nonce.expect("verified block nonce"),
                        proof: Box::new(
                            submitted
                                .proof
                                .integer_edges()
                                .expect("verified block proof"),
                        ),
                    });
                }
            } else {
                next.add_error(submitted.clone())?;
            }
            // Native response handling replaces by the returned job's own ID,
            // which can differ from the lookup key after raw keyreplace calls.
            let replacement_id = next.id.clone();
            self.replace(&replacement_id, next)?;
        }
        Ok(Receipt {
            share: submitted,
            decision,
            job_found,
            response: decision.validity.response(),
            effects,
        })
    }
}
impl Default for Jobs {
    fn default() -> Self {
        Self::new(20, 8192).expect("bounded default job queue")
    }
}
