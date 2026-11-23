// Operational pinning checkpoint. Canonical chain bytes are unchanged.
// A pending event retains its pre-state and durable query/effect decisions.
use super::*;
use ae_hyperchain::{pin, pinning as core};
use ae_types::{AccountPublicKey, Unsigned};
use num_bigint::BigInt;
mod codec;
const KEY: &[u8] = b"\0pinning-agent";
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub events: usize,
    pub integer_bytes: usize,
    pub hash_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 16 << 20,
            events: 1024,
            integer_bytes: 4096,
            hash_bytes: 4096,
        }
    }
}
impl Limits {
    fn hard() -> Self {
        Self {
            bytes: 256 << 20,
            events: 65536,
            integer_bytes: 65536,
            hash_bytes: 65536,
        }
    }
    fn validate(self) -> Result<(), Error> {
        let h = Self::hard();
        if self.bytes == 0
            || self.bytes > h.bytes
            || self.events == 0
            || self.events > h.events
            || self.integer_bytes == 0
            || self.integer_bytes > h.integer_bytes
            || self.hash_bytes == 0
            || self.hash_bytes > h.hash_bytes
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Post {
    pub pin: pin::Parent,
    pub account: AccountPublicKey,
    pub amount: Unsigned,
    pub fee: Unsigned,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentResult {
    Submitted { hash: Vec<u8>, intent: Hash256 },
    Unknown { intent: Hash256, reason: String },
    NotSubmitted(String),
}
impl ParentResult {
    pub fn hash(&self) -> Option<Vec<u8>> {
        if let Self::Submitted { hash, .. } = self {
            Some(hash.clone())
        } else {
            None
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildResult {
    Completed(Hash256),
    Skipped(core::Error),
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Operation {
    pub leader: Option<(AccountPublicKey, bool)>,
    pub post: Option<Post>,
    pub parent: Option<ParentResult>,
    pub included: Option<bool>,
    pub note: Option<ChildResult>,
    pub proof: Option<ChildResult>,
}
impl Operation {
    fn extends(&self, old: &Self) -> bool {
        fn field<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
            a.is_none() || a == b
        }
        field(&old.leader, &self.leader)
            && field(&old.post, &self.post)
            && field(&old.parent, &self.parent)
            && field(&old.included, &self.included)
            && field(&old.note, &self.note)
            && field(&old.proof, &self.proof)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    pub revision: u64,
    pub old_tip: Option<Hash256>,
    pub tip: Hash256,
    pub source: Hash256,
    pub events: Vec<core::Event>,
    pub next: usize,
    pub operation: Operation,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub version: u64,
    pub binding: Hash256,
    pub consumed: Status,
    pub state: core::State,
    pub completed: u64,
    pub pending: Option<Batch>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Storage(Error),
    AlreadyRunning,
    RecoveryRequired,
    Binding,
    Incomplete,
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Storage(e)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}
impl Database {
    pub fn pinning_agent(&self, limits: Limits) -> Result<Option<Checkpoint>, Error> {
        limits.validate()?;
        let _guard = self.writer.lock().map_err(|_| backend_lock())?;
        self.db
            .get(KEY)
            .map_err(backend)?
            .map(|b| Checkpoint::decode(&b, limits))
            .transpose()
    }
    pub(super) fn validate_pinning_agent(&self) -> Result<(), Error> {
        if let Some(c) = self.pinning_agent(Limits::hard())? {
            let status = self.view()?.status();
            if c.pending
                .as_ref()
                .map_or(c.consumed.revision, |p| p.revision)
                > status.revision
            {
                return Err(Error::Corrupt("agent checkpoint ahead"));
            }
        }
        Ok(())
    }
}
pub struct Writer {
    db: Arc<Database>,
    limits: Limits,
    _lease: Arc<()>,
    current: Checkpoint,
    uncertain: bool,
}
impl Writer {
    pub fn start(db: Arc<Database>, binding: Hash256, limits: Limits) -> Result<Self, Failure> {
        limits.validate()?;
        let lease = Arc::new(());
        {
            let mut owner = db.pinning_agent_owner.lock().map_err(|_| backend_lock())?;
            if owner.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *owner = Arc::downgrade(&lease);
        }
        let old = db.pinning_agent(limits)?;
        let current = old.clone().unwrap_or(Checkpoint {
            version: 0,
            binding,
            consumed: db.view()?.status(),
            state: Default::default(),
            completed: 0,
            pending: None,
        });
        let mut writer = Self {
            db,
            limits,
            _lease: lease,
            current,
            uncertain: false,
        };
        if old.is_none() {
            writer.save_with(None, writer.current.clone(), write)?;
        } else if writer.current.binding != binding {
            if writer.current.state.enabled || writer.current.pending.is_some() {
                return Err(Failure::Binding);
            }
            let mut next = writer.current.clone();
            next.binding = binding;
            writer.save(next)?;
        }
        Ok(writer)
    }
    pub fn checkpoint(&self) -> Result<&Checkpoint, Failure> {
        if self.uncertain {
            Err(Failure::RecoveryRequired)
        } else {
            Ok(&self.current)
        }
    }
    pub fn recover(&mut self) -> Result<&Checkpoint, Failure> {
        self.current = self
            .db
            .pinning_agent(self.limits)?
            .ok_or(Error::Corrupt("missing agent checkpoint"))?;
        self.uncertain = false;
        Ok(&self.current)
    }
    pub fn begin(&mut self, batch: Batch) -> Result<(), Failure> {
        let c = self.checkpoint()?;
        if c.pending.is_some()
            || batch.next != 0
            || batch.operation != Operation::default()
            || batch.revision
                != c.consumed
                    .revision
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?
            || batch.old_tip != c.consumed.tip
        {
            return Err(Error::Conflict.into());
        }
        if batch.revision > self.db.view()?.status().revision {
            return Err(Error::Conflict.into());
        }
        let mut next = c.clone();
        next.pending = Some(batch);
        self.save(next)
    }
    pub fn observe(&mut self, operation: Operation) -> Result<(), Failure> {
        let mut next = self.checkpoint()?.clone();
        let p = next.pending.as_mut().ok_or(Error::Conflict)?;
        if p.next >= p.events.len() || !operation.extends(&p.operation) {
            return Err(Error::Conflict.into());
        }
        if operation == p.operation {
            return Ok(());
        }
        p.operation = operation;
        self.save(next)
    }
    pub fn finish(&mut self, state: &core::State) -> Result<(), Failure> {
        let mut next = self.checkpoint()?.clone();
        let p = next.pending.as_mut().ok_or(Error::Conflict)?;
        let event = p.events.get(p.next).ok_or(Error::Conflict)?;
        let mut expected = next.state.clone();
        expected
            .apply(event, &mut Replay(&p.operation))
            .map_err(|_| Failure::Incomplete)?;
        if expected != *state {
            return Err(Error::Conflict.into());
        }
        next.state = expected;
        next.completed = next.completed.checked_add(1).ok_or(Error::ResourceLimit)?;
        p.next += 1;
        p.operation = Operation::default();
        self.save(next)
    }
    pub fn advance(&mut self) -> Result<(), Failure> {
        let mut next = self.checkpoint()?.clone();
        let p = next.pending.take().ok_or(Error::Conflict)?;
        if p.next != p.events.len() {
            return Err(Failure::Incomplete);
        }
        next.consumed = Status {
            revision: p.revision,
            tip: Some(p.tip),
        };
        self.save(next)
    }
    fn save(&mut self, next: Checkpoint) -> Result<(), Failure> {
        let expected = self.checkpoint()?.clone();
        self.save_with(Some(&expected), next, write)
    }
    fn save_with(
        &mut self,
        expected: Option<&Checkpoint>,
        mut next: Checkpoint,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<(), Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        next.version = expected
            .map_or(0, |c| c.version)
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let encoded = next.encode(self.limits)?;
        let _guard = self.db.writer.lock().map_err(|_| backend_lock())?;
        self.db.ensure_policy_recovered()?;
        let raw = self.db.db.get(KEY).map_err(backend)?;
        if raw != expected.map(|c| c.encode(self.limits)).transpose()? {
            return Err(Error::Conflict.into());
        }
        let mut batch = WriteBatch::default();
        batch.put(KEY, encoded);
        self.uncertain = true;
        write(&self.db, batch)?;
        self.current = next;
        self.uncertain = false;
        Ok(())
    }
}
fn write(db: &Database, batch: WriteBatch) -> Result<(), Error> {
    db.db.write_opt(batch, &write_options()).map_err(backend)
}
// Validate completion by rerunning only the deterministic native state machine
// against recorded answers. No service call or new operation is possible here.
struct Replay<'a>(&'a Operation);
#[derive(Debug)]
enum Missing {
    Answer,
    Transaction(core::Error),
}
impl core::Host for Replay<'_> {
    type Error = Missing;
    fn operational(e: &Missing) -> bool {
        !matches!(
            e,
            Missing::Transaction(core::Error::Signing | core::Error::Encoding)
        )
    }
    fn leader(&mut self, _: &BigInt) -> Result<AccountPublicKey, Missing> {
        self.0.leader.map(|v| v.0).ok_or(Missing::Answer)
    }
    fn mapped(&self, who: AccountPublicKey) -> bool {
        self.0.leader.is_some_and(|(l, m)| l == who && m)
    }
    fn post(&mut self, _: AccountPublicKey) -> Result<Option<Vec<u8>>, Missing> {
        self.0
            .parent
            .as_ref()
            .map(ParentResult::hash)
            .ok_or(Missing::Answer)
    }
    fn included(&mut self, hash: Option<&[u8]>) -> Result<bool, Missing> {
        if hash.is_none() {
            Ok(false)
        } else {
            self.0.included.ok_or(Missing::Answer)
        }
    }
    fn note(&mut self, _: AccountPublicKey, _: &[u8]) -> Result<(), Missing> {
        child(&self.0.note)
    }
    fn proof(&mut self, _: AccountPublicKey, _: &[u8]) -> Result<(), Missing> {
        child(&self.0.proof)
    }
}
fn child(value: &Option<ChildResult>) -> Result<(), Missing> {
    match value {
        Some(ChildResult::Completed(_)) => Ok(()),
        Some(ChildResult::Skipped(e)) => Err(Missing::Transaction(*e)),
        None => Err(Missing::Answer),
    }
}
#[cfg(test)]
mod tests;
