// Durable Stratum payout ownership. No signing keys, timers or pool calls live
// here. Exact intent/signature publication precedes any external submission.
use super::*;
use ae_chain::{block::Block, repository as repo};
use ae_mempool::pool::{Event, Location, PushOutcome};
use ae_protocol::Protocol;
use ae_stratum::payout;
use ae_types::{AccountPublicKey, ContractPublicKey, Signature};
mod codec;
mod execution;
pub use execution::{
    ExecutionFailure, FailureReason, Inclusion, ReceiptUnavailable, UnavailableReason,
};
mod record;
pub use record::{Completion, Context, Dispatch, Info, Outcome, Record, Request, Signed};
const META: &[u8] = b"\0stratum-payouts";
const MAX_META: usize = 64 << 20;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub payout: payout::Limits,
    pub tree: ae_trie::OverlayLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 16 << 20,
            payout: Default::default(),
            tree: Default::default(),
        }
    }
}
impl Limits {
    pub(super) fn maximum() -> Self {
        let mut l = Self {
            bytes: MAX_META,
            ..Default::default()
        };
        l.payout.transfers = super::Limits::maximum().transaction.maps;
        l.payout.heap.max_depth = 256;
        l.payout.heap.max_nodes = 1_000_000;
        l.payout.heap.max_bytes = MAX_META;
        l.payout.transaction_bytes = MAX_META;
        l.payout.network_bytes = 65536;
        l.payout.record_bytes = MAX_META;
        l
    }
    fn validate(self) -> Result<(), Failure> {
        if !(1024..=MAX_META).contains(&self.bytes) {
            return Err(Error::ResourceLimit.into());
        }
        payout::call_args(&Default::default(), self.payout)?;
        Ok(())
    }
}
#[derive(Clone, Copy)]
pub(super) struct Owner {
    epoch: u64,
    limits: Limits,
}
// Native top_changed uses the notification height and queries balance when the
// callback runs. Publish context in the same batch as the round mailbox step.
pub(super) fn round_context(
    s: &Stored<'_>,
    v: &repo::View<Snapshot<'_>>,
    owner: Owner,
    height: u64,
) -> Result<Extra, Failure> {
    let mut info = s.payouts(owner.limits)?.ok_or(Error::Uninitialized)?;
    if info.epoch != owner.epoch {
        return Err(Error::Conflict.into());
    }
    info.context = context(v, &info.config, Some(info.context.protocol), owner.limits)?;
    info.context.height = height;
    extra(s, &info, owner.limits)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub caller: AccountPublicKey,
    pub contract: ContractPublicKey,
    pub delay: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferred {
    Empty,
    BalanceUnavailable,
    CachedBalanceLow,
    SentPayment,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Existing(Box<Record>),
    Prepared(Box<Record>),
    Deferred(Deferred),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmation {
    Pending,
    MissingReward,
    Confirmed(Completion),
    Failed(Box<ExecutionFailure>),
    Unavailable(ReceiptUnavailable),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Initialize,
    Context,
    Prepare,
    Sign,
    Submit,
    Receipt,
    Observe,
    Confirm,
}
fn write(db: &Database, batch: WriteBatch) -> Result<(), Error> {
    db.db.write_opt(batch, &write_options()).map_err(backend)
}
fn view<'a>(s: &Stored<'a>, config: repo::ReadConfig) -> Result<repo::View<Snapshot<'a>>, Failure> {
    Ok(repo::View::load_with_config(s.source.clone(), config)?)
}
fn top(v: &repo::View<Snapshot<'_>>) -> Result<ae_codec::headers::KeyHeader, Failure> {
    if v.identity().consensus != ae_protocol::ConsensusType::Pow {
        return Err(Error::Identity.into());
    }
    let hash = v.status().tip.ok_or(Error::Uninitialized)?;
    let b = v.require_block(hash)?;
    match b {
        Block::Key(h) => Ok(h),
        Block::Micro(b) => match v.require_block(Hash256(b.header.common.previous_key_hash))? {
            Block::Key(h) => Ok(h),
            _ => Err(Error::Corrupt("Stratum payout top key").into()),
        },
    }
}
fn account(
    v: &repo::View<Snapshot<'_>>,
    caller: AccountPublicKey,
    limits: Limits,
) -> Result<Option<ae_state::accounts::Account>, Failure> {
    let trees = v.trees(v.status().tip.ok_or(Error::Uninitialized)?, limits.tree)?;
    trees.accounts.lookup(caller).map_err(|e| match e {
        ae_state::accounts::AccountError::Tree(e) => Failure::Storage(e.into()),
        _ => Failure::Storage(Error::Corrupt("Stratum payout account")),
    })
}
fn context(
    v: &repo::View<Snapshot<'_>>,
    config: &Config,
    protocol: Option<Protocol>,
    limits: Limits,
) -> Result<Context, Failure> {
    let header = top(v)?;
    Ok(Context {
        chain: v.status(),
        height: header.common.height,
        protocol: protocol.unwrap_or(
            Protocol::try_from(header.common.version)
                .map_err(|_| Error::Corrupt("Stratum payout protocol"))?,
        ),
        balance: account(v, config.caller, limits)?.map(|a| a.balance().clone()),
    })
}
fn status(s: &Stored<'_>) -> Result<(Vec<u8>, Option<Vec<u8>>), Failure> {
    Ok((
        repo::STATUS.to_vec(),
        s.source.snapshot.get(repo::STATUS).map_err(backend)?,
    ))
}
fn extra(s: &Stored<'_>, info: &Info, limits: Limits) -> Result<Extra, Failure> {
    Ok(Extra {
        key: META.to_vec(),
        before: s.source.snapshot.get(META).map_err(backend)?,
        after: Some(info.encode(limits, s.limits.transaction)?),
    })
}
fn payment(s: &Stored<'_>, id: core::PaymentId) -> Result<Option<core::Payment>, Failure> {
    s.query(|t| match t.get(&core::payment_key(id))? {
        Some(core::Record::Payment(p)) => Ok(Some(p)),
        None => Ok(None),
        _ => Err(core::Error::Corrupt("payout source payment")),
    })
}
impl Stored<'_> {
    pub fn payouts(&self, limits: Limits) -> Result<Option<Info>, Failure> {
        limits.validate()?;
        let info = self
            .source
            .snapshot
            .get_pinned(META)
            .map_err(backend)?
            .map(|raw| Info::decode(&raw, limits, self.limits.transaction))
            .transpose()?;
        if let Some(i) = &info {
            bind(self, i)?;
            if let Some(r) = &i.pending
                && payment(self, r.payment.id)? != Some(r.expected_payment()?)
            {
                return Err(Error::Corrupt("Stratum payout source binding").into());
            }
        }
        Ok(info)
    }
}
fn bind(s: &Stored<'_>, info: &Info) -> Result<(), Failure> {
    let identity = repo::Identity::decode(
        &s.source
            .snapshot
            .get(repo::IDENTITY)
            .map_err(backend)?
            .ok_or(Error::Uninitialized)?,
    )?;
    let chain = repo::Status::decode(
        &s.source
            .snapshot
            .get(repo::STATUS)
            .map_err(backend)?
            .ok_or(Error::Uninitialized)?,
    )?;
    if identity.consensus != ae_protocol::ConsensusType::Pow
        || identity.network != info.network
        || info.context.chain.revision > chain.revision
        || info
            .last
            .as_ref()
            .is_some_and(|c| c.chain.revision > chain.revision)
        || info
            .pending
            .as_ref()
            .is_some_and(|r| r.observed_revision() > chain.revision)
    {
        return Err(Error::Corrupt("Stratum payout chain binding").into());
    }
    Ok(())
}
// Every ordinary accounting mutation must preserve the active payout's source
// row. The journal's own atomic signature/confirmation transitions supply the
// new metadata in the same batch, so no intermediate mismatch is observable.
pub(super) fn validate_plan(
    s: &Stored<'_>,
    plan: &core::Plan,
    extra: &[Extra],
) -> Result<(), Failure> {
    let pending = match extra.iter().find(|e| e.key == META) {
        Some(e) => e
            .after
            .as_ref()
            .map(|raw| Info::decode(raw, Limits::maximum(), s.limits.transaction))
            .transpose()?,
        None => s.payouts(Limits::maximum())?,
    };
    if let Some(info) = &pending {
        bind(s, info)?;
    }
    if let Some(r) = pending.and_then(|i| i.pending) {
        let k = core::payment_key(r.payment.id);
        let actual = match plan.writes().get(&k) {
            Some(v) => v.clone(),
            None => core::Read::get(s, &k)?,
        };
        let expected = core::Record::Payment(r.expected_payment()?).encode(s.limits.transaction)?;
        if actual.as_deref() != Some(expected.as_slice()) {
            return Err(Error::Conflict.into());
        }
    }
    Ok(())
}
impl Writer {
    fn payout_info(&self, epoch: u64, limits: Limits) -> Result<(Stored<'_>, Info), Failure> {
        self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let info = stored.payouts(limits)?.ok_or(Error::Uninitialized)?;
        if self.payout_owner.map(|o| o.epoch) != Some(epoch) || info.epoch != epoch {
            return Err(Error::Conflict.into());
        }
        Ok((stored, info))
    }
    pub fn begin_payouts(&mut self, config: Config, limits: Limits) -> Result<Info, Failure> {
        self.begin_payouts_with(config, limits, |_, db, b| write(db, b))
    }
    fn begin_payouts_with(
        &mut self,
        config: Config,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Info, Failure> {
        let version = self.version()?;
        let s = self.db.stratum(self.limits)?;
        let old = s.payouts(limits)?;
        if old.as_ref().is_some_and(|i| i.config != config) {
            return Err(Error::Identity.into());
        }
        if let Some(i) = old
            .as_ref()
            .filter(|i| Some(i.epoch) == self.payout_owner.map(|o| o.epoch))
        {
            return Ok(i.clone());
        }
        let epoch = old.as_ref().map_or(Ok(1), |i| {
            i.epoch.checked_add(1).ok_or(Error::ResourceLimit)
        })?;
        if self.payout_owner.is_some_and(|o| o.epoch != epoch) {
            return Err(Error::Conflict.into());
        }
        let v = view(&s, self.db.config.clone())?;
        let fresh = context(&v, &config, None, limits)?;
        let mut info = Info {
            config,
            network: v.identity().network.clone(),
            epoch,
            context: fresh,
            next: 1,
            pending: None,
            last: None,
        };
        // The startup context is fresh; an existing pending request remains exact.
        if let Some(old) = old {
            info.next = old.next;
            info.pending = old.pending;
            info.last = old.last;
        }
        if info.pending.is_none()
            && let Some(p) = s
                .query(|t| t.select_payments(true, Some(1)))?
                .into_iter()
                .next()
        {
            info.pending = Some(Record {
                sequence: info.next,
                payment: p,
                request: None,
                signed: None,
            });
            info.next = info.next.checked_add(1).ok_or(Error::ResourceLimit)?;
        }
        let changes = vec![extra(&s, &info, limits)?];
        let checks = vec![status(&s)?];
        drop(v);
        drop(s);
        self.payout_owner = Some(Owner { epoch, limits });
        self.commit_checked(version, vec![], changes, checks, |db, b| {
            write(Stage::Initialize, db, b)
        })?;
        Ok(info)
    }
    pub fn refresh_payout_context(&mut self, epoch: u64, limits: Limits) -> Result<Info, Failure> {
        self.refresh_payout_context_with(epoch, limits, |_, db, b| write(db, b))
    }
    fn refresh_payout_context_with(
        &mut self,
        epoch: u64,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Info, Failure> {
        let (s, mut info) = self.payout_info(epoch, limits)?;
        let version = s.version();
        let v = view(&s, self.db.config.clone())?;
        let next = context(&v, &info.config, Some(info.context.protocol), limits)?;
        if info.context == next {
            return Ok(info);
        }
        info.context = next;
        let changes = vec![extra(&s, &info, limits)?];
        let checks = vec![status(&s)?];
        drop(v);
        drop(s);
        self.commit_checked(version, vec![], changes, checks, |db, b| {
            write(Stage::Context, db, b)
        })?;
        Ok(info)
    }
    pub fn prepare_payout(
        &mut self,
        epoch: u64,
        minimum_miner_gas_price: Unsigned,
        limits: Limits,
    ) -> Result<Selection, Failure> {
        self.prepare_payout_with(epoch, minimum_miner_gas_price, limits, |_, db, b| {
            write(db, b)
        })
    }
    fn prepare_payout_with(
        &mut self,
        epoch: u64,
        price: Unsigned,
        limits: Limits,
        write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Selection, Failure> {
        self.prepare_generated_with(epoch, price, payout::Generation::Native, limits, write)
    }
    pub fn prepare_payout_with_generation(
        &mut self,
        epoch: u64,
        price: Unsigned,
        generation: payout::Generation,
        limits: Limits,
    ) -> Result<Selection, Failure> {
        self.prepare_generated_with(epoch, price, generation, limits, |_, db, b| write(db, b))
    }
    fn prepare_generated_with(
        &mut self,
        epoch: u64,
        price: Unsigned,
        generation: payout::Generation,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Selection, Failure> {
        let (s, mut info) = self.payout_info(epoch, limits)?;
        if let Some(r) = info.pending {
            return Ok(Selection::Existing(Box::new(r)));
        }
        let Some(cached) = &info.context.balance else {
            return Ok(Selection::Deferred(Deferred::BalanceUnavailable));
        };
        if !s.query(|t| t.select_payments(true, Some(1)))?.is_empty() {
            return Ok(Selection::Deferred(Deferred::SentPayment));
        }
        let Some(p) = s
            .query(|t| t.select_payments(false, Some(1)))?
            .into_iter()
            .next()
        else {
            return Ok(Selection::Deferred(Deferred::Empty));
        };
        if p.total > BigInt::from_bytes_be(num_bigint::Sign::Plus, cached.as_bytes()) {
            return Ok(Selection::Deferred(Deferred::CachedBalanceLow));
        }
        let version = s.version();
        let v = view(&s, self.db.config.clone())?;
        let account =
            account(&v, info.config.caller, limits)?.ok_or(payout::Error::AccountUnavailable)?;
        let environment = payout::Environment {
            caller: info.config.caller,
            contract: info.config.contract,
            account_nonce: account.nonce().clone(),
            minimum_miner_gas_price: price,
            height: info.context.height,
            protocol: info.context.protocol,
        };
        let prepared = payout::prepare_with_generation(
            &p,
            &environment,
            Some(account.balance()),
            &info.network,
            generation,
            limits.payout,
        )?;
        let r = Record {
            sequence: info.next,
            payment: p,
            request: Some(Box::new(Request {
                chain: v.status(),
                prepared,
            })),
            signed: None,
        };
        info.next = info.next.checked_add(1).ok_or(Error::ResourceLimit)?;
        info.pending = Some(r.clone());
        let changes = vec![extra(&s, &info, limits)?];
        let checks = vec![status(&s)?];
        drop(v);
        drop(s);
        self.commit_checked(version, vec![], changes, checks, |db, b| {
            write(Stage::Prepare, db, b)
        })?;
        Ok(Selection::Prepared(Box::new(r)))
    }
    fn replace_payout(
        &mut self,
        epoch: u64,
        expected: &Record,
        next: Record,
        commands: Vec<Command>,
        limits: Limits,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        let (s, mut info) = self.payout_info(epoch, limits)?;
        let version = s.version();
        if info.pending.as_ref() == Some(&next) {
            return Ok(next);
        }
        if info.pending.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        info.pending = Some(next.clone());
        let changes = vec![extra(&s, &info, limits)?];
        drop(s);
        self.commit_checked(version, commands, changes, vec![], write)?;
        Ok(next)
    }
    pub fn sign_payout(
        &mut self,
        epoch: u64,
        expected: &Record,
        signature: Signature,
        date: core::Date,
        limits: Limits,
    ) -> Result<Record, Failure> {
        self.sign_payout_with(epoch, expected, signature, date, limits, |_, db, b| {
            write(db, b)
        })
    }
    fn sign_payout_with(
        &mut self,
        epoch: u64,
        expected: &Record,
        signature: Signature,
        date: core::Date,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        let q = expected.request.as_ref().ok_or(Error::Conflict)?;
        if expected.signed.is_some() {
            return Err(Error::Conflict.into());
        }
        date.validate()?;
        let tx = q.prepared.attach_signature(signature)?;
        let mut next = expected.clone();
        next.signed = Some(Signed {
            signature,
            date,
            transaction: tx.encode(),
            dispatch: Dispatch::Ready(0),
        });
        let commands = vec![Command::UpdatePayment {
            payment: Box::new(expected.payment.clone()),
            absolute: q.prepared.absolute().clone(),
            hash: tx.hash(),
            date,
        }];
        self.replace_payout(epoch, expected, next, commands, limits, |db, b| {
            write(Stage::Sign, db, b)
        })
    }
    pub fn begin_payout_submission(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Record, Failure> {
        self.begin_payout_submission_with(epoch, expected, limits, |_, db, b| write(db, b))
    }
    fn begin_payout_submission_with(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        let mut next = expected.clone();
        let s = next.signed.as_mut().ok_or(Error::Conflict)?;
        let Dispatch::Ready(n) = s.dispatch else {
            return Err(Error::Conflict.into());
        };
        s.dispatch = Dispatch::Sending(n.checked_add(1).ok_or(Error::ResourceLimit)?);
        self.replace_payout(epoch, expected, next, vec![], limits, |db, b| {
            write(Stage::Submit, db, b)
        })
    }
    // The caller may use this only when the pool refused to return a ticket.
    // A timeout, lost reply or restart is not evidence of non-submission.
    pub fn unaccepted_payout_submission(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Record, Failure> {
        let mut next = expected.clone();
        let s = next.signed.as_mut().ok_or(Error::Conflict)?;
        let Dispatch::Sending(n) = s.dispatch else {
            return Err(Error::Conflict.into());
        };
        s.dispatch = Dispatch::Ready(n);
        self.replace_payout(epoch, expected, next, vec![], limits, write)
    }
    fn finish_payout_with(
        &mut self,
        epoch: u64,
        expected: &Record,
        outcome: Outcome,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        let mut next = expected.clone();
        let s = next.signed.as_mut().ok_or(Error::Conflict)?;
        let Dispatch::Sending(attempt) = s.dispatch else {
            return Err(Error::Conflict.into());
        };
        s.dispatch = Dispatch::Done { attempt, outcome };
        self.replace_payout(epoch, expected, next, vec![], limits, |db, b| {
            write(Stage::Receipt, db, b)
        })
    }
    pub fn record_payout_submission(
        &mut self,
        epoch: u64,
        expected: &Record,
        submission: crate::pool::Submission,
        limits: Limits,
    ) -> Result<Record, Failure> {
        let PushOutcome::Accepted {
            hash,
            inserted,
            event: Event::Created,
        } = submission.outcome
        else {
            return Err(Error::Conflict.into());
        };
        if expected.hash()? != Some(hash) {
            return Err(Error::Conflict.into());
        }
        self.finish_payout_with(
            epoch,
            expected,
            Outcome::Accepted {
                chain: submission.chain,
                revision: submission.revision,
                inserted,
            },
            limits,
            |_, db, b| write(db, b),
        )
    }
    pub fn reject_payout_submission(
        &mut self,
        epoch: u64,
        expected: &Record,
        reason: String,
        limits: Limits,
    ) -> Result<Record, Failure> {
        self.finish_payout_with(
            epoch,
            expected,
            Outcome::Rejected(reason),
            limits,
            |_, db, b| write(db, b),
        )
    }
    pub fn observe_payout(
        &self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Location, Failure> {
        let (s, info) = self.payout_info(epoch, limits)?;
        if info.pending.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        let hash = expected.hash()?.ok_or(Error::Conflict)?;
        let pool = crate::pool::Stored::load(view(&s, self.db.config.clone())?)?;
        let location = pool.location(hash)?;
        if location != Location::Unknown {
            let tx = pool
                .transaction(hash)?
                .ok_or(Error::Corrupt("Stratum observed transaction"))?;
            if expected
                .signed
                .as_ref()
                .is_some_and(|s| s.transaction != tx.encode())
            {
                return Err(Error::TransactionMetadataConflict.into());
            }
        }
        Ok(location)
    }
    // Reconciliation records observed retention, never a reconstructed push
    // receipt. Unknown transactions keep the original Sending marker intact.
    pub fn reconcile_payout(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Option<Record>, Failure> {
        self.reconcile_payout_with(epoch, expected, limits, |_, db, b| write(db, b))
    }
    fn reconcile_payout_with(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Option<Record>, Failure> {
        let (s, mut info) = self.payout_info(epoch, limits)?;
        if info.pending.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        let Some(signed) = &expected.signed else {
            return Ok(None);
        };
        if matches!(signed.dispatch, Dispatch::Done { .. }) {
            return Ok(Some(expected.clone()));
        }
        let Dispatch::Sending(attempt) = signed.dispatch else {
            return Err(Error::Conflict.into());
        };
        let version = s.version();
        let pool = crate::pool::Stored::load(view(&s, self.db.config.clone())?)?;
        let hash = expected.hash()?.ok_or(Error::Conflict)?;
        let location = pool.location(hash)?;
        if location == Location::Unknown {
            return Ok(None);
        }
        let tx = pool
            .transaction(hash)?
            .ok_or(Error::Corrupt("Stratum observed payout missing"))?;
        if tx.encode() != signed.transaction {
            return Err(Error::TransactionMetadataConflict.into());
        }
        let mut next = expected.clone();
        next.signed.as_mut().expect("signed record").dispatch = Dispatch::Done {
            attempt,
            outcome: Outcome::Observed {
                chain: pool.chain().status(),
                revision: pool.revision(),
                location,
            },
        };
        info.pending = Some(next.clone());
        let changes = vec![extra(&s, &info, limits)?];
        let checks = vec![
            status(&s)?,
            (
                crate::pool::REVISION.to_vec(),
                s.source
                    .snapshot
                    .get(crate::pool::REVISION)
                    .map_err(backend)?,
            ),
        ];
        drop(pool);
        drop(s);
        self.commit_checked(version, vec![], changes, checks, |db, b| {
            write(Stage::Observe, db, b)
        })?;
        Ok(Some(next))
    }
    pub fn confirm_payout(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Confirmation, Failure> {
        self.confirm_payout_with(epoch, expected, limits, |_, db, b| write(db, b))
    }
    // The timer cannot run ahead of its processed key notifications, nor can
    // an old cached height bypass the current canonical depth after a reorg.
    pub fn confirm_scheduled_payout(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
    ) -> Result<Confirmation, Failure> {
        self.confirm_payout_at(epoch, expected, limits, true, |_, db, b| write(db, b))
    }
    fn confirm_payout_with(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
        write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Confirmation, Failure> {
        self.confirm_payout_at(epoch, expected, limits, false, write)
    }
    fn confirm_payout_at(
        &mut self,
        epoch: u64,
        expected: &Record,
        limits: Limits,
        cached: bool,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Confirmation, Failure> {
        let (s, mut info) = self.payout_info(epoch, limits)?;
        let fingerprint = expected.fingerprint(limits, self.limits.transaction)?;
        if let Some(done) = info.last.as_ref().filter(|c| c.record == fingerprint) {
            return Ok(Confirmation::Confirmed(done.clone()));
        }
        if info.pending.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        let hash = expected.hash()?.ok_or(Error::Conflict)?;
        let version = s.version();
        let v = view(&s, self.db.config.clone())?;
        let Some(block) = v.location(hash)? else {
            return Ok(Confirmation::Pending);
        };
        if !v.is_canonical(block)? {
            return Err(Error::Corrupt("Stratum payout noncanonical location").into());
        }
        let Block::Micro(b) = v.require_block(block)? else {
            return Err(Error::Corrupt("Stratum payout inclusion block").into());
        };
        let tx = b
            .transactions
            .iter()
            .find(|tx| tx.hash() == hash)
            .ok_or(Error::Corrupt("Stratum payout inclusion missing"))?;
        if expected
            .signed
            .as_ref()
            .is_some_and(|s| s.transaction != tx.encode())
        {
            return Err(Error::TransactionMetadataConflict.into());
        }
        let height = b.header.common.height;
        let top_height = top(&v)?.common.height;
        let confirmed_height = if cached {
            top_height.min(info.context.height)
        } else {
            top_height
        };
        if confirmed_height
            .checked_sub(height)
            .is_none_or(|n| n < info.config.delay)
        {
            return Ok(Confirmation::Pending);
        }
        // Inclusion alone can represent a reverted or out-of-gas payout.
        // Read the inclusion block's authenticated call tree, never the top's
        // transient calls tree, before publishing any accounting cleanup.
        if let Some(result) =
            execution::check(&v, &info.config, expected, tx, block, height, limits)?
        {
            return Ok(result);
        }
        let payments = s.query(|t| t.payments(expected.payment.id.0))?;
        let pruned = payments.len() == 1;
        if pruned && s.query(|t| t.reward(expected.payment.id.0))?.is_none() {
            return Ok(Confirmation::MissingReward);
        }
        let done = Completion {
            record: fingerprint,
            sequence: expected.sequence,
            payment: expected.payment.id,
            hash,
            block,
            height,
            chain: v.status(),
            pruned,
        };
        info.pending = None;
        info.last = Some(done.clone());
        let commands = vec![if pruned {
            Command::DeleteRecords(expected.payment.id.0)
        } else {
            Command::DeletePayment(expected.payment.id)
        }];
        let changes = vec![extra(&s, &info, limits)?];
        let checks = vec![status(&s)?];
        drop(v);
        drop(s);
        self.commit_checked(version, commands, changes, checks, |db, b| {
            write(Stage::Confirm, db, b)
        })?;
        Ok(Confirmation::Confirmed(done))
    }
}
#[cfg(test)]
mod tests;
