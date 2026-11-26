use super::*;
use crate::pool::{Signed, Submission};
use ae_mempool::pool as policy;
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, Default)]
pub struct SelectOptions {
    pub deadline: Option<Instant>,
    pub ceiling: Option<Instant>,
    pub packed: bool,
}
// The same origin is used by worker deadline checks and candidate packing.
// Conversion is explicit and checked; a different worker cannot reinterpret
// an opaque timestamp supplied without its originating clock.
#[derive(Debug, Clone, Copy)]
pub struct Clock(Instant);
impl Clock {
    pub fn now(self) -> i64 {
        i64::try_from(self.0.elapsed().as_millis()).unwrap_or(i64::MAX)
    }
    pub fn instant(self, milliseconds: i64) -> Result<Instant, Error> {
        let duration = Duration::from_millis(milliseconds.unsigned_abs());
        if milliseconds < 0 {
            self.0.checked_sub(duration)
        } else {
            self.0.checked_add(duration)
        }
        .ok_or(Error::RequestTooLarge)
    }
}
#[derive(Clone)]
pub struct Client {
    pub(super) shared: Arc<Shared>,
}
fn sum(parts: impl IntoIterator<Item = usize>) -> Result<usize, Error> {
    parts.into_iter().try_fold(0usize, |a, b| {
        a.checked_add(b).ok_or(Error::RequestTooLarge)
    })
}
fn policy_bytes(
    policy: &policy::Config,
    failures: &policy::FailureSettings,
    limit: usize,
) -> Result<usize, Error> {
    let mut bytes = sum([
        BASE_WEIGHT,
        policy.nonce_baseline.as_bytes().len(),
        policy.nonce_offset.as_bytes().len(),
        policy.maximum_auth_gas.as_bytes().len(),
        policy.minimum_miner_gas_price.as_bytes().len(),
    ])?;
    let mut charge = |sizes: [usize; 2]| -> Result<(), Error> {
        bytes = sum([bytes, sizes[0], sizes[1]])?;
        if bytes > limit {
            Err(Error::RequestTooLarge)
        } else {
            Ok(())
        }
    };
    for reason in failures.common.keys() {
        charge([reason.capacity(), 64])?;
    }
    for (kind, reasons) in &failures.transactions {
        charge([kind.capacity(), 64])?;
        for reason in reasons.keys() {
            charge([reason.capacity(), 64])?;
        }
    }
    Ok(bytes)
}
impl Client {
    pub fn same_worker(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    pub fn origins_interval(&self) -> Option<Duration> {
        self.shared.settings.origins_interval
    }
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.shared.database.ptr_eq(&Arc::downgrade(db))
    }
    pub fn uses_source(&self, source_id: u64) -> bool {
        self.shared
            .database
            .upgrade()
            .is_some_and(|db| db.source_id == source_id)
    }
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub fn instrumentation(&self) -> &ae_chain::instrumentation::Publisher {
        &self.shared.instrumentation
    }
    pub fn wait_for_change(&self, version: u64, timeout: Duration) -> Info {
        let (c, _) = self
            .shared
            .changed
            .wait_timeout_while(self.shared.lock(), timeout, |c| {
                c.info.running && c.info.version == version
            })
            .unwrap_or_else(|e| e.into_inner());
        c.info.clone()
    }
    // Only the bounded typed operations below can create work. Callers cannot
    // enqueue arbitrary closures or execute code while mailbox locks are held.
    pub(super) fn submit<T, F>(&self, bytes: usize, action: F) -> Result<Ticket<T>, Error>
    where
        T: Send + 'static,
        F: FnOnce(&mut Service) -> Result<T, Failure> + Send + 'static,
    {
        if bytes > self.shared.settings.single_request_bytes {
            return Err(Error::RequestTooLarge);
        }
        let mut c = self.shared.lock();
        if !c.info.accepting {
            return Err(Error::Stopped);
        }
        let total = c
            .info
            .retained_request_bytes
            .checked_add(bytes)
            .ok_or(Error::Saturated)?;
        if c.outstanding >= self.shared.settings.requests
            || total > self.shared.settings.request_bytes
        {
            return Err(Error::Saturated);
        }
        let (reply, ticket) = reply::channel();
        c.queue.push_back(Envelope {
            work: Box::new(Job { reply, action }),
            bytes,
        });
        c.outstanding += 1;
        c.info.queued = c.queue.len();
        c.info.retained_request_bytes = total;
        c.info.submitted = c.info.submitted.saturating_add(1);
        self.shared.signal(&mut c);
        Ok(ticket)
    }
    fn tx_bytes(&self, tx: &Signed) -> Result<usize, Error> {
        let mut config = self.shared.record.transaction_record();
        config.max_record_bytes = config
            .max_record_bytes
            .min(self.shared.settings.single_request_bytes - BASE_WEIGHT);
        let bytes = ae_tx::storage::encode(tx, config)
            .map_err(|e| Error::Service(Failure::Repository(e.into())))?;
        sum([BASE_WEIGHT, bytes.len()])
    }
    pub fn push(
        &self,
        tx: Arc<Signed>,
        event: policy::Event,
        gossip_cached: bool,
    ) -> Result<Ticket<Submission>, Error> {
        if event == policy::Event::Received && gossip_cached {
            let c = self.shared.lock();
            if !c.info.accepting {
                return Err(Error::Stopped);
            }
            let (sender, ticket) = reply::channel();
            sender.send(Ok(Submission {
                outcome: policy::PushOutcome::Cached,
                chain: c.info.pool.chain,
                revision: c.info.pool.revision,
            }));
            return Ok(ticket);
        }
        let bytes = self.tx_bytes(&tx)?;
        let shared = Arc::downgrade(&self.shared);
        self.submit(bytes, move |s| {
            let submission = s.push(tx.clone(), event, gossip_cached)?;
            let shared = shared
                .upgrade()
                .expect("executing worker owns shared state");
            let mut control = shared.lock();
            control.info.pool = s.inspection();
            control.feed.publish(tx, &submission, bytes);
            shared.signal(&mut control);
            Ok(submission)
        })
    }
    pub fn force_push(&self, tx: Arc<Signed>) -> Result<Ticket<Submission>, Error> {
        // The native forced path counts the call before entering the server,
        // even if queue admission or the server subsequently fails.
        self.shared
            .instrumentation
            .counter(ae_chain::instrumentation::Counter::PoolPush, 1);
        let bytes = self.tx_bytes(&tx)?;
        let shared = Arc::downgrade(&self.shared);
        self.submit(bytes, move |s| {
            let submission = s.force_push_counted(tx.clone())?;
            let shared = shared
                .upgrade()
                .expect("executing worker owns shared state");
            let mut control = shared.lock();
            control.info.pool = s.inspection();
            control.feed.publish(tx, &submission, bytes);
            shared.signal(&mut control);
            Ok(submission)
        })
    }
    pub fn refresh(&self) -> Result<Ticket<Inspection>, Error> {
        self.submit(BASE_WEIGHT, |s| s.refresh())
    }
    // Read the owned active/visited tables after earlier accepted work. Unlike
    // refresh, this neither reconciles chain changes nor commits pool writes.
    pub fn inspection(&self) -> Result<Ticket<Inspection>, Error> {
        self.submit(BASE_WEIGHT, |s| s.read_inspection())
    }
    pub fn peek(
        &self,
        max: usize,
        account: Option<AccountPublicKey>,
        nonce: Option<Unsigned>,
    ) -> Result<Ticket<Vec<Arc<Signed>>>, Error> {
        if max > self.shared.settings.max_peek {
            return Err(Error::RequestTooLarge);
        }
        self.submit(
            sum([
                BASE_WEIGHT,
                nonce.as_ref().map_or(0, |n| n.as_bytes().len()),
            ])?,
            move |s| s.peek(max, account, nonce.as_ref()),
        )
    }
    pub fn inspect(&self, hash: Hash256) -> Result<Ticket<policy::Inspection>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.inspect(hash))
    }
    pub fn minimum_miner_gas_price(&self) -> Result<Ticket<Unsigned>, Error> {
        self.submit(BASE_WEIGHT, |s| Ok(s.minimum_miner_gas_price()))
    }
    pub fn next_nonce(&self, account: AccountPublicKey) -> Result<Ticket<Option<Unsigned>>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.next_nonce(account))
    }
    pub fn max_nonce(&self, account: AccountPublicKey) -> Result<Ticket<Option<Unsigned>>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.max_nonce(account))
    }
    pub fn delete(&self, hash: Hash256) -> Result<Ticket<()>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.delete(hash))
    }
    pub fn clock(&self) -> Clock {
        Clock(self.shared.origin)
    }
    pub fn candidate(
        &self,
        gas: u64,
        ignore: BTreeSet<Hash256>,
        options: SelectOptions,
    ) -> Result<Ticket<policy::Candidate>, Error> {
        self.queue_candidate(None, gas, ignore, options)
    }
    pub fn candidate_at(
        &self,
        view: ae_chain::repository::View<crate::snapshots::Reader>,
        block: Hash256,
        gas: u64,
        ignore: BTreeSet<Hash256>,
        options: SelectOptions,
    ) -> Result<Ticket<policy::Candidate>, Error> {
        self.queue_candidate(Some((view, block)), gas, ignore, options)
    }
    fn queue_candidate(
        &self,
        parent: Option<(
            ae_chain::repository::View<crate::snapshots::Reader>,
            Hash256,
        )>,
        gas: u64,
        ignore: BTreeSet<Hash256>,
        options: SelectOptions,
    ) -> Result<Ticket<policy::Candidate>, Error> {
        let bytes = ignore
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(BASE_WEIGHT))
            .and_then(|n| {
                n.checked_add(parent.as_ref().map_or(0, |(v, _)| {
                    std::mem::size_of_val(v) + v.identity().network.len()
                }))
            })
            .ok_or(Error::RequestTooLarge)?;
        let origin = self.shared.origin;
        let stamp = |time: Instant| -> Result<i64, Error> {
            let (negative, duration) = if time >= origin {
                (false, time.duration_since(origin))
            } else {
                (true, origin.duration_since(time))
            };
            let ms = i64::try_from(duration.as_millis()).map_err(|_| Error::RequestTooLarge)?;
            Ok(if negative { -ms } else { ms })
        };
        let options = policy::SelectOptions {
            deadline: options.deadline.map(stamp).transpose()?,
            ceiling: options.ceiling.map(stamp).transpose()?,
            packed: options.packed,
        };
        self.submit(bytes, move |s| {
            let now = || i64::try_from(origin.elapsed().as_millis()).unwrap_or(i64::MAX);
            match parent {
                Some((view, block)) => s.candidate_at(&view, block, gas, &ignore, options, now),
                None => s.candidate(gas, &ignore, options, now),
            }
        })
    }
    pub fn record_failures(
        &self,
        failures: Vec<crate::pool::FailedTransaction>,
    ) -> Result<Ticket<Vec<bool>>, Error> {
        let storage = failures
            .capacity()
            .checked_mul(std::mem::size_of::<crate::pool::FailedTransaction>())
            .ok_or(Error::RequestTooLarge)?;
        let mut bytes = sum([BASE_WEIGHT, storage])?;
        for f in &failures {
            bytes = sum([bytes, f.reason.capacity()])?;
            if bytes > self.shared.settings.single_request_bytes {
                return Err(Error::RequestTooLarge);
            }
        }
        self.submit(bytes, move |s| s.record_failures(&failures))
    }
    pub fn record_failure(
        &self,
        hash: Hash256,
        reason: String,
        tuple: bool,
    ) -> Result<Ticket<bool>, Error> {
        self.submit(sum([BASE_WEIGHT, reason.capacity()])?, move |s| {
            s.record_failure(hash, &reason, tuple)
        })
    }
    pub fn ttl(&self, hash: Hash256) -> Result<Ticket<i128>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.ttl(hash))
    }
    pub fn shorten_ttl(&self, hash: Hash256, ttl: i128) -> Result<Ticket<i128>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.shorten_ttl(hash, ttl))
    }
    pub fn adjust_ttl(&self, difference: i128) -> Result<Ticket<()>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.adjust_ttl(difference))
    }
    pub fn note_origin(
        &self,
        origin: Option<AccountPublicKey>,
        nonce: Unsigned,
    ) -> Result<Ticket<()>, Error> {
        self.submit(sum([BASE_WEIGHT, nonce.as_bytes().len()])?, move |s| {
            s.note_origin(origin, nonce)
        })
    }
    pub fn collect_at(
        &self,
        height: u64,
        synchronized: bool,
        limit: usize,
    ) -> Result<Ticket<Vec<Hash256>>, Error> {
        self.submit(BASE_WEIGHT, move |s| {
            s.collect_at(height, synchronized, limit)
        })
    }
    pub fn collect(
        &self,
        origins: bool,
        expiry_limit: usize,
    ) -> Result<Ticket<Vec<Hash256>>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.collect(origins, expiry_limit))
    }
    pub fn sync_target(&self, height: u64) -> Result<Ticket<()>, Error> {
        self.submit(BASE_WEIGHT, move |s| s.sync_target(height))
    }
    pub fn reconfigure(
        &self,
        policy: policy::Config,
        failures: policy::FailureSettings,
    ) -> Result<Ticket<()>, Error> {
        self.submit(
            policy_bytes(
                &policy,
                &failures,
                self.shared.settings.single_request_bytes,
            )?,
            move |s| s.reconfigure(policy, failures),
        )
    }
    pub fn recover(&self, config: ServiceConfig) -> Result<Ticket<()>, Error> {
        self.submit(
            policy_bytes(
                &config.policy,
                &config.failures,
                self.shared.settings.single_request_bytes,
            )?,
            move |s| s.recover(config),
        )
    }
}
