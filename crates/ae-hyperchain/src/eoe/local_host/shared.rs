use super::*;
use crate::eoe::pool;

#[derive(Clone)]
pub(super) struct Staged {
    client: pool::Client,
    votes: VotePool,
    limits: hc_votes::Limits,
    pub(super) admissions: Vec<pool::Admission>,
    pub(super) bytes: usize,
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> LocalHost<A, C> {
    pub fn with_independent_pool(mut self, hub: pool::Hub) -> Result<Self, Error> {
        if hub.network() != self.network()
            || !self.pool.is_empty()
            || self.pool.epoch() != &Unsigned::from(0)
            || !self.outbox.is_empty()
            || self.independent.is_some()
        {
            return Err(Error::IndependentPool(pool::Error::Configuration));
        }
        self.independent = Some(hub);
        Ok(self)
    }
    pub fn independent_pool(&self) -> Option<&pool::Hub> {
        self.independent.as_ref()
    }
    pub fn inspect_pool<T>(&self, inspect: impl FnOnce(&VotePool) -> T) -> Result<T, Error> {
        match &self.independent {
            Some(hub) => hub
                .client()
                .ok_or(Error::IndependentPool(pool::Error::Closed))?
                .inspect(inspect)
                .map_err(Error::IndependentPool),
            None => Ok(inspect(&self.pool)),
        }
    }
    pub(super) fn stage_vote(&mut self, admission: pool::Admission) -> Result<PushOutcome, Error> {
        let hub = self.independent.as_ref().expect("independent pool");
        if self.staged.is_none() {
            let client = hub
                .client()
                .ok_or(Error::IndependentPool(pool::Error::Closed))?;
            let (votes, limits) = client.snapshot().map_err(Error::IndependentPool)?;
            self.staged = Some(Staged {
                client,
                votes,
                limits,
                admissions: vec![],
                bytes: 0,
            });
        }
        let staged = self.staged.as_mut().expect("staged admissions");
        let bytes = staged
            .bytes
            .checked_add(admission.signed.encode().len())
            .ok_or(Error::ResourceLimit)?;
        if staged.admissions.len().saturating_add(self.outbox.len())
            >= self.config.limits.maximum_outbox_entries
            || bytes.saturating_add(self.outbox_bytes) > self.config.limits.maximum_outbox_bytes
        {
            return Err(Error::ResourceLimit);
        }
        let outcome = staged
            .votes
            .push(
                &admission.signed,
                admission.event,
                &Admission {
                    network: &self.config.network,
                    protocol: admission.protocol,
                    consensus: admission.consensus,
                    limits: staged.limits,
                },
                |_| false,
            )
            .map_err(Error::Pool)?;
        staged.admissions.push(admission);
        staged.bytes = bytes;
        Ok(outcome)
    }
    // No lock spans signing, selected-state access, entropy retrieval or FSM
    // execution. Rebase the accepted batch on the live pool under one gate.
    // Losing the captured pool during a callback is a local commit failure;
    // leave the callback available for explicit retry, never silently rebind.
    pub(in crate::eoe) fn commit_pool(
        &mut self,
        delivery: Option<(&pool::Delivery, [u64; 2])>,
    ) -> Result<Vec<PushOutcome>, Error> {
        let Some(hub) = &self.independent else {
            return Ok(vec![]);
        };
        let (client, admissions) = match &self.staged {
            Some(staged) => (Some(&staged.client), staged.admissions.clone()),
            None => (None, vec![]),
        };
        let outcomes = hub
            .commit(client, admissions, delivery)
            .map_err(Error::PoolCommit)?;
        self.staged = None;
        Ok(outcomes)
    }
}
