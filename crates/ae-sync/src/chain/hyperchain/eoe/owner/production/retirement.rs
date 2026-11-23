use super::*;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct Stopped {
    pub original_parent: Hash256,
    pub parent: Hash256,
    pub micro: Option<chain::Accepted>,
    pub pending_hash: Option<Hash256>,
    pub admission: Option<Result<chain::Report, chain::Failure>>,
    pub previous_failure: Option<Error>,
    pub acknowledged_effects: u64,
    pub producer: Option<producer::Cancelled>,
    pub candidate: Option<candidate::Cancelled>,
    pub discarded_key_effects: VecDeque<inputs::Effect>,
    pub acknowledged_key_effects: u64,
    pub key_effect_error: Option<delivery::Error>,
    pub key_preparation: Option<Result<(), chain::Error>>,
}
pub struct Retiring<O> {
    attempt: Attempt<O>,
    producer: Option<producer::Pending>,
    micro: Option<candidate::Retiring>,
    report: Option<Stopped>,
    ready: bool,
    failure: Option<Error>,
    cache_retired: bool,
    eoe_retired: bool,
}
impl<O> std::fmt::Debug for Retiring<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retiring")
            .field("ready", &self.ready)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
impl Owner {
    pub fn retire_production<O: MapOrder + Clone + Send + Sync + 'static>(
        &mut self,
        mut attempt: Attempt<O>,
    ) -> Result<Retiring<O>, Box<Attempt<O>>> {
        if !attempt.client.same_runtime(&self.client())
            || !Arc::ptr_eq(&attempt._lease.0, &self.attempt)
        {
            return Err(Box::new(attempt));
        }
        let previous_failure = attempt.failure.clone();
        attempt.cancel();
        let micro = attempt.micro.take().map(candidate::Build::retire);
        attempt.completed_micro = None;
        let pending_hash = attempt.block.as_ref().and_then(|b| b.hash().ok());
        let report = Stopped {
            original_parent: attempt.original_parent,
            parent: attempt.parent,
            micro: attempt.accepted_micro.clone(),
            pending_hash,
            admission: None,
            previous_failure,
            acknowledged_effects: attempt.acknowledged,
            producer: None,
            candidate: None,
            discarded_key_effects: VecDeque::new(),
            acknowledged_key_effects: 0,
            key_effect_error: None,
            key_preparation: None,
        };
        Ok(Retiring {
            attempt,
            producer: self.production.take(),
            micro,
            report: Some(report),
            ready: false,
            failure: None,
            cache_retired: false,
            eoe_retired: false,
        })
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Retiring<O> {
    pub(crate) fn retire_eoe(&mut self, old: &super::super::Client) -> Result<(), Error> {
        if !old.is_closed() {
            return Err(Error::Binding);
        }
        if !self.attempt.client.same_runtime(old) {
            return Ok(());
        }
        if let Some(producer) = &mut self.producer {
            producer.retire_eoe(old).map_err(Error::Effects)?;
        }
        if let Some(micro) = &mut self.micro {
            micro.retire_eoe(old).map_err(Error::Build)?;
        }
        if let Some(effects) = &mut self.attempt.effects
            && let Some(error) = effects.retire_eoe(old).map_err(Error::Effects)?
        {
            self.report.as_mut().expect("report").key_effect_error = Some(error);
        }
        self.eoe_retired = true;
        Ok(())
    }
    pub(crate) fn retire_cache(
        &mut self,
        old: &ae_hyperchain::parent_cache::runtime::Client,
    ) -> Result<(), Error> {
        if !old.is_closed() {
            return Err(Error::Binding);
        }
        if !self.attempt.parent_cache.same_runtime(old) {
            return Ok(());
        }
        if let Some(producer) = &mut self.producer {
            producer.retire_cache(old).map_err(Error::Effects)?;
        }
        if let Some(effects) = &mut self.attempt.effects
            && let Some(error) = effects.retire_cache(old).map_err(Error::Effects)?
        {
            self.report.as_mut().expect("report").key_effect_error = Some(error);
        }
        self.cache_retired = true;
        Ok(())
    }
    fn record_candidate(&mut self, result: Result<owned::Prepared<O>, chain::Error>) {
        let report = self.report.as_mut().expect("report");
        report.key_preparation = Some(result.map(|mut key| {
            report.discarded_key_effects.extend(key.effects.drain());
        }));
    }
    pub(crate) fn join_candidate(&mut self) {
        if let Some(mut task) = self.attempt.key_task.take() {
            task.cancel();
            self.record_candidate(task.join());
        }
    }
    pub fn ready(&self) -> bool {
        self.ready && self.failure.is_none()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn report(&self) -> &Stopped {
        self.report.as_ref().expect("retiring report")
    }
    pub(crate) fn take_abandoned_admission(&mut self) -> Option<chain::Ticket> {
        let receipt = self.attempt.take_abandoned_admission();
        if receipt.is_some() {
            self.failure = Some(Error::Closed);
        }
        receipt
    }
    // Drive Owner::step separately to deliver cancellation replies. This only
    // polls accepted work; it never signs, applies candidates or sends effects.
    pub fn step(&mut self, owner: &Owner, importer: &Importer<O>) -> Result<bool, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance(owner, importer);
        if let Err(e) = &result {
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self, owner: &Owner, importer: &Importer<O>) -> Result<bool, Error> {
        if (!self.eoe_retired && !self.attempt.client.same_runtime(&owner.client()))
            || !self.attempt.source.uses_importer(importer)
        {
            return Err(Error::Binding);
        }
        if let Some(task) = &mut self.attempt.key_task {
            let Some(result) = task.poll() else {
                return Ok(false);
            };
            self.attempt.key_task = None;
            self.record_candidate(result);
        }
        let report = self.report.as_mut().expect("report");
        if let Some(receipt) = &mut self.attempt.receipt {
            let Some(result) = receipt.try_result() else {
                return Ok(false);
            };
            self.attempt.receipt = None;
            report.admission = Some(result.clone());
            match result {
                Ok(result) => {
                    if result.accepted.len() != 1
                        || Some(result.accepted[0].hash) != report.pending_hash
                    {
                        return Err(Error::UnexpectedReceipt);
                    }
                    if matches!(self.attempt.phase, Phase::Receipt(Kind::Micro)) {
                        report.parent = result.accepted[0].hash;
                        report.micro = Some(result.accepted[0].clone());
                    }
                }
                Err(f) if known_cancelled_admission(&f) => (),
                Err(f) => return Err(Error::Admission(f)),
            }
        }
        if let Some(p) = &mut self.producer {
            match p.settle() {
                Ok(false) => return Ok(false),
                Ok(true) => (),
                Err(e) if producer::retryable(&e) && !p.in_flight() => (),
                Err(e) if self.cache_retired && cache_departure(&e) && !p.in_flight() => (),
                Err(e) if self.eoe_retired && delivery::eoe_departure(&e) && !p.in_flight() => (),
                Err(e) => return Err(Error::Effects(e)),
            }
            report.producer = Some(self.producer.take().expect("producer").discard());
        }
        if let Some(micro) = &mut self.micro {
            if !micro.step().map_err(Error::Build)? {
                return Ok(false);
            }
            report.candidate = Some(self.micro.take().expect("micro").finish());
        }
        if let Some(effects) = &mut self.attempt.effects {
            match effects.settle() {
                Ok(false) => return Ok(false),
                Ok(true) => (),
                Err(e) if producer::retryable(&e) && !effects.in_flight() => {
                    report.key_effect_error = Some(e)
                }
                Err(e) => return Err(Error::Effects(e)),
            }
            report.acknowledged_key_effects = effects.acknowledged;
            report
                .discarded_key_effects
                .extend(std::mem::take(&mut effects.pending));
            self.attempt.effects = None;
        }
        self.ready = true;
        Ok(true)
    }
    pub fn finish(mut self) -> Result<Stopped, Box<Self>> {
        if !self.ready() {
            return Err(Box::new(self));
        }
        Ok(self.report.take().expect("report"))
    }
}
fn cache_departure(error: &delivery::Error) -> bool {
    matches!(
        error,
        delivery::Error::Closed(delivery::Target::Cache)
            | delivery::Error::ReceiptLost(delivery::Target::Cache)
            | delivery::Error::Cache(ae_hyperchain::parent_cache::runtime::Error::Closed)
    )
}
fn known_cancelled_admission(f: &chain::Failure) -> bool {
    if f.progress.is_none() {
        return f.error == chain::Error::Cancelled;
    }
    f.progress.as_ref().is_some_and(|r| r.accepted.is_empty())
        && (matches!(
            f.error,
            chain::Error::Cancelled | chain::Error::Busy | chain::Error::Validation { .. }
        ) || matches!(
            f.error,
            chain::Error::Storage(repo::Error::Conflict | repo::Error::ResourceLimit)
        ))
}
