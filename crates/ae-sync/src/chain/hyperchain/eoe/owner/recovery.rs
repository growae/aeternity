use super::*;

pub use requests::{ReceiptKind, ReceiptObservation, RetiredRequest};
pub struct Retired {
    pub runtime: votes::eoe::Retirement,
    pub requests: Vec<RetiredRequest>,
    pub producer: Option<producer::Cancelled>,
    pub previous_failure: Option<Error>,
}
impl std::fmt::Debug for Retired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retired")
            .field("runtime", &self.runtime)
            .field("requests", &self.requests)
            .field("producer_retained", &self.producer.is_some())
            .field("previous_failure", &self.previous_failure)
            .finish()
    }
}
pub(super) struct Recovery {
    pub(super) previous_failure: Option<Error>,
    reported: bool,
}
pub(crate) fn departure(error: &Error) -> bool {
    matches!(
        error,
        Error::Closed
            | Error::ReceiptLost(_)
            | Error::Votes(votes::Error::AuthorityClosed)
            | Error::Producer(
                super::super::super::delivery::Error::Closed(
                    super::super::super::delivery::Target::Eoe
                ) | super::super::super::delivery::Error::ReceiptLost(
                    super::super::super::delivery::Target::Eoe
                )
            )
    )
}
impl Owner {
    pub fn replacement_pending(&self) -> bool {
        !self.factory.hub().is_closed()
            && self
                .output
                .as_ref()
                .is_some_and(votes::eoe::Owner::replacement_pending)
            && self.failure.as_ref().is_none_or(departure)
    }
    pub fn runtime_finished(&self) -> bool {
        self.output
            .as_ref()
            .is_none_or(votes::eoe::Owner::is_finished)
    }
    pub fn recovering(&self) -> bool {
        self.recovery.is_some()
    }
    pub(crate) fn retirement_reported(&self) -> bool {
        self.recovery.as_ref().is_some_and(|r| r.reported)
    }
    pub fn runtime_client(&self) -> Client {
        self.client()
    }
    // Join first; only a replaceable process exit is eligible. Operational
    // WorkStopped and terminal epoch/source failures remain explicit faults.
    pub fn begin_recovery(&mut self) -> Result<(), Error> {
        if self.recovery.is_some() {
            return Ok(());
        }
        if !self.replacement_pending() {
            return Err(self.failure.clone().unwrap_or(Error::Configuration));
        }
        if !self.runtime_finished() {
            return Err(Error::Backpressured);
        }
        self.output
            .as_mut()
            .expect("output owner")
            .begin_retirement();
        self.recovery = Some(Recovery {
            previous_failure: self.failure.take(),
            reported: false,
        });
        Ok(())
    }
    // Independent peer traffic does not extend this retirement. All old EOE
    // outputs must be routed first, including final replies under byte pressure.
    pub fn take_retirement(&mut self) -> Option<Retired> {
        let recovery = self.recovery.as_mut()?;
        if recovery.reported || self.pending.is_some() {
            return None;
        }
        if self
            .production
            .as_ref()
            .is_some_and(producer::Pending::in_flight)
        {
            return None;
        }
        let runtime = self.output.as_mut()?.take_retirement()?;
        let requests = self.requests.retire();
        recovery.reported = true;
        Some(Retired {
            runtime,
            requests,
            producer: self.production.take().map(producer::Pending::discard),
            previous_failure: recovery.previous_failure.take(),
        })
    }
    pub fn restart_runtime(&mut self) -> Result<(), Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if !self.recovery.as_ref().is_some_and(|r| r.reported) {
            return Err(Error::Backpressured);
        }
        let runtime = self.factory.spawn().map_err(Error::Source)?;
        self.output
            .as_mut()
            .expect("output owner")
            .replace_runtime(runtime)
            .map_err(|e| Error::Votes(e.error.clone()))?;
        self.recovery = None;
        Ok(())
    }
    pub(super) fn drain_retirement(&mut self) -> Result<Progress, Error> {
        let epoch = self.epochs.step().map_err(Error::Source)?;
        let old = self.client();
        if let Some(producer) = &mut self.production {
            producer.retire_eoe(&old).map_err(Error::Producer)?;
            match producer.settle() {
                Ok(_) => (),
                Err(e)
                    if !producer.in_flight()
                        && (producer::retryable(&e)
                            || super::super::super::delivery::eoe_departure(&e)) => {}
                Err(e) => return Err(Error::Producer(e)),
            }
        }
        if self.pending.is_none() {
            match self
                .output
                .as_mut()
                .expect("output owner")
                .route()
                .map_err(Error::Votes)?
            {
                votes::eoe::Routed::Idle => (),
                votes::eoe::Routed::Published => return Ok(Progress::Advanced),
                votes::eoe::Routed::Backpressured => return Ok(Progress::Backpressured),
                votes::eoe::Routed::Output(output) => self.pending = Some(output),
            }
        }
        if self.pending.is_some() {
            return match self.requests.route(&mut self.pending)? {
                requests::Routed::Completed => Ok(Progress::Advanced),
                requests::Routed::Backpressured => Ok(Progress::Backpressured),
                requests::Routed::Other => {
                    Ok(Progress::Output(self.pending.take().expect("output")))
                }
            };
        }
        Ok(if epoch == super::super::Progress::Idle {
            Progress::Idle
        } else {
            Progress::Advanced
        })
    }
}
