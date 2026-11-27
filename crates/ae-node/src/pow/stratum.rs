use super::*;
use crate::stratum::configured as service;
use std::net::SocketAddr;

fn restart_allowed(last: Option<u64>, second: u64) -> bool {
    last.is_none_or(|last| second.saturating_sub(last) > 5)
}

#[derive(Debug, Clone)]
pub struct StratumStatus {
    pub producer: group::GenerationId,
    pub address: SocketAddr,
    pub healthy: bool,
}
#[derive(Debug)]
pub struct StratumExit {
    pub generation: u64,
    pub producer: group::GenerationId,
    pub start_error: Option<crate::stratum::Error>,
    pub report: service::Report,
}
pub(super) struct Generation {
    pub producer: group::GenerationId,
    pub worker: service::Live,
}
impl Generation {
    pub fn status(&self) -> StratumStatus {
        let h = self.worker.handle();
        StratumStatus {
            producer: self.producer,
            address: h.transport.address(),
            healthy: h.failure().is_none(),
        }
    }
}
impl Owner {
    pub(super) fn collect_stratum(&self) {
        let Some(s) = &self.stratum else {
            return;
        };
        let mut out = lock(&self.shared.outputs);
        if out.len() < self.config.outputs
            && let Some(value) = s.worker.handle().transport.next_output()
        {
            out.push_back(Output::Stratum(value));
        }
    }
    pub(super) async fn stop_stratum(&mut self) -> Result<(), Error> {
        lock(&self.shared.controls).stratum = None;
        if let Some(s) = self.stratum.take() {
            let id = s.producer;
            let report = tokio::task::spawn_blocking(move || s.worker.shutdown())
                .await
                .map_err(|_| Error::Stratum(service::Failure::Task))?;
            debug_assert!(self.pending_stratum.is_none());
            self.pending_stratum = Some(StratumExit {
                generation: self.status.stratum_starts,
                producer: id,
                start_error: None,
                report,
            });
        }
        Ok(())
    }
    pub(super) fn release_stratum(&mut self) -> Result<bool, Error> {
        let Some(pending) = &self.pending_stratum else {
            return Ok(true);
        };
        if !pending.report.settled() {
            return Err(Error::Stratum(service::Failure::Unsettled));
        }
        let mut out = lock(&self.shared.outputs);
        if out.len() >= self.config.outputs {
            return Ok(false);
        }
        out.push_back(Output::StratumRetired(Box::new(
            self.pending_stratum.take().unwrap(),
        )));
        Ok(true)
    }
    pub(super) async fn refresh_stratum(&mut self) -> Result<bool, Error> {
        let Some(config) = self.config.stratum.clone() else {
            return Ok(true);
        };
        let Some(Live::Cuckoo(group)) = &self.production else {
            return Err(Error::Configuration);
        };
        let current = group.client().current().filter(|g| {
            let s = g.producer.status();
            s.running && s.accepting
        });
        if !self.release_stratum()? {
            return Ok(false);
        }
        if let Some(s) = &mut self.stratum {
            if s.worker.handle().failure().is_none() {
                if let Some(current) = &current {
                    s.producer = current.id;
                }
                return Ok(true);
            }
            // aestratum_appsup is permanent one_for_all, intensity 1 / 5s.
            // OTP counts the exact period boundary in integer seconds.
            let second = self.stratum_epoch.elapsed().as_secs();
            if !restart_allowed(self.stratum_last_restart, second) {
                return Err(Error::Stratum(service::Failure::RestartLimit));
            }
            self.stratum_last_restart = Some(second);
            self.stop_stratum().await?;
            if !self.release_stratum()? {
                return Ok(false);
            }
        }
        let Some(current) = current else {
            return Ok(false);
        };
        if self.stopping() {
            return Err(Error::Closed);
        }
        let services = self.services.as_ref().ok_or(Error::NotReady)?;
        let pool = self
            .workers
            .as_ref()
            .ok_or(Error::NotReady)?
            .bindings()
            .admission;
        let (strictly_follow_top, clock) = self
            .input
            .stratum_inputs(self.status.production_starts)
            .ok_or(Error::Configuration)?;
        let inputs = service::Inputs {
            database: services.configured().chain().database().clone(),
            snapshots: services.snapshots().clone(),
            admitter: services.importer().stratum_admitter(),
            producers: self.stratum_source.clone().ok_or(Error::NotReady)?,
            pool,
            clock,
            strictly_follow_top,
        };
        let generation = self
            .status
            .stratum_starts
            .checked_add(1)
            .ok_or(Error::Configuration)?;
        self.status.stratum_starts = generation;
        let result = tokio::task::spawn_blocking(move || service::Live::start(&config, inputs))
            .await
            .map_err(|_| Error::Stratum(service::Failure::Task))?;
        match result {
            Ok(worker) => {
                lock(&self.shared.controls).stratum = Some(worker.handle());
                self.stratum = Some(Generation {
                    producer: current.id,
                    worker,
                });
                Ok(true)
            }
            Err(failure) => {
                let failure = *failure;
                let replaced = matches!(
                    &failure.error,
                    crate::stratum::Error::ProductionController(
                        ae_sync::chain::pow::producer::Error::Closed
                            | ae_sync::chain::pow::producer::Error::Keys(
                                ae_sync::keys::Error::Stopped
                            )
                    )
                );
                self.pending_stratum = Some(StratumExit {
                    generation,
                    producer: current.id,
                    start_error: Some(failure.error),
                    report: failure.report,
                });
                if replaced {
                    Ok(false)
                } else {
                    Err(Error::Stratum(service::Failure::Configuration))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stratum_restart_window_matches_original_supervisor_policy_and_otp_boundaries() {
        let native: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_stratum_supervision.json"
        ))
        .unwrap();
        for row in native["policies"].as_array().unwrap() {
            assert_eq!(row["strategy"], "one_for_all");
            assert_eq!(row["intensity"], 1);
            assert_eq!(row["period"], 5);
        }
        let mut checked = 0;
        for trace in native["budgets"].as_array().unwrap() {
            let mut last = None;
            for row in trace.as_array().unwrap() {
                let second = row["now"].as_u64().unwrap();
                let allowed = restart_allowed(last, second);
                assert_eq!(allowed, row["allowed"].as_bool().unwrap());
                checked += 1;
                if allowed {
                    last = Some(second);
                }
            }
        }
        assert_eq!(checked, 19);
    }
}
