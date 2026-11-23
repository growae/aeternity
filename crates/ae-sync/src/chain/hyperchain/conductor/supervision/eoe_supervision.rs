use super::*;

#[derive(Debug)]
pub struct EoeExit {
    pub child: owner::recovery::Retired,
    pub delivery: Option<delivery::RetiredEoeCall>,
}
#[derive(Debug)]
pub struct EoeSettlement {
    pub generation: u64,
    pub delivery: Option<delivery::RetiredEoeCall>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Supervisor<O> {
    pub(in super::super) fn poll_eoe(
        &mut self,
        owner: &mut owner::Owner,
        now: Instant,
        settle: impl FnOnce(&eoe::Client, &mut EoeSettlement) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.status.eoe_restarting = owner.replacement_pending() || owner.recovering();
        if !self.status.eoe_restarting || self.notices.len() >= self.limit {
            return Ok(());
        }
        if !owner.recovering() {
            if !owner.runtime_finished() {
                return Ok(());
            }
            owner.begin_recovery().map_err(Error::Owner)?;
            self.retiring_eoe = Some(EoeSettlement {
                generation: self.status.eoe_generation,
                delivery: None,
            });
            settle(
                &owner.runtime_client(),
                self.retiring_eoe.as_mut().expect("joined EOE"),
            )?;
        }
        if !owner.retirement_reported() {
            let Some(child) = owner.take_retirement() else {
                return Ok(());
            };
            let settlement = self.retiring_eoe.take().expect("joined EOE");
            self.notices.push_back(Notice::EoeRetired {
                generation: settlement.generation,
                child: Box::new(EoeExit {
                    child,
                    delivery: settlement.delivery,
                }),
            });
        }
        if self.notices.len() >= self.limit {
            return Ok(());
        }
        let origin = *self.origin.get_or_insert(now);
        self.record(now.saturating_duration_since(origin).as_secs())?;
        let replacement =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.restart_runtime()))
                .unwrap_or(Err(owner::Error::WorkerPanicked));
        match replacement {
            Ok(()) => {
                self.status.eoe_generation = self
                    .status
                    .eoe_generation
                    .checked_add(1)
                    .ok_or(Error::Configuration)?;
                self.status.eoe_restarting = false;
                self.notices.push_back(Notice::EoeRestarted {
                    generation: self.status.eoe_generation,
                });
            }
            Err(error) => self.notices.push_back(Notice::EoeStartFailed {
                attempt: self.status.attempts,
                error,
            }),
        }
        Ok(())
    }
}
