use super::*;
#[cfg(test)]
mod tests;

impl<O: MapOrder + Clone + Send + Sync + 'static> Supervisor<O> {
    pub(in super::super) fn poll_keys(
        &mut self,
        owner: &mut owner::Owner,
        now: Instant,
    ) -> Result<(), Error> {
        self.status.keys_restarting = owner.keys_replacement_pending();
        if !self.status.keys_restarting || self.notices.len() >= self.limit {
            return Ok(());
        }
        if !owner.keys_retired() {
            if !owner.keys_finished() {
                return Ok(());
            }
            let (info, previous_failure) = owner.retire_keys().map_err(Error::Owner)?;
            self.notices.push_back(Notice::KeysRetired {
                generation: self.status.key_generation,
                info,
                previous_failure,
            });
        }
        if self.notices.len() >= self.limit {
            return Ok(());
        }
        let origin = *self.origin.get_or_insert(now);
        self.record(now.saturating_duration_since(origin).as_secs())?;
        let generation = self
            .status
            .key_generation
            .checked_add(1)
            .ok_or(Error::Configuration)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            owner.restart_keys(generation)
        }))
        .unwrap_or(Err(owner::Error::WorkerPanicked));
        match result {
            Ok(()) => {
                self.status.key_generation = generation;
                self.status.keys_restarting = false;
                self.notices.push_back(Notice::KeysRestarted { generation });
            }
            Err(error) => self.notices.push_back(Notice::KeysStartFailed {
                attempt: self.status.attempts,
                error,
            }),
        }
        Ok(())
    }
}
