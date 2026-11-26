use super::*;

// Volatile production state is separate from stable consensus/worker owners
// and their accepted receipts. This is a logical session boundary; automatic
// conductor/generator process replacement is composed separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub generation: u64,
    pub producing: bool,
    pub stopping: bool,
    pub stops: u64,
    pub block_producer: bool,
    pub attempts: u64,
    pub completed: u64,
    pub rejected: u64,
    pub retries: u64,
    pub deadline: Option<Instant>,
}
#[derive(Clone, Copy)]
pub(super) struct Config {
    pub producing: bool,
    pub block_producer: bool,
}
pub(super) struct Live<O> {
    pub status: Status,
    pub attempt: Option<production::Attempt<O>>,
    pub restart: bool,
}
impl<O> Live<O> {
    pub fn new(config: Config, generation: u64) -> Self {
        Self {
            status: Status {
                generation,
                producing: config.producing,
                stopping: false,
                stops: 0,
                block_producer: config.block_producer,
                attempts: 0,
                completed: 0,
                rejected: 0,
                retries: 0,
                deadline: None,
            },
            attempt: None,
            restart: config.producing,
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Conductor<O> {
    /// Retire one exact logical production session. Stable services keep their
    /// generations and restart histories. Accepted work is polled to settlement
    /// by step(), and retained outputs must be consumed before replacement.
    /// This does not restart a process or consume the native group budget.
    pub fn restart_production_session(&mut self, generation: u64) -> Result<(), Error> {
        if generation != self.session.status.generation {
            return Err(Error::StaleSession {
                requested: generation,
                current: self.session.status.generation,
            });
        }
        if let Some(error) = &self.status.failure {
            return Err(error.clone());
        }
        if self.session_retirement.is_some() {
            return Err(Error::Backpressured);
        }
        generation
            .checked_add(1)
            .ok_or(Error::SessionGenerationLimit)?;
        let previous = self.session.status.clone();
        self.stop_production()?;
        self.session_retirement = Some(previous);
        Ok(())
    }
    pub(super) fn install_session(&mut self) -> Result<(), Error> {
        assert!(self.session.attempt.is_none() && self.retiring.is_none());
        assert!(self.pending.is_none());
        self.services
            .importer
            .database()
            .publish_selected_height()
            .map_err(Error::Repository)?;
        let retired = self.session_retirement.take().expect("retired session");
        let generation = retired
            .generation
            .checked_add(1)
            .expect("checked generation");
        // Native init(Options) restores configured autostart. A previous
        // interactive start/stop command must not change those options.
        self.session = Live::new(self.session_config, generation);
        self.production_tick = None;
        self.pending = Some(Output::SessionReplaced {
            retired,
            generation,
        });
        Ok(())
    }
}
