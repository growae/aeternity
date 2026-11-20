// aec_resilience lifecycle and aec_chain_state fork-resistance policy,
// aeternity fa2a6d7. Copyright (c) Aeternity developers. ISC: LICENSE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Config {
    // Zero disables resistance. This is local sync policy, not consensus.
    pub depth: u64,
    pub from_start: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Start,
    Synced,
    ConfigChanged,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct State {
    cached_depth: u64,
    // None differs from a recorded disabled value during worker restart.
    active: Option<u64>,
    synced: bool,
}
impl State {
    pub fn configured_depth(self) -> Option<u64> {
        (self.cached_depth > 0).then_some(self.cached_depth)
    }
    pub fn active_depth(self) -> Option<u64> {
        self.active.filter(|d| *d > 0)
    }
    pub fn recorded_active(self) -> Option<u64> {
        self.active
    }
    pub fn synchronized(self) -> bool {
        self.synced
    }
    // The optional effect recomputes the durable finalized height from the
    // selected top, including a microblock top. Publish state only after that
    // effect succeeds. Start also models a worker restart with retained state.
    pub fn transition(mut self, event: Event, config: Config) -> (Self, Option<u64>) {
        let enable = match event {
            Event::Start => {
                self.cached_depth = config.depth;
                self.active.is_none() && config.from_start
            }
            Event::Synced => {
                self.synced = true;
                true
            }
            Event::ConfigChanged => self.synced,
        };
        if enable {
            self.cached_depth = config.depth;
            self.active = Some(config.depth);
            (self, self.active_depth())
        } else {
            (self, None)
        }
    }
    pub fn allows_sync(self, height: u64, top: u64, finalized: Option<u64>) -> bool {
        if let Some(depth) = self.active_depth() {
            height >= top.saturating_sub(depth)
        } else if self.configured_depth().is_some() {
            finalized.is_none_or(|h| height > h)
        } else {
            true
        }
    }
    // Only a changed selected key top updates this value during insertion.
    pub fn key_top_finalized(self, height: u64) -> Option<u64> {
        self.active_depth().and_then(|d| finalized_at(height, d))
    }
}
// A nonpositive result leaves a previously stored height untouched. The
// original can lower a positive finalized height after a config change/reorg.
pub fn finalized_at(top: u64, depth: u64) -> Option<u64> {
    top.checked_sub(depth)?.checked_sub(1).filter(|h| *h > 0)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub generation: u64,
    pub state: State,
}
