// aec_sync lifecycle and progress, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: LICENSE.
use super::*;
use ae_chain::resilience;
mod owner;
pub(super) use owner::Owner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Task {
    pub id: u64,
    pub target: u64,
    pub suspect: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    pub syncing: bool,
    pub percent: f64,
    pub target: u64,
    pub task: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    ZeroTarget,
}
// Caller supplies actual selected height; announced target is not admission.
// Task order resolves equal-height ties exactly as the native fold does.
pub fn progress(tasks: &[Task], top_target: u64, selected: u64) -> Result<Progress, Error> {
    let mut syncing = false;
    let mut target = 0;
    let mut task = None;
    for item in tasks.iter().filter(|t| !t.suspect) {
        syncing = true;
        if item.target > target {
            target = item.target;
            task = Some(item.id);
        }
    }
    if !syncing {
        return Ok(Progress {
            syncing: false,
            percent: 100.0,
            target: top_target,
            task: None,
        });
    }
    if target == 0 {
        return Err(Error::ZeroTarget);
    }
    let percent = (10_000_000.0 * selected as f64 / target as f64).round() / 100_000.0;
    Ok(Progress {
        syncing,
        percent: if percent > 100.0 { 99.9 } else { percent },
        target,
        task,
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneReason {
    Ping,
    Exhausted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerDone {
    pub peer: PeerPublicKey,
    pub generation: net::Generation,
    pub reason: DoneReason,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub tasks: Vec<Task>,
    pub top_target: u64,
    pub selected: Option<Point>,
    pub progress: Result<Progress, Error>,
    pub syncing_transitions: u64,
    pub last_generation_in_sync: bool,
    pub suppressed_generations: u64,
    // A native peer-completion notification is an operational policy signal,
    // not proof of target selection, final generation success or mainnet sync.
    pub peer_completions: u64,
    pub last_peer_done: Option<PeerDone>,
    pub policy: Option<resilience::Snapshot>,
    pub policy_config: Option<resilience::Config>,
    pub finalized_height: Option<u64>,
    pub pending: bool,
}
impl Default for Status {
    fn default() -> Self {
        Self {
            tasks: vec![],
            top_target: 0,
            selected: None,
            progress: progress(&[], 0, 0),
            syncing_transitions: 0,
            last_generation_in_sync: false,
            suppressed_generations: 0,
            peer_completions: 0,
            last_peer_done: None,
            policy: None,
            policy_config: None,
            finalized_height: None,
            pending: true,
        }
    }
}

#[derive(Default)]
pub(super) struct Signal {
    active: bool,
    pub changes: u64,
}
impl Signal {
    pub fn update(&mut self, active: bool) {
        if self.active != active {
            self.changes = self.changes.saturating_add(1);
        }
        self.active = active;
    }
}
impl Status {
    pub(super) fn observe(&mut self, tasks: Vec<Task>, top_target: u64) {
        let previous = self.tasks.iter().any(|t| !t.suspect);
        let syncing = tasks.iter().any(|t| !t.suspect);
        let mut signal = Signal {
            active: previous,
            changes: self.syncing_transitions,
        };
        signal.update(syncing);
        self.syncing_transitions = signal.changes;
        self.tasks = tasks;
        self.top_target = top_target;
        self.progress = progress(
            &self.tasks,
            top_target,
            self.selected.map_or(0, |p| p.height),
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_is_syncing_effects_occur_only_at_state_changes() {
        let corpus: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/erlang_sync_lifecycle.json"
        )))
        .unwrap();
        for row in corpus["transitions"].as_array().unwrap() {
            let mut status = Status::default();
            if row["previous"] == true {
                status.tasks.push(Task {
                    id: 99,
                    target: 1,
                    suspect: false,
                });
            }
            let tasks = row["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| Task {
                    id: t["id"].as_u64().unwrap(),
                    target: t["target"].as_u64().unwrap(),
                    suspect: t["suspect"].as_bool().unwrap(),
                })
                .collect();
            status.observe(tasks, 0);
            assert_eq!(
                status.tasks.iter().any(|t| !t.suspect),
                row["current"].as_bool().unwrap()
            );
            assert_eq!(
                status.syncing_transitions,
                row["effects"].as_array().unwrap().len() as u64
            );
            status.observe(status.tasks.clone(), 0);
            assert_eq!(
                status.syncing_transitions,
                row["effects"].as_array().unwrap().len() as u64
            );
        }
    }
}
