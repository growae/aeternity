// Candidate construction shares admission's transient consensus cache. Its
// trees retain one bounded, immutable database snapshot after this lock ends.
use super::*;
use crate::snapshots;
use repo::hyperchain::{candidate as build, inputs};
pub type Store = ae_trie::Overlay<snapshots::Reader>;

#[derive(Clone, Copy)]
pub enum Input {
    Micro(build::Input),
    Key(build::KeyInput),
}
pub struct Request {
    pub binding: producer::Binding,
    pub input: Input,
    pub parent: Arc<ae_hyperchain::parent_cache::Cache>,
    pub pins: Arc<inputs::Pins>,
    pub limits: inputs::Limits,
}
pub enum Candidate<O> {
    Micro(build::Prepared<snapshots::Reader, O>),
    Key(build::PreparedKey<snapshots::Reader, O>),
}
pub struct Prepared<O> {
    pub outcome: Result<Candidate<O>, Failure>,
    pub effects: inputs::Effects,
    pub metadata: snapshots::Metadata,
}
#[derive(Debug)]
pub struct Failure {
    pub operational: bool,
    pub reason: String,
}
#[derive(Debug)]
pub enum Error<G> {
    Repository(repo::Error),
    Snapshot(snapshots::Error),
    Execution(Failure),
    Guard(G),
    RecoveryRequired,
}
fn failure(operational: bool, reason: impl std::fmt::Debug) -> Failure {
    let mut reason = format!("{reason:?}");
    if reason.len() > 4096 {
        let end = (0..=4096)
            .rev()
            .find(|n| reason.is_char_boundary(*n))
            .unwrap();
        reason.truncate(end);
    }
    Failure {
        operational,
        reason,
    }
}
impl<O: MapOrder + Clone, H: Clone> Owner<O, H> {
    // Parent/pin inputs have already been captured by their services. Only
    // bounded local snapshot reads occur under the cache lock, never RPC or
    // service callbacks. A later selected top does not replace input.parent.
    pub fn prepare_candidate<G>(
        &self,
        snapshots: &snapshots::Owner,
        request: Request,
        check: impl FnMut() -> Result<(), G>,
    ) -> Result<Prepared<O>, Error<G>> {
        self.prepare_candidate_with_inputs(snapshots, request, |_, _, _| Ok(false), check)
    }
    // A worker may resolve typed input demands while retaining this exact
    // child view and base cache. No database writer is held during acquisition.
    pub fn prepare_candidate_with_inputs<G>(
        &self,
        snapshots: &snapshots::Owner,
        request: Request,
        mut acquire: impl FnMut(
            &mut inputs::Captured<snapshots::Reader>,
            &inputs::InputError,
            &mut dyn FnMut() -> Result<(), G>,
        ) -> Result<bool, G>,
        mut check: impl FnMut() -> Result<(), G>,
    ) -> Result<Prepared<O>, Error<G>> {
        if !snapshots.uses_database(&self.database) || !request.binding.matches(&self.config) {
            return Err(Error::Repository(repo::Error::Identity));
        }
        let mut live = self.live.lock().map_err(|_| Error::RecoveryRequired)?;
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(Error::RecoveryRequired);
        }
        check().map_err(Error::Guard)?;
        let captured = snapshots.view().map_err(Error::Snapshot)?;
        let view = &captured.value;
        if live.revision != view.status().revision {
            let checkpoint = view.hyperchain_checkpoint().map_err(Error::Repository)?;
            live.revision = checkpoint.revision;
            live.state = checkpoint.state;
        }
        let mut state = live.state.clone();
        let mut host = inputs::Captured::new(
            view.clone(),
            request.parent,
            request.pins,
            request.binding.parent_start,
            request.binding.parent_length,
            inputs::Effects::default(),
            request.limits,
        )
        .map_err(|e| Error::Execution(failure(true, e)))?;
        let outcome = loop {
            let outcome = match request.input {
                Input::Micro(input) => {
                    build::micro(view, &self.config, input, &mut state, &mut host)
                        .map(Candidate::Micro)
                }
                Input::Key(input) => {
                    build::key(view, &self.config, input, &mut state, &mut host).map(Candidate::Key)
                }
            };
            check().map_err(Error::Guard)?;
            if let Err(e) = &outcome
                && e.is_operational::<inputs::Captured<snapshots::Reader>>()
                && let Some(error) = e.host_error()
                && acquire(&mut host, error, &mut check).map_err(Error::Guard)?
            {
                check().map_err(Error::Guard)?;
                state = live.state.clone();
                continue;
            }
            break outcome.map_err(|e| {
                failure(e.is_operational::<inputs::Captured<snapshots::Reader>>(), e)
            });
        };
        check().map_err(Error::Guard)?;
        if self.database.events().info().failure.is_some() {
            return Err(Error::RecoveryRequired);
        }
        if let Err(e) = outcome.as_ref()
            && e.operational
        {
            return Err(Error::Execution(
                outcome.err().expect("failed construction"),
            ));
        }
        // Modelled native failure retains its cache prefix and staged effects.
        // An operational/guard failure above publishes neither. Accepted block
        // effects in live.host belong exclusively to admission's consumer.
        live.state = state;
        Ok(Prepared {
            outcome,
            effects: host.into_effects(),
            metadata: captured.metadata().clone(),
        })
    }
}
