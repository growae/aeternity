use super::*;
use crate::dry_run::{self, Selector, StateError};

struct Captured<'a, S> {
    snapshot: &'a Snapshot<S>,
}
impl<S: Read> dry_run::Source for Captured<'_, S> {
    type State = ae_state::Trees<Overlay<S>, Overlay<S>>;
    type Error = LookupError;
    fn top(&mut self) -> Result<Option<Hash256>, Self::Error> {
        Ok(self.snapshot.repository.status().tip)
    }
    fn key_header(
        &mut self,
        height: &BigInt,
    ) -> Result<Option<ae_codec::headers::KeyHeader>, Self::Error> {
        let Ok(height) = u64::try_from(height) else {
            return Ok(None);
        };
        let view = &self.snapshot.repository;
        let Some(hash) = view.canonical_key(height).map_err(backend)? else {
            return Ok(None);
        };
        let header = view.require_header(hash).map_err(backend)?;
        if !view.is_canonical(hash).map_err(backend)? {
            return Err(backend(Error::Corrupt("canonical key")));
        }
        match header {
            Header::Key(key) if key.common.height == height => Ok(Some(key)),
            _ => Err(backend(Error::Corrupt("canonical key"))),
        }
    }
    fn header(&mut self, hash: Hash256) -> Result<Option<Header>, Self::Error> {
        self.snapshot.repository.header(hash).map_err(backend)
    }
    fn state(&mut self, hash: Hash256) -> Result<Self::State, StateError<Self::Error>> {
        match self
            .snapshot
            .repository
            .trees(hash, self.snapshot.config.overlays)
        {
            Ok(trees) => Ok(trees),
            Err(Error::Pruned { .. }) => Err(StateError::GarbageCollected),
            Err(Error::Missing(h)) if h == hash => Err(StateError::GarbageCollected),
            Err(e) => Err(StateError::Source(backend(e))),
        }
    }
}
impl<S: Read> Snapshot<S> {
    // Returned trees contain only disposable overlays over this same captured
    // repository. Both VM environments and history remain bound to its reader.
    pub fn dry_run_view(
        &self,
        selector: &Selector,
    ) -> Result<Arc<ContractView<S>>, dry_run::Error<LookupError>> {
        let selected = dry_run::select(&mut Captured { snapshot: self }, selector)?;
        self.selected_view(selected).map_err(dry_run::Error::Source)
    }
}
