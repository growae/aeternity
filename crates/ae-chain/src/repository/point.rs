// Shared point-read budgets for immutable client queries.
use super::{self as repo, Read, View};
use ae_codec::rlp::DecodeLimits;
use ae_trie::NodeReader;
use ae_types::Hash256;
use std::{cell::Cell, rc::Rc};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub read_bytes: usize,
    pub reads: usize,
    pub node_bytes: usize,
    pub value_bytes: usize,
    pub rlp: DecodeLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            read_bytes: 64 << 20,
            reads: 4096,
            node_bytes: 8 << 20,
            value_bytes: 4 << 20,
            rlp: DecodeLimits {
                max_input_bytes: 8 << 20,
                max_items: 65_536,
                max_depth: 64,
            },
        }
    }
}
#[derive(Clone)]
pub(crate) struct Meter<S> {
    source: S,
    bytes: Rc<Cell<usize>>,
    reads: Rc<Cell<usize>>,
    limits: Limits,
}
impl<S: Read> Meter<S> {
    fn read(&self, key: &[u8], limit: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.reads.set(
            self.reads
                .get()
                .checked_sub(1)
                .ok_or(repo::Error::ResourceLimit)?,
        );
        // Pinned storage checks the remaining shared credit before copying.
        let bytes = self.source.get_bounded(key, limit.min(self.bytes.get()))?;
        if let Some(bytes) = &bytes {
            self.bytes.set(
                self.bytes
                    .get()
                    .checked_sub(bytes.len())
                    .ok_or(repo::Error::ResourceLimit)?,
            );
        }
        Ok(bytes)
    }
}
impl<S: Read> NodeReader for Meter<S> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.read(&repo::key(repo::NODES, hash.0), self.limits.node_bytes)
            .map_err(|e| match e {
                repo::Error::ResourceLimit => ae_trie::Error::ResourceLimit,
                repo::Error::Trie(e) => e,
                e => ae_trie::Error::Backend(e.to_string()),
            })
    }
    fn decode_limits(&self) -> DecodeLimits {
        self.limits.rlp
    }
}
impl<S: Read> Read for Meter<S> {
    fn source_id(&self) -> u64 {
        self.source.source_id()
    }
    fn resilience(&self) -> crate::resilience::Snapshot {
        self.source.resilience()
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        self.read(key, self.limits.value_bytes)
    }
    fn get_bounded(&self, key: &[u8], bytes: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.read(key, bytes)
    }
    // This adapter admits point reads only; no unmetered scan fallback.
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
}
pub(crate) fn view<S: Read>(view: &View<S>, limits: Limits) -> Result<View<Meter<S>>, repo::Error> {
    view_with_config(view, limits, view.config().clone())
}
pub(crate) fn view_with_config<S: Read>(
    view: &View<S>,
    limits: Limits,
    config: repo::ReadConfig,
) -> Result<View<Meter<S>>, repo::Error> {
    let source = Meter {
        source: view.source().clone(),
        bytes: Rc::new(Cell::new(limits.read_bytes)),
        reads: Rc::new(Cell::new(limits.reads)),
        limits,
    };
    View::load_with_config(source, config)
}
