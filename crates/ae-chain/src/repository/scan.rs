use super::*;
use ae_trie::scan::{Cursor, Limits, NodeBytes, PageLimits, Progress};

// An integrity scan keeps the same immutable repository snapshot for every
// page, even if insertion or administrative rollback changes the live tip.
// It checks stored MPT structure/content, not decoded account/contract values
// or whether the block was consensus-valid. No data is removed or promoted.
#[derive(Clone)]
pub struct StateScan<S> {
    source: S,
    block: Hash256,
    revision: u64,
    cursor: Cursor,
}
impl<S: Read> StateScan<S> {
    pub fn block(&self) -> Hash256 {
        self.block
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn progress(&self) -> Progress {
        self.cursor.progress()
    }
    pub fn is_complete(&self) -> bool {
        self.cursor.is_complete()
    }
    pub fn next_page(&mut self, limits: PageLimits) -> Result<Vec<NodeBytes>, Error> {
        let page = self.cursor.page(&self.source, limits)?;
        self.cursor = page.next;
        Ok(page.nodes)
    }
}
impl<S: Read> View<S> {
    pub fn scan_state(&self, block: Hash256, limits: Limits) -> Result<StateScan<S>, Error> {
        let record = self.require_record(block)?;
        let header = self.require_block(block)?.header();
        self.require_retained(header.common().height)?;
        if record.roots.state.hash().0 != header.common().state_hash {
            return Err(Error::Corrupt("state binding"));
        }
        // Include the name/oracle expiry caches, which are persisted beside
        // the six roots committed by the header's state hash.
        let cursor = Cursor::new(record.roots.array().into_iter().flatten(), limits)?;
        Ok(StateScan {
            source: self.source.clone(),
            block,
            revision: self.status.revision,
            cursor,
        })
    }
}
