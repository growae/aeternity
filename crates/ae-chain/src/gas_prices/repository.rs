use super::{Error, Header, Limits, Source, Stats};
use crate::repository::{self as repo, Read, Record, Roots, View, point};
use ae_codec::{headers::Header as StoredHeader, rlp};
use ae_protocol::Protocol;
use ae_state::{
    Trees, accounts::Accounts, calls::Calls, channels::Channels, contracts::Contracts,
    names::Names, oracles::Oracles,
};
use ae_types::Hash256;
use num_bigint::BigInt;

// Aggregate read/work limits do not describe retained output size. A runtime
// adapter must additionally reserve the block envelope, one decoded transaction,
// transient codec/state allocations and the retained response.
#[derive(Debug, Clone, Copy)]
pub struct ReadLimits {
    pub reads: point::Limits,
    pub observations: Limits,
    pub proof_bytes: usize,
}
impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            reads: point::Limits {
                read_bytes: 512 << 20,
                reads: 262_144,
                value_bytes: 16 << 20,
                ..Default::default()
            },
            observations: Limits::default(),
            proof_bytes: 16 << 20,
        }
    }
}
pub struct Reader<S> {
    view: View<point::Meter<S>>,
    limits: ReadLimits,
    block_gas_limit: BigInt,
}
impl<S: Read> Reader<S> {
    pub fn new(view: &View<S>, block_gas_limit: BigInt, limits: ReadLimits) -> Result<Self, Error> {
        Self::with_config(view, block_gas_limit, limits, view.config().clone())
    }
    fn with_config(
        view: &View<S>,
        block_gas_limit: BigInt,
        limits: ReadLimits,
        config: repo::ReadConfig,
    ) -> Result<Self, Error> {
        Stats {
            total: block_gas_limit.clone(),
            ..Default::default()
        }
        .check(limits.observations)?;
        Ok(Self {
            view: point::view_with_config(view, limits.reads, config)?,
            limits,
            block_gas_limit,
        })
    }
    pub fn tip(&self) -> Option<Hash256> {
        self.view.status().tip
    }

    // Use this only on a cache miss. Missing records may be cached; decoding,
    // accounting, backend and capacity failures are exceptions, never misses.
    pub fn uncached_stats(
        &self,
        hash: Hash256,
        protocol: u32,
        mut checkpoint: impl FnMut() -> Result<(), Error>,
    ) -> Result<Stats, Error> {
        // Native evaluates both accessors even if either returns missing.
        let block = self
            .view
            .stream_block(hash, self.limits.proof_bytes, &mut checkpoint)?;
        checkpoint()?;
        let bytes = self.view.source().get(&repo::key(repo::RECORDS, hash.0))?;
        let trees = bytes
            .map(|bytes| {
                rlp::decode_with_limits(&bytes, self.limits.reads.rlp)
                    .map_err(|e| Error::from(repo::Error::Trie(e.into())))?;
                let record = Record::decode(&bytes)?;
                if let Some(block) = &block {
                    if record.roots.state.hash().0 != block.header().common().state_hash {
                        return Err(repo::Error::Corrupt("gas observation state binding").into());
                    }
                    // This is local retention policy; keep its error distinct from
                    // the reference's cacheable missing-record value.
                    self.view.require_retained(block.header().common().height)?;
                }
                load(&record.roots, self.view.source().clone())
            })
            .transpose()?;
        let (Some(block), Some(trees)) = (block, trees) else {
            return Err(Error::MissingState(hash));
        };
        let StoredHeader::Micro(header) = block.header() else {
            return Err(Error::InvalidObject);
        };
        if header.common.version != protocol {
            return Err(Error::InvalidObject);
        }
        let protocol = Protocol::try_from(protocol).map_err(|_| Error::InvalidObject)?;
        let height = header.common.height.into();
        let mut stats = Stats {
            total: self.block_gas_limit.clone(),
            ..Default::default()
        };
        stats.check(self.limits.observations)?;
        block.for_each(&mut checkpoint, |tx| {
            stats.transaction(tx, &height, protocol, &trees, self.limits.observations)
        })?;
        Ok(stats)
    }
}

mod service;
pub use service::service_read_weight;
impl<S: Read> Source for Reader<S> {
    fn header(&mut self, hash: Hash256) -> Result<Option<Header>, Error> {
        Ok(self.view.header(hash)?.map(|header| {
            let common = header.common();
            Header {
                previous: Hash256(common.previous_hash),
                protocol: common.version,
                micro: matches!(header, StoredHeader::Micro(_)),
                time: match &header {
                    StoredHeader::Key(h) => h.time,
                    StoredHeader::Micro(h) => h.time,
                },
            }
        }))
    }
    fn stats(&mut self, hash: Hash256, protocol: u32) -> Result<Stats, Error> {
        self.uncached_stats(hash, protocol, || Ok(()))
    }
}

// All native tree roots are loaded, including before Ceres and for empty
// blocks. Preserve resource/backend errors instead of reducing them to a bad
// root, which could otherwise turn an operational failure into a cache entry.
fn load<S: ae_trie::NodeReader + Clone>(roots: &Roots, source: S) -> Result<Trees<S, S>, Error> {
    macro_rules! tree {
        ($expr:expr, $variant:path) => {
            $expr.map_err(|e| match e {
                $variant(e) => Error::from(repo::Error::Trie(e)),
                _ => Error::InvalidObject,
            })?
        };
    }
    Ok(Trees {
        contracts: tree!(
            Contracts::from_root(roots.state.contracts, source.clone()),
            ae_state::contracts::Error::Tree
        ),
        calls: tree!(
            Calls::from_root(roots.state.calls, source.clone()),
            ae_state::contracts::Error::Tree
        ),
        channels: tree!(
            Channels::from_root(roots.state.channels, source.clone()),
            ae_state::channels::Error::Tree
        ),
        names: tree!(
            Names::from_roots(
                roots.state.names,
                roots.names,
                source.clone(),
                source.clone()
            ),
            ae_state::names::Error::Tree
        ),
        oracles: tree!(
            Oracles::from_roots(
                roots.state.oracles,
                roots.oracles,
                source.clone(),
                source.clone()
            ),
            ae_state::oracles::Error::Tree
        ),
        accounts: tree!(
            Accounts::from_root(roots.state.accounts, source),
            ae_state::accounts::AccountError::Tree
        ),
    })
}

#[cfg(test)]
mod tests;
