// aec_block_micro_candidate:create and aec_consensus_bitcoin_ng, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_codec::headers::KeyHeader;
use ae_vm_fate::machine::program::remote::context::Consensus;
use std::convert::Infallible;

pub struct PowParent<'a, A = MemoryStore, C = MemoryStore> {
    pub header: &'a Header,
    pub key: &'a KeyHeader,
    pub view: &'a chain::View<A, C>,
    pub reported_fraud: bool,
}
pub struct PowStart {
    pub now: i128,
    pub micro_cycle_ms: u64,
    // The repository selects the proof discovered against the preceding key
    // generation. A micro predecessor may already have reported that proof.
    pub proof: Option<Box<fraud::Proof>>,
    pub limits: Limits,
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Micro<A, C> {
    pub fn begin_pow<O: MapOrder, S: chain::Source<A, C>>(
        parent: PowParent<'_, A, C>,
        reader: &chain::Reader<A, C, O, S>,
        configured_gas_limit: &ae_types::Unsigned,
        start: PowStart,
    ) -> Result<Self, Error<Infallible, A, C>> {
        let key = parent.key;
        let key_hash = Header::Key(key.clone())
            .hash()
            .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
        let hash = parent
            .header
            .hash()
            .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
        let previous_time = match parent.header {
            Header::Key(previous) if previous == key => i128::from(previous.time) + 1,
            Header::Micro(previous)
                if previous.common.previous_key_hash == key_hash.0
                    && previous.common.height == key.common.height
                    && previous.common.version == key.common.version =>
            {
                i128::from(previous.time) + i128::from(start.micro_cycle_ms)
            }
            _ => return Err(Error::WrongParent),
        };
        if parent.view.trees.roots().hash().0 != parent.header.common().state_hash {
            return Err(Error::Chain(super::super::Error::ParentState));
        }
        if parent.view.environment.consensus != Consensus::Pow
            || parent.view.environment.network_id != reader.profile.network.network_id()
            || parent.view.environment.key_hash != key_hash.0
            || reader.profile.context.in_auth
            || !reader.profile.context.on_chain
            || reader.profile.context.reserved_gas != BigInt::ZERO
        {
            return Err(Error::Chain(super::super::Error::Configuration));
        }
        let time = u64::try_from(start.now.max(previous_time)).map_err(|_| Error::ResourceLimit)?;
        let gas_limit =
            rewards::number(&reader.profile.network.block_gas_limit(configured_gas_limit));
        let mut view = parent.view.clone();
        // Reuse the ordinary PoW execution environment, including computed
        // difficulty and both historical VM configurations. The PoW micro
        // pre-transform is the identity: no HC contract effects are applied.
        reader
            .with_execution(parent.view, |config| {
                crate::execution::with_block_environment(
                    config,
                    key,
                    time,
                    key_hash.0,
                    &gas_limit,
                    |execution| {
                        view.environment = execution
                            .fate
                            .expect("reader FATE configuration")
                            .environment
                            .clone();
                        view.aevm = execution.aevm.cloned();
                    },
                )
            })
            .map_err(Error::ParentDifficulty)?;
        view.context = ae_execution::Context::transaction(
            key.common.height,
            Protocol::try_from(key.common.version)
                .map_err(|e| Error::Chain(super::super::Error::Protocol(e)))?,
        );
        let proof = if matches!(parent.header, Header::Micro(_)) && parent.reported_fraud {
            None
        } else {
            start.proof
        };
        let proof_of_fraud = proof
            .as_ref()
            .map(|p| p.hash().map(|h| h.0))
            .transpose()
            .map_err(Error::Fraud)?;
        Ok(Self {
            block: MicroBlock {
                header: MicroHeader {
                    common: CommonHeader {
                        version: key.common.version,
                        height: key.common.height,
                        previous_hash: hash.0,
                        previous_key_hash: key_hash.0,
                        state_hash: view.trees.roots().hash().0,
                    },
                    time,
                    transactions_hash: [0; 32],
                    proof_of_fraud,
                    signature: [0; 64],
                },
                transactions: vec![],
                proof,
            },
            view,
            key_height: key.common.height,
            gas_limit,
            bytes: 0,
            limits: start.limits,
        })
    }
}
