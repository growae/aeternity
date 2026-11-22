// aec_block_micro_candidate:create_pos/update, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{Parent, Rules, transition};
use crate::{block::MicroBlock, fraud, rewards};
use ae_codec::headers::{CommonHeader, Header, MicroHeader};
use ae_execution::transactions::{accounting, batch};
use ae_hyperchain::{block as hc, chain, signing};
use ae_protocol::Protocol;
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::sync::Arc;
mod key;
mod packing;
pub(crate) use key::template as key_template;
pub use key::{Key, KeyInput};
pub use packing::{Deadlines, PackRequest, Packing, Timeout};
pub type Signed = SignedTransaction<Transaction>;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub transactions: usize,
    pub signed_bytes: usize,
    pub offered: usize,
    pub passes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            transactions: 100_000,
            signed_bytes: 16 << 20,
            offered: 1_000_000,
            passes: 4096,
        }
    }
}
#[derive(Debug)]
pub enum Error<E, A = MemoryStore, C = MemoryStore> {
    Chain(super::Error<E, A, C>),
    Transactions(batch::Error<A, C>),
    Accounting(accounting::Error),
    Transaction(ae_tx::Error),
    Tree(ae_trie::Error),
    Fraud(fraud::Error),
    Signing(signing::Error),
    ResourceLimit,
    WrongParent,
    NoUpdate,
    Full,
    Sequence,
}
impl<E, A, C> Error<E, A, C> {
    pub fn is_operational<H: hc::Host<Error = E>>(&self) -> bool {
        match self {
            Self::Chain(e) => e.is_operational::<H>(),
            Self::Transactions(e) => e.failure.is_operational(),
            Self::Accounting(e) => e.is_operational(),
            Self::ResourceLimit | Self::Tree(_) | Self::Sequence => true,
            _ => false,
        }
    }
}
pub struct Start {
    pub now: i128,
    pub proof: Option<Box<fraud::Proof>>,
    pub limits: Limits,
}
// Unsigned candidate and its actual execution view. All fields remain private:
// updates cannot replace trees independently of transaction/header contents.
#[derive(Clone)]
pub struct Micro<A = MemoryStore, C = MemoryStore> {
    block: MicroBlock,
    view: chain::View<A, C>,
    key_height: u64,
    gas_limit: BigInt,
    bytes: usize,
    limits: Limits,
}
#[derive(Debug)]
pub struct Applied<A, C> {
    pub accepted: Vec<usize>,
    pub rejected: Vec<(usize, batch::Rejection<A, C>)>,
}
fn charge<E, A, C>(transactions: &[Signed], limit: usize) -> Result<usize, Error<E, A, C>> {
    let mut bytes = 0usize;
    for t in transactions {
        bytes = bytes
            .checked_add(t.encode().len())
            .ok_or(Error::ResourceLimit)?;
        if bytes > limit {
            return Err(Error::ResourceLimit);
        }
    }
    Ok(bytes)
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Micro<A, C> {
    pub fn begin<O: MapOrder, S: chain::Source<A, C>, H: hc::Host>(
        parent: &Parent<'_, A, C>,
        reader: &chain::Reader<A, C, O, S>,
        state: &mut hc::State,
        host: &mut H,
        rules: &Rules,
        start: Start,
    ) -> Result<Self, Error<H::Error, A, C>> {
        let Header::Key(key) = parent.header else {
            return Err(Error::WrongParent);
        };
        if key != parent.key || parent.view.trees.roots().hash().0 != key.common.state_hash {
            return Err(Error::Chain(super::Error::ParentState));
        }
        if parent.view.environment.consensus
            != ae_vm_fate::machine::program::remote::context::Consensus::Pos
            || parent.view.environment.network_id != reader.profile.network.network_id()
            || reader.profile.context.in_auth
            || !reader.profile.context.on_chain
            || reader.profile.context.reserved_gas != 0.into()
        {
            return Err(Error::Chain(super::Error::Configuration));
        }
        let height = key
            .common
            .height
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let time = u64::try_from(start.now.max(i128::from(key.time) + 1))
            .map_err(|_| Error::ResourceLimit)?;
        let hash = parent
            .header
            .hash()
            .map_err(|e| Error::Chain(super::Error::Encoding(e)))?;
        let gas = reader
            .profile
            .network
            .block_gas_limit(&rules.configured_gas_limit);
        let mut view =
            transition::frame(parent, key, height, time, hash, &gas).map_err(Error::Chain)?;
        let proof_of_fraud = start
            .proof
            .as_ref()
            .map(|p| p.hash().map(|h| h.0))
            .transpose()
            .map_err(Error::Fraud)?;
        let mut next = state.clone();
        let mut staged = host.clone();
        let transformed = next.pre_transform(
            reader,
            parent.view,
            &parent.view.trees,
            &mut staged,
            hc::Kind::Micro,
            height,
            &rules.consensus,
        );
        let out = match transformed {
            Ok(out) => out,
            Err(e) => {
                let error = Error::Chain(super::Error::Consensus(e));
                if !error.is_operational::<H>() {
                    *state = next;
                    *host = staged;
                }
                return Err(error);
            }
        };
        view.trees = Arc::new(out.trees);
        let block = MicroBlock {
            header: MicroHeader {
                common: CommonHeader {
                    version: key.common.version,
                    height,
                    previous_hash: hash.0,
                    previous_key_hash: hash.0,
                    state_hash: view.trees.roots().hash().0,
                },
                time,
                transactions_hash: [0; 32],
                proof_of_fraud,
                signature: [0; 64],
            },
            transactions: vec![],
            proof: start.proof,
        };
        *state = next;
        *host = staged;
        Ok(Self {
            block,
            view,
            key_height: key.common.height,
            gas_limit: rewards::number(&gas),
            bytes: 0,
            limits: start.limits,
        })
    }
    pub fn block(&self) -> &MicroBlock {
        &self.block
    }
    pub fn trees(&self) -> &Arc<Trees<A, C>> {
        &self.view.trees
    }
    pub fn context(&self) -> &ae_execution::Context {
        &self.view.context
    }
    pub fn signed<E>(
        &self,
        keys: &signing::Keys,
        leader: AccountPublicKey,
    ) -> Result<MicroBlock, Error<E, A, C>> {
        let mut block = self.block.clone();
        block.header = keys
            .sign_micro(&block.header, Some(leader))
            .map_err(Error::Signing)?;
        Ok(block)
    }
    fn refresh<E>(&mut self) -> Result<(), Error<E, A, C>> {
        let encoded = self
            .block
            .transactions
            .iter()
            .map(Signed::encode)
            .collect::<Vec<_>>();
        self.block.header.transactions_hash =
            ae_trie::transaction_root(encoded.iter().map(Vec::as_slice))
                .map_err(Error::Tree)?
                .0;
        self.block.header.common.state_hash = self.view.trees.roots().hash().0;
        Ok(())
    }
    fn used<E>(
        &self,
        txs: &[Signed],
        height: u64,
        trees: &Trees<A, C>,
    ) -> Result<BigInt, Error<E, A, C>> {
        let protocol = self.view.context.protocol;
        txs.iter().try_fold(BigInt::ZERO, |sum, t| {
            let gas = if protocol <= Protocol::Iris {
                t.transaction()
                    .view()
                    .pricing(&height.into(), protocol)
                    .gas_limit
            } else {
                accounting::used_gas(t.transaction().view(), &height.into(), protocol, trees)
                    .map_err(Error::Accounting)?
            };
            Ok(sum + gas)
        })
    }
    fn append<E>(&mut self, txs: Vec<Signed>) -> Result<(), Error<E, A, C>> {
        let count = self
            .block
            .transactions
            .len()
            .checked_add(txs.len())
            .ok_or(Error::ResourceLimit)?;
        let bytes = self
            .bytes
            .checked_add(charge::<E, A, C>(&txs, self.limits.signed_bytes)?)
            .ok_or(Error::ResourceLimit)?;
        if count > self.limits.transactions || bytes > self.limits.signed_bytes {
            return Err(Error::ResourceLimit);
        }
        self.block.transactions.extend(txs);
        self.bytes = bytes;
        Ok(())
    }
    // Native update sorts by origin/nonce, skips invalid transactions, and stops
    // at the first transaction whose declared gas exceeds the remaining gas.
    // All local execution/accounting failures roll back this entire update.
    pub fn update<E, O: MapOrder, S: chain::Source<A, C>>(
        &mut self,
        txs: &[Signed],
        reader: &chain::Reader<A, C, O, S>,
    ) -> Result<Applied<A, C>, Error<E, A, C>> {
        if txs.len() > self.limits.offered {
            return Err(Error::ResourceLimit);
        }
        charge::<E, A, C>(txs, self.limits.signed_bytes)?;
        let height = self.block.header.common.height;
        let mut remaining =
            &self.gas_limit - self.used::<E>(&self.block.transactions, height, &self.view.trees)?;
        if remaining <= BigInt::ZERO {
            return Err(Error::Full);
        }
        let mut sorted = txs
            .iter()
            .enumerate()
            .map(|(i, tx)| {
                let v = tx.transaction().view();
                Ok(((v.origin().map_err(Error::Transaction)?, v.nonce()), i, tx))
            })
            .collect::<Result<Vec<_>, Error<E, A, C>>>()?;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut next = self.clone();
        let mut result = Applied {
            accepted: vec![],
            rejected: vec![],
        };
        for (_, index, tx) in sorted {
            if tx
                .transaction()
                .view()
                .pricing(&height.into(), self.view.context.protocol)
                .gas_limit
                > remaining
            {
                break;
            }
            let mut trees = next.view.trees.as_ref().clone();
            let receipt = reader
                .with_execution(&next.view, |cfg| {
                    batch::apply(
                        std::slice::from_ref(tx),
                        &mut trees,
                        &next.view.context,
                        cfg,
                        &Default::default(),
                    )
                })
                .map_err(Error::Transactions)?;
            if receipt.accepted.is_empty() {
                result
                    .rejected
                    .extend(receipt.rejected.into_iter().map(|(_, e)| (index, e)));
                continue;
            }
            remaining -= next.used::<E>(std::slice::from_ref(tx), height, &trees)?;
            next.view.trees = Arc::new(trees);
            next.append::<E>(vec![tx.clone()])?;
            result.accepted.push(index);
        }
        if result.accepted.is_empty() {
            return Err(Error::NoUpdate);
        }
        next.refresh::<E>()?;
        *self = next;
        Ok(result)
    }
}
