// aec_block_key_candidate and aec_chain_state:fake_key_node, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_codec::headers::CommonHeader;
use ae_consensus::target::{DIFFICULTY_WINDOW, HIGHEST_TARGET, Target, WorkSample, retarget};
use ae_types::AccountPublicKey;

#[derive(Debug, Clone, Copy)]
pub struct Input {
    pub miner: AccountPublicKey,
    pub beneficiary: AccountPublicKey,
    pub calculation_time: u64,
    pub release_info: u32,
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    WrongConsensus,
    Parent,
    Height,
    Protocol(ae_protocol::ProfileError),
    Encoding(HeaderError),
    Transition(Box<super::Error<A, C>>),
    Adjustment { expected: usize, actual: usize },
    Target(TargetError),
}
pub struct Candidate<A = MemoryStore, C = MemoryStore> {
    temporary: KeyHeader,
    prepared: super::Prepared<A, C>,
    final_info: Option<[u8; 4]>,
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Candidate<A, C> {
    pub fn template(
        previous: &Header,
        key: &KeyHeader,
        network: &ae_protocol::NetworkProfile,
        input: Input,
    ) -> Result<KeyHeader, Error<A, C>> {
        let previous_hash = previous.hash().map_err(Error::Encoding)?;
        let key_hash = Header::Key(key.clone()).hash().map_err(Error::Encoding)?;
        if key.common.height != previous.common().height
            || match previous {
                Header::Key(k) => k != key,
                Header::Micro(m) => m.common.previous_key_hash != key_hash.0,
            }
        {
            return Err(Error::Parent);
        }
        let height = previous
            .common()
            .height
            .checked_add(1)
            .ok_or(Error::Height)?;
        let protocol = network.protocol_at(height).map_err(Error::Protocol)?;
        let mut dummy = [0; 32];
        dummy[31] = 123;
        Ok(KeyHeader {
            common: CommonHeader {
                version: protocol as u32,
                height,
                previous_hash: previous_hash.0,
                previous_key_hash: key_hash.0,
                state_hash: dummy,
            },
            miner: input.miner.0,
            beneficiary: input.beneficiary.0,
            target: HIGHEST_TARGET.0,
            seal: [0; 42],
            nonce: 0,
            time: input.calculation_time,
            info: (protocol >= Protocol::Minerva).then_some(input.release_info.to_be_bytes()),
            hole: false,
            end_of_epoch: false,
        })
    }
    pub fn prepare<O: MapOrder>(
        trees: &Trees<A, C>,
        parent: &Parent<'_>,
        history: &rewards::History,
        execution: &transactions::Config<'_, O>,
        rules: &Rules,
        input: Input,
    ) -> Result<Self, Error<A, C>> {
        if execution.consensus != ConsensusType::Pow {
            return Err(Error::WrongConsensus);
        }
        let previous = parent.headers.previous;
        let key = parent.headers.previous_key;
        if trees.roots().hash().0 != previous.common().state_hash {
            return Err(Error::Parent);
        }
        let temporary = Self::template(previous, key, execution.network, input)?;
        let height = temporary.common.height;
        // calculate_state_for_new_keyblock resets generation fees and fraud
        // when its immediate parent is a key; gas fees in calls still close.
        let parent = Parent {
            headers: headers::Context {
                previous,
                previous_key: key,
                recent_keys: parent.headers.recent_keys,
            },
            previous_generation: parent.previous_generation,
            fees: if matches!(previous, Header::Key(_)) {
                0.into()
            } else {
                parent.fees.clone()
            },
            reported_fraud: matches!(previous, Header::Micro(_)) && parent.reported_fraud,
        };
        let prepared = super::prepare(trees, &temporary, &parent, history, execution, rules)
            .map_err(|e| Error::Transition(Box::new(e)))?;
        let final_info = execution
            .network
            .community_fork()
            .and_then(|f| f.info_at(height))
            .map(u32::to_be_bytes)
            .or(temporary.info);
        Ok(Self {
            temporary,
            prepared,
            final_info,
        })
    }
    // A final clock read occurs after state calculation. Neither header is
    // a certified block; ordinary admission must still verify the PoW seal.
    // The initial target is the configured genesis target, not the parent.
    pub fn finish(
        &self,
        time: u64,
        initial_target: Target,
        adjustment: &[KeyHeader],
    ) -> Result<KeyHeader, Error<A, C>> {
        let mut header = self.temporary.clone();
        header.common.state_hash = self.prepared.trees().roots().hash().0;
        header.time = time;
        header.info = self.final_info;
        // Native new_key_header rejects explicit signalling info before
        // Minerva, after state calculation but before target adjustment.
        if header.common.version < Protocol::Minerva as u32 && header.info.is_some() {
            return Err(Error::Encoding(HeaderError::VersionInfo));
        }
        let n = DIFFICULTY_WINDOW + 1;
        header.target = if header.common.height <= n as u64 {
            initial_target.0
        } else {
            if adjustment.len() != n {
                return Err(Error::Adjustment {
                    expected: n,
                    actual: adjustment.len(),
                });
            }
            let samples: Vec<_> = adjustment
                .iter()
                .map(|k| WorkSample {
                    height: k.common.height,
                    timestamp: k.time,
                    target: Target(k.target),
                })
                .collect();
            retarget(&samples).map_err(Error::Target)?.0
        };
        Ok(header)
    }
    pub fn temporary(&self) -> &KeyHeader {
        &self.temporary
    }
    pub fn trees(&self) -> &Trees<A, C> {
        self.prepared.trees()
    }
    pub fn fees(&self) -> &Unsigned {
        self.prepared.fees()
    }
    pub fn reward(&self) -> Option<&rewards::Award> {
        self.prepared.reward()
    }
}
