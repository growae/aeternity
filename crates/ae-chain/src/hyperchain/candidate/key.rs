// aec_block_hole_candidate and aec_chain_state:fake_key_node, fa2a6d7.
// Copyright (c) 2024, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_codec::headers::KeyHeader;
use ae_types::Unsigned;

#[derive(Debug, Clone, Copy)]
pub struct KeyInput {
    pub miner: AccountPublicKey,
    pub beneficiary: AccountPublicKey,
    pub hole: bool,
    pub end_of_epoch: bool,
    pub time: u64,
    pub release_info: u32,
}
// State calculation precedes the final header's clock read. The dummy header
// and its native -1 difficulty never serve as an admitted incoming block.
#[derive(Clone)]
pub struct Key<A = MemoryStore, C = MemoryStore> {
    temporary: KeyHeader,
    view: chain::View<A, C>,
    previous_target: u32,
    final_info: Option<[u8; 4]>,
    fees: Unsigned,
    events: Vec<hc::Event>,
    wait_ms: Option<Unsigned>,
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Key<A, C> {
    pub fn prepare<O: MapOrder, S: chain::Source<A, C>, H: hc::Host>(
        parent: &Parent<'_, A, C>,
        history: &rewards::History,
        reader: &chain::Reader<A, C, O, S>,
        state: &mut hc::State,
        host: &mut H,
        rules: &Rules,
        input: KeyInput,
    ) -> Result<Self, Error<H::Error, A, C>> {
        if parent.view.environment.consensus
            != ae_vm_fate::machine::program::remote::context::Consensus::Pos
            || parent.view.environment.network_id != reader.profile.network.network_id()
            || rules.reward_delay < ae_protocol::coinbase::FRAUD_REPORT_DELAY
            || reader.profile.context.in_auth
            || !reader.profile.context.on_chain
            || reader.profile.context.reserved_gas != 0.into()
        {
            return Err(Error::Chain(super::super::Error::Configuration));
        }
        if parent.view.trees.roots().hash().0 != parent.header.common().state_hash {
            return Err(Error::Chain(super::super::Error::ParentState));
        }
        let key_hash = Header::Key(parent.key.clone())
            .hash()
            .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
        let linked = match parent.header {
            Header::Key(k) => k == parent.key,
            Header::Micro(m) => m.common.previous_key_hash == key_hash.0,
        };
        if !linked {
            return Err(Error::WrongParent);
        }
        let temporary = template(parent.header, parent.key, &reader.profile.network, input)?;
        let height = temporary.common.height;
        let info = temporary.info;
        let hash = Header::Key(temporary.clone())
            .hash()
            .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
        let gas = reader
            .profile
            .network
            .block_gas_limit(&rules.configured_gas_limit);
        let mut view = transition::frame(parent, &temporary, height, input.time, hash, &gas)
            .map_err(Error::Chain)?;
        view.environment.difficulty = (-1).into();
        if let Some(a) = &mut view.aevm {
            a.environment.difficulty = (-1).into();
        }
        let mut next = state.clone();
        let mut staged = host.clone();
        let out = transition::key_state(
            &temporary,
            parent,
            history,
            reader,
            &mut next,
            &mut staged,
            rules,
            view.clone(),
        )
        .map_err(Error::Chain);
        if !out.as_ref().is_err_and(|e| e.is_operational::<H>()) {
            *state = next;
            *host = staged;
        }
        let out = out?;
        view.trees = Arc::new(out.trees);
        let final_info = reader
            .profile
            .network
            .community_fork()
            .and_then(|f| f.info_at(height))
            .map(u32::to_be_bytes)
            .or(info);
        Ok(Self {
            temporary,
            view,
            previous_target: parent.key.target,
            final_info,
            fees: out.fees,
            events: out.consensus,
            wait_ms: out.wait_ms,
        })
    }
    pub fn finish<E>(&self, time: u64) -> Result<KeyHeader, Error<E, A, C>> {
        let mut header = self.temporary.clone();
        header.common.state_hash = self.view.trees.roots().hash().0;
        header.target = if header.end_of_epoch {
            u32::try_from(header.common.height).map_err(|_| Error::ResourceLimit)?
        } else {
            self.previous_target
                .checked_add(u32::from(!header.hole))
                .ok_or(Error::ResourceLimit)?
        };
        header.time = time;
        header.info = self.final_info;
        Ok(header)
    }
    pub fn trees(&self) -> &Arc<Trees<A, C>> {
        &self.view.trees
    }
    pub fn fees(&self) -> &Unsigned {
        &self.fees
    }
    // Native calculation discards these events; expose them for inspection,
    // not as a second durable block-admission notification stream.
    pub fn events(&self) -> &[hc::Event] {
        &self.events
    }
    pub fn wait_ms(&self) -> Option<&Unsigned> {
        self.wait_ms.as_ref()
    }
    pub fn calculation_environment(
        &self,
    ) -> &ae_vm_fate::machine::program::remote::context::Environment {
        &self.view.environment
    }
}

pub(crate) fn template<E, A, C>(
    previous: &Header,
    previous_key: &KeyHeader,
    network: &ae_protocol::NetworkProfile,
    input: KeyInput,
) -> Result<KeyHeader, Error<E, A, C>> {
    let parent_hash = previous
        .hash()
        .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
    let key_hash = Header::Key(previous_key.clone())
        .hash()
        .map_err(|e| Error::Chain(super::super::Error::Encoding(e)))?;
    let height = previous
        .common()
        .height
        .checked_add(u64::from(matches!(previous, Header::Key(_))))
        .ok_or(Error::ResourceLimit)?;
    let protocol = network
        .protocol_at(height)
        .map_err(|e| Error::Chain(super::super::Error::Protocol(e)))?;
    let info = (protocol >= Protocol::Minerva).then_some(input.release_info.to_be_bytes());
    let mut dummy = [0; 32];
    dummy[31] = 123;
    Ok(KeyHeader {
        common: CommonHeader {
            version: protocol as u32,
            height,
            previous_hash: parent_hash.0,
            previous_key_hash: key_hash.0,
            state_hash: dummy,
        },
        miner: input.miner.0,
        beneficiary: input.beneficiary.0,
        target: u32::MAX,
        seal: [0; 42],
        nonce: 0,
        time: input.time,
        info,
        hole: input.hole,
        end_of_epoch: input.end_of_epoch,
    })
}
