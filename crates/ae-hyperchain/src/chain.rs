// aec_chain_hc and aec_consensus_hc target_parent_heights, fa2a6d7.
// Copyright (c) Aeternity. ISC notice in repository LICENSE.
use crate::{
    consensus_contract::{self, Contract, Contracts},
    parent_cache::runtime::{TargetError, Targets},
};
use ae_execution::{Context, contracts, transactions};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeWriter};
use ae_types::Unsigned;
use ae_vm_fate::{
    Limits,
    machine::{self, program::remote::context::Environment},
    maps::{MapOrder, listing::Overrides},
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
pub mod source;
use source::Read;
pub use source::{LookupError, Source};

#[derive(Debug, Clone)]
pub struct View<A = MemoryStore, C = MemoryStore> {
    pub trees: Arc<Trees<A, C>>,
    pub context: Context,
    pub environment: Environment,
    pub history: Arc<ae_execution::chain_history::Snapshot>,
    pub aevm: Option<contracts::aevm::Config>,
}
#[derive(Debug, Clone)]
pub enum RunEnv<A = MemoryStore, C = MemoryStore> {
    Top,
    Height(BigInt),
    Hash(Vec<u8>),
    View(Arc<View<A, C>>),
}
// The node publishes a coherent selected index and immutable views together.
// A durable node may build these views from persisted roots; no chain data is
// fabricated for an absent selection or an unavailable state snapshot.
#[derive(Debug, Clone)]
pub struct Selection<A = MemoryStore, C = MemoryStore> {
    pub top: Option<BigInt>,
    pub heights: BTreeMap<BigInt, Vec<u8>>,
    pub views: BTreeMap<Vec<u8>, Arc<View<A, C>>>,
}
impl<A, C> Default for Selection<A, C> {
    fn default() -> Self {
        Self {
            top: None,
            heights: BTreeMap::new(),
            views: BTreeMap::new(),
        }
    }
}
#[derive(Clone)]
pub struct Selected<A = MemoryStore, C = MemoryStore>(Arc<RwLock<Arc<Selection<A, C>>>>);
impl<A, C> Selected<A, C> {
    pub fn new(selection: Selection<A, C>) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(selection))))
    }
    pub fn replace(&self, selection: Selection<A, C>) {
        *self.0.write().expect("HC selection") = Arc::new(selection);
    }
    pub fn snapshot(&self) -> Arc<Selection<A, C>> {
        self.0.read().expect("HC selection").clone()
    }
}
#[derive(Clone)]
pub struct Profile<O> {
    pub contracts: Contracts,
    pub network: NetworkProfile,
    pub context: machine::Context,
    pub order: O,
    pub overrides: Overrides,
    pub diagnostic: ae_vm_fate::diagnostic::Profile,
    pub limits: Limits,
    pub maximum_steps: usize,
    pub maximum_history_reads: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Native {
    FunctionClause,
    Badmatch(Value),
    BadmatchQuery(Value),
    BadmatchChainTooShort,
    Badarith,
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    ChainTooShort,
    ViewUnavailable(Vec<u8>),
    ResourceLimit,
    Lookup(LookupError),
    Native(Native),
    Contract(Box<consensus_contract::Error<A, C>>),
}
impl<A, C> Error<A, C> {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Lookup(e) => e.is_operational(),
            Self::ViewUnavailable(_) | Self::ResourceLimit => true,
            Self::Contract(e) => !e.is_native(),
            Self::ChainTooShort | Self::Native(_) => false,
        }
    }
}
impl<A, C> From<LookupError> for Error<A, C> {
    fn from(error: LookupError) -> Self {
        match error {
            LookupError::Unavailable(hash) => Self::ViewUnavailable(hash),
            LookupError::ResourceLimit => Self::ResourceLimit,
            e => Self::Lookup(e),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Staker {
    pub address: ObjectPayload,
    pub stake: Value,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochInfo {
    pub epoch: Value,
    pub first: BigInt,
    pub length: BigInt,
    pub last: BigInt,
    pub seed: Option<Vec<u8>>,
    pub validators: Option<Vec<Staker>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinReward {
    pub base: Value,
    pub current: Value,
    pub carry_over: Value,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkVote {
    pub producer: ObjectPayload,
    pub hash: Vec<u8>,
    pub signature: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LengthVote {
    pub producer: ObjectPayload,
    pub epoch_delta: Value,
    pub signature: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeInfo {
    pub epoch: Value,
    pub fork: Option<Vec<u8>>,
    pub epoch_length: Option<Value>,
    pub parent_hash: Vec<u8>,
    pub producer: ObjectPayload,
    pub votes: Option<Vec<ForkVote>>,
    pub length_votes: Option<Vec<LengthVote>>,
}
fn tuple(value: &Value, n: usize) -> Result<&[Value], Native> {
    match value {
        Value::Tuple(v) if v.len() == n => Ok(v),
        _ => Err(Native::Badmatch(value.clone())),
    }
}
fn clause_tuple(value: &Value, n: usize) -> Result<&[Value], Native> {
    tuple(value, n).map_err(|_| Native::FunctionClause)
}
fn number(v: &Value) -> Result<&BigInt, Native> {
    if let Value::Integer(n) = v {
        Ok(n)
    } else {
        Err(Native::Badarith)
    }
}
fn bytes(v: &Value) -> Result<Vec<u8>, Native> {
    if let Value::Bytes(b) = v {
        Ok(b.clone())
    } else {
        Err(Native::FunctionClause)
    }
}
fn address(v: &Value) -> Result<ObjectPayload, Native> {
    if let Value::Object(ObjectKind::Address, a) = v {
        Ok(a.clone())
    } else {
        Err(Native::FunctionClause)
    }
}
fn list(v: &Value) -> Result<&[Value], Native> {
    if let Value::List(xs) = v {
        Ok(xs)
    } else {
        Err(Native::FunctionClause)
    }
}
fn option<T>(
    v: &Value,
    some: impl FnOnce(&Value) -> Result<T, Native>,
) -> Result<Option<T>, Native> {
    match v {
        Value::Variant {
            arities,
            tag,
            values,
        } if *arities == [0.into(), 1.into()] => match (*tag, values.as_slice()) {
            (0, []) => Ok(None),
            (1, [v]) => some(v).map(Some),
            _ => Err(Native::FunctionClause),
        },
        _ => Err(Native::FunctionClause),
    }
}
fn stakers(v: &Value) -> Result<Vec<Staker>, Native> {
    list(v)?
        .iter()
        .map(|v| {
            let p = clause_tuple(v, 2)?;
            Ok(Staker {
                address: address(&p[0])?,
                stake: p[1].clone(),
            })
        })
        .collect()
}
pub fn decode_epoch_info(epoch: Value, v: &Value) -> Result<EpochInfo, Native> {
    let p = tuple(v, 4)?;
    let seed = option(&p[2], bytes)?;
    let validators = option(&p[3], stakers)?;
    let first = number(&p[0])?.clone();
    let length = number(&p[1])?.clone();
    let last = &first + &length - 1;
    Ok(EpochInfo {
        epoch,
        first,
        length,
        last,
        seed,
        validators,
    })
}
pub fn decode_finalize(v: &Value) -> Result<FinalizeInfo, Native> {
    let p = clause_tuple(v, 7)?;
    // Header patterns match before any option decoder is entered.
    let parent_hash = bytes(&p[3])?;
    let producer = address(&p[4])?;
    let epoch_length = option(&p[2], |v| Ok(v.clone()))?;
    let fork = option(&p[1], bytes)?;
    let votes = option(&p[5], |v| {
        list(v)?
            .iter()
            .map(|v| {
                let p = clause_tuple(v, 4)?;
                Ok(ForkVote {
                    producer: address(&p[0])?,
                    hash: bytes(&p[1])?,
                    signature: {
                        bytes(&p[2])?;
                        bytes(&p[3])?
                    },
                })
            })
            .collect()
    })?;
    let length_votes = option(&p[6], |v| {
        list(v)?
            .iter()
            .map(|v| {
                let p = clause_tuple(v, 4)?;
                Ok(LengthVote {
                    producer: address(&p[0])?,
                    epoch_delta: p[1].clone(),
                    signature: {
                        bytes(&p[2])?;
                        bytes(&p[3])?
                    },
                })
            })
            .collect()
    })?;
    Ok(FinalizeInfo {
        epoch: p[0].clone(),
        fork,
        epoch_length,
        parent_hash,
        producer,
        votes,
        length_votes,
    })
}
#[derive(Clone)]
pub struct Reader<A, C, O, S = Selected<A, C>> {
    pub selected: S,
    pub profile: Arc<Profile<O>>,
    // Keep the tree types explicit even for a backend whose snapshot resolves
    // them lazily. This private field is supplied by the constructor.
    marker: std::marker::PhantomData<fn() -> (A, C)>,
}
pub type PinnedReader<A, C, O, S> = Reader<A, C, O, source::Pinned<S>>;
pub type PinResult<A, C, O, S> = Result<PinnedReader<A, C, O, S>, Error<A, C>>;
impl<A, C, O, S> Reader<A, C, O, S> {
    pub fn new(selected: S, profile: Arc<Profile<O>>) -> Self {
        Self {
            selected,
            profile,
            marker: std::marker::PhantomData,
        }
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, S: Source<A, C>>
    Reader<A, C, O, S>
{
    pub fn pin(&self) -> PinResult<A, C, O, S::Snapshot> {
        Ok(Reader::new(
            source::Pinned(self.selected.capture()?),
            self.profile.clone(),
        ))
    }
    pub fn view(&self, env: &RunEnv<A, C>) -> Result<Arc<View<A, C>>, Error<A, C>> {
        if let RunEnv::View(view) = env {
            return Ok(view.clone());
        }
        if matches!(env, RunEnv::Height(h) if h.sign() == Sign::Minus) {
            return Err(Error::Native(Native::FunctionClause));
        }
        let selection = self.selected.capture()?;
        let height;
        let hash = match env {
            RunEnv::Top => {
                height = selection.top_height()?.ok_or(Error::ChainTooShort)?;
                if height.sign() == Sign::Minus {
                    return Err(Error::Native(Native::FunctionClause));
                }
                std::borrow::Cow::Owned(selection.key_hash(&height)?.ok_or(Error::ChainTooShort)?)
            }
            RunEnv::Height(h) => {
                if h.sign() == Sign::Minus {
                    return Err(Error::Native(Native::FunctionClause));
                }
                std::borrow::Cow::Owned(selection.key_hash(h)?.ok_or(Error::ChainTooShort)?)
            }
            RunEnv::Hash(h) => std::borrow::Cow::Borrowed(h.as_slice()),
            RunEnv::View(_) => unreachable!(),
        };
        selection.view(&hash).map_err(Into::into)
    }
    pub fn query(
        &self,
        env: &RunEnv<A, C>,
        contract: Contract,
        endpoint: &[u8],
        arguments: Vec<Value>,
    ) -> Result<Value, Error<A, C>> {
        let view = self.view(env)?;
        self.with_execution(&view, |execution| {
            consensus_contract::query(
                self.profile.contracts,
                contract,
                &view.trees,
                &view.context,
                execution,
                endpoint,
                arguments,
                self.profile.limits,
            )
            .map_err(|e| Error::Contract(Box::new(e)))
        })
    }
    pub(crate) fn with_execution<T>(
        &self,
        view: &View<A, C>,
        f: impl FnOnce(&transactions::Config<'_, O>) -> T,
    ) -> T {
        let p = &self.profile;
        let vm = contracts::fate::Config {
            context: &p.context,
            order: &p.order,
            overrides: &p.overrides,
            environment: &view.environment,
            history: view.history.clone(),
            auth_tx: None,
            metering_floor: None,
            diagnostic_profile: p.diagnostic,
        };
        let execution = transactions::Config {
            network: &p.network,
            consensus: match view.environment.consensus {
                machine::program::remote::context::Consensus::Pow => ConsensusType::Pow,
                machine::program::remote::context::Consensus::Pos => ConsensusType::Pos,
            },
            fate: Some(&vm),
            aevm: view.aevm.as_ref(),
            signature_limits: Default::default(),
            channel_limits: Default::default(),
            channel_state_atoms: None,
            oracle_limits: Default::default(),
            fate_limits: p.limits,
            maximum_fate_steps: p.maximum_steps,
            aevm_limits: Default::default(),
        };
        f(&execution)
    }
    pub fn epoch(&self, env: &RunEnv<A, C>) -> Result<Value, Error<A, C>> {
        self.query(env, Contract::Election, b"epoch", vec![])
    }
    pub fn epoch_length(&self, env: &RunEnv<A, C>) -> Result<Value, Error<A, C>> {
        self.query(env, Contract::Election, b"epoch_length", vec![])
    }
    fn required_query(
        &self,
        env: &RunEnv<A, C>,
        contract: Contract,
        endpoint: &[u8],
        arguments: Vec<Value>,
    ) -> Result<Value, Error<A, C>> {
        self.query(env, contract, endpoint, arguments)
            .map_err(|e| match e {
                Error::ChainTooShort => Error::Native(Native::BadmatchChainTooShort),
                e => e,
            })
    }
    pub fn epoch_info(&self, env: &RunEnv<A, C>) -> Result<EpochInfo, Error<A, C>> {
        let v = self.required_query(env, Contract::Election, b"epoch_info", vec![])?;
        let p = tuple(&v, 2).map_err(|_| Error::Native(Native::BadmatchQuery(v.clone())))?;
        decode_epoch_info(p[0].clone(), &p[1]).map_err(Error::Native)
    }
    pub fn epoch_info_for_epoch(
        &self,
        env: &RunEnv<A, C>,
        epoch: Value,
    ) -> Result<EpochInfo, Error<A, C>> {
        let v = self.required_query(
            env,
            Contract::Election,
            b"epoch_info_epoch",
            vec![epoch.clone()],
        )?;
        decode_epoch_info(epoch, &v).map_err(Error::Native)
    }
    pub fn validators(&self, env: &RunEnv<A, C>) -> Result<Vec<Vec<Value>>, Error<A, C>> {
        let v = self.required_query(env, Contract::Staking, b"sorted_validators", vec![])?;
        list(&v)
            .map_err(Error::Native)?
            .iter()
            .map(|v| {
                if let Value::Tuple(v) = v {
                    Ok(v.clone())
                } else {
                    Err(Error::Native(Native::FunctionClause))
                }
            })
            .collect()
    }
    pub fn validator_schedule(
        &self,
        env: &RunEnv<A, C>,
        seed: Vec<u8>,
        validators: Vec<Staker>,
        length: Value,
    ) -> Result<Vec<ObjectPayload>, Error<A, C>> {
        let args = vec![
            Value::Bytes(seed),
            Value::List(
                validators
                    .into_iter()
                    .map(|s| {
                        Value::Tuple(vec![Value::Object(ObjectKind::Address, s.address), s.stake])
                    })
                    .collect(),
            ),
            length,
        ];
        let v = self.required_query(env, Contract::Election, b"validator_schedule", args)?;
        list(&v)
            .map_err(Error::Native)?
            .iter()
            .map(|v| address(v).map_err(Error::Native))
            .collect()
    }
    pub fn pin_info(&self, env: &RunEnv<A, C>) -> Result<Option<Value>, Error<A, C>> {
        let v = self.required_query(env, Contract::Election, b"pin_info", vec![])?;
        option(&v, |v| Ok(v.clone())).map_err(Error::Native)
    }
    pub fn pin_reward(&self, env: &RunEnv<A, C>) -> Result<PinReward, Error<A, C>> {
        let v = self.required_query(env, Contract::Election, b"pin_reward_info", vec![])?;
        let p = tuple(&v, 3).map_err(Error::Native)?;
        Ok(PinReward {
            base: p[0].clone(),
            current: p[1].clone(),
            carry_over: p[2].clone(),
        })
    }
    pub fn finalize_info(&self, env: &RunEnv<A, C>) -> Result<Option<FinalizeInfo>, Error<A, C>> {
        let v = self.required_query(env, Contract::Election, b"finalize_info", vec![])?;
        option(&v, decode_finalize).map_err(Error::Native)
    }
    pub fn epoch_start_height(&self, epoch: &BigInt) -> Result<BigInt, Error<A, C>> {
        let reader = self.pin()?;
        let mut height = reader
            .selected
            .0
            .top_height()?
            .ok_or(Error::ChainTooShort)?;
        for _ in 0..self.profile.maximum_history_reads {
            let info = reader.epoch_info(&RunEnv::Height(height))?;
            // Erlang's guards compare arbitrary terms. Integer contract epochs
            // take the ordinary path; other decoded terms compare above numbers.
            if let Value::Integer(n) = info.epoch {
                if &n == epoch {
                    return Ok(info.first);
                }
                if &n < epoch {
                    return Err(Error::ChainTooShort);
                }
            }
            height = info.first - 2;
        }
        Err(Error::ResourceLimit)
    }
}
#[derive(Clone)]
pub struct ParentTargets<A, C, O, S = Selected<A, C>> {
    pub reader: Reader<A, C, O, S>,
    pub epoch_length: Unsigned,
    pub start_height: Unsigned,
}
pub fn target_heights(epoch: &BigInt, length: &Unsigned, start: &Unsigned) -> Vec<BigInt> {
    let length = BigInt::from_bytes_be(Sign::Plus, length.as_bytes());
    let start = BigInt::from_bytes_be(Sign::Plus, start.as_bytes());
    vec![(epoch - 2) * &length + &start, (epoch + 2) * length + start]
}
impl<
    A: NodeWriter + Clone + Send + Sync + 'static,
    C: NodeWriter + Clone + Send + Sync + 'static,
    O: MapOrder + Clone + Send + Sync + 'static,
    S: Source<A, C> + Send + Sync + 'static,
> Targets for ParentTargets<A, C, O, S>
{
    fn targets(&mut self, height: &Unsigned) -> Result<Vec<BigInt>, TargetError> {
        match self.reader.epoch(&RunEnv::Height(BigInt::from_bytes_be(
            Sign::Plus,
            height.as_bytes(),
        ))) {
            Ok(Value::Integer(epoch)) => Ok(target_heights(
                &epoch,
                &self.epoch_length,
                &self.start_height,
            )),
            Ok(_) => Err(TargetError::Native),
            Err(error) if !error.is_operational() => Err(TargetError::Native),
            Err(_) => Err(TargetError::Unavailable),
        }
    }
}
