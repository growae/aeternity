// aec_consensus_hc init_epochs, step, step_micro, step_eoe, add_reward.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::consensus_contract::{self as cc, Contract, Contracts};
use ae_execution::{Context, transactions};
use ae_state::Trees;
use ae_trie::NodeWriter;
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::{
    abi,
    maps::MapOrder,
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::BigInt;

#[derive(Debug, Clone)]
pub enum Transition {
    Initialize {
        length: BigInt,
        pin_reward: BigInt,
    },
    Key {
        leader: AccountPublicKey,
    },
    Micro {
        leader: AccountPublicKey,
    },
    End {
        leader: AccountPublicKey,
        seed: Vec<u8>,
        next_pin_reward: BigInt,
        carry_over: bool,
    },
    Reward {
        height: BigInt,
        beneficiary: AccountPublicKey,
        amount: Unsigned,
    },
}
fn address(key: AccountPublicKey) -> Value {
    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(key.0.to_vec()))
}
impl Transition {
    fn call(&self) -> (&'static [u8], Vec<Value>, Unsigned) {
        let (name, args) = match self {
            Self::Initialize { length, pin_reward } => (
                b"init_epochs".as_slice(),
                vec![
                    Value::Integer(length.clone()),
                    Value::Integer(pin_reward.clone()),
                ],
            ),
            Self::Key { leader } => (b"step".as_slice(), vec![address(*leader)]),
            Self::Micro { leader } => (b"step_micro".as_slice(), vec![address(*leader)]),
            Self::End {
                leader,
                seed,
                next_pin_reward,
                carry_over,
            } => (
                b"step_eoe".as_slice(),
                vec![
                    address(*leader),
                    Value::Bytes(seed.clone()),
                    Value::Integer(next_pin_reward.clone()),
                    Value::Boolean(*carry_over),
                ],
            ),
            Self::Reward {
                height,
                beneficiary,
                amount,
            } => {
                return (
                    b"add_reward",
                    vec![Value::Integer(height.clone()), address(*beneficiary)],
                    amount.clone(),
                );
            }
        };
        (name, args, 0.into())
    }
}
// The VM owns staking/election semantics. No parallel Rust reimplementation of
// those contract algorithms may replace execution of the deployed bytecode.
pub fn apply<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    contracts: Contracts,
    transition: &Transition,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, cc::Error<A, C>> {
    let (name, args, amount) = transition.call();
    let calldata =
        abi::create_calldata(name, args, execution.fate_limits).map_err(cc::Error::Calldata)?;
    let prepared = cc::prepare(contracts, Contract::Election, trees, calldata, amount)?;
    Ok(cc::execute(prepared, context, execution)?.trees)
}
