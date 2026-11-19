use super::*;
use ae_vm_fate::{
    maps::{MapOrder, Traversal},
    value::{ObjectKind, ObjectPayload, Value},
};
use num_traits::{FromPrimitive, ToPrimitive};
use payload::{FORK_HASH, HEIGHT, LENGTH_DELTA, SIGNATURE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientState {
    Fork {
        hash: Option<Vec<u8>>,
    },
    Length {
        length: Option<BigInt>,
        delta: Option<BigInt>,
    },
}
impl ClientState {
    pub fn empty(mode: Mode) -> Self {
        match mode {
            Mode::Fork => Self::Fork { hash: None },
            Mode::Length => Self::Length {
                length: None,
                delta: None,
            },
        }
    }
    pub fn mode(&self) -> Mode {
        match self {
            Self::Fork { .. } => Mode::Fork,
            Self::Length { .. } => Mode::Length,
        }
    }
    pub fn initialize(
        &mut self,
        epoch: &Unsigned,
        hash: &[u8],
        parents: &BTreeMap<Unsigned, ParentBlock>,
        length: &Unsigned,
        block_time: &Unsigned,
    ) -> Result<(), Failure> {
        *self = match self {
            Self::Fork { .. } => Self::Fork {
                hash: Some(hash.to_vec()),
            },
            Self::Length { .. } => Self::Length {
                length: Some(big(length)),
                delta: Some(calculate_delta(epoch, parents, length, block_time)?),
            },
        };
        Ok(())
    }
    pub fn proposal(&self, height: &Unsigned) -> Result<Payload, Failure> {
        let (name, value) = match self {
            Self::Fork { hash: Some(hash) } => (FORK_HASH, Field::Binary(hash.clone())),
            Self::Length {
                delta: Some(delta), ..
            } => (LENGTH_DELTA, Field::Integer(delta.clone())),
            _ => return Err(Failure::Native(Native::FunctionClause)),
        };
        Ok(BTreeMap::from([
            (HEIGHT.to_vec(), Field::Integer(big(height))),
            (name.to_vec(), value),
        ]))
    }
    pub fn vote(&self, proposal: &Payload, height: &Unsigned) -> Result<Option<Payload>, Failure> {
        let expected = self.proposal(height)?;
        let key = match self {
            Self::Fork { .. } => FORK_HASH,
            Self::Length { .. } => LENGTH_DELTA,
        };
        Ok((proposal.get(key) == expected.get(key)).then_some(expected))
    }
    fn finalize_argument(&self, proposal: &Payload) -> Result<(&'static [u8], Value), Failure> {
        match self {
            Self::Fork { .. } => match proposal.get(FORK_HASH) {
                Some(Field::Binary(hash)) => Ok((b"finalize_epoch", Value::Bytes(hash.clone()))),
                _ => Err(Failure::Native(Native::FunctionClause)),
            },
            Self::Length {
                length: Some(length),
                ..
            } => match proposal.get(LENGTH_DELTA) {
                Some(Field::Integer(delta)) => {
                    Ok((b"finalize_epoch_length", Value::Integer(length + delta)))
                }
                _ => Err(Failure::Native(Native::FunctionClause)),
            },
            _ => Err(Failure::Native(Native::Badarith)),
        }
    }
}
pub fn calculate_delta(
    epoch: &Unsigned,
    parents: &BTreeMap<Unsigned, ParentBlock>,
    length: &Unsigned,
    block_time: &Unsigned,
) -> Result<BigInt, Failure> {
    if epoch <= &4.into() {
        return Ok(0.into());
    }
    let expected = big(length) * big(block_time);
    let next = epoch.add(&1.into());
    let actual = match (parents.get(epoch), parents.get(&next)) {
        (Some(a), Some(b)) => big(&b.time) - big(&a.time),
        _ => expected.clone(),
    };
    let numerator = (actual - expected)
        .to_f64()
        .filter(|n| n.is_finite())
        .ok_or(Failure::Native(Native::Badarith))?;
    let denominator = big(block_time)
        .to_f64()
        .filter(|n| n.is_finite() && *n != 0.0)
        .ok_or(Failure::Native(Native::Badarith))?;
    let ratio = numerator / denominator;
    if !ratio.is_finite() {
        return Err(Failure::Native(Native::Badarith));
    }
    let rounded = BigInt::from_f64(if ratio < 0.0 {
        ratio.ceil()
    } else {
        ratio.floor()
    })
    .ok_or(Failure::Native(Native::Badarith))?;
    Ok(if ratio < 0.0 && rounded <= -big(length) {
        BigInt::from(1) - big(length)
    } else {
        rounded
    })
}
fn address(k: AccountPublicKey) -> Value {
    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(k.0.to_vec()))
}
pub struct FinalizeInput<'a> {
    pub client: &'a ClientState,
    pub epoch: &'a Unsigned,
    pub leader: AccountPublicKey,
    pub seed: &'a [u8],
    pub proposal: &'a Payload,
    pub votes: &'a BTreeMap<AccountPublicKey, Payload>,
}
pub fn finalize_calldata(
    input: FinalizeInput<'_>,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Vec<u8>, Failure> {
    let FinalizeInput {
        client,
        epoch,
        leader,
        seed,
        proposal,
        votes,
    } = input;
    if votes.len() > limits.maximum_validators || seed.len() > limits.maximum_bytes {
        return Err(Failure::ResourceLimit);
    }
    let mut budget = Budget::new(limits);
    budget.payload(proposal)?;
    budget.add(seed.len())?;
    budget.add(epoch.as_bytes().len())?;
    for fields in votes.values() {
        budget.add(32)?;
        budget.payload(fields)?;
    }
    let (name, argument) = client.finalize_argument(proposal)?;
    let keys: Vec<_> = votes.keys().map(|k| Value::String(k.0.to_vec())).collect();
    let indices = order.indices(
        &keys.iter().collect::<Vec<_>>(),
        Traversal::Fold,
        limits.fate,
    )?;
    if indices.len() != keys.len() {
        return Err(Failure::InvalidOrder);
    }
    let mut seen = vec![false; keys.len()];
    let entries: Vec<_> = votes.iter().collect();
    let mut values = Vec::new();
    for i in indices {
        let slot = seen.get_mut(i).ok_or(Failure::InvalidOrder)?;
        if *slot {
            return Err(Failure::InvalidOrder);
        }
        *slot = true;
        let (voter, fields) = entries[i];
        payload::check_payload(fields, limits)?;
        let parameter = match client.mode() {
            Mode::Fork => match fields.get(FORK_HASH) {
                Some(Field::Binary(b)) => Value::Bytes(b.clone()),
                _ => return Err(Failure::Native(Native::FunctionClause)),
            },
            Mode::Length => match fields.get(LENGTH_DELTA) {
                Some(Field::Integer(n)) => Value::Integer(n.clone()),
                _ => return Err(Failure::Native(Native::FunctionClause)),
            },
        };
        let Some(Field::Binary(signature)) = fields.get(SIGNATURE) else {
            return Err(Failure::Native(Native::FunctionClause));
        };
        values.push(Value::Tuple(vec![
            address(*voter),
            parameter,
            Value::Bytes(payload::sign_data(fields, limits)?),
            Value::Bytes(signature.clone()),
        ]));
    }
    values.reverse(); // maps:fold prepends every vote to its accumulator.
    let encoded = ae_vm_fate::abi::create_calldata(
        name,
        vec![
            Value::Integer(big(epoch)),
            argument,
            Value::Bytes(seed.to_vec()),
            address(leader),
            Value::List(values),
        ],
        limits.fate,
    )?;
    if encoded.len() > limits.maximum_bytes {
        return Err(Failure::ResourceLimit);
    }
    Ok(encoded)
}
