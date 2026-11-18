// aefa_fate_op:make_fate_tx and make_fate_base_tx, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error, names, remote::State};
use ae_codec::{
    object::{self, ObjectTag, Value as Field},
    rlp::{DecodeLimits, Rlp},
};
use ae_trie::NodeReader;
use ae_tx::{TransactionKind as Kind, WireTransaction};
use ae_types::{Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits,
    machine::{
        Exception,
        program::remote::{HostFailure, names::Read},
    },
    types::Version,
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};
use std::borrow::Cow;

type Failure = HostFailure<Error>;
fn native(reason: &'static str) -> Failure {
    HostFailure::Exception(Exception::Native(reason))
}
fn scalar(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn integer(tx: &WireTransaction, name: &str) -> Result<BigInt, Failure> {
    match tx.field(name) {
        Some(Field::Integer(n)) => Ok(scalar(n)),
        _ => Err(native("function_clause")),
    }
}
fn id(tx: &WireTransaction, name: &str) -> Result<Id, Failure> {
    match tx.field(name) {
        Some(Field::Id(id)) => Ok(*id),
        _ => Err(native("function_clause")),
    }
}
fn key(tx: &WireTransaction, name: &str, tag: IdTag) -> Result<[u8; 32], Failure> {
    let id = id(tx, name)?;
    if id.tag != tag {
        return Err(native("function_clause"));
    }
    Ok(id.value)
}
fn bytes<'a>(tx: &'a WireTransaction, name: &str) -> Result<&'a [u8], Failure> {
    match tx.field(name) {
        Some(Field::Binary(b)) => Ok(b),
        _ => Err(native("function_clause")),
    }
}
fn address(key: impl Into<Vec<u8>>) -> Value {
    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(key.into()))
}
fn variant(arities: &[u8], tag: usize, values: Vec<Value>) -> Value {
    Value::Variant {
        arities: arities.iter().map(|n| (*n).into()).collect(),
        tag,
        values,
    }
}
fn option(value: Option<Value>) -> Value {
    variant(
        &[0, 1],
        usize::from(value.is_some()),
        value.into_iter().collect(),
    )
}
fn wrapper(tx: &WireTransaction, field: &str) -> Result<Value, Failure> {
    Ok(variant(
        &[2],
        0,
        vec![
            address(key(tx, field, IdTag::Account)?),
            Value::Integer(integer(tx, "fee")?),
        ],
    ))
}
fn budget_value(value: &Field, limits: &mut Limits, depth: usize) -> Result<(), Failure> {
    if depth > limits.max_depth {
        return Err(HostFailure::ResourceLimit);
    }
    limits.max_nodes = limits
        .max_nodes
        .checked_sub(1)
        .ok_or(HostFailure::ResourceLimit)?;
    let size = match value {
        Field::Integer(n) => n.as_bytes().len(),
        Field::Binary(b) => b.len(),
        Field::Id(_) => 33,
        Field::Boolean(_) => 0,
        Field::List(v) | Field::Tuple(v) => {
            for v in v {
                budget_value(v, limits, depth + 1)?;
            }
            0
        }
        Field::Record(v) => {
            for (k, v) in v {
                limits.max_bytes = limits
                    .max_bytes
                    .checked_sub(k.len())
                    .ok_or(HostFailure::ResourceLimit)?;
                budget_value(v, limits, depth + 1)?;
            }
            0
        }
    };
    limits.max_bytes = limits
        .max_bytes
        .checked_sub(size)
        .ok_or(HostFailure::ResourceLimit)?;
    Ok(())
}
fn decode_limits(limits: Limits) -> DecodeLimits {
    DecodeLimits {
        max_input_bytes: limits.max_bytes,
        max_items: limits.max_nodes,
        max_depth: limits.max_depth,
    }
}
fn decode_error(error: ae_tx::Error) -> Failure {
    if matches!(
        error,
        ae_tx::Error::Object(object::ObjectError::Rlp(
            ae_codec::rlp::DecodeError::ResourceLimit
        ))
    ) {
        HostFailure::ResourceLimit
    } else {
        HostFailure::Backend(Error::AuthenticationTransaction(error))
    }
}
fn inner(tx: &WireTransaction, limits: Limits) -> Result<WireTransaction, Failure> {
    let signed = object::decode_envelope_with_limits(bytes(tx, "tx")?, decode_limits(limits))
        .map_err(|e| decode_error(e.into()))?;
    if signed.tag != ObjectTag::SignedTx || signed.version != 1.into() {
        return Err(native("function_clause"));
    }
    let [Rlp::List(_), Rlp::Bytes(raw)] = signed.fields.as_slice() else {
        return Err(native("function_clause"));
    };
    WireTransaction::decode_with_limits(raw, decode_limits(limits)).map_err(decode_error)
}

// The environment supplies a transaction already admitted by its callback.
// Schema decoding here exposes fields; it does not authenticate GA/payer wrappers.
pub fn project<A: NodeReader, C: NodeReader>(
    state: &State<A, C>,
    vm: Version,
    mut limits: Limits,
) -> Result<(BigInt, Value), Failure> {
    let Some(root) = &state.auth_tx else {
        let value = option(None);
        ae_vm_fate::value::check_limits(&value, limits).map_err(|_| HostFailure::ResourceLimit)?;
        return Ok((8.into(), value));
    };
    let mut tx = Cow::Borrowed(root.as_ref());
    let mut metas = Vec::new();
    let mut payer = None;
    let mut wrappers = 0usize;
    loop {
        for (_, value) in tx.fields() {
            budget_value(value, &mut limits, 0)?;
        }
        match tx.kind() {
            Kind::GaMeta => metas.push(wrapper(&tx, "ga_id")?),
            Kind::PayingFor => payer = Some(wrapper(&tx, "payer_id")?),
            _ => break,
        }
        wrappers = wrappers.checked_add(1).ok_or(HostFailure::ResourceLimit)?;
        if wrappers > limits.max_depth {
            return Err(HostFailure::ResourceLimit);
        }
        tx = Cow::Owned(inner(&tx, limits)?);
    }
    let (base_cells, base) = base(state, &tx, vm, limits)?;
    let origin_field = match tx.kind() {
        Kind::Spend | Kind::OracleQuery => "sender_id",
        Kind::OracleRegister
        | Kind::NameClaim
        | Kind::NamePreclaim
        | Kind::NameUpdate
        | Kind::NameTransfer
        | Kind::NameRevoke => "account_id",
        Kind::OracleExtend | Kind::OracleResponse => "oracle_id",
        Kind::ContractCall => "caller_id",
        Kind::ContractCreate | Kind::GaAttach => "owner_id",
        Kind::ChannelCreate => "initiator_id",
        Kind::ChannelWithdraw => "to_id",
        _ => "from_id",
    };
    let origin = if tx.kind() == Kind::ContractCall {
        id(&tx, "caller_id")?.value
    } else {
        key(
            &tx,
            origin_field,
            if matches!(tx.kind(), Kind::OracleExtend | Kind::OracleResponse) {
                IdTag::Oracle
            } else {
                IdTag::Account
            },
        )?
    };
    let ttl = integer(&tx, "ttl")?;
    let cells = base_cells
        + if payer.is_some() { 23u64 } else { 8 }
        + BigInt::from(metas.len()) * 14u64
        + 24u64;
    let value = option(Some(Value::Tuple(vec![
        option(payer),
        Value::List(metas),
        address(origin),
        Value::Integer(integer(&tx, "fee")?),
        if ttl == 0.into() {
            Value::MaxTtl
        } else {
            Value::Integer(ttl)
        },
        base,
    ])));
    ae_vm_fate::value::check_limits(&value, limits).map_err(|_| HostFailure::ResourceLimit)?;
    Ok((cells, value))
}
fn base<A: NodeReader, C: NodeReader>(
    state: &State<A, C>,
    tx: &WireTransaction,
    vm: Version,
    limits: Limits,
) -> Result<(BigInt, Value), Failure> {
    use Kind::*;
    let mut extra = 0usize;
    let (tag, values) = match tx.kind() {
        Spend => {
            let payload = bytes(tx, "payload")?;
            extra = 4 + 2 + payload.len() / 8;
            (
                0,
                vec![
                    address(id(tx, "recipient_id")?.value),
                    Value::Integer(integer(tx, "amount")?),
                    Value::String(payload.to_vec()),
                ],
            )
        }
        OracleRegister => (1, vec![]),
        OracleQuery => (2, vec![]),
        OracleResponse => (3, vec![]),
        OracleExtend => (4, vec![]),
        NamePreclaim => (5, vec![]),
        NameClaim => {
            let name = bytes(tx, "name")?;
            extra = name.len() / 8;
            (6, vec![Value::String(name.to_vec())])
        }
        NameUpdate | NameRevoke => {
            extra = 4;
            (
                if tx.kind() == NameUpdate { 7 } else { 8 },
                vec![Value::Bytes(key(tx, "name_id", IdTag::Name)?.to_vec())],
            )
        }
        NameTransfer => {
            extra = 8;
            (
                9,
                vec![
                    address(key(tx, "recipient_id", IdTag::Account)?),
                    Value::Bytes(key(tx, "name_id", IdTag::Name)?.to_vec()),
                ],
            )
        }
        ChannelCreate => {
            extra = 4;
            (10, vec![address(key(tx, "responder_id", IdTag::Account)?)])
        }
        ChannelDeposit | ChannelWithdraw => {
            extra = 6;
            (
                if tx.kind() == ChannelDeposit { 11 } else { 12 },
                vec![
                    address(key(tx, "channel_id", IdTag::Channel)?),
                    Value::Integer(integer(tx, "amount")?),
                ],
            )
        }
        ChannelForceProgress | ChannelCloseMutual | ChannelCloseSolo | ChannelSlash
        | ChannelSettle | ChannelSnapshotSolo => {
            extra = 4;
            let tag = match tx.kind() {
                ChannelForceProgress => 13,
                ChannelCloseMutual => 14,
                ChannelCloseSolo => 15,
                ChannelSlash => 16,
                ChannelSettle => 17,
                _ => 18,
            };
            (tag, vec![address(key(tx, "channel_id", IdTag::Channel)?)])
        }
        ContractCreate => {
            extra = 2;
            (19, vec![Value::Integer(integer(tx, "amount")?)])
        }
        ContractCall => {
            let target = id(tx, "contract_id")?;
            extra = 6;
            let key = match target.tag {
                IdTag::Contract => target.value.to_vec(),
                IdTag::Name => {
                    if vm < Version::Three {
                        return Err(HostFailure::Unavailable("make_fate_base_tx"));
                    }
                    let (tree, cache) = match &state.onchain {
                        Some(v) => (&v.names, &v.name_cache),
                        None => (&state.names, &state.name_cache),
                    };
                    cache.budget(limits)?;
                    // The original deliberately ignores _API1: this read does not publish its cache.
                    match names::read_hash(
                        tree,
                        &mut cache.clone(),
                        target.value,
                        Some(b"contract_pubkey"),
                        limits,
                    )? {
                        Some(Read::Pointer(ae_codec::name_pointer::PointerValue::Id(id))) => {
                            extra += 2000;
                            id.value.to_vec()
                        }
                        Some(Read::Pointer(ae_codec::name_pointer::PointerValue::Data(bytes))) => {
                            extra += 2000;
                            bytes
                        }
                        _ => {
                            return Err(HostFailure::Primop {
                                operation: "aens_resolve",
                                reason: "no_contract_pubkey",
                            });
                        }
                    }
                }
                _ => return Err(native("case_clause")),
            };
            (
                20,
                vec![address(key), Value::Integer(integer(tx, "amount")?)],
            )
        }
        GaAttach => (21, vec![]),
        other => {
            return Err(HostFailure::AuthTransactionType(match other {
                ChannelOffchain => "channel_offchain_tx",
                ChannelSetDelegates => "channel_set_delegates_tx",
                HcVote => "hc_vote_tx",
                _ => unreachable!("wrappers removed"),
            }));
        }
    };
    let cells = BigInt::from(48usize) + BigInt::from(values.len()) + BigInt::from(extra);
    Ok((
        cells,
        variant(
            &[
                3, 0, 0, 0, 0, 0, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 0,
            ],
            tag,
            values,
        ),
    ))
}
