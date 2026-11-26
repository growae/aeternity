use super::*;
use ae_codec::name_pointer::PointerValue;
use ae_protocol::oracles::Ttl;
use ae_tx::{ContractOperation, NameOperation, OracleOperation, TransactionLeaf};

fn ttl(budget: &Budget, kind: &str, value: &Unsigned) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    out.text("type", kind)?;
    out.number("value", value)?;
    Ok(out.finish())
}
fn typed_ttl(budget: &Budget, value: &Ttl) -> Result<Value, Error> {
    match value {
        Ttl::Delta(n) => ttl(budget, "delta", n),
        Ttl::Block(n) => ttl(budget, "block", n),
    }
}
pub(super) fn render(tx: &TransactionLeaf, budget: &Budget) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    let mut version = 1;
    let kind = match tx {
        TransactionLeaf::Spend(t) => {
            out.encoded("sender_id", ApiType::AccountPubkey, t.sender().as_ref())?;
            out.id("recipient_id", t.recipient())?;
            out.number("amount", t.amount())?;
            out.number("fee", t.fee())?;
            out.ttl(t.ttl())?;
            out.number("nonce", t.nonce())?;
            out.encoded("payload", ApiType::Bytearray, t.payload())?;
            "SpendTx"
        }
        TransactionLeaf::Name(t) => {
            out.encoded("account_id", ApiType::AccountPubkey, t.account().as_ref())?;
            out.number("nonce", t.nonce())?;
            out.number("fee", t.fee())?;
            out.ttl(t.ttl())?;
            match t.operation() {
                NameOperation::Preclaim { commitment } => {
                    out.encoded("commitment_id", ApiType::Commitment, commitment)?;
                    "NamePreclaimTx"
                }
                NameOperation::Claim {
                    name,
                    salt,
                    name_fee,
                } => {
                    // The callback's version is 2 even for legacy wire v1.
                    version = 2;
                    out.raw("name", name)?;
                    out.number("name_salt", salt)?;
                    if let Some(fee) = name_fee {
                        out.number("name_fee", fee)?;
                    }
                    "NameClaimTx"
                }
                NameOperation::Update {
                    name,
                    name_ttl,
                    client_ttl,
                    pointers,
                } => {
                    out.encoded("name_id", ApiType::Name, name)?;
                    out.number("name_ttl", name_ttl)?;
                    out.number("client_ttl", client_ttl)?;
                    out.insert(
                        "pointers",
                        budget.array(pointers, |p| {
                            let mut v = Object::new(budget)?;
                            v.raw("key", &p.key)?;
                            v.encoded("encoded_key", ApiType::Bytearray, &p.key)?;
                            match &p.value {
                                PointerValue::Id(id) => v.id("id", *id)?,
                                PointerValue::Data(data) => {
                                    version = 2;
                                    v.encoded("id", ApiType::Bytearray, data)?;
                                }
                            }
                            Ok(v.finish())
                        })?,
                    )?;
                    "NameUpdateTx"
                }
                NameOperation::Transfer { name, recipient } => {
                    out.encoded("name_id", ApiType::Name, name)?;
                    out.id("recipient_id", *recipient)?;
                    "NameTransferTx"
                }
                NameOperation::Revoke { name } => {
                    out.encoded("name_id", ApiType::Name, name)?;
                    "NameRevokeTx"
                }
            }
        }
        TransactionLeaf::Oracle(t) => {
            out.number("nonce", t.nonce())?;
            out.number("fee", t.fee())?;
            out.ttl(t.ttl())?;
            match t.operation() {
                OracleOperation::Register {
                    query_format,
                    response_format,
                    query_fee,
                    oracle_ttl,
                    abi_version,
                } => {
                    out.encoded("account_id", ApiType::AccountPubkey, t.account().as_ref())?;
                    out.raw("query_format", query_format)?;
                    out.raw("response_format", response_format)?;
                    out.number("query_fee", query_fee)?;
                    out.insert("oracle_ttl", typed_ttl(budget, oracle_ttl)?)?;
                    out.number("abi_version", abi_version)?;
                    "OracleRegisterTx"
                }
                OracleOperation::Extend { delta } => {
                    out.encoded("oracle_id", ApiType::OraclePubkey, t.account().as_ref())?;
                    out.insert("oracle_ttl", ttl(budget, "delta", delta)?)?;
                    "OracleExtendTx"
                }
                OracleOperation::Query {
                    oracle,
                    query,
                    query_fee,
                    query_ttl,
                    response_ttl,
                } => {
                    out.encoded("sender_id", ApiType::AccountPubkey, t.account().as_ref())?;
                    out.id("oracle_id", *oracle)?;
                    out.raw("query", query)?;
                    out.number("query_fee", query_fee)?;
                    out.insert("query_ttl", typed_ttl(budget, query_ttl)?)?;
                    out.insert("response_ttl", ttl(budget, "delta", response_ttl)?)?;
                    "OracleQueryTx"
                }
                OracleOperation::Response {
                    query_id,
                    response,
                    response_ttl,
                } => {
                    out.encoded("oracle_id", ApiType::OraclePubkey, t.account().as_ref())?;
                    out.encoded("query_id", ApiType::OracleQueryId, query_id)?;
                    out.raw("response", response)?;
                    out.insert("response_ttl", ttl(budget, "delta", response_ttl)?)?;
                    "OracleRespondTx"
                }
            }
        }
        TransactionLeaf::Contract(t) => {
            out.number("nonce", t.nonce())?;
            out.number("fee", t.fee())?;
            out.ttl(t.ttl())?;
            out.number("amount", t.amount())?;
            out.number("gas", t.gas())?;
            out.number("gas_price", t.gas_price())?;
            match t.operation() {
                ContractOperation::Create {
                    version,
                    code,
                    deposit,
                    call_data,
                } => {
                    out.id("owner_id", t.actor())?;
                    out.small("vm_version", u64::from(version.vm))?;
                    out.small("abi_version", u64::from(version.abi))?;
                    out.encoded("code", ApiType::ContractBytearray, code)?;
                    out.number("deposit", deposit)?;
                    out.encoded("call_data", ApiType::ContractBytearray, call_data)?;
                    "ContractCreateTx"
                }
                ContractOperation::Call {
                    contract,
                    abi_version,
                    call_data,
                } => {
                    out.id("caller_id", t.actor())?;
                    out.id("contract_id", *contract)?;
                    out.number("abi_version", abi_version)?;
                    out.encoded("call_data", ApiType::ContractBytearray, call_data)?;
                    "ContractCallTx"
                }
            }
        }
        TransactionLeaf::GaAttach(t) => {
            out.encoded("owner_id", ApiType::AccountPubkey, t.owner().as_ref())?;
            out.number("nonce", t.nonce())?;
            out.encoded("code", ApiType::ContractBytearray, t.code())?;
            budget.charge(t.auth_fun().len().saturating_mul(12).saturating_add(128))?;
            let mut auth = String::from("0x");
            for byte in t.auth_fun() {
                use std::fmt::Write;
                write!(&mut auth, "{byte:02x}").expect("string formatting");
            }
            out.insert("auth_fun", Value::String(auth))?;
            out.small("vm_version", u64::from(t.version().vm))?;
            out.small("abi_version", u64::from(t.version().abi))?;
            out.number("fee", t.fee())?;
            out.ttl(t.ttl())?;
            out.number("gas", t.gas())?;
            out.number("gas_price", t.gas_price())?;
            out.encoded("call_data", ApiType::ContractBytearray, t.call_data())?;
            "GAAttachTx"
        }
        TransactionLeaf::HcVote(t) => {
            out.encoded("voter_id", ApiType::AccountPubkey, t.voter().as_ref())?;
            out.number("epoch", t.epoch())?;
            let mut data = Object::new(budget)?;
            for (k, v) in t.data() {
                budget.charge(k.len().saturating_mul(6).saturating_add(64))?;
                let key = super::super::text::binary(k);
                data.raw(&key, v)?;
            }
            out.insert("data", data.finish())?;
            // aetx replaces the callback's numeric type with its Swagger name.
            "HCVoteTx"
        }
        TransactionLeaf::Channel(t) => {
            let (kind, v) = channels::render(t, &mut out)?;
            version = v;
            kind
        }
    };
    out.text("type", kind)?;
    out.small("version", version)?;
    Ok(out.finish())
}
