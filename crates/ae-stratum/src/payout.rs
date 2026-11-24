// aestratum send_payment/payout_call_tx_args, fa2a6d7; aeb_aevm_abi 009e036.
// Exact preparation is deterministic. Key ownership, storage and pool submission
// belong to the caller; this module has no external effects.
use crate::{
    accounting::Payment,
    proof::Number,
    rewards::{self, Amounts},
};
use ae_codec::api::{self, ApiType};
use ae_crypto::{blake2b_256, verify_ed25519};
use ae_protocol::Protocol;
use ae_tx::{CanonicalTransaction, ContractOperation, ContractTransaction, SignedTransaction};
use ae_types::{AccountPublicKey, ContractPublicKey, Hash256, Id, IdTag, Signature, Unsigned};
use ae_vm_aevm::heap::{self, Type, Value};
use ae_vm_fate::maps::Traversal;
use num_bigint::{BigInt, Sign};
mod codec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Limit,
    Address,
    NegativeAmount,
    AlreadySent,
    AccountUnavailable,
    InsufficientBalance {
        balance: Unsigned,
        gross: BigInt,
    },
    NonpositiveNet {
        gross: BigInt,
        fee: Unsigned,
        run_fee: Unsigned,
        net: BigInt,
    },
    Rewards(rewards::Error),
    Heap(heap::Error),
    Transaction(ae_tx::Error),
    Signature,
    Corrupt(&'static str),
    Accounting(crate::accounting::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<rewards::Error> for Error {
    fn from(e: rewards::Error) -> Self {
        Self::Rewards(e)
    }
}
impl From<heap::Error> for Error {
    fn from(e: heap::Error) -> Self {
        Self::Heap(e)
    }
}
impl From<ae_tx::Error> for Error {
    fn from(e: ae_tx::Error) -> Self {
        Self::Transaction(e)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub transfers: rewards::Limits,
    pub heap: heap::Limits,
    pub transaction_bytes: usize,
    pub network_bytes: usize,
    pub record_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            transfers: rewards::Limits {
                entries: 1000,
                ..Default::default()
            },
            heap: heap::Limits {
                max_depth: 32,
                max_nodes: 100_000,
                max_bytes: 1 << 20,
            },
            transaction_bytes: 2 << 20,
            network_bytes: 1024,
            record_bytes: 8 << 20,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        self.transfers.validate()?;
        if self.heap.max_depth > 256
            || self.heap.max_nodes > 1_000_000
            || self.heap.max_bytes > 64 << 20
            || self.transaction_bytes > 64 << 20
            || self.network_bytes > 65536
            || self.record_bytes > 64 << 20
            || self.record_bytes == 0
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    pub caller: AccountPublicKey,
    pub contract: ContractPublicKey,
    pub account_nonce: Unsigned,
    pub minimum_miner_gas_price: Unsigned,
    pub height: u64,
    pub protocol: Protocol,
}
fn signed(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn unsigned(n: &BigInt) -> Result<Unsigned, Error> {
    crate::integer(n).map_err(|_| Error::Limit)?;
    if n.sign() == Sign::Minus {
        return Err(Error::NegativeAmount);
    }
    Unsigned::from_field_bytes(&n.to_bytes_be().1).map_err(|_| Error::Limit)
}
fn check_unsigned(n: &Unsigned) -> Result<(), Error> {
    if n.as_bytes().len() > (crate::MAX_INTEGER_BITS / 8) as usize {
        return Err(Error::Limit);
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    transaction: ContractTransaction,
    run_gas: Unsigned,
    run_fee: Unsigned,
}
impl Call {
    pub fn transaction(&self) -> &ContractTransaction {
        &self.transaction
    }
    pub fn run_gas(&self) -> &Unsigned {
        &self.run_gas
    }
    pub fn run_fee(&self) -> &Unsigned {
        &self.run_fee
    }
}
// The native fold prepends entries. Sorting by recipient public key instead of
// preserving the OTP binary-key map traversal changes calldata and signatures.
pub fn call_args(transfers: &Amounts, limits: Limits) -> Result<Vec<(BigInt, BigInt)>, Error> {
    limits.validate()?;
    if transfers.len() > limits.transfers.entries {
        return Err(Error::Limit);
    }
    let mut bytes = 0usize;
    for (key, amount) in transfers {
        crate::integer(amount).map_err(|_| Error::Limit)?;
        bytes = bytes
            .checked_add(96 + key.len() + amount.bits().div_ceil(8) as usize)
            .ok_or(Error::Limit)?;
        if key.len() > limits.transfers.key_bytes || bytes > limits.transfers.bytes {
            return Err(Error::Limit);
        }
    }
    let map = transfers
        .iter()
        .map(|(k, v)| (k.clone(), Number::Integer(v.clone())))
        .collect();
    let order = rewards::indices(&map, Traversal::Fold, limits.transfers)?;
    let entries: Vec<_> = transfers.iter().collect();
    order
        .into_iter()
        .rev()
        .map(|i| {
            let (address, amount) = entries[i];
            let text = std::str::from_utf8(address).map_err(|_| Error::Address)?;
            let (kind, key) = api::decode_with_limit(text, limits.transfers.key_bytes)
                .map_err(|_| Error::Address)?;
            if kind != ApiType::AccountPubkey {
                return Err(Error::Address);
            }
            Ok((BigInt::from_bytes_be(Sign::Plus, &key), amount.clone()))
        })
        .collect()
}
fn calldata(arguments: Vec<(BigInt, BigInt)>, limits: heap::Limits) -> Result<Vec<u8>, Error> {
    let argument_type = Type::Tuple(vec![Type::List(Box::new(Type::Tuple(vec![
        Type::Word,
        Type::Word,
    ])))]);
    let mut type_bytes = b"payout".to_vec();
    type_bytes.extend(heap::encode(&Value::Type(argument_type), limits)?);
    type_bytes.extend(heap::encode(&Value::Type(Type::Word), limits)?);
    let hash = blake2b_256(&type_bytes);
    let args = Value::List(
        arguments
            .into_iter()
            .map(|(address, amount)| {
                Value::Tuple(vec![Value::Integer(address), Value::Integer(amount)])
            })
            .collect(),
    );
    Ok(heap::encode(
        &Value::Tuple(vec![
            Value::Integer(BigInt::from_bytes_be(Sign::Plus, &hash.0)),
            Value::Tuple(vec![args]),
        ]),
        limits,
    )?)
}
pub fn call(transfers: &Amounts, environment: &Environment, limits: Limits) -> Result<Call, Error> {
    limits.validate()?;
    check_unsigned(&environment.account_nonce)?;
    check_unsigned(&environment.minimum_miner_gas_price)?;
    let data = calldata(call_args(transfers, limits)?, limits.heap)?;
    let amount = transfers.values().try_fold(BigInt::from(0), |sum, n| {
        let value = sum + n;
        crate::integer(&value).map_err(|_| Error::Limit)?;
        Ok::<_, Error>(value)
    })?;
    let mut transaction = ContractTransaction::new(
        Id {
            tag: IdTag::Account,
            value: environment.caller.0,
        },
        ContractOperation::Call {
            contract: Id {
                tag: IdTag::Contract,
                value: environment.contract.0,
            },
            abi_version: 1.into(),
            call_data: data,
        },
    )?;
    let nonce = environment.account_nonce.add(&1.into());
    check_unsigned(&nonce)?;
    transaction.set_nonce(nonce);
    transaction.set_amount(unsigned(&amount)?);
    // Native <<Int256:256>> = <<1:256/little>> is 1 << 248.
    let mut placeholder = [0; 32];
    placeholder[0] = 1;
    let placeholder = Unsigned::from_field_bytes(&placeholder).map_err(|_| Error::Limit)?;
    transaction.set_fee(placeholder.clone());
    transaction.set_gas(placeholder.clone());
    transaction.set_gas_price(placeholder);
    if transaction.wire_size() > limits.transaction_bytes {
        return Err(Error::Limit);
    }
    let fee_gas = transaction.fee_gas();
    let price = environment
        .minimum_miner_gas_price
        .clone()
        .max(environment.protocol.minimum_gas_price().into());
    let run_gas = Unsigned::from(transfers.len() as u64)
        .mul_u64(22000)
        .add(&250.into());
    let fee = unsigned(&(signed(&price) * signed(&fee_gas)))?;
    let run_fee = unsigned(&(signed(&price) * signed(&run_gas)))?;
    transaction.set_gas_price(price);
    transaction.set_gas(fee_gas);
    transaction.set_fee(fee);
    if transaction.wire_size() > limits.transaction_bytes {
        return Err(Error::Limit);
    }
    Ok(Call {
        transaction,
        run_gas,
        run_fee,
    })
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub payment: rewards::Payment,
    pub environment: Environment,
    pub balance: Unsigned,
    pub network: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    request: Request,
    net: BigInt,
    absolute: Amounts,
    estimate: Call,
    call: Call,
    signing_bytes: Vec<u8>,
}
impl Prepared {
    pub fn payment_id(&self) -> (u64, u32) {
        self.request.payment.id
    }
    pub fn gross(&self) -> &BigInt {
        &self.request.payment.total
    }
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn net(&self) -> &BigInt {
        &self.net
    }
    pub fn absolute(&self) -> &Amounts {
        &self.absolute
    }
    pub fn estimate(&self) -> &Call {
        &self.estimate
    }
    pub fn call(&self) -> &Call {
        &self.call
    }
    pub fn signing_bytes(&self) -> &[u8] {
        &self.signing_bytes
    }
    pub fn attach_signature(
        &self,
        signature: Signature,
    ) -> Result<SignedTransaction<ContractTransaction>, Error> {
        if !verify_ed25519(
            &self.call.transaction.signer(),
            &self.signing_bytes,
            &signature,
        ) {
            return Err(Error::Signature);
        }
        Ok(SignedTransaction::new(
            self.call.transaction.clone(),
            vec![signature],
        ))
    }
    pub fn signing_hash(&self) -> Hash256 {
        blake2b_256(&self.signing_bytes)
    }
}
pub fn prepare(
    payment: &Payment,
    environment: &Environment,
    balance: Option<&Unsigned>,
    network: &[u8],
    limits: Limits,
) -> Result<Prepared, Error> {
    limits.validate()?;
    if payment.tx_hash.is_some() {
        return Err(Error::AlreadySent);
    }
    crate::integer(&payment.total).map_err(|_| Error::Limit)?;
    let balance = balance.ok_or(Error::AccountUnavailable)?;
    check_unsigned(balance)?;
    if payment.total >= signed(balance) {
        return Err(Error::InsufficientBalance {
            balance: balance.clone(),
            gross: payment.total.clone(),
        });
    }
    if network.len() > limits.network_bytes {
        return Err(Error::Limit);
    }
    let first = rewards::absolute(&payment.relative, &payment.total, limits.transfers)?;
    let estimate = call(&first, environment, limits)?;
    let net = &payment.total - signed(estimate.transaction.fee()) - signed(&estimate.run_fee);
    if net.sign() != Sign::Plus {
        return Err(Error::NonpositiveNet {
            gross: payment.total.clone(),
            fee: estimate.transaction.fee().clone(),
            run_fee: estimate.run_fee.clone(),
            net,
        });
    }
    let absolute = rewards::absolute(&payment.relative, &net, limits.transfers)?;
    let call = call(&absolute, environment, limits)?;
    let mut signing_bytes = network.to_vec();
    signing_bytes.extend(call.transaction.encode());
    Ok(Prepared {
        request: Request {
            payment: rewards::Payment {
                id: payment.id,
                total: payment.total.clone(),
                relative: payment.relative.clone(),
            },
            environment: environment.clone(),
            balance: balance.clone(),
            network: network.to_vec(),
        },
        net,
        absolute,
        estimate,
        call,
        signing_bytes,
    })
}
