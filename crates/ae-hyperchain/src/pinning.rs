// aec_pinning_agent lifecycle and child transactions, aeternity fa2a6d7.
// Supported by the Aeternity Foundation. ISC notice in repository LICENSE.
use crate::signing::Keys;
use ae_tx::{
    CanonicalTransaction, ContractOperation, ContractTransaction, SignedTransaction, Spend,
    Transaction, TransactionLeaf,
};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use num_bigint::BigInt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Epoch {
        epoch: BigInt,
        first: BigInt,
        last: BigInt,
    },
    Top {
        height: BigInt,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Active {
    pub next_last: BigInt,
    // Native callback errors have no usable transaction hash.
    pub parent_hash: Option<Vec<u8>>,
    pub child_note: bool,
    pub leader: AccountPublicKey,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    pub enabled: bool,
    // An unmapped new epoch disables pinning but retains these native fields.
    pub active: Option<Active>,
}
pub trait Host {
    type Error;
    fn operational(error: &Self::Error) -> bool;
    fn leader(&mut self, height: &BigInt) -> Result<AccountPublicKey, Self::Error>;
    fn mapped(&self, leader: AccountPublicKey) -> bool;
    fn post(&mut self, leader: AccountPublicKey) -> Result<Option<Vec<u8>>, Self::Error>;
    fn included(&mut self, hash: Option<&[u8]>) -> Result<bool, Self::Error>;
    fn note(&mut self, leader: AccountPublicKey, hash: &[u8]) -> Result<(), Self::Error>;
    fn proof(&mut self, leader: AccountPublicKey, hash: &[u8]) -> Result<(), Self::Error>;
}
impl State {
    // Host effects may already have executed when an error returns. An owner
    // must retain their receipts; it cannot replay this event blindly.
    pub fn apply<H: Host>(&mut self, event: &Event, host: &mut H) -> Result<(), H::Error> {
        match event {
            Event::Epoch { last, .. } => {
                let leader = host.leader(last)?;
                if host.mapped(leader) {
                    let parent_hash = host.post(leader)?;
                    self.active = Some(Active {
                        next_last: last - 1,
                        parent_hash,
                        child_note: false,
                        leader,
                    });
                    self.enabled = true;
                } else {
                    self.enabled = false;
                }
            }
            Event::Top { height } if self.enabled => {
                let active = self.active.as_ref().expect("enabled pinning state");
                let mut noted = active.child_note;
                if !noted && host.included(active.parent_hash.as_deref())? {
                    // Only a successfully returned native hash can be included.
                    if let Some(hash) = &active.parent_hash {
                        if let Err(error) = host.note(active.leader, hash)
                            && H::operational(&error)
                        {
                            return Err(error);
                        }
                        noted = true;
                    }
                }
                if height == &active.next_last {
                    if noted {
                        host.proof(
                            active.leader,
                            active.parent_hash.as_deref().expect("noted parent"),
                        )?;
                    }
                    self.enabled = false;
                }
                self.active.as_mut().expect("active pin").child_note = noted;
            }
            Event::Top { .. } => (),
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    Signing,
    Encoding,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub hash_bytes: usize,
    pub integer_bytes: usize,
    pub network_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            hash_bytes: 4096,
            integer_bytes: 4096,
            network_bytes: 4096,
        }
    }
}
pub struct TransactionInput<'a> {
    pub leader: AccountPublicKey,
    pub nonce: Unsigned,
    pub minimum_gas_price: Unsigned,
    pub parent_hash: &'a [u8],
    pub network: &'a [u8],
    pub keys: &'a Keys,
    pub limits: Limits,
}
fn sign(
    tx: Transaction,
    input: &TransactionInput<'_>,
) -> Result<SignedTransaction<Transaction>, Error> {
    let mut message = input.network.to_vec();
    message.extend_from_slice(&tx.encode());
    let signature = input
        .keys
        .sign(input.leader, &message)
        .map_err(|_| Error::Signing)?;
    Ok(SignedTransaction::new(tx, vec![signature]))
}
fn check(input: &TransactionInput<'_>) -> Result<(), Error> {
    if input.parent_hash.len() > input.limits.hash_bytes
        || input.network.len() > input.limits.network_bytes
        || [&input.nonce, &input.minimum_gas_price]
            .iter()
            .any(|n| n.as_bytes().len() > input.limits.integer_bytes)
    {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}
pub fn note(input: TransactionInput<'_>) -> Result<SignedTransaction<Transaction>, Error> {
    check(&input)?;
    let mut tx = Spend::new(input.leader, Id::new(IdTag::Account, input.leader.0))
        .map_err(|_| Error::Encoding)?;
    tx.set_nonce(input.nonce.clone());
    tx.set_amount(1.into());
    tx.set_fee(input.minimum_gas_price.mul_u64(1_000_000));
    tx.set_payload(
        crate::pin::encode_child(input.parent_hash, input.limits.hash_bytes.saturating_add(3))
            .map_err(|_| Error::ResourceLimit)?,
    );
    sign(Transaction::new(TransactionLeaf::Spend(tx)), &input)
}
pub fn proof(
    contract: [u8; 32],
    input: TransactionInput<'_>,
) -> Result<SignedTransaction<Transaction>, Error> {
    check(&input)?;
    let call_data = ae_vm_fate::abi::create_calldata(
        b"pin",
        vec![ae_vm_fate::value::Value::Bytes(input.parent_hash.to_vec())],
        Default::default(),
    )
    .map_err(|_| Error::Encoding)?;
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, input.leader.0),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, contract),
            abi_version: 3.into(),
            call_data,
        },
    )
    .map_err(|_| Error::Encoding)?;
    tx.set_nonce(input.nonce.clone());
    tx.set_fee(input.minimum_gas_price.mul_u64(1_000_000));
    tx.set_gas(1_000_000.into());
    tx.set_gas_price(input.minimum_gas_price.clone());
    sign(Transaction::new(TransactionLeaf::Contract(tx)), &input)
}
