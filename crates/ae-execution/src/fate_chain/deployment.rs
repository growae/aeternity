// aefa_fate_op:put_contract/unput_contract and aefa_chain_api, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error, TransferEvent, remote::State};
use ae_codec::{
    object::ObjectTag,
    rlp::{self, Rlp},
};
use ae_protocol::Protocol;
use ae_state::{
    accounts::Account,
    contracts::{Code, Contract, Contracts, Nonce, store::Contents},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits, encoding,
    machine::{Exception, program::remote::HostFailure},
    value::Value,
};
use num_bigint::Sign;
use std::collections::BTreeMap;

type Failure = HostFailure<Error>;
fn native(reason: &'static str) -> Failure {
    HostFailure::Exception(Exception::Native(reason))
}
fn backend(e: ae_state::contracts::Error) -> Failure {
    match e {
        ae_state::contracts::Error::AlreadyPresent => native("already_present"),
        e => HostFailure::Backend(Error::Contracts(e)),
    }
}
fn account(e: ae_state::accounts::AccountError) -> Failure {
    HostFailure::Backend(Error::Accounts(e))
}

#[derive(Debug, Clone)]
pub struct Pending {
    pub contract: Contract,
    pub writes: Contents,
}
// Pending contracts must not be serialized into the base trie during execution.
// Removing an unpublished creation drops its metadata/store writes as well;
// deleting an existing contract keeps the old store rows in the base trie.
#[derive(Debug, Clone, Default)]
pub struct Cache {
    pub pending: BTreeMap<[u8; 32], Pending>,
}
impl Cache {
    // Load a pending contract only when the VM requests its store. Keep the
    // initial metadata in the write cache, as in initial_contract_store/0.
    pub fn initial_store<S: NodeReader>(
        &self,
        key: &[u8; 32],
        backend: S,
        limits: Limits,
    ) -> Result<Option<ae_state::contracts::store::Store<S>>, Failure> {
        self.budget(limits)?;
        let Some(pending) = self.pending.get(key) else {
            return Ok(None);
        };
        let mut store = ae_state::contracts::store::Store::new(ae_trie::Trie::new(backend));
        store.put_map(pending.writes.clone());
        Ok(Some(store))
    }
    pub fn budget(&self, mut limits: Limits) -> Result<Limits, Failure> {
        for entry in self.pending.values() {
            let c = &entry.contract;
            let bytes = match &c.code {
                Code::Inline(b) => b.len(),
                Code::Reference(_) => 33,
            };
            let size = bytes
                .checked_add(64)
                .and_then(|s| s.checked_add(c.log.len()))
                .and_then(|s| s.checked_add(c.deposit.as_bytes().len()))
                .and_then(|s| s.checked_add(c.referrers.len().checked_mul(32)?))
                .ok_or(HostFailure::ResourceLimit)?;
            limits.max_bytes = limits
                .max_bytes
                .checked_sub(size)
                .ok_or(HostFailure::ResourceLimit)?;
            limits.max_nodes = limits
                .max_nodes
                .checked_sub(1)
                .ok_or(HostFailure::ResourceLimit)?;
            for (k, v) in &entry.writes {
                limits.max_bytes = limits
                    .max_bytes
                    .checked_sub(k.len())
                    .and_then(|s| s.checked_sub(v.len()))
                    .ok_or(HostFailure::ResourceLimit)?;
                limits.max_nodes = limits
                    .max_nodes
                    .checked_sub(1)
                    .ok_or(HostFailure::ResourceLimit)?;
            }
        }
        Ok(limits)
    }
    pub fn record<S: NodeReader>(
        &self,
        base: &Contracts<S>,
        key: [u8; 32],
    ) -> Result<Option<Contract>, Error> {
        match self.pending.get(&key) {
            Some(v) => Ok(Some(v.contract.clone())),
            None => Ok(base.record(key)?),
        }
    }
    // This is the contract portion of original final_trees. The caller still
    // has to finalize VM stores and atomically publish the complete chain state.
    pub fn materialize<S: NodeWriter + Clone>(
        &self,
        base: &Contracts<S>,
        limits: Limits,
    ) -> Result<Contracts<S>, Failure> {
        self.budget(limits)?;
        let mut out = base.clone();
        for entry in self.pending.values() {
            if matches!(entry.contract.code, Code::Reference(_))
                && entry.contract.object_version == 1
            {
                return Err(native("function_clause"));
            }
            if out
                .is_contract(entry.contract.public_key)
                .map_err(backend)?
            {
                out.enter(&entry.contract, &entry.writes).map_err(backend)?;
            } else {
                out.insert(&entry.contract, &entry.writes)
                    .map_err(backend)?;
            }
        }
        Ok(out)
    }
}
#[derive(Debug, Clone, Copy)]
pub enum Source<'a> {
    Inline(&'a [u8]),
    Reference(&'a [u8]),
}
impl Source<'_> {
    pub fn event(self) -> TransferEvent {
        // tx_event_data has no create/clone clauses: it returns the atom error.
        TransferEvent::DeploymentCall {
            info: match self {
                Self::Inline(_) => "Call.create",
                Self::Reference(_) => "Call.clone",
            },
        }
    }
}
// Preserve deserialize_type_and_vsn before the field template, and the
// left-to-right field checks before a later missing/extra-field failure.
fn wrapper_payable(bytes: &[u8], limits: Limits) -> Result<bool, Failure> {
    let raw = rlp::decode_with_limits(
        bytes,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    )
    .map_err(|e| match e {
        rlp::DecodeError::ResourceLimit | rlp::DecodeError::LengthOverflow => {
            HostFailure::ResourceLimit
        }
        rlp::DecodeError::EmptyInput => native("function_clause"),
        rlp::DecodeError::Truncated => native("badmatch"),
        rlp::DecodeError::LeadingZeroLength => native("leading_zeroes_in_size"),
        rlp::DecodeError::TrailingBytes => native("trailing"),
    })?;
    let Rlp::List(fields) = raw else {
        return Err(native("badmatch"));
    };
    let [tag, version, rest @ ..] = fields.as_slice() else {
        return Err(native("badmatch"));
    };
    fn number(raw: &Rlp<'_>) -> Result<Unsigned, Failure> {
        let Rlp::Bytes(b) = raw else {
            return Err(native("illegal_field"));
        };
        Unsigned::from_field_bytes(b).map_err(|_| native("illegal_field"))
    }
    let tag = number(tag)?;
    let version = number(version)?;
    let tag = tag
        .to_u64()
        .ok()
        .and_then(|n| ObjectTag::try_from(n).ok())
        .ok_or_else(|| native("function_clause"))?;
    let version = version.to_u64().unwrap_or(u64::MAX);
    if tag != ObjectTag::CompilerSophia || !(1..=3).contains(&version) {
        return Err(native("illegal_code_object"));
    }
    let count = match version {
        1 => 3,
        2 => 4,
        _ => 5,
    };
    for (i, field) in rest.iter().take(count).enumerate() {
        let valid = match (i, field) {
            (1, Rlp::List(entries)) => entries.iter().all(|entry| match entry {
                Rlp::List(items) if items.len() == if version == 3 { 5 } else { 4 } => {
                    items.iter().enumerate().all(|(j, v)| {
                        if version == 3 && j == 2 {
                            matches!(v, Rlp::Bytes([0] | [1]))
                        } else {
                            matches!(v, Rlp::Bytes(_))
                        }
                    })
                }
                _ => false,
            }),
            (1, _) => false,
            (4, v) => matches!(v, Rlp::Bytes([0] | [1])),
            (_, Rlp::Bytes(_)) => true,
            _ => false,
        };
        if !valid {
            return Err(native("illegal_field"));
        }
    }
    if rest.len() != count {
        return Err(native("illegal_template_or_values"));
    }
    Ok(version < 3 || matches!(rest[4], Rlp::Bytes([1])))
}

fn unsigned(amount: &Value) -> Result<Unsigned, Failure> {
    match amount {
        Value::Integer(n) if n.sign() != Sign::Minus => {
            Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("positive magnitude"))
        }
        _ => Err(native("function_clause")),
    }
}
fn next_nonce<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    creator: AccountPublicKey,
    protocol: u32,
) -> Result<Unsigned, Failure> {
    let local = state.accounts.lookup(creator).map_err(account)?;
    let owner = match local {
        Some(a) => a,
        None => state
            .onchain
            .as_ref()
            .and_then(|v| v.accounts.lookup(creator).transpose())
            .transpose()
            .map_err(account)?
            .ok_or_else(|| native("badmatch"))?,
    };
    let nonce = owner.nonce().add(&1.into());
    let protocol = Protocol::try_from(protocol)
        .map_err(|_| HostFailure::Backend(Error::UnsupportedProtocol(protocol)))?;
    let context = crate::Context {
        height: state.height,
        protocol,
        origin: crate::Origin::Contract,
        dry_run: state.authentication.dry_run,
        payer: state.authentication.payer,
        ga_authenticated: state.authentication.ga_authenticated.clone(),
        ga_nonces: state.authentication.ga_nonces.clone(),
    };
    let mut changes = BTreeMap::new();
    crate::prepare_sender_with_nonce(
        &state.accounts,
        &mut changes,
        creator,
        &nonce,
        &context,
        true,
    )
    .map_err(|e| match e {
        crate::Error::State(e) => account(e),
        _ => native("badmatch"),
    })?;
    state
        .accounts
        .apply(&changes.into_values().collect::<Vec<_>>())
        .map_err(account)?;
    Ok(nonce)
}
fn transfer<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    from: [u8; 32],
    to: [u8; 32],
    amount: &Value,
) -> Result<(), Failure> {
    let amount = unsigned(amount)?;
    match super::transfer_value(
        &mut state.accounts,
        AccountPublicKey(from),
        AccountPublicKey(to),
        &amount,
    )
    .map_err(HostFailure::Backend)?
    {
        super::Transfer::Transferred(events) => {
            state.events.splice(0..0, *events);
            Ok(())
        }
        super::Transfer::Rejected(_) => Err(native("badmatch")),
    }
}
pub fn put<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    creator: [u8; 32],
    source: Source<'_>,
    amount: &Value,
    vm: u16,
    protocol: u32,
    limits: Limits,
) -> Result<[u8; 32], Failure> {
    let limits = state.contract_cache.budget(limits)?;
    ae_vm_fate::value::check_limits(amount, limits).map_err(|_| HostFailure::ResourceLimit)?;
    let size = match source {
        Source::Inline(b) | Source::Reference(b) => b.len(),
    };
    if size > limits.max_bytes {
        return Err(HostFailure::ResourceLimit);
    }
    let mut next = state.clone();
    let owner = AccountPublicKey(creator);
    let nonce = next_nonce(&mut next, owner, protocol)?;
    let (code, vm) = match source {
        Source::Inline(b) => (Code::Inline(b.to_vec()), vm),
        Source::Reference(k) => {
            let k = k.try_into().map_err(|_| native("badmatch"))?;
            let (final_key, vm) = super::final_ref_from(
                |k| next.contract_cache.record(&next.contracts, k),
                k,
                limits.max_nodes,
            )
            .map_err(|e| match e {
                Error::ResourceLimit => HostFailure::ResourceLimit,
                Error::InvalidReferenceId(_) => native("function_clause"),
                e => HostFailure::Backend(e),
            })?
            .ok_or_else(|| native("badmatch"))?;
            (Code::Reference(Id::new(IdTag::Contract, final_key)), vm)
        }
    };
    let contract = Contract::new(
        owner,
        Nonce::Number(&nonce),
        ae_protocol::contracts::Version { vm, abi: 3 },
        code,
        0.into(),
    )
    .map_err(|_| native("illegal"))?;
    let payable = match &contract.code {
        Code::Inline(bytes) => wrapper_payable(bytes, limits)?,
        Code::Reference(id) => next
            .accounts
            .lookup(AccountPublicKey(id.value))
            .map_err(account)?
            .ok_or_else(|| native("badmatch"))?
            .is_payable(),
    };
    let key = contract.public_key;
    let receiver = AccountPublicKey(key);
    let new_account = match next.accounts.lookup(receiver).map_err(account)? {
        Some(mut a) => {
            a.set_payable(payable);
            a
        }
        None => {
            let mut a = Account::new(receiver, 0.into());
            a.set_non_payable(!payable);
            a
        }
    };
    next.accounts.enter(&new_account).map_err(account)?;
    let metadata = encoding::encode(&Value::Map(BTreeMap::new()), limits)
        .map_err(|_| HostFailure::ResourceLimit)?;
    next.contract_cache.pending.insert(
        key,
        Pending {
            contract,
            writes: BTreeMap::from([(vec![0, 0], metadata)]),
        },
    );
    next.contract_cache.budget(limits)?;
    transfer(&mut next, creator, key, amount)?;
    *state = next;
    Ok(key)
}
// The refund target is the engine's current contract at the cleanup site.
// The VM continuation determines that context; this primitive must not infer it.
pub fn remove<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    contract: [u8; 32],
    current: [u8; 32],
    amount: &Value,
    limits: Limits,
) -> Result<(), Failure> {
    state.contract_cache.budget(limits)?;
    ae_vm_fate::value::check_limits(amount, limits).map_err(|_| HostFailure::ResourceLimit)?;
    let mut next = state.clone();
    next.contract_cache.pending.remove(&contract);
    next.contracts.delete(contract).map_err(backend)?;
    transfer(&mut next, contract, current, amount)?;
    *state = next;
    Ok(())
}
