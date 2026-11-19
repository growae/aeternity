// aesc_utils:verify_signatures_{offchain,onchain}, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    Context,
    contracts::{
        self,
        channel_auth::{self, error, native, reject},
    },
    transactions::{self, Config, Error, SignatureError},
};
use ae_protocol::Protocol;
use ae_state::{
    Trees,
    channels::Channel,
    contracts::{Code, Contract},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::{SignedTransaction, Transaction, Wrapper};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{maps::MapOrder, native::Term};
#[derive(Clone, Copy)]
pub enum Authorization<'a> {
    Onchain,
    Offchain(&'a Channel),
}
fn authorization<A: NodeReader, C: NodeReader>(
    mode: Authorization<'_>,
    key: AccountPublicKey,
    trees: &Trees<A, C>,
    meta: bool,
) -> Result<Option<ae_state::accounts::Authorization>, Error<A, C>> {
    match mode {
        Authorization::Offchain(c) => c
            .authorization(Id::new(IdTag::Account, key.0))
            .map(|a| a.cloned())
            .map_err(|e| match e {
                ae_state::channels::Error::NotParticipant => reject("id_is_not_a_participant"),
                e => error(contracts::Error::ChannelState(e)),
            }),
        Authorization::Onchain => trees
            .accounts
            .lookup(key)
            .map_err(|e| error(e.into()))?
            .map(|a| a.authorization().cloned())
            .ok_or_else(|| {
                if meta {
                    native(Term::tagged(
                        "not_present",
                        vec![Term::Binary(key.0.to_vec())],
                    ))
                } else {
                    reject("account_not_in_trees")
                }
            }),
    }
}
// Checked identities are supplied by the caller. Preserve duplicate entries:
// Erlang list subtraction removes one signer occurrence per authentication.
pub fn verify<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    signed: &SignedTransaction<Transaction>,
    mode: Authorization<'_>,
    trees: &Trees<A, C>,
    context: &Context,
    checked: &[AccountPublicKey],
    config: &Config<'_, O>,
) -> Result<(), Error<A, C>> {
    let mut view = signed.transaction().view();
    let mut signatures = signed.signatures();
    let mut authenticated = checked.to_vec();
    let mut bytes = config.signature_limits.max_encoded_bytes;
    if view.layers().len() > config.signature_limits.max_layers {
        return Err(SignatureError::ResourceLimit.into());
    }
    while let Some(layer) = view.layer() {
        let Wrapper::GaMeta(meta) = layer.wrapper() else {
            break;
        };
        let auth = authorization(mode, meta.account, trees, true)?
            .ok_or_else(|| reject("meta_tx_for_basic_account"))?;
        channel_auth::verify_data(
            meta,
            &auth.function,
            matches!(mode, Authorization::Onchain),
            config,
        )?;
        let store_key = match mode {
            Authorization::Offchain(c) => {
                c.auth_store_key(Id::new(IdTag::Account, meta.account.0)).0
            }
            Authorization::Onchain => {
                if auth.contract.tag != IdTag::Contract {
                    return Err(native(Term::Atom("function_clause")));
                }
                auth.contract.value
            }
        };
        let loaded = load_contract(auth.contract.value, trees)?;
        let store = trees.contracts.read_store(store_key);
        let Some(contract) = loaded else {
            return Err(reject("signature_verification_failed_no_contract"));
        };
        match store {
            Ok(_) => {}
            Err(ae_state::contracts::Error::NoSuchStore) => {
                return Err(reject("signature_verification_failed_no_state"));
            }
            Err(e) => return Err(error(e.into())),
        };
        let inner = view.inner().expect("meta inner");
        let mut hash_tx = inner;
        if matches!(mode, Authorization::Offchain(_)) {
            while matches!(
                hash_tx.layer().map(|l| l.wrapper()),
                Some(Wrapper::GaMeta(_))
            ) {
                hash_tx = hash_tx.inner().unwrap();
            }
        }
        if hash_tx.wire_size() > bytes {
            return Err(SignatureError::ResourceLimit.into());
        }
        let encoded = hash_tx.encode();
        let cost = encoded
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(config.network.network_id().len()))
            .ok_or(SignatureError::ResourceLimit)?;
        bytes = bytes
            .checked_sub(cost)
            .ok_or(SignatureError::ResourceLimit)?;
        let mut input = config.network.network_id().to_vec();
        input.extend_from_slice(&encoded);
        let input = if context.protocol >= Protocol::Ceres {
            meta.serialize_auth_data(&input)
        } else {
            input
        };
        let mut env = context.clone();
        env.ga_transaction_hash = Some(ae_crypto::blake2b_256(&input));
        let extra = inner
            .pricing(&context.height.into(), context.protocol)
            .inner_gas_limit;
        let extra =
            Unsigned::from_field_bytes(&extra.to_bytes_be().1).expect("nonnegative inner gas");
        channel_auth::verify_call(
            meta,
            meta.gas.add(&extra),
            contract,
            store_key,
            trees,
            &env,
            config,
        )?;
        authenticated.insert(0, meta.account);
        signatures = layer.inner_signatures();
        view = inner;
    }
    let mut signers = transactions::signer_keys(
        view,
        config.network,
        config.signature_limits,
        &trees.channels,
    )
    .map_err(|e| {
        if e.is_operational() {
            return Error::Signature(e);
        }
        let reason = match e {
            SignatureError::ChannelNotFound => "channel_not_found",
            SignatureError::ChannelDoesNotExist => "channel_does_not_exist",
            SignatureError::InnerIncorrectlySigned => "inner_tx_incorrectly_signed",
            SignatureError::VerificationFailed => "signature_verification_failed",
            _ => "signature_check_failed",
        };
        native(Term::tagged(
            "badmatch",
            vec![Term::tagged("error", vec![Term::Atom(reason)])],
        ))
    })?;
    for key in authenticated {
        if let Some(i) = signers.iter().position(|k| *k == key) {
            signers.remove(i);
        }
    }
    if context.protocol >= Protocol::Lima {
        for key in &signers {
            if authorization(mode, *key, trees, false)?.is_some() {
                return Err(reject("ga_using_signature_not_allowed"));
            }
        }
    }
    if signers.len() != signatures.len() {
        return Err(reject("signature_check_failed"));
    }
    if signers.is_empty() {
        return Ok(());
    }
    if view.wire_size() > bytes {
        return Err(SignatureError::ResourceLimit.into());
    }
    let encoded = view.encode();
    if encoded.len() > bytes {
        return Err(SignatureError::ResourceLimit.into());
    }
    ae_tx::verify_signatures(
        &encoded,
        signatures,
        &signers,
        config.network,
        context.protocol,
        false,
    )
    .map_err(|_| reject("signature_check_failed"))
}

fn id_term(id: Id) -> Term {
    let tag = match id.tag {
        IdTag::Account => "account",
        IdTag::Name => "name",
        IdTag::Commitment => "commitment",
        IdTag::Oracle => "oracle",
        IdTag::Contract => "contract",
        IdTag::Channel => "channel",
    };
    Term::tagged("id", vec![Term::Atom(tag), Term::Binary(id.value.to_vec())])
}
fn load_contract<A: NodeReader, C: NodeReader>(
    key: [u8; 32],
    trees: &Trees<A, C>,
) -> Result<Option<Contract>, Error<A, C>> {
    let Some(mut contract) = trees.contracts.record(key).map_err(|e| error(e.into()))? else {
        return Ok(None);
    };
    if let Code::Reference(id) = contract.code {
        if id.tag != IdTag::Contract {
            return Err(native(Term::Atom("function_clause")));
        }
        let reference = trees
            .contracts
            .record(id.value)
            .map_err(|e| error(e.into()))?
            .ok_or_else(|| native(Term::tagged("badmatch", vec![Term::Atom("none")])))?;
        contract.code = match reference.code {
            Code::Inline(code) => Code::Inline(code),
            Code::Reference(id) => {
                return Err(native(Term::tagged(
                    "badmatch",
                    vec![Term::tagged("ref", vec![id_term(id)])],
                )));
            }
        };
    }
    Ok(Some(contract))
}
