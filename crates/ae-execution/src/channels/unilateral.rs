// aesc_utils and aetx_utils, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use crate::transactions::{self, Config};
use ae_state::transport::{self, Proofs};
use ae_vm_fate::maps::MapOrder;

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub transaction: ae_tx::TransactionLimits,
    pub state: transport::Limits,
}

pub(super) fn deserialize(
    bytes: &[u8],
    limits: Limits,
    atoms: Option<&std::collections::BTreeSet<Vec<u8>>>,
) -> Result<Option<SignedTransaction<Transaction>>, Error> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let signed = SignedTransaction::decode_with_state_config(
        bytes,
        limits.transaction,
        transport::Config {
            limits: limits.state,
            existing_name_status_atoms: atoms,
        },
    )
    .map_err(|e| {
        if e.is_operational() {
            Error::Codec(e)
        } else {
            Error::Rejected("payload_deserialization_failed")
        }
    })?;
    require(
        signed
            .transaction()
            .layers()
            .iter()
            .all(|l| matches!(l.wrapper(), Wrapper::GaMeta(_)))
            && signed.transaction().leaf().kind() == TransactionKind::ChannelOffchain,
        "bad_offchain_state_type",
    )?;
    Ok(Some(signed))
}
pub(super) fn state(signed: &SignedTransaction<Transaction>) -> (&[u8; 32], &Unsigned, &[u8]) {
    let TransactionLeaf::Channel(t) = signed.transaction().leaf() else {
        unreachable!()
    };
    let Operation::Offchain {
        channel,
        state_hash,
        ..
    } = t.operation()
    else {
        unreachable!()
    };
    (channel, t.nonce(), state_hash)
}
fn account<S: NodeReader>(accounts: &Accounts<S>, key: AccountPublicKey) -> Result<Account, Error> {
    accounts
        .lookup(key)?
        .ok_or(Error::Rejected("account_not_found"))
}
pub(super) fn check_account<S: NodeReader>(
    accounts: &Accounts<S>,
    sender: AccountPublicKey,
    nonce: &Unsigned,
    fee: &Unsigned,
    context: &Context,
) -> Result<(), Error> {
    let payer = context.payer.unwrap_or(sender);
    let a = account(accounts, sender)?;
    if sender == payer {
        require(a.balance() >= fee, "insufficient_funds")?;
    }
    if a.authorization().is_some() {
        require(nonce == &0.into(), "nonce_in_ga_tx_should_be_zero")?;
        require(context.ga_nonces.contains_key(&sender), "bad_ga_tx_env")?;
    } else {
        require(nonce > a.nonce(), "tx_nonce_already_used_for_account")?;
        require(
            nonce == &a.nonce().add(&1.into()),
            "tx_nonce_too_high_for_account",
        )?;
    }
    if sender != payer {
        require(
            account(accounts, payer)?.balance() >= fee,
            "insufficient_funds",
        )?;
    }
    Ok(())
}
pub(super) fn spend<S: NodeWriter + Clone>(
    accounts: &mut Accounts<S>,
    sender: AccountPublicKey,
    nonce: &Unsigned,
    fee: &Unsigned,
    context: &Context,
) -> Result<(), Error> {
    let payer = context.payer.unwrap_or(sender);
    let mut a = account(accounts, sender)?;
    if sender == payer {
        a.spend(fee, nonce.clone())?;
        accounts.enter(&a)?;
    } else {
        a.set_nonce(nonce.clone());
        let mut p = account(accounts, payer)?;
        p.spend_without_nonce_bump(fee)?;
        accounts.enter(&a)?;
        accounts.enter(&p)?;
    }
    Ok(())
}
pub(super) fn allowed(
    c: &Channel,
    sender: AccountPublicKey,
    kind: TransactionKind,
    height_protocol: Protocol,
) -> Result<(), Error> {
    if c.peers().contains(&sender) {
        return Ok(());
    }
    require(
        kind == TransactionKind::ChannelSlash
            || (matches!(
                kind,
                TransactionKind::ChannelSnapshotSolo | TransactionKind::ChannelForceProgress
            ) && height_protocol >= Protocol::Iris),
        "account_not_peer",
    )?;
    let delegates = c.delegate_pubkeys(None).map_err(|e| match e {
        ae_state::channels::Error::InvalidIdType => Error::Native(NativeFailure::FunctionClause),
        e => Error::State(e),
    })?;
    require(delegates.contains(&sender), "account_not_peer_or_delegate")
}
fn balances(
    c: &Channel,
    proof: &Proofs,
    limits: transport::Limits,
) -> Result<[Unsigned; 2], Error> {
    // Both original lookups are evaluated before testing whether either peer is absent.
    let a = proof_account(proof, c.initiator, limits)?;
    let b = proof_account(proof, c.responder, limits)?;
    let (Some(a), Some(b)) = (a, b) else {
        return reject("wrong_channel_peers");
    };
    require(
        a.balance().add(b.balance()) <= c.channel_amount,
        "poi_amounts_change_channel_funds",
    )?;
    Ok([a.balance().clone(), b.balance().clone()])
}
fn proof_account(
    proof: &Proofs,
    key: AccountPublicKey,
    limits: transport::Limits,
) -> Result<Option<Account>, Error> {
    let result = proof.lookup_account(key, limits).map_err(|e| match e {
        transport::Error::Object(e) => {
            transport::Error::Account(ae_state::accounts::AccountError::Object(e))
        }
        e => e,
    });
    match result {
        Err(transport::Error::NativeBadarg) => Err(Error::Native(NativeFailure::Badarg)),
        Err(transport::Error::NativeFunctionClause | transport::Error::InvalidValue) => {
            Err(Error::Native(NativeFailure::FunctionClause))
        }
        Err(transport::Error::Account(error)) => {
            if matches!(
                error,
                ae_state::accounts::AccountError::Object(ae_codec::object::ObjectError::Rlp(
                    ae_codec::rlp::DecodeError::ResourceLimit
                ))
            ) {
                return Err(Error::ResourceLimit);
            }
            let value = proof
                .accounts
                .as_ref()
                .expect("account proof")
                .lookup(&key.0, limits)
                .map_err(Error::Proof)?
                .expect("account value");
            Err(Error::Native(NativeFailure::ProofAccount { value, error }))
        }
        result => result.map_err(Error::Proof),
    }
}
fn delegates(ids: &[Id]) -> Result<Vec<AccountPublicKey>, Error> {
    ids.iter()
        .map(|id| {
            if id.tag == IdTag::Account {
                Ok(AccountPublicKey(id.value))
            } else {
                Err(Error::Native(NativeFailure::FunctionClause))
            }
        })
        .collect()
}

pub fn process<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ChannelTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    source: Option<&SignedTransaction<Transaction>>,
    config: &Config<'_, O>,
    mut limits: Limits,
) -> Result<Receipt, transactions::Error<A, C>> {
    let (key, sender, payload, proof) = match tx.operation() {
        Operation::CloseSolo {
            channel,
            from,
            payload,
            proof,
        }
        | Operation::Slash {
            channel,
            from,
            payload,
            proof,
        } => (*channel, *from, payload, Some(proof)),
        Operation::SnapshotSolo {
            channel,
            from,
            payload,
        }
        | Operation::SetDelegates {
            channel,
            from,
            payload,
            ..
        } => (*channel, *from, payload, None),
        _ => return Err(Error::Unsupported(tx.kind()).into()),
    };
    limits.transaction.max_layers = limits
        .transaction
        .max_layers
        .min(config.signature_limits.max_layers);
    limits.transaction.max_total_bytes = limits
        .transaction
        .max_total_bytes
        .min(config.signature_limits.max_encoded_bytes);
    limits.transaction.rlp.max_input_bytes = limits
        .transaction
        .rlp
        .max_input_bytes
        .min(config.signature_limits.max_encoded_bytes);
    let valid = context.origin == crate::Origin::Contract
        || tx
            .valid_at_protocol_with_config(
                context.protocol,
                limits.transaction,
                transport::Config {
                    limits: limits.state,
                    existing_name_status_atoms: config.channel_state_atoms,
                },
            )
            .map_err(Error::Codec)?;
    crate::check_simple(tx.wire_size(), tx.fee(), tx.ttl(), valid, context)?;
    let c = trees.channels.lookup(key).map_err(Error::State)?;
    // Storage/decode exhaustion remains retryable even when get_vals would
    // select an earlier semantic channel error in the unbounded reference.
    let decoded = match deserialize(payload, limits, config.channel_state_atoms) {
        Err(e @ Error::Codec(_)) => return Err(e.into()),
        result => result,
    };
    let mut c = c.ok_or(Error::Rejected("channel_does_not_exist"))?;
    let decoded = decoded?;
    if decoded.is_none() {
        match tx.kind() {
            TransactionKind::ChannelSlash => {
                return Err(Error::Rejected("slash_must_have_payload").into());
            }
            TransactionKind::ChannelSnapshotSolo => {
                return Err(Error::Rejected("snapshot_must_have_payload").into());
            }
            _ => {}
        }
    }
    check_account(&trees.accounts, sender, tx.nonce(), tx.fee(), context)?;
    if decoded.is_none() {
        peer(&c, sender)?;
    }
    if tx.kind() == TransactionKind::ChannelSlash {
        require(c.is_solo_closing(), "channel_not_closing")?;
    } else {
        require(c.is_active(), "channel_not_active")?;
    }
    if let Some(signed) = &decoded {
        let (payload_key, payload_round, _) = state(signed);
        require(payload_key == &key, "bad_state_channel_pubkey")?;
        round(&c, payload_round)?;
        allowed(
            &c,
            sender,
            tx.kind(),
            config.network.protocol_at(context.height),
        )?;
        authentication::verify(
            signed,
            authentication::Authorization::Offchain(&c),
            trees,
            context,
            &[],
            config,
        )?;
    }
    let (expected_round, expected_hash) = decoded
        .as_ref()
        .map(|s| {
            let (_, r, h) = state(s);
            (r, h)
        })
        .unwrap_or((&c.round, c.state_hash.as_slice()));
    let proof_balances = if let Some(proof) = proof {
        require(
            proof.hash().0.as_slice() == expected_hash,
            if decoded.is_some() {
                "invalid_poi_hash"
            } else {
                "invalid_poi_hash_in_channel"
            },
        )?;
        Some(balances(&c, proof, limits.state)?)
    } else {
        None
    };
    if let Operation::SetDelegates {
        state_hash, round, ..
    } = tx.operation()
    {
        require(state_hash == expected_hash, "unexpected_state_hash")?;
        require(round == expected_round, "unexpected_round")?;
    }
    let mut next = trees.clone();
    if let Some(balances) = proof_balances {
        spend(&mut next.accounts, sender, tx.nonce(), tx.fee(), context)?;
        c.close_solo(
            balances,
            &context.height.into(),
            decoded.as_ref().map(|s| {
                let (_, r, h) = state(s);
                (r.clone(), h.to_vec())
            }),
        );
        next.channels.enter(&c).map_err(Error::State)?;
    } else {
        if let Operation::SetDelegates {
            initiator_delegates,
            responder_delegates,
            ..
        } = tx.operation()
        {
            c.set_delegates(
                &delegates(initiator_delegates)?,
                &delegates(responder_delegates)?,
            );
        }
        if let Some(s) = &decoded {
            let (_, r, h) = state(s);
            c.snapshot(r.clone(), h.to_vec());
        }
        if c.object_version < 3
            && let Delegates::Separate {
                initiator,
                responder,
            } = &c.delegates
        {
            return Err(Error::Native(NativeFailure::LegacyDelegates {
                initiator: initiator.clone(),
                responder: responder.clone(),
            })
            .into());
        }
        next.channels.enter(&c).map_err(Error::State)?;
        spend(&mut next.accounts, sender, tx.nonce(), tx.fee(), context)?;
    }
    let mut events = vec![];
    let payer = context.payer.unwrap_or(sender);
    delta(
        &mut events,
        if proof.is_some() { sender } else { payer },
        tx.fee().clone(),
        true,
        "Channel.fee",
    );
    if let Some(source) = source {
        if source
            .signatures()
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(source.transaction().view().wire_size()))
            .is_none_or(|n| n > config.signature_limits.max_encoded_bytes)
        {
            return Err(Error::ResourceLimit.into());
        }
        let encoded = source.encode();
        if encoded.len() > config.signature_limits.max_encoded_bytes {
            return Err(Error::ResourceLimit.into());
        }
        events.push(Event::Channel {
            channel: key,
            transaction_hash: ae_crypto::blake2b_256(&encoded),
            transaction_type: event_view(source).kind(),
        });
    }
    events.reverse();
    *trees = next;
    Ok(Receipt {
        transaction_hash: None,
        gas: crate::simple_gas(tx.wire_size()),
        call: None,
        events,
        context: context.clone(),
    })
}
