// aesc_force_progress_tx and aesc_utils, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::unilateral::{allowed, check_account, deserialize, spend, state};
use super::*;
use crate::{
    contracts,
    transactions::{Config, Error as TxError},
};
use ae_state::{
    calls::{Call, ReturnType, call_id, ga_call_id},
    transport,
};
use ae_vm_fate::{maps::MapOrder, native::Term};
use contracts::channel_auth::native;

fn product(a: &Unsigned, b: &Unsigned) -> Unsigned {
    Unsigned::from_field_bytes(
        &(num_bigint::BigUint::from_bytes_be(a.as_bytes())
            * num_bigint::BigUint::from_bytes_be(b.as_bytes()))
        .to_bytes_be(),
    )
    .unwrap()
}
fn next_round(c: &Channel, round: &Unsigned) -> Result<(), Error> {
    require(round != &c.round, "same_round")?;
    require(round > &c.round, "old_round")
}
fn balance<A: NodeReader, C: NodeReader>(
    trees: &Trees<A, C>,
    key: AccountPublicKey,
) -> Result<Unsigned, TxError<A, C>> {
    trees
        .accounts
        .lookup(key)
        .map_err(crate::Error::from)?
        .map(|a| a.balance().clone())
        .ok_or_else(|| {
            native(Term::tagged(
                "not_present",
                vec![Term::Binary(key.0.to_vec())],
            ))
        })
}
pub fn process<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ChannelTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    source: Option<&SignedTransaction<Transaction>>,
    config: &Config<'_, O>,
) -> Result<Receipt, TxError<A, C>> {
    let Operation::ForceProgress {
        channel: key,
        from: sender,
        payload,
        round,
        update,
        state_hash,
        trees: encoded,
    } = tx.operation()
    else {
        return Err(Error::Unsupported(tx.kind()).into());
    };
    let mut limits = config.channel_limits;
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
    let state_config = transport::Config {
        limits: limits.state,
        existing_name_status_atoms: config.channel_state_atoms,
    };
    if context.origin != crate::Origin::Contract {
        let size =
            if context.origin == crate::Origin::Generalized && context.protocol < Protocol::Iris {
                0
            } else {
                tx.wire_size()
            };
        let fee = Unsigned::from(tx.base_gas(context.protocol))
            .add(&Unsigned::from(size as u64).mul_u64(20))
            .mul_u64(context.protocol.minimum_gas_price());
        if tx.fee() < &fee {
            return Err(crate::Error::TooLowFee.into());
        }
        if tx
            .gas_price()
            .is_some_and(|p| p < &context.protocol.minimum_gas_price().into())
        {
            return Err(Error::Rejected("too_low_gas_price").into());
        }
        if tx.ttl() != &0.into() && tx.ttl() < &context.height.into() {
            return Err(crate::Error::TtlExpired.into());
        }
        if !tx
            .valid_at_protocol_with_config(context.protocol, limits.transaction, state_config)
            .map_err(Error::Codec)?
        {
            return Err(crate::Error::InvalidAtProtocol.into());
        }
    }
    let c = trees.channels.lookup(*key).map_err(Error::State)?;
    let decoded = match deserialize(payload, limits, config.channel_state_atoms) {
        Err(e @ Error::Codec(_)) => return Err(e.into()),
        r => r,
    };
    let mut c = c.ok_or(Error::Rejected("channel_does_not_exist"))?;
    let decoded = decoded?;
    if let Some(signed) = &decoded {
        let (payload_key, r, _) = state(signed);
        require(payload_key == key, "bad_state_channel_pubkey")?;
        next_round(&c, r)?;
        allowed(
            &c,
            *sender,
            tx.kind(),
            config
                .network
                .protocol_at(context.height)
                .map_err(Error::Profile)?,
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
    let ae_tx::channels::Update::CallContract {
        caller,
        contract,
        abi_version,
        gas,
        gas_price,
        ..
    } = update
    else {
        return Err(Error::Rejected("update_not_call").into());
    };
    next_round(&c, round)?;
    if caller == sender {
        peer(&c, *sender)?;
    } else {
        require(context.protocol >= Protocol::Iris, "not_caller")?;
        require(c.is_solo_closing(), "channel_not_closing")?;
        let role = c
            .role(Id::new(IdTag::Account, caller.0))
            .ok_or(Error::Rejected("not_caller_or_delegate"))?;
        let delegates = c.delegate_pubkeys(Some(role)).map_err(|e| match e {
            ae_state::channels::Error::InvalidIdType => {
                Error::Native(NativeFailure::FunctionClause)
            }
            e => Error::State(e),
        })?;
        require(delegates.contains(sender), "not_caller_or_delegate")?;
    }
    let (payload_round, payload_hash) = decoded
        .as_ref()
        .map(|s| {
            let (_, r, h) = state(s);
            (r, h)
        })
        .unwrap_or((&c.round, c.state_hash.as_slice()));
    require(payload_round.add(&1.into()) == *round, "wrong_round")?;
    let mut off = Trees::deserialize_from_binary(
        encoded.bytes(),
        trees.accounts.tree().store().clone(),
        trees.contracts.tree().store().clone(),
        state_config,
    )
    .map_err(Error::Proof)?;
    require(
        off.roots().hash().0.as_slice() == payload_hash,
        "invalid_poi_hash",
    )?;
    require(
        off.contracts
            .is_contract(*contract)
            .map_err(Error::Contract)?,
        "contract_missing",
    )?;
    let loaded = contracts::channel::load(*contract, &off)?;
    require(
        abi_version == &u64::from(loaded.version.abi).into(),
        "wrong_abi_version",
    )?;
    require(
        loaded.version.can_call(context.protocol),
        "unknown_vm_version",
    )?;
    let ae_state::contracts::Code::Inline(bytes) = &loaded.code else {
        unreachable!()
    };
    let code = contracts::fate::decode_code(bytes, config.fate_limits)
        .map_err(contracts::channel_auth::error)?;
    require(
        ae_protocol::contracts::serialization_available(
            abi_version,
            &u64::from(code.version).into(),
            context.protocol,
        )
        .map_err(|_| Error::Native(NativeFailure::FunctionClause))?,
        "illegal_contract_compiler_version",
    )?;
    check_account(
        &trees.accounts,
        *sender,
        tx.nonce(),
        &tx.fee().add(&product(gas, gas_price)),
        context,
    )?;
    let source = source.ok_or(Error::Native(NativeFailure::MissingSignedTransaction))?;
    if source
        .signatures()
        .len()
        .checked_mul(64)
        .and_then(|n| n.checked_add(source.transaction().view().wire_size()))
        .is_none_or(|n| n > config.signature_limits.max_encoded_bytes)
    {
        return Err(Error::ResourceLimit.into());
    }
    let source_bytes = source.encode();
    if source_bytes.len() > config.signature_limits.max_encoded_bytes {
        return Err(Error::ResourceLimit.into());
    }
    let hash = ae_crypto::blake2b_256(&source_bytes);
    off.calls.prune();
    let call = match offchain::apply(
        update,
        &mut off,
        trees,
        context,
        round,
        &c.channel_reserve,
        config,
    ) {
        Ok(Some(call)) => call,
        Ok(None) => unreachable!(),
        Err(TxError::Channel(e))
            if matches!(e.as_ref(), Error::Native(NativeFailure::OffchainUpdate(_))) =>
        {
            let mut failed = Call::new(
                Id::new(IdTag::Account, caller.0),
                round.clone(),
                *contract,
                None,
                round.clone(),
                gas_price.clone(),
            );
            failed.gas_used = gas.clone();
            failed.return_type = ReturnType::Error;
            failed.return_value = b"invalid_call".to_vec();
            off.calls.insert(&failed).map_err(Error::Contract)?;
            // The original catch returns Trees where its caller matches a pair.
            // Retain the native failure and its complete canonical state payload.
            return Err(Error::Native(NativeFailure::ForceProgressCatch(
                off.serialize_to_binary(limits.state)
                    .map_err(Error::Proof)?,
            ))
            .into());
        }
        Err(e) => return Err(e),
    };
    let computed = off.roots().hash();
    let mut next = trees.clone();
    let used = product(&call.gas_used, &call.gas_price);
    spend(
        &mut next.accounts,
        *sender,
        tx.nonce(),
        &tx.fee().add(&used),
        context,
    )?;
    let mut receipt_call = call;
    receipt_call.contract = hash.0;
    receipt_call.call_target = None;
    receipt_call.id = match context.ga_nonces.get(caller) {
        Some(n) => {
            ga_call_id(
                n.as_slice()
                    .try_into()
                    .map_err(|_| Error::Native(NativeFailure::FunctionClause))?,
                hash.0,
            )
            .0
        }
        None => call_id(caller.0, round, hash.0).0,
    };
    next.calls.insert(&receipt_call).map_err(|e| match e {
        ae_state::contracts::Error::AlreadyPresent => native(Term::tagged(
            "already_present",
            vec![Term::Binary(
                [hash.0.as_slice(), receipt_call.id.as_slice()].concat(),
            )],
        )),
        e => contracts::channel_auth::error(e.into()),
    })?;
    // Closing-channel balance reads happen even when the submitted hash differs.
    let peer_balances = if c.is_active() {
        None
    } else {
        Some([balance(&off, c.initiator)?, balance(&off, c.responder)?])
    };
    let balances_match = peer_balances
        .as_ref()
        .is_none_or(|b| b[0].add(&b[1]) <= c.channel_amount);
    if computed.0.as_slice() == state_hash && balances_match {
        let balances = match peer_balances {
            Some(b) => b,
            None => [balance(&off, c.initiator)?, balance(&off, c.responder)?],
        };
        c.force_progress(
            state_hash.clone(),
            round.clone(),
            balances,
            &context.height.into(),
            decoded.is_some(),
        );
        next.channels.enter(&c).map_err(Error::State)?;
    }
    let payer = context.payer.unwrap_or(*sender);
    let mut events = vec![];
    delta(&mut events, payer, tx.fee().clone(), true, "Channel.fee");
    delta(&mut events, payer, used, true, "Channel.amount");
    events.push(Event::Channel {
        channel: *key,
        transaction_hash: hash,
        transaction_type: event_view(source).kind(),
    });
    events.reverse();
    *trees = next;
    Ok(Receipt {
        transaction_hash: None,
        gas: Unsigned::from(tx.base_gas(context.protocol))
            .add(&Unsigned::from(tx.wire_size() as u64).mul_u64(20))
            .add(gas),
        call: Some(receipt_call),
        events,
        context: context.clone(),
    })
}
