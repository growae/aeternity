// Oracle primitives shared by transaction callbacks and FATE chain hosts.
// Reference: aeprimop.erl, fa2a6d7. Copyright (c) 2019, Aeternity Anstalt.
// ISC notice in repository LICENSE. Callers provide an atomic outer checkpoint.
use super::{Failure, abi, function_clause};
use crate::{Context, Error};
use ae_state::oracles::{Oracle, Oracles, Query, ga_query_id};
use ae_trie::NodeWriter;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

pub(crate) fn register<S: NodeWriter, C: NodeWriter>(
    tree: &mut Oracles<S, C>,
    oracle: Oracle,
    context: &Context,
    limits: abi::Limits,
) -> Result<Oracle, Error> {
    if tree.oracle(oracle.public_key)?.is_some() {
        return Err(Failure::AlreadyOracle.into());
    }
    abi::validate_registration(
        &oracle.abi_version,
        &oracle.query_format,
        &oracle.response_format,
        context.protocol,
        limits,
    )?;
    tree.enter_oracle(&oracle)?;
    Ok(oracle)
}
pub(crate) fn extend<S: NodeWriter, C: NodeWriter>(
    tree: &mut Oracles<S, C>,
    key: [u8; 32],
    delta: &Unsigned,
) -> Result<Oracle, Error> {
    if delta == &0.into() {
        return Err(Failure::ZeroExtensionTtl.into());
    }
    let mut oracle = tree.oracle(key)?.ok_or(Failure::NotActiveOracle)?;
    oracle.expires_by = oracle.expires_by.add(delta);
    tree.enter_oracle(&oracle)?;
    Ok(oracle)
}
pub(crate) struct NewQuery<'a> {
    pub ident: Id,
    pub oracle: [u8; 32],
    pub sender: AccountPublicKey,
    pub nonce: &'a Unsigned,
    pub content: &'a [u8],
    pub fee: &'a Unsigned,
    pub delta: &'a Unsigned,
    pub response_ttl: &'a Unsigned,
}
pub(crate) fn query<S: NodeWriter, C: NodeWriter>(
    tree: &mut Oracles<S, C>,
    request: NewQuery<'_>,
    context: &Context,
    limits: abi::Limits,
) -> Result<Query, Error> {
    let oracle = tree
        .oracle(request.oracle)?
        .ok_or(Failure::OracleDoesNotExist)?;
    if request.fee < &oracle.query_fee {
        return Err(Failure::QueryFeeTooLow.into());
    }
    let expires_by = Unsigned::from(context.height).add(request.delta);
    if expires_by.add(request.response_ttl) > oracle.expires_by {
        return Err(Failure::TooLongTtl.into());
    }
    abi::validate_content(
        &oracle.abi_version,
        &oracle.query_format,
        request.content,
        context.protocol,
        limits,
    )?;
    let mut query = Query::new(
        request.ident,
        request.sender,
        request.nonce.clone(),
        request.content.to_vec(),
        request.fee.clone(),
        expires_by,
        request.response_ttl.clone(),
    )
    .map_err(|e| match e {
        ae_state::oracles::Error::ZeroResponseTtl => Error::from(Failure::IllegalQuerySpec),
        e => e.into(),
    })?;
    query.oracle = Id::new(IdTag::Oracle, request.oracle);
    if let Some(nonce) = context.ga_nonces.get(&request.sender) {
        query.id = ga_query_id(
            nonce.as_slice().try_into().map_err(|_| function_clause())?,
            request.oracle,
        )
        .0;
    }
    if tree.query(request.oracle, query.id)?.is_some() {
        return Err(Failure::QueryAlreadyPresent.into());
    }
    tree.enter_query(&query)?;
    Ok(query)
}
pub(crate) fn respond<S: NodeWriter, C: NodeWriter>(
    tree: &mut Oracles<S, C>,
    key: [u8; 32],
    id: [u8; 32],
    response: &[u8],
    ttl: Option<&Unsigned>,
    context: &Context,
    limits: abi::Limits,
) -> Result<Query, Error> {
    let mut query = tree.query(key, id)?.ok_or(Failure::NoMatchingQuery)?;
    if ttl.is_some_and(|ttl| ttl != &query.response_ttl) {
        return Err(Failure::WrongResponseTtl.into());
    }
    let oracle = tree.oracle(key)?.ok_or(Failure::OracleDoesNotExist)?;
    if query.oracle.tag != IdTag::Oracle {
        return Err(function_clause());
    }
    if query.oracle.value != key {
        return Err(Failure::WrongOracle.into());
    }
    abi::validate_content(
        &oracle.abi_version,
        &oracle.response_format,
        response,
        context.protocol,
        limits,
    )?;
    if !query.is_open() {
        return Err(Failure::QueryClosed.into());
    }
    query.add_response(&context.height.into(), Some(response.to_vec()));
    tree.enter_query(&query)?;
    Ok(query)
}
