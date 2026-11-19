// aevm_ae_primops spend/address branches, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use crate::runtime::StoreHost;
use ae_types::{Id, IdTag};
use num_bigint::BigInt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressKind {
    Oracle,
    Contract,
    Payable,
}
// Returned chain errors are protocol results. Native faults are caught by the
// primitive wrapper; host and resource faults remain retryable operational stops.
pub type HostResult<T, E> = Result<Result<T, String>, Fault<E>>;
pub trait ChainHost: StoreHost {
    type Spend;
    type Oracle;
    fn oracle_height(
        &self,
        op: Word,
        _chain: &Self::Chain,
        _budget: &mut Budget,
    ) -> Result<BigUint, Fault<Self::Error>> {
        Err(Stop::UnimplementedPrimop(op).into())
    }
    fn prepare_oracle(
        &self,
        request: OracleWrite<'_>,
        _chain: &Self::Chain,
        _budget: &mut Budget,
    ) -> HostResult<(Self::Oracle, usize), Self::Error> {
        Err(Stop::UnimplementedPrimop(request.operation()).into())
    }
    fn apply_oracle(
        &self,
        op: Word,
        _transaction: Self::Oracle,
        _signature: Option<[u8; 64]>,
        _chain: &Self::Chain,
        _budget: &mut Budget,
    ) -> HostResult<(crate::heap::Value, Self::Chain), Self::Error> {
        Err(Stop::UnimplementedPrimop(op).into())
    }
    fn oracle(
        &self,
        request: OracleRequest<'_>,
        _chain: &Self::Chain,
        _budget: &mut Budget,
    ) -> HostResult<crate::heap::Value, Self::Error> {
        Err(Stop::UnimplementedPrimop(request.operation()).into())
    }
    fn get_store(&self, chain: &Self::Chain) -> Result<maps::Store, Fault<Self::Error>>;
    fn prepare_spend(
        &self,
        recipient: Id,
        value: &BigInt,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Self::Spend, Self::Error>;
    fn apply_spend(
        &self,
        spend: Self::Spend,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Self::Chain, Self::Error>;
    fn address(
        &self,
        kind: AddressKind,
        address: Word,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Word, Self::Error>;
}
struct Bound<'a, H: ChainHost> {
    host: &'a H,
    chain: &'a H::Chain,
}
impl<H: ChainHost> MapHost for Bound<'_, H> {
    type Error = H::Error;
    fn get_store(&self) -> Result<maps::Store, Fault<Self::Error>> {
        self.host.get_store(self.chain)
    }
}
fn run<H: ChainHost>(
    s: &State,
    allowance: &BigUint,
    value: &BigInt,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    let op = calls::words(data, 1, b).map_err(Fault::widen)?[0];
    let id = op.unsigned().to_u16();
    if matches!(id, Some(300..=599)) {
        return Ok((
            call_local_inner(s, allowance, data, &Bound { host, chain }, b)?,
            chain.clone(),
        ));
    }
    if matches!(id, Some(104..=106 | 110 | 111)) {
        return oracle::call(s, allowance, data, chain, host, b);
    }
    if matches!(id, Some(100..=103)) {
        return oracle_write::call(s, allowance, value, data, chain, host, b);
    }
    let error = |e| Ok((Outcome::Error(e), chain.clone()));
    let base: BigUint = match id {
        Some(1) => 12000u16.into(),
        Some(600 | 601) => 5000u16.into(),
        Some(610) => 3000u16.into(),
        Some(100..=106 | 110 | 111 | 200..=205) => return Err(Stop::UnimplementedPrimop(op).into()),
        _ => return error(Error::IllegalCall),
    };
    let state = Box::new(execution::snapshot(s, b, true)?);
    if allowance < &base || s.auth_tx_hash.is_some() {
        return error(Error::BaseGas {
            op,
            required: base,
            state,
        });
    }
    let id = id.expect("known chain primitive");
    if id != 1
        && !(matches!(s.version, Version::Sophia3 | Version::Sophia4)
            && (id != 610 || s.version == Version::Sophia4))
    {
        return error(Error::OutOfGas);
    }
    let args = calls::words(data, 2, b).map_err(Fault::widen)?;
    let (output, next) = if id == 1 {
        b.bytes(usize::try_from(value.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
        b.work(1)?;
        let spend = match host.prepare_spend(Id::new(IdTag::Account, args[1].0), value, chain, b)? {
            Ok(spend) => spend,
            Err(_) => return error(Error::IllegalCall),
        };
        b.work(1)?;
        match host.apply_spend(spend, chain, b)? {
            Ok(next) => (Word::ZERO, next),
            Err(reason) => {
                b.bytes(reason.len())?;
                return error(Error::Chain(reason));
            }
        }
    } else {
        let kind = match id {
            600 => AddressKind::Oracle,
            601 => AddressKind::Contract,
            _ => AddressKind::Payable,
        };
        b.work(1)?;
        match host.address(kind, args[1], chain, b)? {
            Ok(word) => (word, chain.clone()),
            Err(reason) => {
                b.bytes(reason.len())?;
                return error(Error::Chain(reason));
            }
        }
    };
    b.bytes(32)?;
    Ok((
        Outcome::Complete {
            data: output.0.to_vec(),
            gas_spent: base,
            state,
        },
        next,
    ))
}
pub fn call<H: ChainHost>(
    state: &State,
    allowance: &BigUint,
    value: &BigInt,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    limits: Limits,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    call_inner(
        state,
        allowance,
        value,
        data,
        chain,
        host,
        &mut Budget::new(limits),
    )
}
pub(crate) fn call_inner<H: ChainHost>(
    state: &State,
    allowance: &BigUint,
    value: &BigInt,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    match run(state, allowance, value, data, chain, host, b) {
        Err(Fault::Native(_)) => Ok((Outcome::Error(Error::IllegalCall), chain.clone())),
        result => result,
    }
}
