// aevm_ae_primops AENS mutations, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::*;
use crate::heap::{self, Type as T, Value as V};

#[derive(Debug, Clone, Copy)]
pub enum NameWrite<'a> {
    Preclaim {
        account: Word,
        commitment: Word,
    },
    Claim {
        account: Word,
        name: &'a [u8],
        salt: Word,
        fee: Option<Word>,
    },
    Transfer {
        account: Word,
        recipient: Word,
        hash: Word,
    },
    Revoke {
        account: Word,
        hash: Word,
    },
}
impl NameWrite<'_> {
    pub fn operation(self) -> Word {
        Word::from(match self {
            Self::Preclaim { .. } => 201,
            Self::Claim { .. } => 202,
            Self::Transfer { .. } => 204,
            Self::Revoke { .. } => 205,
        })
    }
}
fn word(v: &V) -> Word {
    let V::Integer(n) = v else { unreachable!() };
    Word::wrapping(n)
}
fn string(v: &V) -> &[u8] {
    let V::String(b) = v else { unreachable!() };
    b
}
fn signature(v: &V) -> [u8; 64] {
    let V::Tuple(v) = v else { unreachable!() };
    let mut out = [0; 64];
    out[..32].copy_from_slice(&word(&v[0]).0);
    out[32..].copy_from_slice(&word(&v[1]).0);
    out
}
pub(super) fn call<H: ChainHost>(
    s: &State,
    allowance: &BigUint,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    let op = calls::words(data, 1, b).map_err(Fault::widen)?[0];
    let id = op.unsigned().to_u16().expect("AENS write ID");
    let state = Box::new(execution::snapshot(s, b, true)?);
    let base = BigUint::from(12000u16);
    let error = |e| Ok((Outcome::Error(e), chain.clone()));
    if allowance < &base || s.auth_tx_hash.is_some() {
        return error(Error::BaseGas {
            op,
            required: base,
            state,
        });
    }
    if id == 203 {
        return error(Error::IllegalAens(op));
    }
    macro_rules! returned {
        ($e:expr) => {
            match $e? {
                Ok(v) => v,
                Err(e) => {
                    b.bytes(e.len())?;
                    return error(Error::Chain(e));
                }
            }
        };
    }
    let modern = s.version == Version::Sophia4;
    let sign = T::Tuple(vec![T::Word; 2]);
    let name_type = if modern { T::String } else { T::Word };
    let types = match id {
        201 => vec![T::Word, T::Word, T::Word, sign],
        202 if modern => vec![T::Word, T::Word, T::String, T::Word, T::Word, sign],
        202 => vec![T::Word, T::Word, T::String, T::Word, sign],
        204 => vec![T::Word, T::Word, T::Word, name_type, sign],
        _ => vec![T::Word, T::Word, name_type, sign],
    };
    let V::Tuple(args) =
        heap::decode_with_budget(&T::Tuple(types), data, b).map_err(oracle::heap_fault)?
    else {
        unreachable!()
    };
    let account = word(&args[1]);
    let request = match id {
        201 => NameWrite::Preclaim {
            account,
            commitment: word(&args[2]),
        },
        202 => NameWrite::Claim {
            account,
            name: string(&args[2]),
            salt: word(&args[3]),
            fee: modern.then(|| word(&args[4])),
        },
        _ => {
            let a = &args[if id == 204 { 3 } else { 2 }];
            let hash = if modern {
                // aens:get_name_hash runs before the chain preparation callback.
                b.work(1)?;
                returned!(host.name_hash(op, string(a), b))
            } else {
                word(a)
            };
            if id == 204 {
                NameWrite::Transfer {
                    account,
                    recipient: word(&args[2]),
                    hash,
                }
            } else {
                NameWrite::Revoke { account, hash }
            }
        }
    };
    b.work(1)?;
    let (tx, size) = returned!(host.prepare_name(request, chain, b));
    let dynamic = if id == 201 || id == 202 {
        BigUint::from(size) * 20u8
    } else {
        BigUint::default()
    };
    if &base + &dynamic > *allowance {
        return error(Error::DynamicGas {
            op,
            required: dynamic,
            state,
        });
    }
    let signature = signature(args.last().expect("signature"));
    b.work(1)?;
    let next = returned!(host.apply_name(op, tx, signature, chain, b));
    b.bytes(32)?;
    Ok((
        Outcome::Complete {
            data: vec![0; 32],
            gas_spent: base + dynamic,
            state,
        },
        next,
    ))
}
