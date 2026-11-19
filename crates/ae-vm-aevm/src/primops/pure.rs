// aevm_ae_primops crypto/auth branches and aec_governance base gas, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_crypto::{
    blake2b_256, keccak256, recover_secp256k1_address, sha256, verify_ed25519, verify_secp256k1,
};
use ae_types::{AccountPublicKey, Signature};

fn illegal<E>() -> Problem<E> {
    Problem::Returned(Error::IllegalCall)
}
fn tuple_bytes<E>(
    data: &[u8],
    pointer: Word,
    words: usize,
    b: &mut Budget,
) -> Result<Vec<u8>, Problem<E>> {
    if data.get(..32) == Some(pointer.0.as_slice()) {
        return Err(illegal());
    }
    let start = pointer.unsigned().to_usize().ok_or_else(illegal)?;
    let size = words.checked_mul(32).ok_or_else(illegal)?;
    let end = start.checked_add(size).ok_or_else(illegal)?;
    let bytes = data.get(start..end).ok_or_else(illegal)?;
    b.work(words)?;
    b.bytes(size)?;
    Ok(bytes.to_vec())
}
fn string<E>(data: &[u8], pointer: Word, b: &mut Budget) -> Result<Vec<u8>, Problem<E>> {
    if data.get(..32) == Some(pointer.0.as_slice()) {
        return Err(illegal());
    }
    let start = pointer.unsigned().to_usize().ok_or_else(illegal)?;
    let body = start.checked_add(32).ok_or_else(illegal)?;
    let size = BigUint::from_bytes_be(data.get(start..body).ok_or_else(illegal)?)
        .to_usize()
        .ok_or_else(illegal)?;
    let end = body.checked_add(size).ok_or_else(illegal)?;
    let bytes = data.get(body..end).ok_or_else(illegal)?;
    b.bytes(size)?;
    b.work(size.div_ceil(32))?;
    Ok(bytes.to_vec())
}
fn option(value: Option<&[u8; 32]>) -> Vec<u8> {
    let mut out = Word::from(32).0.to_vec();
    out.extend_from_slice(&Word::from(u64::from(value.is_some())).0);
    if let Some(value) = value {
        out.extend_from_slice(value);
    }
    out
}
fn run<H: MapHost>(
    s: &State,
    allowance: &BigUint,
    data: &[u8],
    host: &H,
    b: &mut Budget,
) -> Result<Outcome, Problem<H::Error>> {
    let op = calls::words(data, 1, b)?[0];
    let id = op.unsigned().to_u16();
    if matches!(id, Some(300..=399)) {
        return super::run(s, data, host, b);
    }
    let base = match id {
        Some(400 | 410 | 420 | 421) => 1300u16,
        Some(401..=405) => 30,
        Some(500) => 0,
        Some(400..=599) => return Ok(Outcome::Error(Error::IllegalCall)),
        _ => return Err(Stop::UnimplementedPrimop(op).into()),
    };
    let next = execution::snapshot(s, b, true)?;
    let mut gas_spent = BigUint::from(base);
    if allowance < &gas_spent {
        return Ok(Outcome::Error(Error::BaseGas {
            op,
            required: gas_spent,
            state: Box::new(next),
        }));
    }
    let id = id.expect("known primitive");
    let enabled = match id {
        410 | 500 => matches!(s.version, Version::Sophia3 | Version::Sophia4),
        420 | 421 => s.version == Version::Sophia4,
        _ => matches!(
            s.version,
            Version::Sophia2 | Version::Sophia3 | Version::Sophia4
        ),
    };
    if !enabled {
        return Ok(Outcome::Error(Error::OutOfGas));
    }
    let output = match id {
        401..=403 => {
            let args = calls::words(data, 3, b)?;
            let parent = Some(BigUint::from_bytes_be(&data[..32]));
            let ty = store::parse_type(data, args[1].unsigned(), parent, b)?;
            let (bytes, gas) = serialize(&ty, args[2], s, b)?;
            gas_spent += gas;
            b.work(bytes.len().div_ceil(32))?;
            match id {
                401 => keccak256(&bytes),
                402 => sha256(&bytes),
                _ => blake2b_256(&bytes),
            }
            .0
            .to_vec()
        }
        404 | 405 => {
            let args = calls::words(data, 2, b)?;
            let bytes = string(data, args[1], b)?;
            if id == 404 {
                sha256(&bytes)
            } else {
                blake2b_256(&bytes)
            }
            .0
            .to_vec()
        }
        500 => {
            let hash = s
                .auth_tx_hash
                .as_ref()
                .map(|h| <&[u8; 32]>::try_from(h.as_slice()).map_err(|_| illegal()))
                .transpose()?;
            option(hash)
        }
        _ => {
            let args = calls::words(data, if id == 421 { 3 } else { 4 }, b)?;
            b.work(1)?;
            if id == 400 {
                let signature = tuple_bytes(data, args[3], 2, b)?;
                Word::from(u64::from(verify_ed25519(
                    &AccountPublicKey(args[2].0),
                    &args[1].0,
                    &Signature(signature.try_into().expect("64 bytes")),
                )))
                .0
                .to_vec()
            } else if id == 410 {
                let key = tuple_bytes(data, args[2], 2, b)?;
                let signature = tuple_bytes(data, args[3], 2, b)?;
                let valid = verify_secp256k1(
                    &key.try_into().expect("64 bytes"),
                    &args[1].0,
                    &signature.try_into().expect("64 bytes"),
                )
                .map_err(|_| illegal())?;
                Word::from(u64::from(valid)).0.to_vec()
            } else {
                let signature = tuple_bytes(data, args[if id == 420 { 3 } else { 2 }], 3, b)?;
                let recovered = recover_secp256k1_address(
                    &args[1].0,
                    signature[..65].try_into().expect("65 bytes"),
                );
                if id == 420 {
                    Word::from(u64::from(recovered.is_some_and(|a| a == args[2].0[..20])))
                        .0
                        .to_vec()
                } else {
                    let padded = recovered.map(|a| {
                        let mut word = [0u8; 32];
                        word[..20].copy_from_slice(&a);
                        word
                    });
                    option(padded.as_ref())
                }
            }
        }
    };
    b.bytes(output.len())?;
    Ok(Outcome::Complete {
        data: output,
        gas_spent,
        state: Box::new(next),
    })
}
// VM-state primitive families. Protocol-local result marshalling is a separate
// classification: authentication results use remote binary import semantics.
pub fn call_local<H: MapHost>(
    state: &State,
    allowance: &BigUint,
    data: &[u8],
    host: &H,
    limits: Limits,
) -> Result<Outcome, Fault<H::Error>> {
    call_local_inner(state, allowance, data, host, &mut Budget::new(limits))
}
pub(crate) fn call_local_inner<H: MapHost>(
    state: &State,
    allowance: &BigUint,
    data: &[u8],
    host: &H,
    b: &mut Budget,
) -> Result<Outcome, Fault<H::Error>> {
    match run(state, allowance, data, host, b) {
        Ok(out) => Ok(out),
        Err(Problem::Returned(e)) => Ok(Outcome::Error(e)),
        Err(Problem::Host(e)) => Err(Fault::Host(e)),
        Err(Problem::Fault(Fault::Stop(e))) => Err(Fault::Stop(e)),
        Err(Problem::Fault(Fault::Native(_))) => Ok(Outcome::Error(Error::IllegalCall)),
        Err(Problem::Fault(Fault::Host(e))) => match e {},
    }
}
