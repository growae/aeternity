// emcl e988f69 emcl_nif.c / emcl.erl and MCL 11976be operator.hpp.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use super::{
    Fr, Profile,
    field::{Field, Fp},
    scalar,
};
use num_bigint::BigUint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerConversionError {
    // mcl getStr returns zero; emcl then raises {badmatch,{error,bad_value}}.
    DecimalBufferRejected,
    // getStr writes the terminator one byte past the NIF's stack buffer.
    // No deterministic VM result is justified by the original C behavior.
    UndefinedDecimalBufferWrite,
}

fn decimal_result(value: BigUint, capacity: usize) -> Result<BigUint, IntegerConversionError> {
    let digits = value.to_str_radix(10).len();
    if digits == capacity {
        Err(IntegerConversionError::UndefinedDecimalBufferWrite)
    } else if digits >= capacity - 1 {
        Err(IntegerConversionError::DecimalBufferRejected)
    } else {
        Ok(value)
    }
}

pub fn fr_to_integer(_: Profile, raw: &Fr) -> Result<BigUint, IntegerConversionError> {
    decimal_result(scalar::decode(raw), 64)
}

pub fn fp_to_integer(_: Profile, raw: &[u8; 48]) -> Result<BigUint, IntegerConversionError> {
    // fromMont multiplies the raw limbs by the ordinary integer one, not the
    // Montgomery encoding of one. Every 384-bit input is accepted by emcl.
    decimal_result(decode_fp(raw), 96)
}

fn decode_fp(raw: &[u8; 48]) -> BigUint {
    Fp::read(raw).mul(&Fp(BigUint::from(1u8))).0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_raw_decode_matches_original_large_buffer_and_get_mpz() {
        let rows: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/erlang_bls_reverse.json"))
                .unwrap();
        for row in rows.as_array().unwrap() {
            let raw = hex::decode(row["raw"].as_str().unwrap()).unwrap();
            let integer = match row["field"].as_str().unwrap() {
                "fr" => scalar::decode(&raw.try_into().unwrap()),
                "fp" => decode_fp(&raw.try_into().unwrap()),
                _ => unreachable!(),
            };
            assert_eq!(integer.to_str_radix(10), row["decoded"].as_str().unwrap());
        }
    }
}
