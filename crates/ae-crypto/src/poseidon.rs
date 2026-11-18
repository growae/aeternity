// aeu_poseidon:hash3 and aeu_poseidon_constants, pinned fa2a6d7.
// Original implementation by Hans Svensson (2021).
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use num_bigint::BigUint;
use std::sync::LazyLock;

mod constants;

fn integer(limbs: [u64; 4]) -> BigUint {
    BigUint::from_slice(
        &limbs
            .into_iter()
            .flat_map(|x| [x as u32, (x >> 32) as u32])
            .collect::<Vec<_>>(),
    )
}

static MODULUS: LazyLock<BigUint> = LazyLock::new(|| integer(constants::MODULUS));
static ROUNDS: LazyLock<[BigUint; 195]> = LazyLock::new(|| constants::ROUNDS.map(integer));
static MATRIX: LazyLock<[[BigUint; 3]; 3]> =
    LazyLock::new(|| constants::MATRIX.map(|row| row.map(integer)));

// Two canonical, big-endian scalar-field inputs. The FATE instruction applies
// its stricter historical admission rule separately. No secret inputs or
// constant-time guarantees: this function executes public contract arithmetic.
pub fn poseidon_bls12_381(left: &[u8; 32], right: &[u8; 32]) -> Option<[u8; 32]> {
    let left = BigUint::from_bytes_be(left);
    let right = BigUint::from_bytes_be(right);
    let modulus = &*MODULUS;
    if &left >= modulus || &right >= modulus {
        return None;
    }
    let mut state = [BigUint::default(), left, right];
    for (round, constants) in ROUNDS.chunks_exact(3).enumerate() {
        for (i, item) in state.iter_mut().enumerate() {
            *item += &constants[i];
            if i == 0 || !(4..=60).contains(&round) {
                let square = &*item * &*item % modulus;
                *item = (&*item * &square % modulus) * square % modulus;
            }
        }
        state = std::array::from_fn(|i| {
            (0..3).map(|j| &MATRIX[i][j] * &state[j]).sum::<BigUint>() % modulus
        });
    }
    let bytes = state[0].to_bytes_be();
    let mut result = [0; 32];
    result[32 - bytes.len()..].copy_from_slice(&bytes);
    Some(result)
}
