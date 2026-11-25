#![allow(dead_code)]
// Explicit captured test-runtime choices; not a deployed-mainnet attestation.
use ae_node::configuration::execution::Specification;
use ae_vm_fate::{
    maps::order::{Atom, Mixing, Otp26, WordSize},
    value::Value as Fate,
};
use serde_json::{Value, json};
pub fn value(profile: &Value) -> Value {
    let word = match profile["word_size"].as_u64().unwrap() {
        32 => WordSize::Bits32,
        64 => WordSize::Bits64,
        _ => unreachable!(),
    };
    let atoms = profile["atoms"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                Atom::from_name(a[0].as_str().unwrap()).unwrap(),
                a[1].as_u64().unwrap() as u32,
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mixes: Vec<_> = [("jenkins", Mixing::Jenkins), ("crc32c", Mixing::Crc32c)]
        .into_iter()
        .filter(|(_, mix)| {
            Otp26::new(word, *mix, atoms.clone())
                .unwrap()
                .hash(&Fate::Integer(0.into()), Default::default())
                .unwrap() as u64
                == profile["hash_zero"].as_u64().unwrap()
        })
        .collect();
    assert_eq!(mixes.len(), 1);
    let atoms: serde_json::Map<_, _> = atoms
        .into_iter()
        .map(|(a, i)| (a.name().into(), json!(i)))
        .collect();
    json!({"version":1,"fate_word_size":profile["word_size"],"fate_mixing":mixes[0].0,"fate_atoms":atoms,"arithmetic_word_size":64,"unicode":"otp26-unicode15","bls":"mcl11976be-llvm64-vint-arm64","diagnostic":"release","aevm_float":"otp26-word64","aevm_store":"otp26-word64-crc32c","channel_state_atoms":null})
}
pub fn default_value() -> Value {
    value(
        &serde_json::from_str::<Value>(include_str!(
            "../../../ae-vm-fate/tests/fixtures/erlang_fate_order_0.json"
        ))
        .unwrap(),
    )
}
pub fn specification(profile: &Value) -> Specification {
    Specification::from_value(&value(profile)).unwrap()
}
