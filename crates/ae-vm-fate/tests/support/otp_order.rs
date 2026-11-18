use ae_vm_fate::{
    Limits,
    maps::order::{Atom, Mixing, Otp26, WordSize},
    value::Value,
};
use serde_json::Value as J;
pub fn profile(fixture: &J) -> Otp26 {
    let word_size = match fixture["word_size"].as_u64().unwrap() {
        32 => WordSize::Bits32,
        64 => WordSize::Bits64,
        _ => unreachable!(),
    };
    let atoms: std::collections::BTreeMap<_, _> = fixture["atoms"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                Atom::from_name(a[0].as_str().unwrap()).unwrap(),
                a[1].as_u64().unwrap() as u32,
            )
        })
        .collect();
    // The zero fingerprint identifies the runtime build's mixing function;
    // all other hashes, ordering and finalized roots are independent outputs.
    let candidates = [Mixing::Jenkins, Mixing::Crc32c]
        .into_iter()
        .map(|mix| Otp26::new(word_size, mix, atoms.clone()).unwrap())
        .filter(|p| {
            p.hash(&Value::Integer(0.into()), Limits::default())
                .unwrap() as u64
                == fixture["hash_zero"].as_u64().unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        candidates.len(),
        1,
        "unrecognized runtime mixing fingerprint"
    );
    candidates.into_iter().next().unwrap()
}
