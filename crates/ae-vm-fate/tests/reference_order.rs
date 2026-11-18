use ae_vm_fate::{
    Error, Limits,
    maps::{
        MapOrder, Traversal,
        order::{self, Atom, Mixing, Otp26, WordSize},
    },
    value::Value,
};
use serde_json::Value as J;
use std::cmp::Ordering;
#[path = "support/otp_order.rs"]
mod otp_order;
#[path = "support/values.rs"]
mod values;
use values::{rows, value};
fn fixtures() -> [J; 2] {
    [
        serde_json::from_str(include_str!("fixtures/erlang_fate_order_0.json")).unwrap(),
        serde_json::from_str(include_str!("fixtures/erlang_fate_order_1000.json")).unwrap(),
    ]
}
#[test]
fn explicit_otp_profiles_match_native_hashes_term_order_and_hamt_traversal() {
    for fixture in fixtures() {
        let profile = otp_order::profile(&fixture);
        let values: Vec<_> = rows(&fixture["values"])
            .iter()
            .map(|r| value(&r["value"]))
            .collect();
        for (i, (v, row)) in values.iter().zip(rows(&fixture["values"])).enumerate() {
            assert_eq!(
                profile.hash(v, Limits::default()).unwrap(),
                row["hash"].as_u64().unwrap() as u32,
                "hash {i}: {v:?}"
            );
        }
        for row in rows(&fixture["comparisons"]) {
            let a = &values[row[0].as_u64().unwrap() as usize];
            let b = &values[row[1].as_u64().unwrap() as usize];
            let expected = match row[2].as_i64().unwrap() {
                -1 => Ordering::Less,
                0 => Ordering::Equal,
                1 => Ordering::Greater,
                _ => unreachable!(),
            };
            assert_eq!(
                order::compare(a, b, Limits::default()).unwrap(),
                expected,
                "comparison {row}"
            );
        }
        for (i, row) in rows(&fixture["orders"]).iter().enumerate() {
            let keys: Vec<_> = rows(&row["keys"]).iter().map(value).collect();
            let refs: Vec<_> = keys.iter().collect();
            for (mode, field) in [
                (Traversal::Fold, "fold"),
                (Traversal::ToList, "list"),
                (Traversal::TermSorted, "sorted"),
            ] {
                let indices = profile.indices(&refs, mode, Limits::default()).unwrap();
                assert_eq!(
                    indices
                        .into_iter()
                        .map(|j| keys[j].clone())
                        .collect::<Vec<_>>(),
                    rows(&row[field]).iter().map(value).collect::<Vec<_>>(),
                    "order {i} {mode:?}"
                );
            }
        }
    }
}
#[test]
fn profile_selection_is_explicit_complete_and_bounded() {
    let fixtures = fixtures();
    let a = otp_order::profile(&fixtures[0]);
    let b = otp_order::profile(&fixtures[1]);
    assert_ne!(
        a.hash(&Value::Bits(1.into()), Limits::default()).unwrap(),
        b.hash(&Value::Bits(1.into()), Limits::default()).unwrap()
    );
    assert!(matches!(
        Otp26::new(WordSize::Bits64, Mixing::Jenkins, Default::default()),
        Err(Error::InvalidOrderProfile)
    ));
    let duplicate = Atom::ALL.into_iter().map(|a| (a, 0)).collect();
    assert!(matches!(
        Otp26::new(WordSize::Bits64, Mixing::Jenkins, duplicate),
        Err(Error::InvalidOrderProfile)
    ));
    let large = Atom::ALL
        .into_iter()
        .enumerate()
        .map(|(i, a)| (a, (1 << 26) + i as u32))
        .collect();
    assert!(matches!(
        Otp26::new(WordSize::Bits32, Mixing::Jenkins, large),
        Err(Error::InvalidOrderProfile)
    ));
    let value = Value::String(vec![1; 100]);
    assert_eq!(
        a.hash(
            &value,
            Limits {
                max_bytes: 99,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert!(
        a.hash(
            &value,
            Limits {
                max_bytes: 100,
                ..Limits::default()
            }
        )
        .is_ok()
    );
    let deep = (0..20).fold(Value::Integer(1.into()), |v, _| Value::Tuple(vec![v]));
    assert_eq!(
        a.hash(
            &deep,
            Limits {
                max_depth: 8,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        a.indices(
            &[&value],
            Traversal::Fold,
            Limits {
                max_nodes: 0,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
}
