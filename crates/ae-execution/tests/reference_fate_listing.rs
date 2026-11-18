use ae_execution::fate_compat;
use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_vm_fate::{
    Error, Limits, encoding,
    maps::{MapError, MapOrder, Traversal, listing::Overrides},
    value::{ObjectKind, ObjectPayload, Value},
};
use serde_json::Value as J;
use std::{collections::BTreeMap, sync::LazyLock};
#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
#[path = "../../ae-vm-fate/tests/support/values.rs"]
mod values;
use values::{bytes, rows, string, value};
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_listing.json")).unwrap()
});
struct NoOrder;
impl MapOrder for NoOrder {
    fn indices(&self, _: &[&Value], _: Traversal, _: Limits) -> Result<Vec<usize>, Error> {
        panic!("this path must not depend on a runtime map profile")
    }
}
#[test]
fn all_historical_listing_outputs_match_original_helpers() {
    let profile = otp_order::profile(&FIXTURE["profile"]);
    let tables: BTreeMap<_, _> = rows(&FIXTURE["tables"])
        .iter()
        .map(|t| {
            let entries: Vec<_> = rows(&t["entries"])
                .iter()
                .map(|p| (bytes(&p[0]), bytes(&p[1])))
                .collect();
            (
                string(&t["name"]),
                Overrides::from_encoded(
                    entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
                    Limits::default(),
                )
                .unwrap(),
            )
        })
        .collect();
    for (i, row) in rows(&FIXTURE["cases"]).iter().enumerate() {
        let Value::Map(map) = value(&row["map"]) else {
            unreachable!()
        };
        for run in rows(&row["runs"]) {
            let expected = &FIXTURE["results"][run[1].as_u64().unwrap() as usize];
            let result = tables[string(&row["table"])].list(
                map.clone(),
                run[0].as_u64().unwrap() as u32,
                &profile,
                Limits::default(),
            );
            match result {
                Ok(out) => assert_eq!(
                    out.value.as_ref(),
                    &value(&expected["value"]),
                    "case {i}, protocol {}",
                    run[0]
                ),
                Err(MapError::Data(Error::UnorderedValue)) => {
                    assert_eq!(expected["exception"], true, "case {i}, protocol {}", run[0])
                }
                Err(e) => panic!("unexpected error case {i}: {e:?}"),
            }
        }
    }
}
#[test]
fn public_network_assets_are_selected_explicitly_and_iris_ignores_overrides() {
    for (profile, name) in [
        (NetworkProfile::mainnet(), "mainnet"),
        (NetworkProfile::uat(), "uat"),
    ] {
        let table = fate_compat::map_ordering(&profile, Limits::default()).unwrap();
        let source = rows(&FIXTURE["tables"])
            .iter()
            .find(|t| t["name"] == name)
            .unwrap();
        assert_eq!(
            profile.pre_iris_map_ordering().len(),
            rows(&source["entries"]).len()
        );
        for ((key, expected), pair) in profile
            .pre_iris_map_ordering()
            .iter()
            .zip(rows(&source["entries"]))
        {
            assert_eq!(*key, bytes(&pair[0]));
            assert_eq!(*expected, bytes(&pair[1]));
            let Value::Map(map) = encoding::decode(key, Limits::default()).unwrap() else {
                unreachable!()
            };
            let expected = encoding::decode(expected, Limits::default()).unwrap();
            let before = table
                .list(map.clone(), 4, &NoOrder, Limits::default())
                .unwrap();
            assert!(matches!(before.value, std::borrow::Cow::Borrowed(_)));
            assert_eq!(before.value.as_ref(), &expected);
            let after = table.list(map, 5, &NoOrder, Limits::default()).unwrap();
            assert_ne!(after.value.as_ref(), &expected);
        }
    }
    let custom = NetworkProfile::custom(
        b"ae_dev".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Lima,
        }],
    )
    .unwrap();
    assert!(custom.pre_iris_map_ordering().is_empty());
}
#[test]
fn loader_and_listing_limits_are_cumulative_and_do_not_require_serializable_values() {
    let pairs = [(&[0u8][..], &[2u8][..]), (&[4u8][..], &[6u8][..])];
    for limits in [
        Limits {
            max_nodes: 3,
            ..Limits::default()
        },
        Limits {
            max_bytes: 3,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            Overrides::from_encoded(pairs, limits),
            Err(Error::ResourceLimit)
        ));
    }
    Overrides::from_encoded(
        pairs,
        Limits {
            max_nodes: 4,
            max_bytes: 4,
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(matches!(
        Overrides::from_encoded([(&[0, 0][..], &[0][..])], Limits::default()),
        Err(Error::TrailingBytes)
    ));
    let malformed = Value::Object(
        ObjectKind::Address,
        ObjectPayload::List(vec![ObjectPayload::Bytes(vec![1, 2, 3])]),
    );
    let map = BTreeMap::from([(Value::Integer(0.into()), malformed)]);
    assert!(matches!(
        Overrides::default().list(
            map.clone(),
            5,
            &NoOrder,
            Limits {
                max_bytes: 2,
                ..Limits::default()
            }
        ),
        Err(MapError::Data(Error::ResourceLimit))
    ));
    assert!(
        Overrides::default()
            .list(
                map,
                5,
                &NoOrder,
                Limits {
                    max_bytes: 3,
                    ..Limits::default()
                }
            )
            .is_ok()
    );
    let deep = (0..20).fold(Value::Tombstone, |v, _| Value::List(vec![v]));
    assert_eq!(
        ae_vm_fate::value::check_limits(
            &deep,
            Limits {
                max_depth: 8,
                ..Limits::default()
            }
        )
        .unwrap_err(),
        Error::ResourceLimit
    );
    ae_vm_fate::value::check_limits(&deep, Limits::default()).unwrap();
}
