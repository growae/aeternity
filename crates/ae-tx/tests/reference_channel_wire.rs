use ae_protocol::Protocol;
use ae_tx::{CanonicalTransaction, Transaction, TransactionRef, channels::Update};
use ae_types::Unsigned;
use serde_json::{Value as J, json};
use std::sync::LazyLock;
fn unpack(text: &str) -> J {
    fn u(v: &J, pool: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                u(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            J::Object(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), u(v, pool))).collect()),
            J::Array(a) => J::Array(a.iter().map(|v| u(v, pool)).collect()),
            _ => v.clone(),
        }
    }
    let v: J = serde_json::from_str(text).unwrap();
    if v["format"] == "interned-json-v1" {
        u(&v["root"], v["values"].as_array().unwrap())
    } else {
        v
    }
}
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("fixtures/erlang_channel_wire.json")));
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn number(n: &Unsigned) -> J {
    json!({"integer":num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string()})
}
fn check(t: TransactionRef<'_>, r: &J, label: &J) {
    assert_eq!(hex::encode(t.encode()), r["encoded"], "{label} encoding");
    assert_eq!(
        t.wire_size(),
        r["size"].as_u64().unwrap() as usize,
        "{label} size"
    );
    assert_eq!(number(&t.nonce()), r["nonce"], "{label} nonce");
    assert_eq!(number(t.fee()), r["fee"], "{label} fee");
    assert_eq!(
        t.ttl().map(number).unwrap_or(json!({"atom":"max_ttl"})),
        r["ttl"],
        "{label} ttl"
    );
    assert_eq!(
        t.gas_price()
            .map(number)
            .unwrap_or(json!({"atom":"undefined"})),
        r["gas_price"],
        "{label} gas price"
    );
    if let Some(b) = r["origin"]["ok"]["binary"].as_str() {
        assert_eq!(hex::encode(t.origin().unwrap().0), b, "{label} origin");
    } else {
        assert!(t.origin().is_err(), "{label} origin");
    }
    for p in r["prices"].as_array().unwrap() {
        let protocol = Protocol::try_from(p["protocol"].as_u64().unwrap() as u32).unwrap();
        assert_eq!(
            json!({"ok":{"atom":t.valid_at_protocol(protocol).unwrap().to_string()}}),
            p["valid"],
            "{label} protocol {protocol:?}"
        );
        assert_eq!(
            json!({"ok":{"integer":t.gas_limit(&50.into(), protocol).to_string()}}),
            p["gas"],
            "{label} gas preflight {protocol:?}"
        );
        let costs = t.pricing(&50.into(), protocol);
        for (name, n) in [
            ("gas", costs.gas_limit),
            ("inner_gas", costs.inner_gas_limit),
            ("fee_gas", costs.fee_gas),
            ("minimum_fee", costs.minimum_fee),
            ("minimum_block_gas", costs.minimum_block_gas),
            ("minimum_gas_price", costs.minimum_gas_price),
        ] {
            assert_eq!(
                json!({"ok":{"integer":n.to_string()}}),
                p[name],
                "{label} {protocol:?} {name}"
            );
        }
    }
}
#[test]
fn channel_decoders_preserve_original_admission_canonical_bytes_and_metadata() {
    for r in DATA["transactions"].as_array().unwrap() {
        let got = Transaction::decode(&bytes(&r["input"]));
        if r["result"]["ok"].is_object() {
            let t = got.unwrap_or_else(|e| panic!("{}: {e:?}", r["label"]));
            check(t.view(), &r["result"]["ok"], &r["label"]);
        } else {
            assert!(got.is_err(), "{}", r["label"]);
        }
    }
}
#[test]
fn channel_wrappers_preserve_normalization_protocol_rules_and_historical_pricing() {
    for r in DATA["wrappers"].as_array().unwrap() {
        let result = Transaction::decode(&bytes(&r["input"]));
        if r["result"]["ok"].is_object() {
            let t = result.unwrap();
            check(t.view(), &r["result"]["ok"], &r["label"]);
        } else {
            assert!(result.is_err(), "{}", r["label"]);
        }
    }
}
#[test]
fn six_offchain_update_types_match_original_decoding_and_amount_extraction() {
    for r in DATA["updates"].as_array().unwrap() {
        let got = Update::decode(&bytes(&r["input"]));
        if r["result"]["ok"].is_object() {
            let u = got.unwrap();
            let expected = &r["result"]["ok"];
            assert_eq!(
                hex::encode(u.encode()),
                expected["encoded"],
                "{}",
                r["name"]
            );
            assert_eq!(
                u.amounts()
                    .map(|(a, p, g)| json!({"tuple":[number(a),number(p),number(g)]}))
                    .unwrap_or(json!({"atom":"not_call"})),
                expected["amounts"]
            );
            assert_eq!(
                matches!(u, Update::CallContract { .. }),
                expected["is_call"].as_bool().unwrap()
            );
            assert_eq!(
                matches!(u, Update::CreateContract { .. }),
                expected["is_create"].as_bool().unwrap()
            );
        } else {
            assert!(got.is_err(), "{}", r["name"]);
        }
    }
}

#[test]
fn embedded_channel_state_and_payload_limits_propagate_and_allow_retry() {
    use ae_state::transport::{Config, Limits};
    use ae_tx::{TransactionKind, TransactionLeaf, TransactionLimits};
    let mut checked = 0;
    for r in DATA["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .chain(DATA["wrappers"].as_array().unwrap())
    {
        if !r["result"]["ok"].is_object() {
            continue;
        }
        let b = bytes(&r["input"]);
        let tx = Transaction::decode(&b).unwrap();
        if tx.leaf().kind() == TransactionKind::ChannelForceProgress {
            for limits in [
                Limits {
                    max_work_bytes: 0,
                    ..Default::default()
                },
                Limits {
                    max_entries: 0,
                    ..Default::default()
                },
                Limits {
                    max_input_bytes: 1,
                    ..Default::default()
                },
            ] {
                let e = Transaction::decode_with_state_config(
                    &b,
                    TransactionLimits::default(),
                    Config {
                        limits,
                        ..Default::default()
                    },
                )
                .unwrap_err();
                assert!(e.is_operational(), "{e:?}");
                assert_eq!(
                    Transaction::decode_with_state_config(
                        &b,
                        TransactionLimits::default(),
                        Config::default()
                    )
                    .unwrap(),
                    tx
                );
                checked += 1;
            }
        }
        if let TransactionLeaf::Channel(c) = tx.leaf()
            && c.operation().payload().is_some_and(|p| !p.is_empty())
            && c.valid_at_protocol(Protocol::Iris).unwrap()
        {
            let e = c
                .valid_at_protocol_with_config(
                    Protocol::Iris,
                    TransactionLimits {
                        rlp: ae_codec::rlp::DecodeLimits {
                            max_input_bytes: 1,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Config::default(),
                )
                .unwrap_err();
            assert!(e.is_operational());
            assert!(c.valid_at_protocol(Protocol::Iris).unwrap());
            checked += 1;
        }
    }
    assert!(checked > 50);
    println!("Channel state/payload limit retries: {checked}");
}

#[test]
fn wrapped_force_progress_preserves_explicit_name_atom_profile() {
    use ae_codec::object::Value;
    use ae_state::{Trees, names::Name, transport::Config};
    use ae_tx::{GaMeta, TransactionKind, TransactionLimits, WireTransaction, Wrapper};
    use ae_types::AccountPublicKey;
    let r=DATA["transactions"].as_array().unwrap().iter().find(|r|r["label"]==json!({"tuple":[{"atom":"base"},{"atom":"aesc_force_progress_tx"},{"integer":"1"}]})).unwrap();
    let wire = WireTransaction::decode(&bytes(&r["input"])).unwrap();
    let mut trees: Trees = Trees {
        accounts: Default::default(),
        contracts: ae_state::contracts::Contracts::new(ae_trie::MemoryStore::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let mut name = Name::new([42; 32], AccountPublicKey([1; 32]), 100.into());
    name.status = b"channel_fixture_status".to_vec();
    trees.names.enter_name(&name).unwrap();
    let state = trees.serialize_to_binary(Default::default()).unwrap();
    let fields = wire
        .fields()
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                if k == "offchain_trees" {
                    Value::Binary(state.clone())
                } else {
                    v.clone()
                },
            )
        })
        .collect();
    let b = WireTransaction::new(TransactionKind::ChannelForceProgress, 1, fields)
        .unwrap()
        .encode();
    let e = Transaction::decode(&b).unwrap_err();
    assert!(
        matches!(
            e,
            ae_tx::Error::StateTransport(ae_state::transport::Error::MissingAtomProfile(_))
        ) && e.is_operational()
    );
    let atoms = std::collections::BTreeSet::from([name.status]);
    let config = Config {
        existing_name_status_atoms: Some(&atoms),
        ..Default::default()
    };
    let tx =
        Transaction::decode_with_state_config(&b, TransactionLimits::default(), config).unwrap();
    let wrapped = tx.wrap(
        Wrapper::GaMeta(GaMeta {
            account: AccountPublicKey([1; 32]),
            auth_data: vec![],
            abi_version: 3.into(),
            fee: 1.into(),
            gas: 1.into(),
            gas_price: 1.into(),
            ttl: None,
        }),
        vec![],
    );
    assert!(
        Transaction::decode(&wrapped.encode())
            .unwrap_err()
            .is_operational()
    );
    assert_eq!(
        Transaction::decode_with_state_config(
            &wrapped.encode(),
            TransactionLimits::default(),
            config
        )
        .unwrap(),
        wrapped
    );
}
