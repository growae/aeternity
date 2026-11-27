use ae_chain::{
    admission::{self, BodyError, PowError},
    block::{Block, DecodeConfig},
    fraud,
};
use ae_codec::headers::{Header, HeaderMode};
use ae_protocol::{Activation, NetworkProfile, Protocol};
use serde_json::{Value as J, json};
use std::sync::LazyLock;
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack_fixture(include_str!("fixtures/erlang_block_body.json")));
fn unpack_fixture(source: &str) -> J {
    let d: J = serde_json::from_str(source).unwrap();
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
    u(&d["root"], d["values"].as_array().unwrap())
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn field<'a>(v: &'a J, k: &str) -> &'a J {
    assert_eq!(v[0], "map");
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(k))
        .unwrap()[1]
}
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn profile(row: &J) -> NetworkProfile {
    NetworkProfile::custom(
        b"ae_block_reference".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap()
}
fn fraud_result(e: &fraud::Error) -> J {
    match e {
        fraud::Error::BadPublicKeyLength => {
            json!({"kind":"native","class":"error","reason":atom("badarg")})
        }
        e => json!({"kind":"ok","value":tuple(vec![atom("error"),atom(match e {
            fraud::Error::NotSiblings=>"not_siblings",fraud::Error::SameHeader=>"same_header",fraud::Error::Signature=>"fraud_header_dont_match_leader_key",e=>panic!("{e:?}"),
        })])}),
    }
}
fn result(value: Result<(), PowError>) -> J {
    let reason = match value {
        Ok(()) => return json!({"kind":"ok","value":atom("ok")}),
        Err(PowError::Header(e)) => tuple(vec![
            atom("header"),
            atom(match e {
                ae_consensus::headers::Error::IncorrectProofOfWork => "incorrect_pow",
                ae_consensus::headers::Error::FutureTimestamp => "block_from_the_future",
                ae_consensus::headers::Error::MicroInterval => "bad_micro_block_interval",
                ae_consensus::headers::Error::Protocol(_) => "protocol_version_mismatch",
                e => panic!("unexpected header error {e:?}"),
            }),
        ]),
        Err(PowError::Body(BodyError::Fraud(e))) => {
            let r = fraud_result(&e);
            if r["kind"] == "native" {
                return r;
            }
            tuple(vec![atom("block"), r["value"][1][1].clone()])
        }
        Err(PowError::Body(e)) => tuple(vec![
            atom("block"),
            atom(match e {
                BodyError::TransactionsHash => "malformed_txs_hash",
                BodyError::GasLimit => "gas_limit_exceeded",
                BodyError::MinimumFee => "invalid_minimal_tx_fee",
                BodyError::ProofHash => "pof_hash_mismatch",
                e => panic!("unexpected body error {e:?}"),
            }),
        ]),
    };
    json!({"kind":"ok","value":tuple(vec![atom("error"),reason])})
}
fn check_row(row: &J) -> Block {
    let b = Block::decode(&bytes(&row["input"]), DecodeConfig::default()).unwrap();
    assert_eq!(row["result"]["kind"], "ok");
    let expected = &row["result"]["value"];
    assert_eq!(bin(b.encode().unwrap()), *field(expected, "encoded"));
    assert_eq!(bin(b.hash().unwrap().0), *field(expected, "hash"));
    assert_eq!(
        bin(b.header().encode().unwrap()),
        *field(expected, "header")
    );
    if let Block::Micro(m) = &b {
        assert_eq!(
            json!([
                "list",
                m.transactions
                    .iter()
                    .map(|t| bin(t.encode()))
                    .collect::<Vec<_>>()
            ]),
            *field(expected, "txs")
        );
        assert_eq!(
            json!(["integer", admission::gas(m).unwrap().to_string()]),
            *field(expected, "gas")
        );
    }
    assert_eq!(
        result(admission::validate_pow(
            &b,
            &profile(row),
            None,
            &admission::PowRules {
                edge_bits: Default::default(),
                now_ms: 10000,
                micro_cycle_ms: 3000,
                configured_gas_limit: 6_000_000.into(),
                body_limits: Default::default()
            }
        )),
        row["admission"],
        "{} {}",
        row["protocol"],
        row["name"]
    );
    b
}
#[test]
fn blocks_all_transaction_families_and_normalized_signed_bodies_match_original() {
    let rows = DATA["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 288);
    let mut kinds = std::collections::BTreeSet::new();
    let mut normalized = 0;
    for row in rows {
        let b = check_row(row);
        if row["name"] == "normalization" {
            assert_ne!(b.encode().unwrap(), bytes(&row["input"]));
            normalized += 1;
        }
        if let Block::Micro(m) = b {
            for s in m.transactions {
                kinds.insert(format!("{:?}", s.transaction().view().kind()));
            }
        }
    }
    assert_eq!(normalized, 8);
    assert_eq!(kinds.len(), 27);
}
#[test]
fn fraud_proofs_hashes_signatures_and_optional_body_behavior_match_original() {
    let rows = DATA["proofs"].as_array().unwrap();
    assert_eq!(rows.len(), 72);
    for row in rows {
        let b = check_row(&row["block"]);
        check_row(&row["mismatch"]);
        let Block::Micro(b) = b else { panic!() };
        match b.proof {
            Some(p) => {
                assert_eq!(
                    hex::encode(p.hash().unwrap().0),
                    row["hash"].as_str().unwrap()
                );
                assert_eq!(json!([hex::encode(p.encode().unwrap())]), row["proof"]);
                let r = p.validate().map_or_else(
                    |e| fraud_result(&e),
                    |_| json!({"kind":"ok","value":atom("ok")}),
                );
                assert_eq!(r, row["validation"], "{} {}", row["protocol"], row["name"]);
                assert_eq!(
                    fraud::Proof::decode(
                        &p.encode().unwrap(),
                        HeaderMode::ProofOfWork,
                        Default::default()
                    )
                    .unwrap(),
                    *p
                );
            }
            None => {
                assert_eq!(row["hash"], "");
                assert_eq!(row["proof"], json!([]));
            }
        }
    }
}
#[test]
fn admission_order_historical_gas_and_timestamp_boundaries_match_original() {
    let rows = DATA["admission"].as_array().unwrap();
    assert_eq!(rows.len(), 632);
    for row in rows {
        let b = Block::decode(&bytes(&row["input"]), DecodeConfig::default()).unwrap();
        let previous = (!row["previous"].is_null())
            .then(|| Header::decode(&bytes(&row["previous"]), HeaderMode::ProofOfWork).unwrap());
        let gas = row["limit"].as_str().unwrap().parse().unwrap();
        let r = admission::validate_pow(
            &b,
            &profile(row),
            previous.as_ref(),
            &admission::PowRules {
                edge_bits: Default::default(),
                now_ms: 10000,
                micro_cycle_ms: row["cycle"].as_u64().unwrap_or(3000),
                configured_gas_limit: gas,
                body_limits: Default::default(),
            },
        );
        assert_eq!(
            result(r),
            row["result"],
            "{} {}",
            row["protocol"],
            row["name"]
        );
    }
}
#[test]
fn malformed_bodies_and_local_limits_never_become_accepted_blocks() {
    let rows = DATA["mutations"].as_array().unwrap();
    assert_eq!(rows.len(), 64);
    for row in rows {
        let error = Block::decode(&bytes(&row["input"]), DecodeConfig::default()).unwrap_err();
        assert!(!error.is_operational(), "{error:?}");
        assert!(row["result"]["kind"] == "native" || row["result"]["value"][1][0] == atom("error"));
    }
    let row = DATA["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "all")
        .unwrap();
    let input = bytes(&row["input"]);
    for mode in 0..5 {
        let mut config = DecodeConfig::default();
        match mode {
            0 => config.max_block_bytes = input.len() - 1,
            1 => config.max_transactions = 0,
            2 => config.body.max_items = 1,
            3 => config.transaction.max_total_bytes = 0,
            _ => config.state.limits.max_work_bytes = 0,
        }
        assert!(Block::decode(&input, config).unwrap_err().is_operational());
    }
    let b = Block::decode(&input, DecodeConfig::default()).unwrap();
    let Block::Micro(m) = b else { panic!() };
    for limits in [
        admission::Limits {
            max_transactions: 0,
            ..Default::default()
        },
        admission::Limits {
            max_signed_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(
            admission::validate_body(&m, &6_000_000.into(), limits)
                .unwrap_err()
                .is_operational()
        );
    }
    for network in [NetworkProfile::mainnet(), NetworkProfile::uat()] {
        assert_eq!(network.block_gas_limit(&0.into()), 6_000_000.into());
        assert_eq!(network.block_gas_limit(&u64::MAX.into()), 6_000_000.into());
    }
}

#[path = "blocks/execution.rs"]
mod execution;
