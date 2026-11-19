use ae_execution::{
    Context, Origin,
    transactions::{self, SignatureError},
};
use ae_mempool::hc_votes::{Admission, Error, Event, Limits, PushOutcome, VotePool};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_tx::{CanonicalTransaction, HcVote, SignedTransaction, Transaction, TransactionLeaf};
use ae_types::Unsigned;
use serde_json::{Value as J, json};
use std::sync::LazyLock;
static FIXTURE: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_hc_vote.json")).unwrap());
fn rows(key: &str) -> &'static [J] {
    FIXTURE[key].as_array().unwrap()
}
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn unsigned(v: &J) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn protocol(row: &J) -> Protocol {
    Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap()
}
fn consensus(row: &J) -> ConsensusType {
    match row["mode"].as_str().unwrap() {
        "pow" => ConsensusType::Pow,
        "pos" => ConsensusType::Pos,
        _ => unreachable!(),
    }
}
fn normalize(v: &J) -> J {
    if let Some(items) = v.as_array() {
        if items.len() == 2 && items[0] == "map" {
            let mut pairs: Vec<_> = items[1].as_array().unwrap().iter().map(normalize).collect();
            pairs.sort_by_key(|x| x[0].to_string());
            return json!(["map", pairs]);
        }
        J::Array(items.iter().map(normalize).collect())
    } else {
        v.clone()
    }
}
fn bin(b: &[u8]) -> J {
    json!(["binary", hex::encode(b)])
}
fn decimal(n: &Unsigned) -> String {
    num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string()
}
fn integer(n: &Unsigned) -> J {
    json!(["integer", decimal(n)])
}
fn prices(tx: &Transaction, p: Protocol) -> J {
    let v = tx.view().pricing(&50.into(), p);
    json!([
        v.gas_limit.to_string(),
        v.inner_gas_limit.to_string(),
        v.minimum_fee.to_string(),
        v.minimum_gas_price.to_string(),
        v.minimum_block_gas.to_string()
    ])
}
fn pool_error(e: &Error) -> &'static str {
    match e {
        Error::OnlyHcVoteAllowed => "only_hc_vote_tx_allowed",
        Error::InvalidAtProtocol => "invalid_at_protocol",
        Error::Signature(SignatureError::CheckFailed) => "signature_check_failed",
        _ => panic!("unexpected pool failure: {e:?}"),
    }
}
#[test]
fn hc_wire_metadata_and_pricing_match_original() {
    for (i, row) in rows("wire").iter().enumerate() {
        let b = bytes(&row["wire"]);
        let decoded = HcVote::decode(&b);
        let expected = &row["result"]["ok"];
        if expected.is_null() {
            assert!(decoded.is_err(), "wire {i}");
            assert!(Transaction::decode(&b).is_err(), "wire {i}");
            continue;
        }
        let vote = decoded.unwrap();
        assert_eq!(hex::encode(vote.encode()), expected["encoded"], "wire {i}");
        assert_eq!(
            vote.wire_size(),
            expected["wire_size"].as_u64().unwrap() as usize
        );
        assert_eq!(hex::encode(vote.voter().0), expected["voter"]);
        assert_eq!(decimal(vote.epoch()), expected["epoch"]);
        assert_eq!(decimal(vote.vote_type()), expected["type"]);
        assert_eq!(
            json!(
                vote.data()
                    .iter()
                    .map(|(k, v)| [hex::encode(k), hex::encode(v)])
                    .collect::<Vec<_>>()
            ),
            expected["data"]
        );
        assert_eq!(vote.fee(), &0.into());
        assert_eq!(vote.gas(), &0.into());
        assert_eq!(vote.nonce(), &0.into());
        assert_eq!(vote.ttl(), &0.into());
        let client = vote.for_client();
        let view = json!([
            "map",
            [
                [bin(b"voter_id"), bin(client.voter_id.as_bytes())],
                [bin(b"epoch"), integer(client.epoch)],
                [bin(b"type"), integer(client.vote_type)],
                [
                    bin(b"data"),
                    json!([
                        "map",
                        client
                            .data
                            .iter()
                            .map(|(k, v)| json!([bin(k), bin(v)]))
                            .collect::<Vec<_>>()
                    ])
                ]
            ]
        ]);
        assert_eq!(normalize(&view), normalize(&expected["client"]));
        let tx = Transaction::decode(&b).unwrap();
        assert_eq!(
            hex::encode(tx.view().origin().unwrap().0),
            expected["origin"]
        );
        assert_eq!(tx.view().nonce(), 0.into());
        assert_eq!(tx.view().fee(), &0.into());
        assert!(tx.view().ttl().is_none());
        assert!(tx.view().gas_price().is_none());
        for p in 1..=8 {
            let protocol = Protocol::try_from(p).unwrap();
            assert_eq!(prices(&tx, protocol), expected["prices"][p as usize - 1]);
            for (j, consensus) in [ConsensusType::Pow, ConsensusType::Pos]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    tx.view().valid_at_consensus(protocol, consensus).unwrap(),
                    expected["valid"][(p as usize - 1) * 2 + j]
                );
            }
        }
    }
}
#[test]
fn hc_signatures_wrappers_and_admission_match_original() {
    let network = NetworkProfile::mainnet();
    for (i, row) in rows("signatures").iter().enumerate() {
        let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        let tx = signed.transaction();
        let p = protocol(row);
        let c = consensus(row);
        assert_eq!(hex::encode(signed.encode()), row["encoded"], "row {i}");
        assert_eq!(hex::encode(signed.hash().0), row["hash"], "row {i}");
        assert_eq!(
            tx.view().valid_at_consensus(p, c).unwrap(),
            row["valid"],
            "row {i}"
        );
        assert_eq!(prices(tx, p), row["prices"], "row {i}");
        let verified = transactions::verify(
            tx.view(),
            signed.signatures(),
            &network,
            p,
            false,
            Default::default(),
        );
        let result = match verified {
            Ok(()) => "ok",
            Err(SignatureError::CheckFailed) => "signature_check_failed",
            Err(e) => panic!("row {i}: {e:?}"),
        };
        assert_eq!(result, row["verify"], "row {i}");
        let mut pool = VotePool::default();
        let admission = Admission {
            network: &network,
            protocol: p,
            consensus: c,
            limits: Default::default(),
        };
        let outcome = pool.push(&signed, Event::Received, &admission, |_| false);
        let result = match &outcome {
            Ok(PushOutcome::Accepted {
                inserted: true,
                event: Event::Received,
                ..
            }) => "ok",
            Err(e) => pool_error(e),
            _ => panic!("row {i}: {outcome:?}"),
        };
        assert_eq!(result, row["admission"], "row {i}");
        assert_eq!(pool.len(), usize::from(result == "ok"));
    }
}
#[test]
fn hc_on_chain_callbacks_reject_without_changing_state() {
    let network = NetworkProfile::mainnet();
    for row in rows("processing") {
        let tx = Transaction::decode(&bytes(&row["wire"])).unwrap();
        let mut trees = ae_state::Trees {
            accounts: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
            contracts: Default::default(),
            calls: Default::default(),
            channels: Default::default(),
        };
        let before = trees.roots();
        let mut context = Context::transaction(50, protocol(row));
        context.dry_run = row["dry_run"].as_bool().unwrap();
        context.origin = match row["context"].as_str().unwrap() {
            "aetx_transaction" => Origin::Transaction,
            "aetx_ga" => Origin::Generalized,
            "aetx_contract" => Origin::Contract,
            _ => unreachable!(),
        };
        let mut config = transactions::Config::primitives(&network);
        config.consensus = consensus(row);
        let e = transactions::process(&tx, &mut trees, &context, &config).unwrap_err();
        let reason = match e {
            transactions::Error::HcVoteCannotBeProcessed => "hc_vote_tx_cant_be_processed",
            transactions::Error::Primitive(ae_execution::Error::OracleTransaction(
                ae_execution::oracles::Failure::TooLowAbsoluteTtl,
            )) => "too_low_abs_ttl",
            e => panic!("unexpected {e:?}"),
        };
        assert_eq!(
            json!({"kind":"ok","value":["tuple",[["atom","error"],["atom",reason]]]}),
            row["result"]
        );
        assert_eq!(trees.roots(), before);
    }
}
#[test]
fn hc_pool_order_events_and_epoch_transitions_match_original() {
    let network = NetworkProfile::mainnet();
    let mut pool = VotePool::default();
    for (i, row) in rows("pool").iter().enumerate() {
        if row["operation"] == "epoch" {
            pool.update_epoch(unsigned(&row["epoch"]));
        } else {
            let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
            let admission = Admission {
                network: &network,
                protocol: protocol(row),
                consensus: consensus(row),
                limits: Default::default(),
            };
            let before = pool.clone();
            let cached = row["cached"].as_bool().unwrap();
            let result = pool.push(
                &signed,
                if row["event"] == "tx_received" {
                    Event::Received
                } else {
                    Event::Created
                },
                &admission,
                |_| cached,
            );
            let (reason, events) = match result {
                Ok(PushOutcome::Cached) => {
                    assert_eq!(pool, before);
                    ("ok", json!([]))
                }
                Ok(PushOutcome::Accepted { hash, event, .. }) => (
                    "ok",
                    json!([{"event":match event { Event::Created=>"tx_created",Event::Received=>"tx_received" },"hash":hex::encode(hash.0)}]),
                ),
                Err(e) => {
                    assert_eq!(pool, before);
                    (pool_error(&e), json!([]))
                }
            };
            assert_eq!(reason, row["result"], "step {i}");
            assert_eq!(events, row["events"], "step {i}");
        }
        let expected = &row["after_state"];
        assert_eq!(
            pool.len(),
            expected["size"].as_u64().unwrap() as usize,
            "step {i}"
        );
        assert_eq!(decimal(pool.epoch()), expected["epoch"], "step {i}");
        for peek in expected["peeks"].as_array().unwrap() {
            let epoch = unsigned(&peek["epoch"]);
            let kind = (!peek["type"].is_null()).then(|| unsigned(&peek["type"]));
            let actual: Vec<_> = pool
                .peek(&epoch, kind.as_ref())
                .map(|vote| hex::encode(vote.encode()))
                .collect();
            assert_eq!(json!(actual), peek["votes"], "step {i}");
        }
    }
}
#[test]
fn hc_local_limits_are_atomic_and_retryable() {
    let network = NetworkProfile::mainnet();
    let row = rows("signatures")
        .iter()
        .find(|r| r["mode"] == "pos" && r["label"] == "normal")
        .unwrap();
    let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let admission = Admission {
        network: &network,
        protocol: protocol(row),
        consensus: ConsensusType::Pos,
        limits: Limits::default(),
    };
    for limits in [
        Limits {
            maximum_encoding_work_bytes: 0,
            ..Limits::default()
        },
        Limits {
            maximum_entries: 0,
            ..Limits::default()
        },
        Limits {
            maximum_retained_wire_bytes: 0,
            ..Limits::default()
        },
        Limits {
            maximum_encoded_bytes: 0,
            ..Limits::default()
        },
        Limits {
            maximum_encoded_bytes: signed.encode().len() - 1,
            ..Limits::default()
        },
        Limits {
            signatures: transactions::SignatureLimits {
                max_encoded_bytes: 0,
                ..Default::default()
            },
            ..Limits::default()
        },
    ] {
        let mut pool = VotePool::default();
        let before = pool.clone();
        let limited = Admission {
            limits,
            ..admission
        };
        assert!(matches!(
            pool.push(&signed, Event::Created, &limited, |_| false),
            Err(Error::ResourceLimit | Error::Signature(SignatureError::ResourceLimit))
        ));
        assert_eq!(pool, before);
        assert!(matches!(
            pool.push(&signed, Event::Created, &admission, |_| false),
            Ok(PushOutcome::Accepted { inserted: true, .. })
        ));
    }
    let mut pool = VotePool::default();
    pool.push(&signed, Event::Created, &admission, |_| false)
        .unwrap();
    let before = pool.clone();
    let full = Admission {
        limits: Limits {
            maximum_entries: 0,
            maximum_retained_wire_bytes: 0,
            ..Limits::default()
        },
        ..admission
    };
    assert!(matches!(
        pool.push(&signed, Event::Received, &full, |_| false),
        Ok(PushOutcome::Accepted {
            inserted: false,
            event: Event::Received,
            ..
        })
    ));
    assert_eq!(pool, before);
    let TransactionLeaf::HcVote(vote) = signed.transaction().leaf() else {
        unreachable!()
    };
    let different = HcVote::new(
        vote.voter(),
        vote.epoch().clone(),
        vote.vote_type().clone(),
        Default::default(),
    );
    let mut seed = [0; 32];
    seed[31] = 1;
    let key = ae_crypto::Ed25519SecretKey::from_seed(&seed);
    let tx = Transaction::new(TransactionLeaf::HcVote(different));
    let mut payload = network.signing_prefix(false);
    payload.extend_from_slice(&tx.encode());
    let next = SignedTransaction::new(tx, vec![key.sign(&payload)]);
    assert_eq!(
        pool.push(&next, Event::Created, &full, |_| false),
        Err(Error::ResourceLimit)
    );
    assert_eq!(pool, before);
    assert!(matches!(
        pool.push(&next, Event::Created, &admission, |_| false),
        Ok(PushOutcome::Accepted { inserted: true, .. })
    ));
    pool.update_epoch(1.into());
    pool.update_epoch(2.into());
    assert!(pool.is_empty());
    assert_eq!(pool.retained_wire_bytes(), 0);
    let wrapped_row = rows("signatures")
        .iter()
        .find(|r| r["label"] == "meta2")
        .unwrap();
    let wrapped = SignedTransaction::<Transaction>::decode(&bytes(&wrapped_row["signed"])).unwrap();
    let no_layers = Admission {
        limits: Limits {
            signatures: transactions::SignatureLimits {
                max_layers: 0,
                ..Default::default()
            },
            ..Limits::default()
        },
        ..admission
    };
    let before = pool.clone();
    assert_eq!(
        pool.push(&wrapped, Event::Created, &no_layers, |_| true),
        Err(Error::ResourceLimit)
    );
    assert_eq!(pool, before);
    assert_eq!(
        pool.push(&wrapped, Event::Created, &admission, |_| true),
        Ok(PushOutcome::Cached)
    );
    assert_eq!(pool, before);
    let raw = bytes(&rows("wire")[1]["wire"]);
    assert!(
        HcVote::decode_with_limits(
            &raw,
            ae_codec::rlp::DecodeLimits {
                max_items: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(HcVote::decode(&raw).is_ok());
}
