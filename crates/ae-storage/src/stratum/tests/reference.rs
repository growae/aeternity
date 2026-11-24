// The unchanged original Mnesia trace is shared with the pure-core tests.
// Normalize only allocated keys; all record payloads and float bits are exact.
use super::*;
use serde_json::Value as J;
fn tuple(v: &J) -> &[J] {
    v["tuple"].as_array().unwrap()
}
fn bin(v: &J) -> Vec<u8> {
    hex::decode(v["binary"].as_str().unwrap()).unwrap()
}
fn int(v: &J) -> BigInt {
    v["integer"].as_str().unwrap().parse().unwrap()
}
fn uint(v: &J) -> u64 {
    v["integer"].as_str().unwrap().parse().unwrap()
}
fn sort(k: u64) -> u64 {
    u64::MAX - (1_000_000 - k)
}
fn h(v: &J) -> Hash256 {
    Hash256(bin(v).try_into().unwrap())
}
fn number(v: &J) -> Number {
    if v.get("integer").is_some() {
        Number::Integer(int(v))
    } else {
        Number::Float(u64::from_str_radix(v["float"].as_str().unwrap(), 16).unwrap())
    }
}
fn map<T>(v: &J, f: impl Fn(&J) -> T) -> BTreeMap<Vec<u8>, T> {
    v["map"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (bin(&p[0]), f(&p[1])))
        .collect()
}
fn optional<T>(v: &J, f: impl FnOnce(&J) -> T) -> Option<T> {
    if v.get("atom").is_some() {
        assert_eq!(v["atom"], "undefined");
        None
    } else {
        Some(f(v))
    }
}
fn date(v: &J) -> core::Date {
    let t = tuple(v);
    core::Date(
        tuple(&t[0])
            .iter()
            .chain(tuple(&t[1]).iter())
            .map(|v| uint(v) as u32)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    )
}
fn record(v: &J) -> Record {
    let t = tuple(v);
    match t[0]["atom"].as_str().unwrap() {
        "aestratum_hash" => Record::Hash {
            hash: h(&t[1]),
            key: sort(uint(&t[2])),
        },
        "aestratum_round" => {
            assert_eq!(t[2]["atom"], "undefined");
            Record::Round(sort(uint(&t[1])))
        }
        "aestratum_share" => Record::Share(core::Share {
            key: sort(uint(&t[1])),
            hash: h(&t[2]),
            target: int(&t[3]),
            miner: bin(&t[4]),
        }),
        "aestratum_reward" => Record::Reward(core::Reward {
            height: uint(&t[1]),
            hash: h(&t[2]),
            pool: core::Shares::Relative(map(&t[3], number)),
            miners: core::Shares::Relative(map(&t[4], number)),
            amount: int(&t[5]),
            round_key: sort(uint(&t[6])),
        }),
        "aestratum_payment" => Record::Payment(core::Payment {
            id: (uint(&tuple(&t[1])[0]), uint(&tuple(&t[1])[1]) as u32),
            total: int(&t[2]),
            relative: map(&t[3], number),
            gross: None,
            absolute: optional(&t[4], |v| map(v, int)),
            tx_hash: optional(&t[5], h),
            nonce: optional(&t[6], int),
            fee: optional(&t[7], int),
            gas: optional(&t[8], int),
            run_gas: optional(&t[9], int),
            date: optional(&t[10], date),
        }),
        "aestratum_candidate" => Record::Candidate(core::Candidate {
            hash: h(&t[1]),
            header: bin(&t[2]),
            record: bin(&t[3]),
            date: date(&t[4]),
        }),
        x => panic!("record {x}"),
    }
}
fn expected(step: &J) -> Vec<Record> {
    let mut out = vec![];
    for name in [
        "aestratum_hash",
        "aestratum_share",
        "aestratum_round",
        "aestratum_reward",
        "aestratum_payment",
        "aestratum_candidate",
    ] {
        out.extend(step["state"][name].as_array().unwrap().iter().map(record));
    }
    out.sort_by_key(Record::key);
    out
}
fn payments(v: &J) -> Vec<core::Payment> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let Record::Payment(p) = record(v) else {
                panic!()
            };
            p
        })
        .collect()
}
#[test]
fn native_twenty_seven_step_trace_matches_durable_tables_and_queries_after_every_reopen() {
    let data: J = serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_accounting.json"
    ))
    .unwrap();
    let trace = data["database"].as_array().unwrap();
    assert_eq!(trace.len(), 27);
    let temp = Temp::new();
    let mut next = u64::MAX;
    let mut revision = 0;
    for step in trace {
        let db = database(&temp);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let name = step["name"].as_str().unwrap();
        let want = expected(step);
        let original = rows(&db);
        let old_info = db.stratum(Default::default()).unwrap().info();
        let result = &step["result"]["ok"];
        let command = match name {
            "round_1" | "round_2" | "round_3" => {
                next -= 1;
                Some(Command::Round)
            }
            "share_a" | "share_b" | "share_c" | "share_zero" => {
                let Record::Share(s) = record(&tuple(result)[1]) else {
                    panic!()
                };
                next -= 1;
                Some(Command::Share {
                    hash: s.hash,
                    target: s.target,
                    miner: s.miner,
                })
            }
            "solution" => Some(Command::Solution {
                candidate: hash(1),
                block: hash(11),
            }),
            "missing_solution" => {
                assert_eq!(
                    w.commit(
                        w.version().unwrap(),
                        vec![Command::Solution {
                            candidate: hash(0),
                            block: hash(11)
                        }]
                    ),
                    Err(Failure::Accounting(core::Error::MissingHash))
                );
                assert_eq!(rows(&db), original);
                None
            }
            "store_reward" => {
                let Record::Reward(r) = record(&tuple(result)[1]) else {
                    panic!()
                };
                Some(Command::Reward(r))
            }
            "store_payments" => {
                let r = db
                    .stratum(Default::default())
                    .unwrap()
                    .query(|t| t.reward(71))
                    .unwrap()
                    .unwrap();
                Some(Command::StorePayments {
                    reward: r,
                    percent: Number::Float(1.5f64.to_bits()),
                })
            }
            "update_payment" => {
                let stored = db.stratum(Default::default()).unwrap();
                let p = stored
                    .query(|t| t.select_payments(false, Some(1)))
                    .unwrap()
                    .remove(0);
                Some(Command::UpdatePayment {
                    payment: Box::new(p),
                    absolute: [(b"pool".to_vec(), 15.into())].into(),
                    hash: hash(4),
                    date: core::Date([2026, 11, 24, 1, 2, 3]),
                })
            }
            "delete_payment" => Some(Command::DeletePayment((71, 0))),
            "candidate_a" | "candidate_b" => {
                let Record::Candidate(c) = record(&tuple(result)[1]) else {
                    panic!()
                };
                Some(Command::Candidate(c))
            }
            "candidate_cleanup" => Some(Command::DeleteCandidatesBefore(core::Date([
                2026, 1, 2, 0, 0, 0,
            ]))),
            "delete_records" => Some(Command::DeleteRecords(71)),
            "clear_rounds" => Some(Command::Clear(Table::Rounds)),
            _ => None,
        };
        if let Some(command) = command {
            let done = commit(&mut w, vec![command]);
            revision += 1;
            assert_eq!(done.version.revision(), revision);
            if matches!(
                name,
                "round_1"
                    | "round_2"
                    | "round_3"
                    | "share_a"
                    | "share_b"
                    | "share_c"
                    | "share_zero"
                    | "store_reward"
                    | "update_payment"
                    | "candidate_a"
                    | "candidate_b"
            ) {
                assert_eq!(
                    done.replies,
                    [core::Reply::Record(Box::new(record(&tuple(result)[1])))],
                    "reply {name}"
                );
            } else if name == "store_payments" {
                assert_eq!(
                    done.replies,
                    [core::Reply::Records(
                        result
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|p| record(&tuple(p)[1]))
                            .collect()
                    )]
                );
            } else {
                assert_eq!(result["atom"], "ok");
                assert_eq!(done.replies, [core::Reply::Unit], "reply {name}");
            }
        } else {
            assert_eq!(db.stratum(Default::default()).unwrap().info(), old_info);
        }
        let stored = db.stratum(Default::default()).unwrap();
        stored.verify().unwrap();
        assert_eq!(stored.info().revision, revision);
        assert_eq!(stored.info().next_key, Some(next));
        assert_eq!(rows(&db), want, "native state {name}");
        match name {
            "range" => assert_eq!(
                stored.query(|t| t.shares_range(sort(999998), 2)).unwrap(),
                (sort(uint(&tuple(result)[1])), sort(uint(&tuple(result)[2])))
            ),
            "rewards" => assert_eq!(
                stored
                    .query(|t| t.miner_rewards(sort(999998), &100.into(), 2))
                    .unwrap(),
                (
                    map(&tuple(result)[1], number),
                    sort(uint(&tuple(result)[2]))
                )
            ),
            "reward_zero_target" => assert_eq!(
                stored.query(|t| t.miner_rewards(sort(999998), &0.into(), 2)),
                Err(Failure::Accounting(core::Error::Rewards(
                    ae_stratum::rewards::Error::Arithmetic
                )))
            ),
            "unpaid" | "paid" => {
                let paid = name == "paid";
                assert_eq!(
                    stored.query(|t| t.select_payments(paid, None)).unwrap(),
                    payments(&tuple(result)[0])
                );
                assert_eq!(
                    stored.query(|t| t.select_payments(paid, Some(1))).unwrap(),
                    payments(&tuple(result)[1])
                );
                let Record::Payment(p) = record(&tuple(&tuple(result)[2])[1]) else {
                    panic!()
                };
                assert_eq!(
                    stored.query(|t| t.select_payments(false, Some(1))).unwrap(),
                    vec![p]
                );
                if paid {
                    assert_eq!(
                        stored.query(|t| t.payments(71)).unwrap(),
                        payments(&tuple(result)[3])
                    );
                }
            }
            "keys" => {
                let k = stored.query(|t| t.keys(71)).unwrap().unwrap();
                assert_eq!(k.hashes, vec![hash(11), hash(1)]);
                assert_eq!(k.shares, vec![sort(999998), sort(999999)]);
                assert_eq!(k.rounds, vec![sort(999997), sort(1000000)]);
                assert_eq!(k.rewards, vec![71]);
                assert_eq!(k.payments, vec![(71, 0), (71, 1)]);
            }
            "candidate_lookup" => {
                assert_eq!(stored.query(|t| t.candidate(hash(1))).unwrap(), None);
                let Record::Candidate(c) = record(&tuple(&tuple(result)[1])[1]) else {
                    panic!()
                };
                assert_eq!(stored.query(|t| t.candidate(hash(2))).unwrap(), Some(c));
            }
            "missing_reward" => {
                assert_eq!(stored.query(|t| t.reward(71)).unwrap(), None);
                assert_eq!(stored.query(|t| t.keys(71)).unwrap(), None);
            }
            "no_range" => assert_eq!(
                stored.query(|t| t.shares_range(sort(999998), 2)),
                Err(Failure::Accounting(core::Error::NoRange))
            ),
            _ => {}
        }
        assert_eq!(db.view().unwrap().status(), Default::default());
        drop(stored);
        drop(w);
        drop(db);
        let reopened = database(&temp);
        assert_eq!(rows(&reopened), want, "reopened {name}");
        reopened
            .stratum(Default::default())
            .unwrap()
            .verify()
            .unwrap();
    }
}
