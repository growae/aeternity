use ae_stratum::{
    accounting::*,
    proof::Number,
    rewards::{self, Map},
};
use ae_types::Hash256;
use ae_vm_fate::maps::Traversal;
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{cell::Cell, collections::BTreeMap};
fn int(n: impl ToString) -> Value {
    json!({"integer":n.to_string()})
}
fn atom(s: &str) -> Value {
    json!({"atom":s})
}
fn tuple(v: Vec<Value>) -> Value {
    json!({"tuple":v})
}
fn binary(b: impl AsRef<[u8]>) -> Value {
    json!({"binary":hex::encode(b)})
}
fn number(n: &Number) -> Value {
    match n {
        Number::Integer(n) => int(n),
        Number::Float(n) => json!({"float":format!("{n:016x}")}),
    }
}
fn map(m: &Map) -> Value {
    json!({"map":m.iter().map(|(k,v)|json!([binary(k),number(v)])).collect::<Vec<_>>()})
}
fn amounts(m: &rewards::Amounts) -> Value {
    json!({"map":m.iter().map(|(k,v)|json!([binary(k),int(v)])).collect::<Vec<_>>()})
}
fn input_number(v: &Value) -> Number {
    if let Some(i) = v.get("i") {
        Number::Integer(i.as_str().unwrap().parse().unwrap())
    } else {
        Number::Float(u64::from_str_radix(v["f"].as_str().unwrap(), 16).unwrap())
    }
}
fn input_map(v: &Value) -> Map {
    v.as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                hex::decode(p[0].as_str().unwrap()).unwrap(),
                input_number(&p[1]),
            )
        })
        .collect()
}
fn native_result(r: Result<Value, rewards::Error>) -> Value {
    match r {
        Ok(v) => json!({"ok":v}),
        Err(e) => {
            json!({"error":{"class":"error","reason":match e{rewards::Error::Arithmetic=>"badarith",rewards::Error::BadMatch=>"badmatch",e=>panic!("resource error {e:?}")}}})
        }
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_stratum_accounting.json")).unwrap()
}
fn float(n: f64) -> Number {
    Number::Float(n.to_bits())
}
fn date(d: Date) -> Value {
    tuple(vec![
        tuple(d.0[..3].iter().map(int).collect()),
        tuple(d.0[3..].iter().map(int).collect()),
    ])
}
fn optional<T>(v: &Option<T>, f: impl FnOnce(&T) -> Value) -> Value {
    v.as_ref().map_or_else(|| atom("undefined"), f)
}
fn payment(p: &Payment) -> Value {
    tuple(vec![
        atom("aestratum_payment"),
        tuple(vec![int(p.id.0), int(p.id.1)]),
        int(&p.total),
        map(&p.relative),
        optional(&p.absolute, amounts),
        optional(&p.tx_hash, |h| binary(h)),
        optional(&p.nonce, |n| int(n)),
        optional(&p.fee, |n| int(n)),
        optional(&p.gas, |n| int(n)),
        optional(&p.run_gas, |n| int(n)),
        optional(&p.date, |d| date(*d)),
    ])
}
fn shares(s: &Shares) -> Value {
    match s {
        Shares::Relative(m) => map(m),
        Shares::Transformed => atom("transformed"),
    }
}
fn record(r: &Record) -> Value {
    match r {
        Record::Hash { hash, key } => tuple(vec![atom("aestratum_hash"), binary(hash), int(key)]),
        Record::Round(k) => tuple(vec![atom("aestratum_round"), int(k), atom("undefined")]),
        Record::Share(s) => tuple(vec![
            atom("aestratum_share"),
            int(s.key),
            binary(s.hash),
            int(&s.target),
            binary(&s.miner),
        ]),
        Record::Reward(r) => tuple(vec![
            atom("aestratum_reward"),
            int(r.height),
            binary(r.hash),
            shares(&r.pool),
            shares(&r.miners),
            int(&r.amount),
            int(r.round_key),
        ]),
        Record::Payment(p) => payment(p),
        Record::Candidate(c) => tuple(vec![
            atom("aestratum_candidate"),
            binary(c.hash),
            binary(&c.header),
            binary(&c.record),
            date(c.date),
        ]),
    }
}
fn ok(v: Value) -> Value {
    tuple(vec![atom("ok"), v])
}
fn hash(n: u8) -> Hash256 {
    let mut h = [0; 32];
    h[31] = n;
    Hash256(h)
}
fn reward() -> Reward {
    Reward {
        height: 71,
        hash: hash(11),
        amount: 1001.into(),
        pool: Shares::Relative([(b"pool".to_vec(), float(1.0))].into()),
        miners: Shares::Relative(
            [
                (b"miner-a".to_vec(), float(0.25)),
                (b"miner-b".to_vec(), float(0.75)),
            ]
            .into(),
        ),
        round_key: 999997,
    }
}
fn basic_payment(i: u32) -> Payment {
    rewards::Payment {
        id: (71, i),
        total: 100.into(),
        relative: [(b"a".to_vec(), float(1.0))].into(),
    }
    .into()
}
fn candidate(n: u8) -> Candidate {
    Candidate {
        hash: hash(n),
        header: format!("header-{}", if n == 1 { "a" } else { "b" }).into_bytes(),
        record: format!("opaque-record-{}", if n == 1 { "a" } else { "b" }).into_bytes(),
        date: Date([2026, 1, n.into(), 0, 0, 0]),
    }
}
#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct Memory {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl Read for Memory {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.rows.get(key).cloned())
    }
    fn scan(&self, start: &[u8], end: &[u8], limit: usize, reverse: bool) -> Result<Scan, Error> {
        let rows = self.rows.range(start.to_vec()..end.to_vec());
        let rows: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = if reverse {
            Box::new(rows.rev())
        } else {
            Box::new(rows)
        };
        let mut entries: Entries = rows
            .take(limit + 1)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let more = entries.len() > limit;
        entries.truncate(limit);
        Ok(Scan { entries, more })
    }
}
impl Memory {
    fn commit(&mut self, p: Plan) {
        for (k, v) in p.originals() {
            assert_eq!(self.rows.get(k), v.as_ref());
        }
        for (k, v) in p.writes() {
            if let Some(v) = v {
                self.rows.insert(k.clone(), v.clone());
            } else {
                self.rows.remove(k);
            }
        }
    }
    fn run<T>(
        &mut self,
        f: impl FnOnce(&mut Transaction<'_, Self>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let (result, p) = prepare(self, Limits::default(), f)?;
        self.commit(p);
        Ok(result)
    }
    fn apply(&mut self, o: Operation) -> Reply {
        self.run(|tx| tx.apply(o)).unwrap()
    }
    fn state(&self) -> Value {
        let names = [
            "aestratum_hash",
            "aestratum_share",
            "aestratum_round",
            "aestratum_reward",
            "aestratum_payment",
            "aestratum_candidate",
        ];
        let mut state = serde_json::Map::new();
        for (table, name) in Table::ALL.into_iter().zip(names) {
            let values = self
                .rows
                .range(table.prefix()..table.end())
                .map(|(k, v)| record(&Record::decode(k, v, Limits::default()).unwrap()))
                .collect();
            state.insert(name.into(), Value::Array(values));
        }
        Value::Object(state)
    }
}
#[test]
fn original_rewards_match_all_float_bits_batches_and_binary_map_orders() {
    let data = fixture();
    let cases = data["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 220);
    let l = rewards::Limits::default();
    for (c, expected) in cases.iter().zip(data["rewards"].as_array().unwrap()) {
        assert_eq!(c["id"], expected["id"]);
        let m = input_map(&c["map"]);
        let keys: Vec<_> = m.keys().collect();
        for (mode, name) in [
            (Traversal::Fold, "fold_order"),
            (Traversal::ToList, "list_order"),
        ] {
            let order = rewards::indices(&m, mode, l).unwrap();
            assert_eq!(
                json!(
                    order
                        .into_iter()
                        .map(|i| hex::encode(keys[i]))
                        .collect::<Vec<_>>()
                ),
                expected[name],
                "order {} {name}",
                c["id"]
            );
        }
        let output = (|| {
            Ok(match c["op"].as_str().unwrap() {
                "relative" => map(&rewards::relative(&m, &input_number(&c["sum"]), l)?),
                "absolute" => amounts(&rewards::absolute(
                    &m,
                    &c["amount"].as_str().unwrap().parse().unwrap(),
                    l,
                )?),
                "absolute_normalized" => amounts(&rewards::absolute(
                    &rewards::relative(&m, &rewards::sum(&m, l)?, l)?,
                    &c["amount"].as_str().unwrap().parse().unwrap(),
                    l,
                )?),
                "chunks" => Value::Array(
                    rewards::chunks(&m, c["size"].as_u64().unwrap() as usize, l)?
                        .iter()
                        .map(map)
                        .collect(),
                ),
                "payments" => Value::Array(
                    rewards::payments(
                        c["height"].as_u64().unwrap(),
                        &c["amount"].as_str().unwrap().parse().unwrap(),
                        &input_map(&c["pool"]),
                        &rewards::relative(&m, &rewards::sum(&m, l)?, l)?,
                        &input_number(&c["percent"]),
                        l,
                    )?
                    .into_iter()
                    .map(|p| payment(&p.into()))
                    .collect(),
                ),
                x => panic!("operation {x}"),
            })
        })();
        assert_eq!(
            native_result(output),
            expected["result"],
            "case {}",
            c["id"]
        );
    }
}
fn keys(k: Option<Keys>) -> Value {
    let Some(k) = k else {
        return json!({"map":[]});
    };
    json!({"map":[[atom("aestratum_hash"),json!(k.hashes.iter().map(binary).collect::<Vec<_>>())],[atom("aestratum_payment"),json!(k.payments.iter().map(|(h,i)|tuple(vec![int(h),int(i)])).collect::<Vec<_>>())],[atom("aestratum_reward"),json!(k.rewards.iter().map(int).collect::<Vec<_>>())],[atom("aestratum_round"),json!(k.rounds.iter().map(int).collect::<Vec<_>>())],[atom("aestratum_share"),json!(k.shares.iter().map(int).collect::<Vec<_>>())]]})
}
fn reply(r: Reply) -> Value {
    match r {
        Reply::Unit => atom("ok"),
        Reply::Record(r) => ok(record(&r)),
        Reply::Records(rs) => json!(rs.iter().map(|r| ok(record(r))).collect::<Vec<_>>()),
    }
}
fn oldest(p: Vec<Payment>) -> Value {
    p.first().map_or_else(|| atom("none"), |p| ok(payment(p)))
}
#[test]
fn original_database_trace_matches_every_result_and_all_six_tables() {
    let data = fixture();
    let steps = data["database"].as_array().unwrap();
    assert_eq!(steps.len(), 27);
    let mut db = Memory::default();
    for step in steps {
        let name = step["name"].as_str().unwrap();
        let before = db.clone();
        let output = db.run(|tx| {
            let op = match name {
                "round_1" => Operation::Round(1000000),
                "round_2" => Operation::Round(999997),
                "round_3" => Operation::Round(999995),
                "share_a" | "share_b" | "share_c" | "share_zero" => {
                    let (k, h, t, m) = match name {
                        "share_a" => (999999, 1, 10, "miner-a"),
                        "share_b" => (999998, 1, 20, "miner-b"),
                        "share_c" => (999996, 2, 30, "miner-a"),
                        _ => (999994, 3, 0, "miner-c"),
                    };
                    let s = Share {
                        key: k,
                        hash: hash(h),
                        target: t.into(),
                        miner: m.as_bytes().into(),
                    };
                    tx.apply(Operation::Share(s.clone()))?;
                    return Ok(tuple(vec![
                        atom("ok"),
                        record(&Record::Share(s)),
                        record(&Record::Hash {
                            hash: hash(h),
                            key: k,
                        }),
                    ]));
                }
                "solution" => Operation::Solution {
                    candidate: hash(1),
                    block: hash(11),
                },
                "missing_solution" => Operation::Solution {
                    candidate: hash(0),
                    block: hash(11),
                },
                "range" => {
                    let (a, b) = tx.shares_range(999998, 2)?;
                    return Ok(tuple(vec![atom("ok"), int(a), int(b)]));
                }
                "rewards" | "reward_zero_target" => {
                    let (m, k) = tx.miner_rewards(
                        999998,
                        &if name == "rewards" {
                            100.into()
                        } else {
                            0.into()
                        },
                        2,
                    )?;
                    return Ok(tuple(vec![atom("ok"), map(&m), int(k)]));
                }
                "store_reward" => Operation::Reward(reward()),
                "store_payments" => Operation::StorePayments {
                    reward: reward(),
                    percent: float(1.5),
                },
                "unpaid" => {
                    return Ok(tuple(vec![
                        json!(
                            tx.select_payments(false, None)?
                                .iter()
                                .map(payment)
                                .collect::<Vec<_>>()
                        ),
                        json!(
                            tx.select_payments(false, Some(1))?
                                .iter()
                                .map(payment)
                                .collect::<Vec<_>>()
                        ),
                        oldest(tx.select_payments(false, Some(1))?),
                        json!({"atom":!tx.select_payments(true,Some(1))?.is_empty()}),
                        json!({"atom":!tx.select_payments(false,Some(1))?.is_empty()}),
                    ]));
                }
                "update_payment" => Operation::UpdatePayment {
                    payment: Box::new(tx.select_payments(false, Some(1))?.remove(0)),
                    absolute: [(b"pool".to_vec(), 15.into())].into(),
                    hash: hash(4),
                    date: Date([2026, 11, 24, 1, 2, 3]),
                },
                "paid" => {
                    return Ok(tuple(vec![
                        json!(
                            tx.select_payments(true, None)?
                                .iter()
                                .map(payment)
                                .collect::<Vec<_>>()
                        ),
                        json!(
                            tx.select_payments(true, Some(1))?
                                .iter()
                                .map(payment)
                                .collect::<Vec<_>>()
                        ),
                        oldest(tx.select_payments(false, Some(1))?),
                        json!(tx.payments(71)?.iter().map(payment).collect::<Vec<_>>()),
                    ]));
                }
                "keys" => return Ok(keys(tx.keys(71)?)),
                "delete_payment" => Operation::DeletePayment((71, 0)),
                "candidate_a" => Operation::Candidate(candidate(1)),
                "candidate_b" => Operation::Candidate(candidate(2)),
                "candidate_cleanup" => {
                    Operation::DeleteCandidatesBefore(Date([2026, 1, 2, 0, 0, 0]))
                }
                "candidate_lookup" => {
                    return Ok(tuple(
                        [1, 2]
                            .into_iter()
                            .map(|h| {
                                Ok(tx.candidate(hash(h))?.map_or_else(
                                    || tuple(vec![atom("error"), atom("not_found")]),
                                    |c| ok(record(&Record::Candidate(c))),
                                ))
                            })
                            .collect::<Result<_, Error>>()?,
                    ));
                }
                "delete_records" => Operation::DeleteRecords(71),
                "missing_reward" => {
                    assert_eq!(tx.reward(71)?, None);
                    return Ok(tuple(vec![
                        tuple(vec![atom("error"), atom("not_found")]),
                        keys(tx.keys(71)?),
                    ]));
                }
                "clear_rounds" => Operation::Clear(Table::Rounds),
                "no_range" => {
                    assert_eq!(tx.shares_range(999998, 2), Err(Error::NoRange));
                    return Ok(tuple(vec![atom("error"), atom("no_range")]));
                }
                x => panic!("step {x}"),
            };
            Ok(reply(tx.apply(op)?))
        });
        let output = match output {
            Ok(v) => json!({"ok":v}),
            Err(e) => {
                assert_eq!(db, before);
                assert!(matches!(
                    (name, e),
                    ("missing_solution", Error::MissingHash)
                        | (
                            "reward_zero_target",
                            Error::Rewards(rewards::Error::Arithmetic)
                        )
                ));
                json!({"error":{"class":"exit","reason":"aborted"}})
            }
        };
        assert_eq!(output, step["result"], "result {name}");
        assert_eq!(db.state(), step["state"], "state {name}");
    }
}
#[test]
fn range_overlay_matches_materialized_order_in_both_directions() {
    let mut db = Memory::default();
    for i in 0..260 {
        db.apply(Operation::Payment(basic_payment(i)));
    }
    for reverse in [false, true] {
        for limit in 1..=17 {
            let ((), _) = prepare(&db, Limits::default(), |tx| {
                let mut expected = db.rows.clone();
                for i in 0..260 {
                    if i % 3 != 0 {
                        tx.apply(Operation::DeletePayment((71, i)))?;
                        expected.remove(&payment_key((71, i)));
                    }
                }
                for i in [0, 7, 258, 260, 261, 999] {
                    let p = basic_payment(i);
                    tx.apply(Operation::Payment(p.clone()))?;
                    expected.insert(
                        payment_key(p.id),
                        Record::Payment(p).encode(Limits::default())?,
                    );
                }
                for (start, end) in [
                    (Table::Payments.prefix(), Table::Payments.end()),
                    (payment_key((71, 5)), payment_key((71, 170))),
                    (payment_key((71, 259)), Table::Payments.end()),
                ] {
                    let mut all: Vec<_> = expected
                        .range(start.clone()..end.clone())
                        .map(|(k, _)| k.clone())
                        .collect();
                    if reverse {
                        all.reverse();
                    }
                    let (actual, more) = tx.range(&start, &end, limit, reverse)?;
                    assert_eq!(
                        actual.iter().map(Record::key).collect::<Vec<_>>(),
                        all.iter().take(limit).cloned().collect::<Vec<_>>()
                    );
                    assert_eq!(more, all.len() > limit);
                }
                Ok(())
            })
            .unwrap();
        }
    }
}
struct Fault<'a> {
    db: &'a Memory,
    left: Cell<usize>,
}
impl Fault<'_> {
    fn step(&self) -> Result<(), Error> {
        let n = self.left.get();
        if n == 0 {
            return Err(Error::Backend("injected read".into()));
        }
        self.left.set(n - 1);
        Ok(())
    }
}
impl Read for Fault<'_> {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.step()?;
        self.db.get(k)
    }
    fn scan(&self, s: &[u8], e: &[u8], n: usize, r: bool) -> Result<Scan, Error> {
        self.step()?;
        self.db.scan(s, e, n, r)
    }
}
#[test]
fn failed_transactions_discard_every_staged_write_and_cannot_ignore_mutation_errors() {
    let mut db = Memory::default();
    db.apply(Operation::Share(Share {
        key: 1,
        hash: hash(1),
        target: 10.into(),
        miner: b"a".to_vec(),
    }));
    let before = db.clone();
    let mut successful = None;
    for i in 0..20 {
        let f = Fault {
            db: &db,
            left: Cell::new(i),
        };
        let result = prepare(&f, Limits::default(), |tx| {
            tx.apply(Operation::Round(2))?;
            tx.apply(Operation::Solution {
                candidate: hash(1),
                block: hash(11),
            })?;
            tx.apply(Operation::Reward(reward()))?;
            tx.apply(Operation::StorePayments {
                reward: reward(),
                percent: float(1.5),
            })?;
            tx.apply(Operation::Candidate(candidate(1)))?;
            Ok(())
        });
        assert_eq!(db, before);
        match result {
            Ok(((), p)) => {
                successful = Some((i, p));
                break;
            }
            Err(e) => assert_eq!(e, Error::Backend("injected read".into())),
        }
    }
    let (count, plan) = successful.unwrap();
    assert!(count >= 9);
    db.commit(plan);
    assert!(db.rows.contains_key(&key(Table::Hashes, hash(11))));
    let result = prepare(&db, Limits::default(), |tx| {
        tx.apply(Operation::Round(3))?;
        let _ = tx.apply(Operation::Solution {
            candidate: hash(99),
            block: hash(11),
        });
        Ok(())
    });
    assert!(matches!(result, Err(Error::Aborted)));
}
#[test]
fn quotas_dates_and_nonfinite_numbers_abort_before_publication() {
    let db = Memory::default();
    let l = Limits {
        records: 1,
        ..Limits::default()
    };
    assert!(matches!(
        prepare(&db, l, |tx| tx.apply(Operation::Share(Share {
            key: 1,
            hash: hash(1),
            target: 1.into(),
            miner: vec![]
        }))),
        Err(Error::Limit)
    ));
    for d in [
        Date([2026, 2, 29, 0, 0, 0]),
        Date([2024, 2, 30, 0, 0, 0]),
        Date([2026, 0, 1, 0, 0, 0]),
        Date([2026, 1, 1, 24, 0, 0]),
    ] {
        let mut c = candidate(1);
        c.date = d;
        assert!(Record::Candidate(c).encode(Limits::default()).is_err());
    }
    Date([2024, 2, 29, 23, 59, 59]).validate().unwrap();
    for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut p = basic_payment(0);
        p.relative.insert(vec![], float(f));
        assert!(Record::Payment(p).encode(Limits::default()).is_err());
    }
    let mut p = basic_payment(0);
    p.total = BigInt::from(1) << 16384;
    assert!(Record::Payment(p).encode(Limits::default()).is_err());
    let mut c = candidate(1);
    c.record = vec![0; 1024];
    assert!(matches!(
        Record::Candidate(c).encode(Limits {
            record_bytes: 512,
            ..Limits::default()
        }),
        Err(Error::Limit)
    ));
    let result = prepare(&db, Limits::default(), |tx| tx.range(&[9], &[1], 1, false));
    assert!(matches!(result, Err(Error::Corrupt("accounting range"))));
}
#[test]
fn record_codec_rejects_corruption_wrong_keys_and_noncanonical_aliases() {
    let mut p = basic_payment(u32::MAX);
    p.id.0 = u64::MAX;
    p.total = -(BigInt::from(1) << 1024usize);
    p.relative.insert(b"negative-zero".to_vec(), float(-0.0));
    p.absolute = Some([(vec![], (-17).into())].into());
    p.tx_hash = Some(hash(4));
    p.nonce = Some(0.into());
    p.fee = Some(123.into());
    p.gas = Some(1.into());
    p.run_gas = Some((-1).into());
    p.date = Some(Date([2026, 11, 24, 1, 2, 3]));
    let mut r = reward();
    r.pool = Shares::Transformed;
    r.miners = Shares::Transformed;
    for r in [
        Record::Payment(p),
        Record::Reward(r),
        Record::Round(u64::MAX),
        Record::Hash {
            hash: hash(0),
            key: 0,
        },
        Record::Candidate(candidate(1)),
        Record::Share(Share {
            key: 0,
            hash: hash(1),
            target: (-1).into(),
            miner: vec![],
        }),
    ] {
        let raw = r.encode(Limits::default()).unwrap();
        assert_eq!(
            Record::decode(&r.key(), &raw, Limits::default()).unwrap(),
            r
        );
        for end in 0..raw.len() {
            assert!(Record::decode(&r.key(), &raw[..end], Limits::default()).is_err());
        }
        let mut trailing = raw.clone();
        trailing.push(0);
        assert!(Record::decode(&r.key(), &trailing, Limits::default()).is_err());
        let mut key = r.key();
        key.push(0);
        assert!(Record::decode(&key, &raw, Limits::default()).is_err());
    }
    // RLP accepts a long-form spelling of the same payload; record codec rejects it.
    let r = Record::Round(1);
    let raw = r.encode(Limits::default()).unwrap();
    let mut alias = vec![0xf8, raw[0] - 0xc0];
    alias.extend_from_slice(&raw[1..]);
    assert!(Record::decode(&r.key(), &alias, Limits::default()).is_err());
    let mut db = Memory::default();
    db.rows
        .insert(key(Table::Rounds, 1u64.to_be_bytes()), vec![0]);
    assert!(db.run(|tx| tx.records(Table::Rounds)).is_err());
}
#[test]
fn payment_updates_keep_native_whole_record_upsert_and_height_order() {
    let mut db = Memory::default();
    let old = basic_payment(1);
    db.apply(Operation::Payment(old.clone()));
    let mut changed = old.clone();
    changed.total = 999.into();
    db.apply(Operation::Payment(changed));
    db.apply(Operation::UpdatePayment {
        payment: Box::new(old.clone()),
        absolute: [(b"a".to_vec(), 10.into())].into(),
        hash: hash(5),
        date: Date([2026, 11, 24, 0, 0, 0]),
    });
    let p = db
        .run(|tx| tx.select_payments(true, Some(1)))
        .unwrap()
        .remove(0);
    assert_eq!(p.total, old.total);
    assert_eq!(p.tx_hash, Some(hash(5)));
    db.apply(Operation::DeletePayment(old.id));
    db.apply(Operation::UpdatePayment {
        payment: Box::new(old),
        absolute: BTreeMap::new(),
        hash: hash(6),
        date: Date([2026, 11, 24, 0, 0, 0]),
    });
    assert_eq!(db.run(|tx| tx.payments(71)).unwrap().len(), 1);
    let mut p = basic_payment(0);
    p.id = (u64::MAX, u32::MAX);
    db.apply(Operation::Payment(p.clone()));
    assert_eq!(db.run(|tx| tx.payments(u64::MAX)).unwrap(), vec![p]);
}
