use super::*;
use crate::tests::{Temp, identity, plan};
fn config() -> Config {
    Config::from_percentages(
        [(b"pool".to_vec(), Number::Float(1.5f64.to_bits()))].into(),
        2,
        180,
        Default::default(),
    )
    .unwrap()
}
#[test]
fn initialization_is_durable_idempotent_and_keeps_existing_rounds() {
    let t = Temp::new();
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    db.commit(plan(&db)).unwrap();
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let info = w.begin_rounds(config(), Default::default()).unwrap();
    let before = db.stratum(Default::default()).unwrap().info();
    assert_eq!(before.counts[2], 1);
    assert_eq!(w.begin_rounds(config(), Default::default()).unwrap(), info);
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
    let encoded = info.encode(Default::default(), Default::default()).unwrap();
    assert_eq!(
        Info::decode(&encoded, Default::default(), Default::default()).unwrap(),
        info
    );
    drop(w);
    drop(db);
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert_eq!(w.begin_rounds(config(), Default::default()).unwrap(), info);
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
    let mut changed = config();
    changed.delay += 1;
    assert!(w.begin_rounds(changed, Default::default()).is_err());
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
}

fn database(t: &Temp) -> Arc<Database> {
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        db.commit(plan(&db)).unwrap();
    }
    db
}
fn info(db: &Database) -> Info {
    db.stratum(Default::default())
        .unwrap()
        .rounds(Default::default())
        .unwrap()
        .unwrap()
}
fn commit(w: &mut Writer, commands: Vec<Command>) {
    w.commit(w.version().unwrap(), commands).unwrap();
}
// Storage protocol tests install explicit canonical-read fixtures. These are
// not PoW-valid chains; actual saved-block admission is tested in ae-node.
fn synthetic_top(db: &Database, height: u64) -> Hash256 {
    let view = db.view().unwrap();
    let prior = view.status();
    let Block::Key(mut h) = view.require_block(view.identity().genesis).unwrap() else {
        panic!()
    };
    h.common.height = height;
    h.common.previous_hash = prior.tip.unwrap().0;
    h.common.previous_key_hash = prior.tip.unwrap().0;
    h.time += prior.revision;
    let block = Block::Key(h);
    let hash = block.hash().unwrap();
    let mut record = view.require_record(view.identity().genesis).unwrap();
    record.difficulty = record.difficulty.add(&1u64.into());
    let change = repo::Change {
        revision: prior.revision + 1,
        inserted: Some(hash),
        removed: vec![],
        discarded: vec![],
        old_tip: prior.tip,
        new_tip: hash,
        ancestor: prior.tip,
        detached: vec![],
        attached: vec![hash],
        requeue: vec![],
        confirm: vec![],
    };
    let mut batch = WriteBatch::default();
    batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
    batch.put(repo::key(repo::CANONICAL, hash.0), []);
    batch.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0);
    batch.put(
        repo::key(repo::EVENTS, change.revision.to_be_bytes()),
        change.encode(),
    );
    batch.put(
        repo::STATUS,
        repo::Status {
            revision: change.revision,
            tip: Some(hash),
        }
        .encode(),
    );
    db.db.write_opt(batch, &write_options()).unwrap();
    hash
}
fn reference() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_rounds.json"
    ))
    .unwrap()
}
fn reward_key(db: &Database, height: u64, fees: u64) -> Hash256 {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_candidates.json"
    ))
    .unwrap();
    let row = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == format!("ae_mainnet_{height}"))
        .unwrap();
    let candidate = ae_stratum::candidate::Candidate::decode(
        &hex::decode(row["sealing"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let mut header = candidate.header().clone();
    header.nonce = row["nonce"].as_u64().unwrap();
    header.seal = std::array::from_fn(|i| row["proof"][i].as_u64().unwrap() as u32);
    let block = Block::Key(header);
    let hash = block.hash().unwrap();
    assert_eq!(hex::encode(hash.0), row["block_hash"].as_str().unwrap());
    let view = db.view().unwrap();
    let mut record = view.require_record(view.identity().genesis).unwrap();
    record.fees = fees.into();
    let mut batch = WriteBatch::default();
    batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
    batch.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0);
    batch.put(repo::key(repo::CANONICAL, hash.0), []);
    db.db.write_opt(batch, &write_options()).unwrap();
    hash
}
fn prepared(db: &Arc<Database>, mode: &str, height: u64, fees: u64) -> (Writer, Hash256) {
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(config(), Default::default()).unwrap();
    let hash = reward_key(db, height, fees);
    for (i, (miner, target)) in [(b"miner-a".to_vec(), 10u64), (b"miner-b".to_vec(), 20)]
        .into_iter()
        .enumerate()
    {
        commit(
            &mut w,
            vec![Command::Share {
                hash: if i == 0 { Hash256([91; 32]) } else { hash },
                target: if mode == "zero" {
                    0.into()
                } else {
                    target.into()
                },
                miner,
            }],
        );
    }
    match mode {
        "no_index" => commit(&mut w, vec![Command::Clear(core::Table::Hashes)]),
        "empty" => commit(&mut w, vec![Command::Clear(core::Table::Shares)]),
        "existing" => commit(
            &mut w,
            vec![Command::Reward(core::Reward {
                height,
                hash,
                pool: core::Shares::Transformed,
                miners: core::Shares::Transformed,
                amount: 77.into(),
                round_key: u64::MAX,
            })],
        ),
        _ => {}
    }
    synthetic_top(db, height + 180);
    w.capture_rounds(Default::default()).unwrap();
    w.step_rounds(info(db).next, Default::default()).unwrap();
    if mode == "no_header" {
        db.db
            .delete(repo::key(repo::KEYS, height.to_be_bytes()))
            .unwrap();
    }
    if mode == "no_fees" {
        db.db.delete(repo::key(repo::RECORDS, hash.0)).unwrap();
    }
    (w, hash)
}
fn atom(s: &str) -> serde_json::Value {
    serde_json::json!({"atom":s})
}
fn integer_json(n: impl ToString) -> serde_json::Value {
    serde_json::json!({"integer":n.to_string()})
}
fn tuple(v: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({"tuple":v})
}
fn binary(b: impl AsRef<[u8]>) -> serde_json::Value {
    serde_json::json!({"binary":hex::encode(b)})
}
fn map(m: &Map) -> serde_json::Value {
    serde_json::json!({"map":m.iter().map(|(k,v)|serde_json::json!([binary(k),match v{Number::Integer(n)=>integer_json(n),Number::Float(n)=>serde_json::json!({"float":format!("{n:016x}")})}])).collect::<Vec<_>>()})
}
fn payment(p: core::Payment) -> serde_json::Value {
    assert!(
        p.absolute.is_none()
            && p.tx_hash.is_none()
            && p.date.is_none()
            && p.fee.is_none()
            && p.nonce.is_none()
            && p.gas.is_none()
            && p.run_gas.is_none()
    );
    tuple(vec![
        atom("aestratum_payment"),
        tuple(vec![integer_json(p.id.0), integer_json(p.id.1)]),
        integer_json(p.total),
        map(&p.relative),
        atom("undefined"),
        atom("undefined"),
        atom("undefined"),
        atom("undefined"),
        atom("undefined"),
        atom("undefined"),
        atom("undefined"),
    ])
}
fn reward(r: Option<core::Reward>) -> serde_json::Value {
    match r {
        None => tuple(vec![atom("error"), atom("not_found")]),
        Some(r) => {
            assert_eq!(r.pool, core::Shares::Transformed);
            assert_eq!(r.miners, core::Shares::Transformed);
            tuple(vec![
                atom("ok"),
                tuple(vec![
                    atom("aestratum_reward"),
                    integer_json(r.height),
                    binary(r.hash),
                    atom("transformed"),
                    atom("transformed"),
                    integer_json(r.amount),
                    integer_json(1_000_000 - (u64::MAX - r.round_key)),
                ]),
            ])
        }
    }
}
#[test]
fn original_reward_callbacks_match_amounts_float_bits_payment_batches_and_error_boundaries() {
    let f = reference();
    for input in f["inputs"].as_array().unwrap() {
        let name = input["name"].as_str().unwrap();
        let expected = f["rewards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == name)
            .unwrap();
        let t = Temp::new();
        let db = database(&t);
        let mode = input["mode"].as_str().unwrap();
        let height = input["height"].as_u64().unwrap();
        let fees = input["fees"].as_str().unwrap().parse().unwrap();
        let (mut w, _) = prepared(&db, mode, height, fees);
        let sequence = info(&db).next;
        let result = w.step_rounds(sequence, Default::default());
        assert_eq!(
            result.is_ok(),
            expected["result"] == atom("ok"),
            "{name}: {result:?}"
        );
        let stored = db.stratum(Default::default()).unwrap();
        assert_eq!(
            stored.info().counts[2],
            expected["rounds"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            reward(stored.query(|tx| tx.reward(height)).unwrap()),
            expected["reward"],
            "{name}"
        );
        let payments = stored
            .query(|tx| tx.payments(height))
            .unwrap()
            .into_iter()
            .map(payment)
            .collect::<Vec<_>>();
        assert_eq!(serde_json::json!(payments), expected["payments"], "{name}");
        if let Ok(receipt) = result {
            let before = stored.info();
            drop(stored);
            assert_eq!(
                w.step_rounds(sequence, Default::default()).unwrap(),
                receipt
            );
            assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
        } else {
            assert!(info(&db).computing(), "{name}");
            assert!(!w.recovery_required());
        }
    }
}

fn drain(w: &mut Writer, db: &Database) -> Vec<Receipt> {
    let mut out = vec![];
    loop {
        let state = w.capture_rounds(Default::default()).unwrap();
        if state.pending.is_empty() {
            break;
        }
        out.push(w.step_rounds(state.next, Default::default()).unwrap());
        assert!(out.len() < 100);
    }
    assert!(info(db).pending.is_empty());
    out
}
#[test]
fn original_deferred_mailbox_traces_preserve_duplicate_rounds_and_latest_height() {
    for row in reference()["queue"].as_array().unwrap() {
        let t = Temp::new();
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let initial = w.begin_rounds(config(), Default::default()).unwrap();
        reward_key(&db, 1, 0);
        let h2 = reward_key(&db, 2, 0);
        if row["existing_reward"] == true {
            commit(
                &mut w,
                vec![Command::Reward(core::Reward {
                    height: 2,
                    hash: h2,
                    pool: core::Shares::Transformed,
                    miners: core::Shares::Transformed,
                    amount: 1.into(),
                    round_key: u64::MAX,
                })],
            );
        }
        let mut receipts = vec![];
        for height in row["source_heights"].as_array().unwrap() {
            synthetic_top(&db, height.as_u64().unwrap());
            if row["name"] == "sequential" {
                receipts.extend(drain(&mut w, &db));
            }
        }
        receipts.extend(drain(&mut w, &db));
        let lookups: Vec<_> = receipts
            .iter()
            .filter(|r| {
                matches!(
                    r.outcome,
                    Outcome::NotOurShare | Outcome::Reward { .. } | Outcome::NoRange
                )
            })
            .map(|r| r.height - config().delay)
            .collect();
        assert_eq!(
            serde_json::json!(lookups),
            row["reward_lookups"],
            "{}",
            row["name"]
        );
        assert_eq!(
            db.stratum(Default::default()).unwrap().info().counts[2] - 1,
            row["rounds"].as_u64().unwrap()
        );
        assert_eq!(info(&db).protocol, initial.protocol);
        let before = db.stratum(Default::default()).unwrap().info();
        assert!(drain(&mut w, &db).is_empty());
        assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
    }
}
#[test]
fn persisted_history_recovers_after_reopen_and_capacity_delays_without_dropping_events() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(config(), Default::default()).unwrap();
    reward_key(&db, 1, 0);
    reward_key(&db, 2, 0);
    synthetic_top(&db, 181);
    synthetic_top(&db, 182);
    let status = db.view().unwrap().status();
    drop(w);
    drop(db);
    let db = database(&t);
    assert_eq!(db.events().info().records, 0);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let limits = Limits {
        pending: 1,
        events: 1,
        ..Default::default()
    };
    let mut receipts = vec![];
    for _ in 0..8 {
        let state = w.capture_rounds(limits).unwrap();
        if !state.pending.is_empty() {
            receipts.push(w.step_rounds(state.next, limits).unwrap());
        }
    }
    assert_eq!(info(&db).cursor, status.revision);
    assert!(info(&db).pending.is_empty());
    assert_eq!(receipts.iter().filter(|r| r.round.is_some()).count(), 2);
    assert_eq!(db.stratum(Default::default()).unwrap().info().counts[2], 3);
    let before = db.stratum(Default::default()).unwrap().info();
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(config(), Default::default()).unwrap();
    assert!(drain(&mut w, &db).is_empty());
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
}
fn setup_stage(db: &Arc<Database>, stage: Stage) {
    if stage == Stage::Initialize {
        return;
    }
    if stage == Stage::Capture {
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        w.begin_rounds(config(), Default::default()).unwrap();
        let hash = reward_key(db, 1, 23);
        commit(
            &mut w,
            vec![
                Command::Share {
                    hash: Hash256([91; 32]),
                    target: 10.into(),
                    miner: b"miner-a".to_vec(),
                },
                Command::Share {
                    hash,
                    target: 20.into(),
                    miner: b"miner-b".to_vec(),
                },
            ],
        );
        synthetic_top(db, 181);
    } else {
        drop(prepared(db, "normal", 1, 23));
    }
}
fn perform_stage(
    w: &mut Writer,
    db: &Database,
    stage: Stage,
    write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
) -> Result<(), Failure> {
    match stage {
        Stage::Initialize => w
            .begin_rounds_with(config(), Default::default(), write)
            .map(|_| ()),
        Stage::Capture => w.capture_rounds_with(Default::default(), write).map(|_| ()),
        Stage::Round | Stage::Finish => w
            .step_rounds_with(info(db).next, Default::default(), write)
            .map(|_| ()),
    }
}
fn finish_stage(w: &mut Writer, db: &Database, stage: Stage) {
    w.begin_rounds(config(), Default::default()).unwrap();
    if stage != Stage::Initialize {
        drain(w, db);
        let stored = db.stratum(Default::default()).unwrap();
        assert_eq!(stored.info().counts[1..5], [2, 2, 1, 2]);
        assert_eq!(stored.info().next_key, Some(u64::MAX - 4));
    } else {
        assert_eq!(
            db.stratum(Default::default()).unwrap().info().counts,
            [0, 0, 1, 0, 0, 0]
        );
    }
}
#[test]
fn uncertain_round_writes_resume_exact_batches_without_duplicate_allocation_or_payments() {
    for stage in [
        Stage::Initialize,
        Stage::Capture,
        Stage::Round,
        Stage::Finish,
    ] {
        for applied in [false, true] {
            let t = Temp::new();
            let db = database(&t);
            setup_stage(&db, stage);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let mut injected = false;
            let result = perform_stage(&mut w, &db, stage, |at, db, b| {
                if at == stage {
                    injected = true;
                    if applied {
                        db.db.write_opt(b, &write_options()).map_err(backend)?;
                    }
                    Err(Error::Backend("lost exact response".into()))
                } else {
                    db.db.write_opt(b, &write_options()).map_err(backend)
                }
            });
            assert!(result.is_err() && injected && w.recovery_required());
            let recovery = w.recover().unwrap();
            assert_eq!(matches!(recovery, Recovery::Applied(_)), applied);
            finish_stage(&mut w, &db, stage);
            let before = db.stratum(Default::default()).unwrap().info();
            finish_stage(&mut w, &db, stage);
            assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
        }
    }
}
#[test]
fn failed_reward_lookup_retains_the_original_round_and_resumes_after_data_restoration() {
    for mode in ["no_header", "no_fees"] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, hash) = prepared(&db, mode, 1, 23);
        let sequence = info(&db).next;
        assert!(w.step_rounds(sequence, Default::default()).is_err());
        let pending = info(&db);
        assert!(pending.computing());
        let count = db.stratum(Default::default()).unwrap().info().counts[2];
        drop(w);
        drop(db);
        let db = database(&t);
        assert_eq!(info(&db), pending);
        assert_eq!(reward_key(&db, 1, 23), hash);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let receipt = w.step_rounds(sequence, Default::default()).unwrap();
        assert!(matches!(receipt.outcome, Outcome::Reward { .. }));
        assert_eq!(
            db.stratum(Default::default()).unwrap().info().counts[2],
            count
        );
    }
}
#[test]
fn corrupt_round_metadata_and_missing_history_are_never_silently_rebased() {
    for kind in ["future", "header", "round", "sequence"] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, _) = prepared(&db, "normal", 1, 23);
        let mut state = info(&db);
        match kind {
            "future" => {
                state.cursor += 1;
                db.db
                    .put(
                        META,
                        state
                            .encode(Default::default(), Default::default())
                            .unwrap(),
                    )
                    .unwrap();
            }
            "header" => {
                let rev = state.pending.front().unwrap().revision();
                let mut h = state.header.clone();
                h.time += 1;
                state.pending[0] = Job::Top {
                    revision: rev,
                    header: Box::new(h),
                };
                db.db
                    .put(
                        META,
                        state
                            .encode(Default::default(), Default::default())
                            .unwrap(),
                    )
                    .unwrap();
            }
            "round" => {
                let seq = state.next;
                assert!(
                    w.step_rounds_with(seq, Default::default(), |at, db, b| {
                        if at == Stage::Finish {
                            Err(Error::Backend("retain compute".into()))
                        } else {
                            db.db.write_opt(b, &write_options()).map_err(backend)
                        }
                    })
                    .is_err()
                );
                w.recover().unwrap();
                let Job::Compute { round, .. } = info(&db).pending[0] else {
                    panic!()
                };
                db.db
                    .delete(key(&core::key(core::Table::Rounds, round.to_be_bytes())))
                    .unwrap();
            }
            "sequence" => {
                let Value::List(mut v) = Value::decode(&db.db.get(META).unwrap().unwrap()).unwrap()
                else {
                    panic!()
                };
                v[5] = number(0);
                db.db.put(META, Value::List(v).encode()).unwrap();
            }
            _ => unreachable!(),
        }
        drop(w);
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err(), "{kind}");
    }
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(config(), Default::default()).unwrap();
    synthetic_top(&db, 181);
    let state = info(&db);
    db.db
        .delete(repo::key(repo::EVENTS, 2u64.to_be_bytes()))
        .unwrap();
    assert!(w.capture_rounds(Default::default()).is_err());
    assert_eq!(info(&db), state);
}
#[test]
fn schema_seventeen_reopens_with_stable_bindings_and_existing_accounting() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(&mut w, vec![Command::Round]);
    let before = db.stratum(Default::default()).unwrap().info();
    drop(w);
    let identity = db.view().unwrap().identity().clone();
    let binding = identity.binding();
    let Value::List(mut v) = Value::decode(&identity.encode()).unwrap() else {
        panic!()
    };
    v[0] = number(17);
    db.db.put(repo::IDENTITY, Value::List(v).encode()).unwrap();
    drop(db);
    let db = database(&t);
    assert_eq!(db.view().unwrap().identity().binding(), binding);
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(config(), Default::default()).unwrap();
    assert_eq!(db.stratum(Default::default()).unwrap().info().counts[2], 1);
}
mod recovery;
