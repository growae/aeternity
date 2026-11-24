use super::*;
use crate::tests::Temp;
use ae_chain::repository::{Roots, ledger};
use ae_codec::headers::Header;
use ae_crypto::Ed25519SecretKey;
use ae_protocol::{Activation, NetworkProfile};
use ae_state::{
    Trees,
    accounts::Account,
    genesis::{self, Network},
};
fn key(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
fn signer() -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(7))
}
fn config() -> Config {
    Config {
        caller: signer().public_key(),
        contract: ContractPublicKey(key(29)),
        delay: 180,
    }
}
const GROSS: u64 = 1_000_000_000_000_000_000;
fn network() -> NetworkProfile {
    NetworkProfile::custom(
        b"ae_payout_local".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Roma,
        }],
    )
    .unwrap()
}
fn genesis_trees() -> Trees {
    let mut t = Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        channels: Default::default(),
        calls: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    t.accounts
        .enter(&Account::new(
            config().caller,
            (GROSS + 1_000_000_000).into(),
        ))
        .unwrap();
    t
}
fn identity() -> repo::Identity {
    let Header::Key(mut h) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    h.common.state_hash = genesis_trees().roots().hash().0;
    repo::Identity::new(&network(), Header::Key(h).hash().unwrap(), Hash256::ZERO)
}
fn database(t: &Temp) -> Arc<Database> {
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        let Header::Key(mut h) = genesis::header(Network::Uat).unwrap() else {
            panic!()
        };
        let trees = genesis_trees();
        h.common.state_hash = trees.roots().hash().0;
        db.commit(
            ledger::bootstrap(
                &db.view().unwrap(),
                &network(),
                &h,
                &trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
    db
}
fn commit(w: &mut Writer, c: Vec<Command>) {
    w.commit(w.version().unwrap(), c).unwrap();
}
fn pay(id: core::PaymentId) -> core::Payment {
    ae_stratum::rewards::Payment {
        id,
        total: GROSS.into(),
        relative: [(
            ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &key(1)).into_bytes(),
            Number::Float(1.0f64.to_bits()),
        )]
        .into(),
    }
    .into()
}
fn stored(db: &Database) -> Info {
    db.stratum(Default::default())
        .unwrap()
        .payouts(Default::default())
        .unwrap()
        .unwrap()
}
// Explicit state/top fixtures exercise storage protocols, not PoW admission.
fn set_top(
    db: &Database,
    height: u64,
    protocol: Protocol,
    balance: Option<u64>,
    nonce: u64,
) -> Hash256 {
    let v = db.view().unwrap();
    let prior = v.status();
    let old = prior.tip.unwrap();
    let mut trees = v.trees(old, Default::default()).unwrap();
    match balance {
        Some(n) => {
            let mut a = Account::new(config().caller, n.into());
            a.set_nonce(nonce.into());
            trees.accounts.enter(&a).unwrap();
        }
        None => trees.accounts.delete(config().caller).unwrap(),
    }
    let Block::Key(mut h) = v.require_block(v.identity().genesis).unwrap() else {
        panic!()
    };
    h.common.height = height;
    h.common.version = protocol as u32;
    h.common.previous_hash = old.0;
    h.common.previous_key_hash = Header::Key(top(&v).unwrap()).hash().unwrap().0;
    h.common.state_hash = trees.roots().hash().0;
    h.time += prior.revision;
    let block = Block::Key(h);
    let hash = block.hash().unwrap();
    let mut record = v.require_record(old).unwrap();
    record.roots = Roots::of(&trees);
    record.difficulty = record.difficulty.add(&1.into());
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
    let mut b = WriteBatch::default();
    for (h, raw) in trees.accounts.tree().store().pending() {
        b.put(repo::key(repo::NODES, h.0), raw);
    }
    b.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    b.put(repo::key(repo::RECORDS, hash.0), record.encode());
    b.put(repo::key(repo::CANONICAL, hash.0), []);
    b.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0);
    b.put(
        repo::key(repo::EVENTS, change.revision.to_be_bytes()),
        change.encode(),
    );
    b.put(
        repo::STATUS,
        repo::Status {
            revision: change.revision,
            tip: Some(hash),
        }
        .encode(),
    );
    write(db, b).unwrap();
    hash
}
fn setup(db: &Arc<Database>) -> (Writer, u64) {
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(&mut w, vec![Command::Payment(pay((100, 0)))]);
    let info = w.begin_payouts(config(), Default::default()).unwrap();
    (w, info.epoch)
}
fn prepared(w: &mut Writer, epoch: u64) -> Record {
    let Selection::Prepared(r) = w
        .prepare_payout(epoch, 0.into(), Default::default())
        .unwrap()
    else {
        panic!()
    };
    *r
}
fn signed(w: &mut Writer, epoch: u64, r: &Record) -> Record {
    w.sign_payout(
        epoch,
        r,
        signer().sign(r.request.as_ref().unwrap().prepared.signing_bytes()),
        core::Date([2026, 1, 1, 0, 0, 0]),
        Default::default(),
    )
    .unwrap()
}
#[test]
fn intent_and_signature_survive_reopen_without_changing_nonce_or_source_payment() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, epoch) = setup(&db);
    let r = prepared(&mut w, epoch);
    let snap = db.stratum(Default::default()).unwrap();
    let next = signed(&mut w, epoch, &r);
    assert_eq!(
        snap.payouts(Default::default()).unwrap().unwrap().pending,
        Some(r.clone())
    );
    drop(snap);
    assert_eq!(
        payment(&db.stratum(Default::default()).unwrap(), r.payment.id).unwrap(),
        Some(next.expected_payment().unwrap())
    );
    set_top(&db, 281, Protocol::Ceres, Some(12), 999);
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let info = w.begin_payouts(config(), Default::default()).unwrap();
    assert_eq!(info.context.protocol, Protocol::Ceres);
    assert_eq!(info.epoch, epoch + 1);
    assert_eq!(info.pending, Some(next.clone()));
    assert_eq!(
        w.prepare_payout(info.epoch, 9.into(), Default::default())
            .unwrap(),
        Selection::Existing(Box::new(next.clone()))
    );
    assert!(
        w.begin_payout_submission(epoch, &next, Default::default())
            .is_err()
    );
    assert_eq!(
        next.request
            .unwrap()
            .prepared
            .request()
            .environment
            .account_nonce,
        0.into()
    );
}
#[test]
fn ordinary_accounting_mutations_cannot_discard_or_replace_an_active_source() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let before = stored(&db);
    for c in [
        Command::DeletePayment(r.payment.id),
        Command::Clear(core::Table::Payments),
        {
            let mut p = r.payment.clone();
            p.total += 1;
            Command::Payment(p)
        },
    ] {
        assert!(w.commit(w.version().unwrap(), vec![c]).is_err());
        assert_eq!(stored(&db), before);
    }
    commit(&mut w, vec![Command::Payment(pay((101, 0)))]);
    assert_eq!(stored(&db), before);
    let s = signed(&mut w, e, &r);
    assert_eq!(stored(&db).pending, Some(s));
}
#[test]
fn cached_and_fresh_balance_gates_are_distinct_and_failed_preparation_keeps_rows() {
    let t = Temp::new();
    let db = database(&t);
    set_top(&db, 280, Protocol::Roma, Some(GROSS), 0);
    let (mut w, e) = setup(&db);
    assert!(matches!(
        w.prepare_payout(e, 0.into(), Default::default()),
        Err(Failure::Payout(payout::Error::InsufficientBalance { .. }))
    ));
    assert!(stored(&db).pending.is_none());
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    assert!(matches!(
        w.prepare_payout(e, 0.into(), Default::default()).unwrap(),
        Selection::Prepared(_)
    ));
}
fn pool_config() -> crate::pool::Config {
    let mut c = crate::pool::Config::default();
    c.policy.minimum_miner_gas_price = 1.into();
    c
}
fn signed_tx(r: &Record) -> ae_mempool::pool::Signed {
    ae_mempool::pool::Signed::decode(&r.signed.as_ref().unwrap().transaction).unwrap()
}
#[test]
fn real_pool_worker_retention_resolves_unknown_submission_without_new_signed_bytes() {
    use crate::pool::worker::{Settings, Worker};
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let r = w
        .begin_payout_submission(e, &r, Default::default())
        .unwrap();
    assert_eq!(w.reconcile_payout(e, &r, Default::default()).unwrap(), None);
    let worker = Worker::spawn(
        db.clone(),
        network(),
        pool_config(),
        Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let client = worker.client();
    let receipt = client
        .push(Arc::new(signed_tx(&r)), Event::Created, false)
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(
        receipt.outcome,
        PushOutcome::Accepted { inserted: true, .. }
    ));
    let submitted = client.info().submitted;
    // Lose only the journal receipt: the real worker has durably retained it.
    drop(w);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
    let observed = w
        .reconcile_payout(e, &r, Default::default())
        .unwrap()
        .unwrap();
    assert!(matches!(
        observed.signed.as_ref().unwrap().dispatch,
        Dispatch::Done {
            outcome: Outcome::Observed {
                location: Location::Pool,
                ..
            },
            ..
        }
    ));
    assert_eq!(
        observed.signed.as_ref().unwrap().transaction,
        r.signed.as_ref().unwrap().transaction
    );
    assert_eq!(
        w.reconcile_payout(e, &observed, Default::default())
            .unwrap(),
        Some(observed.clone())
    );
    assert!(
        w.begin_payout_submission(e, &observed, Default::default())
            .is_err()
    );
    assert_eq!(client.info().submitted, submitted);
    assert_eq!(
        w.confirm_payout(e, &observed, Default::default()).unwrap(),
        Confirmation::Pending
    );
    worker.shutdown().unwrap();
}
#[test]
fn push_receipts_are_exact_and_failed_or_lost_outcomes_do_not_release_the_payment() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let sending = w
        .begin_payout_submission(e, &r, Default::default())
        .unwrap();
    assert_eq!(
        w.begin_payout_submission(e, &r, Default::default())
            .unwrap(),
        sending
    );
    let rejected = w
        .reject_payout_submission(
            e,
            &sending,
            "deliberate native rejection".into(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        w.reject_payout_submission(
            e,
            &sending,
            "deliberate native rejection".into(),
            Default::default()
        )
        .unwrap(),
        rejected
    );
    assert!(
        w.reject_payout_submission(e, &sending, "different reason".into(), Default::default())
            .is_err()
    );
    assert!(
        w.unaccepted_payout_submission(e, &rejected, Default::default())
            .is_err()
    );
    assert_eq!(
        w.prepare_payout(e, 7.into(), Default::default()).unwrap(),
        Selection::Existing(Box::new(rejected.clone()))
    );
    assert_eq!(stored(&db).pending, Some(rejected));
}
#[test]
fn refused_ticket_can_retry_only_the_same_signed_transaction_and_valid_receipt() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let sending = w
        .begin_payout_submission(e, &r, Default::default())
        .unwrap();
    let ready = w
        .unaccepted_payout_submission(e, &sending, Default::default())
        .unwrap();
    let sending = w
        .begin_payout_submission(e, &ready, Default::default())
        .unwrap();
    assert!(matches!(
        sending.signed.as_ref().unwrap().dispatch,
        Dispatch::Sending(2)
    ));
    assert_eq!(
        sending.signed.as_ref().unwrap().transaction,
        r.signed.as_ref().unwrap().transaction
    );
    let mut service = crate::pool::Service::start(db.clone(), network(), pool_config()).unwrap();
    let receipt = service
        .push(Arc::new(signed_tx(&sending)), Event::Created, false)
        .unwrap();
    let mut bad = receipt.clone();
    bad.outcome = PushOutcome::Cached;
    assert!(
        w.record_payout_submission(e, &sending, bad, Default::default())
            .is_err()
    );
    let mut bad = receipt.clone();
    bad.outcome = PushOutcome::Accepted {
        hash: Hash256::ZERO,
        inserted: true,
        event: Event::Created,
    };
    assert!(
        w.record_payout_submission(e, &sending, bad, Default::default())
            .is_err()
    );
    let before = stored(&db);
    let mut future = receipt.clone();
    future.chain.revision += 1;
    assert!(
        w.record_payout_submission(e, &sending, future, Default::default())
            .is_err()
    );
    assert_eq!(stored(&db), before);
    let done = w
        .record_payout_submission(e, &sending, receipt.clone(), Default::default())
        .unwrap();
    assert_eq!(
        w.record_payout_submission(e, &sending, receipt, Default::default())
            .unwrap(),
        done
    );
}
fn reward(w: &mut Writer, include: bool) {
    commit(
        w,
        vec![
            Command::Round,
            Command::Share {
                hash: Hash256(key(11)),
                target: 10.into(),
                miner: b"fixture-share".to_vec(),
            },
        ],
    );
    let round =
        w.db.stratum(Default::default())
            .unwrap()
            .info()
            .next_key
            .unwrap();
    commit(w, vec![Command::Round]);
    if include {
        commit(
            w,
            vec![Command::Reward(core::Reward {
                height: 100,
                hash: Hash256(key(11)),
                pool: core::Shares::Transformed,
                miners: core::Shares::Transformed,
                amount: GROSS.into(),
                round_key: round,
            })],
        );
    }
}
// Authenticated inclusion checks use real transaction bytes, with explicitly
// synthetic block/state metadata. Execution/admission is a separate node gate.
fn include(db: &Database, r: &Record, height: u64) -> Hash256 {
    include_call(db, r, height, |_| {})
}
fn include_call(
    db: &Database,
    r: &Record,
    height: u64,
    edit: impl FnOnce(&mut Option<ae_state::calls::Call>),
) -> Hash256 {
    let v = db.view().unwrap();
    let k = top(&v).unwrap();
    let mut common = k.common.clone();
    common.height = height;
    common.previous_hash = v.status().tip.unwrap().0;
    common.previous_key_hash = Header::Key(k).hash().unwrap().0;
    let tx = signed_tx(r);
    let ae_tx::TransactionLeaf::Contract(t) = tx.transaction().leaf() else {
        panic!()
    };
    let mut call = ae_state::calls::Call::new(
        t.actor(),
        t.nonce().clone(),
        config().contract.0,
        None,
        height.into(),
        t.gas_price().clone(),
    );
    call.gas_used = 200.into();
    call.return_value = vec![0; 32];
    let amount = t.amount().as_bytes();
    call.return_value[32 - amount.len()..].copy_from_slice(amount);
    let mut call = Some(call);
    edit(&mut call);
    let mut trees = v
        .trees(v.status().tip.unwrap(), Default::default())
        .unwrap();
    trees.calls.prune();
    if let Some(call) = call {
        trees.calls.insert(&call).unwrap();
    }
    common.state_hash = trees.roots().hash().0;
    let block = Block::Micro(ae_chain::block::MicroBlock {
        header: ae_codec::headers::MicroHeader {
            common,
            transactions_hash: ae_trie::transaction_root([tx.encode().as_slice()])
                .unwrap()
                .0,
            time: 10,
            proof_of_fraud: None,
            signature: [0; 64],
        },
        transactions: vec![tx.clone()],
        proof: None,
    });
    let h = block.hash().unwrap();
    let mut rec = v.require_record(v.status().tip.unwrap()).unwrap();
    rec.roots = Roots::of(&trees);
    let mut b = WriteBatch::default();
    for (h, raw) in trees.calls.tree().store().pending() {
        b.put(repo::key(repo::NODES, h.0), raw);
    }
    b.put(repo::key(repo::BLOCKS, h.0), block.encode().unwrap());
    b.put(repo::key(repo::RECORDS, h.0), rec.encode());
    b.put(repo::key(repo::CANONICAL, h.0), []);
    b.put(repo::key(repo::LOCATIONS, tx.hash().0), h.0);
    b.put(
        repo::key(repo::TRANSACTIONS, tx.hash().0),
        ae_tx::storage::encode(&tx, v.config().transaction_record()).unwrap(),
    );
    write(db, b).unwrap();
    h
}
#[test]
fn confirmation_checks_canonical_inclusion_depth_and_atomically_prunes_only_completed_reward() {
    for partial in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        reward(&mut w, true);
        if partial {
            commit(&mut w, vec![Command::Payment(pay((100, 1)))]);
        }
        let r = prepared(&mut w, e);
        let r = signed(&mut w, e, &r);
        let r = w
            .begin_payout_submission(e, &r, Default::default())
            .unwrap();
        let block = include(&db, &r, 100);
        set_top(&db, 279, Protocol::Roma, Some(GROSS + 1), 0);
        assert_eq!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Pending
        );
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let before = db.stratum(Default::default()).unwrap();
        let Confirmation::Confirmed(c) = w.confirm_payout(e, &r, Default::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(c.block, block);
        assert_eq!(c.pruned, !partial);
        assert_eq!(
            before.payouts(Default::default()).unwrap().unwrap().pending,
            Some(r.clone())
        );
        drop(before);
        assert_eq!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Confirmed(c)
        );
        assert!(stored(&db).pending.is_none());
        let counts = db.stratum(Default::default()).unwrap().info().counts;
        assert_eq!(counts, if partial { [1, 1, 2, 1, 1, 0] } else { [0; 6] });
        drop(w);
        drop(db);
        let db = database(&t);
        assert!(stored(&db).pending.is_none());
    }
}
#[test]
fn missing_reward_and_inconsistent_or_detached_locations_preserve_pending_evidence() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    reward(&mut w, false);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let block = include(&db, &r, 100);
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::MissingReward
    );
    assert_eq!(stored(&db).pending, Some(r.clone()));
    db.db.delete(repo::key(repo::CANONICAL, block.0)).unwrap();
    assert!(w.confirm_payout(e, &r, Default::default()).is_err());
    assert_eq!(stored(&db).pending, Some(r.clone()));
    db.db
        .delete(repo::key(repo::LOCATIONS, r.hash().unwrap().unwrap().0))
        .unwrap();
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    db.db
        .put(
            repo::key(repo::LOCATIONS, r.hash().unwrap().unwrap().0),
            db.view().unwrap().identity().genesis.0,
        )
        .unwrap();
    assert!(w.confirm_payout(e, &r, Default::default()).is_err());
    assert_eq!(stored(&db).pending, Some(r));
}
#[test]
fn legacy_sent_records_restore_oldest_without_manufacturing_a_signing_request() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let mut p = pay((100, 1));
    p.tx_hash = Some(Hash256(key(44)));
    let mut later = p.clone();
    later.id = (101, 0);
    later.tx_hash = Some(Hash256(key(45)));
    commit(
        &mut w,
        vec![
            Command::Payment(later),
            Command::Payment(pay((99, 0))),
            Command::Payment(p.clone()),
        ],
    );
    let i = w.begin_payouts(config(), Default::default()).unwrap();
    let r = i.pending.clone().unwrap();
    assert_eq!(r.payment, p);
    assert!(r.request.is_none() && r.signed.is_none());
    assert_eq!(
        w.prepare_payout(i.epoch, 1.into(), Default::default())
            .unwrap(),
        Selection::Existing(Box::new(r.clone()))
    );
    assert!(
        w.sign_payout(
            i.epoch,
            &r,
            Signature([0; 64]),
            core::Date([2026, 1, 1, 0, 0, 0]),
            Default::default()
        )
        .is_err()
    );
    assert_eq!(stored(&db), i);
}
mod recovery;
fn setup_stage(db: &Arc<Database>, at: Stage) {
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(&mut w, vec![Command::Payment(pay((100, 0)))]);
    if at == Stage::Initialize {
        return;
    }
    let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
    if matches!(at, Stage::Context | Stage::Prepare) {
        return;
    }
    let r = prepared(&mut w, e);
    if at == Stage::Sign {
        return;
    }
    let r = signed(&mut w, e, &r);
    if at == Stage::Submit {
        return;
    }
    if at == Stage::Confirm {
        reward(&mut w, true);
        include(db, &r, 100);
        set_top(db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        return;
    }
    let r = w
        .begin_payout_submission(e, &r, Default::default())
        .unwrap();
    if at == Stage::Observe {
        let mut service =
            crate::pool::Service::start(db.clone(), network(), pool_config()).unwrap();
        service
            .push(Arc::new(signed_tx(&r)), Event::Created, false)
            .unwrap();
    }
}
fn perform_stage(
    w: &mut Writer,
    db: &Database,
    at: Stage,
    write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
) -> Result<(), Failure> {
    if at == Stage::Initialize {
        w.begin_payouts_with(config(), Default::default(), write)?;
        return Ok(());
    }
    let e = w.begin_payouts(config(), Default::default())?.epoch;
    if at == Stage::Context {
        set_top(db, 281, Protocol::Ceres, Some(GROSS + 2), 9);
        w.refresh_payout_context_with(e, Default::default(), write)?;
        return Ok(());
    }
    if at == Stage::Prepare {
        w.prepare_payout_with(e, 0.into(), Default::default(), write)?;
        return Ok(());
    }
    let r = stored(db).pending.unwrap();
    match at {
        Stage::Sign => {
            w.sign_payout_with(
                e,
                &r,
                signer().sign(r.request.as_ref().unwrap().prepared.signing_bytes()),
                core::Date([2026, 1, 1, 0, 0, 0]),
                Default::default(),
                write,
            )?;
        }
        Stage::Submit => {
            w.begin_payout_submission_with(e, &r, Default::default(), write)?;
        }
        Stage::Receipt => {
            w.finish_payout_with(
                e,
                &r,
                Outcome::Rejected("captured rejection".into()),
                Default::default(),
                write,
            )?;
        }
        Stage::Observe => {
            w.reconcile_payout_with(e, &r, Default::default(), write)?;
        }
        Stage::Confirm => {
            w.confirm_payout_with(e, &r, Default::default(), write)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}
fn finish_stage(w: &mut Writer, db: &Database, at: Stage) {
    let i = w.begin_payouts(config(), Default::default()).unwrap();
    let e = i.epoch;
    if at == Stage::Initialize {
        return;
    }
    if at == Stage::Context {
        w.refresh_payout_context(e, Default::default()).unwrap();
        return;
    }
    if at == Stage::Confirm && i.pending.is_none() {
        assert!(i.last.is_some());
        return;
    }
    let mut r = match i.pending {
        Some(r) => r,
        None => prepared(w, e),
    };
    if at == Stage::Prepare {
        return;
    }
    if r.signed.is_none() {
        r = signed(w, e, &r);
    }
    if at == Stage::Sign {
        return;
    }
    if at == Stage::Confirm {
        assert!(matches!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Confirmed(_)
        ));
        return;
    }
    if matches!(r.signed.as_ref().unwrap().dispatch, Dispatch::Ready(_)) {
        r = w
            .begin_payout_submission(e, &r, Default::default())
            .unwrap();
    }
    if at == Stage::Submit {
        return;
    }
    if matches!(r.signed.as_ref().unwrap().dispatch, Dispatch::Sending(_)) {
        if at == Stage::Observe {
            w.reconcile_payout(e, &r, Default::default())
                .unwrap()
                .unwrap();
        } else {
            w.reject_payout_submission(e, &r, "captured rejection".into(), Default::default())
                .unwrap();
        }
    }
    assert_eq!(
        stored(db).pending.as_ref().unwrap().hash().unwrap(),
        r.hash().unwrap()
    );
}
#[test]
fn ambiguous_writes_recover_every_payout_boundary_without_recreating_external_requests() {
    for at in [
        Stage::Initialize,
        Stage::Context,
        Stage::Prepare,
        Stage::Sign,
        Stage::Submit,
        Stage::Receipt,
        Stage::Observe,
        Stage::Confirm,
    ] {
        for applied in [false, true] {
            let t = Temp::new();
            let db = database(&t);
            setup_stage(&db, at);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let mut reached = false;
            assert!(
                perform_stage(&mut w, &db, at, |stage, db, b| {
                    assert_eq!(stage, at);
                    reached = true;
                    if applied {
                        write(db, b)?;
                    }
                    Err(Error::Backend("injected lost WAL acknowledgement".into()))
                })
                .is_err()
            );
            assert!(reached && w.recovery_required());
            assert_eq!(w.version(), Err(Failure::RecoveryRequired));
            let recovery = w.recover().unwrap();
            assert_eq!(matches!(recovery, Recovery::Applied(_)), applied);
            assert!(!w.recovery_required());
            finish_stage(&mut w, &db, at);
            let before = stored(&db);
            finish_stage(&mut w, &db, at);
            assert_eq!(stored(&db), before);
            drop(w);
            drop(db);
            let db = database(&t);
            assert_eq!(stored(&db), before);
        }
    }
}
#[test]
fn schema_upgrade_preserves_bindings_and_rejects_corrupt_or_oversized_pending_intents() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let info = stored(&db);
    let binding = db.view().unwrap().identity().binding();
    let raw = info.encode(Default::default(), Default::default()).unwrap();
    assert!(
        db.stratum(Default::default())
            .unwrap()
            .payouts(Limits {
                bytes: 1024,
                ..Default::default()
            })
            .is_err()
    );
    assert_eq!(stored(&db), info);
    let mut wrong = r.clone();
    wrong.signed.as_mut().unwrap().transaction[0] ^= 1;
    assert!(
        wrong
            .encode(Default::default(), Default::default())
            .is_err()
    );
    let mut wrong = r.clone();
    wrong.payment.total += 1;
    assert!(
        wrong
            .encode(Default::default(), Default::default())
            .is_err()
    );
    let mut wrong = info.clone();
    wrong.network.push(1);
    assert!(
        wrong
            .encode(Default::default(), Default::default())
            .is_err()
    );
    let mut wrong = info.clone();
    wrong.next += 1;
    assert!(
        wrong
            .encode(Default::default(), Default::default())
            .is_err()
    );
    let mut identity = Value::decode(&identity().encode()).unwrap();
    let Value::List(fields) = &mut identity else {
        panic!()
    };
    fields[0] = number(18);
    let old = identity.encode();
    db.db.put(repo::IDENTITY, &old).unwrap();
    drop(w);
    drop(db);
    let db = database(&t);
    assert_eq!(stored(&db), info);
    assert_eq!(db.view().unwrap().identity().binding(), binding);
    assert_eq!(
        db.db.get(repo::IDENTITY).unwrap().unwrap(),
        super::tests::identity().encode()
    );
    db.db.put(META, b"corrupt").unwrap();
    db.db.put(repo::IDENTITY, &old).unwrap();
    drop(db);
    assert!(Database::open(&t.0, super::tests::identity()).is_err());
    let rawdb = rocksdb::DB::open_default(&t.0).unwrap();
    assert_eq!(rawdb.get(repo::IDENTITY).unwrap(), Some(old));
    rawdb.put(META, &raw).unwrap();
    drop(rawdb);
    let db = database(&t);
    assert_eq!(stored(&db), info);
}

mod reference;

mod rounds;

mod execution;

mod gas;
