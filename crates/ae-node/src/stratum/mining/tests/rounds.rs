use super::*;
use ae_stratum::{accounting as core, proof::Number};
use storage::rounds::{Config, Outcome};
fn configured(importer: &chain::Importer) -> Inputs {
    let mut inputs = inputs(importer);
    inputs.rounds = Some(super::super::rounds::Inputs::new(
        Config::from_percentages(
            [(b"pool".to_vec(), Number::Float(1.5f64.to_bits()))].into(),
            2,
            0,
            Default::default(),
        )
        .unwrap(),
    ));
    inputs
}
fn owner(db: &Arc<Database>, importer: &chain::Importer, limits: storage::Limits) -> Accounting {
    Accounting::start_mining(
        db.clone(),
        limits,
        Default::default(),
        Default::default(),
        configured(importer),
    )
    .unwrap()
}
fn submit_first(owner: &Accounting) -> Id {
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let h = first();
    let c = candidate(&h);
    assert!(publish(&client, c.clone()).published);
    let mut share = input(&c, &h);
    share.account = "ak_miner-a".into();
    share.target = 10.into();
    share.solution = None;
    accepted(&accounting, &share);
    let mut solution = input(&c, &h);
    solution.account = "ak_miner-b".into();
    solution.target = 20.into();
    accepted(&accounting, &solution)
}
fn drained(db: &Database, client: &Client, height: u64) -> storage::rounds::Info {
    until(|| {
        client.rounds().unwrap().is_some_and(|info| {
            info.height() == height
                && info.pending.is_empty()
                && info.cursor == db.view().unwrap().status().revision
        })
    });
    client.rounds().unwrap().unwrap()
}
fn check_first_reward(db: &Database) {
    let stored = db.stratum(Default::default()).unwrap();
    let reward = stored.query(|tx| tx.reward(1)).unwrap().unwrap();
    assert_eq!(reward.hash, Header::Key(first()).hash().unwrap());
    assert_eq!(reward.amount.to_string(), "1000000000000000000");
    assert_eq!(reward.pool, core::Shares::Transformed);
    assert_eq!(reward.miners, core::Shares::Transformed);
    let payments = stored.query(|tx| tx.payments(1)).unwrap();
    assert_eq!(payments.len(), 2);
    assert_eq!(payments[0].total.to_string(), "15000000000000000");
    assert_eq!(payments[1].total.to_string(), "985000000000000000");
    assert_eq!(
        payments[1].relative,
        [
            (b"ak_miner-a".to_vec(), Number::Float(0x3fd5555555555555)),
            (b"ak_miner-b".to_vec(), Number::Float(0x3fe5555555555555)),
        ]
        .into()
    );
    assert!(
        payments
            .iter()
            .all(|p| p.tx_hash.is_none() && p.absolute.is_none())
    );
    assert_eq!(stored.info().counts[1..5], [2, 2, 1, 2]);
}
#[test]
fn saved_mainnet_solution_creates_reward_and_micro_top_does_not_create_a_round() {
    let temp = Temp::new();
    let db = open(&temp);
    let importer = importer(&db, 1);
    let owner = owner(&db, &importer, Default::default());
    let client = owner.client().mining().unwrap();
    let cursor = db.events().cursor();
    let id = submit_first(&owner);
    let record = done(&client, id);
    assert!(matches!(
        record.state,
        journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Committed(_)))
    ));
    let info = drained(&db, &client, 1);
    assert!(matches!(
        info.last.as_ref().unwrap().outcome,
        Outcome::Reward { .. }
    ));
    check_first_reward(&db);
    let hash = Header::Key(first()).hash().unwrap();
    assert_eq!(
        db.view()
            .unwrap()
            .require_record(hash)
            .unwrap()
            .roots
            .state
            .hash()
            .0,
        first().common.state_hash
    );
    assert_eq!(
        db.events().after(&cursor, 8).unwrap().records[0]
            .context()
            .origin,
        Origin::Created
    );
    let micro = fixture::blocks(&NetworkProfile::mainnet()).remove(1);
    assert!(matches!(micro, Block::Micro(_)));
    settle(
        &mut importer
            .import_created_block(micro.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    let after = drained(&db, &client, 1);
    assert!(after.cursor > info.cursor);
    assert_eq!(after.next, info.next);
    assert_eq!(after.last, info.last);
    check_first_reward(&db);
    assert!(owner.shutdown().failure.is_none());
    stop(importer);
}
#[test]
fn reopen_replays_durable_canonical_history_and_rewards_owned_external_blocks() {
    let temp = Temp::new();
    let db = open(&temp);
    let importer = importer(&db, 1);
    let owner = owner(&db, &importer, Default::default());
    let client = owner.client().mining().unwrap();
    done(&client, submit_first(&owner));
    drained(&db, &client, 1);
    assert!(owner.shutdown().failure.is_none());
    drop(client);
    let blocks = fixture::blocks(&NetworkProfile::mainnet());
    let hash = blocks[2].hash().unwrap();
    // The native ownership test uses the current hash index, irrespective of
    // whether this actor submitted the canonical block itself.
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    writer
        .commit(
            writer.version().unwrap(),
            vec![storage::Command::Share {
                hash,
                target: 30.into(),
                miner: b"ak_miner-c".to_vec(),
            }],
        )
        .unwrap();
    drop(writer);
    for block in &blocks[1..3] {
        settle(
            &mut importer
                .import_created_block(block.encode().unwrap(), WALL)
                .unwrap(),
        )
        .unwrap();
    }
    let status = db.view().unwrap().status();
    stop(importer);
    drop(db);
    let db = open(&temp);
    assert_eq!(db.events().info().records, 0);
    let importer = self::importer(&db, 1);
    let owner = self::owner(&db, &importer, Default::default());
    let client = owner.client().mining().unwrap();
    let info = drained(&db, &client, 2);
    assert_eq!(info.cursor, status.revision);
    assert!(matches!(info.last.unwrap().outcome, Outcome::Reward { hash: h, .. } if h == hash));
    let stored = db.stratum(Default::default()).unwrap();
    assert_eq!(stored.info().counts[1..5], [3, 3, 2, 4]);
    assert_eq!(stored.query(|tx| tx.reward(2)).unwrap().unwrap().hash, hash);
    let before = stored.info();
    drop(stored);
    assert!(owner.shutdown().failure.is_none());
    let owner = self::owner(&db, &importer, Default::default());
    drained(&db, &owner.client().mining().unwrap(), 2);
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().counts,
        before.counts
    );
    assert_eq!(db.view().unwrap().status(), status);
    assert!(owner.shutdown().failure.is_none());
    stop(importer);
}
#[test]
fn resource_failure_retains_computation_and_restart_finishes_before_opening_ingress() {
    let temp = Temp::new();
    let db = open(&temp);
    let importer = importer(&db, 1);
    let mut limits = storage::Limits::default();
    limits.transaction.maps.entries = 1;
    let owner = owner(&db, &importer, limits);
    let account = owner.client();
    submit_first(&owner);
    until(|| account.is_closed());
    assert!(matches!(owner.shutdown().failure, Some(Error::Storage(_))));
    let stored = db.stratum(Default::default()).unwrap();
    let pending = stored.rounds(Default::default()).unwrap().unwrap();
    assert!(pending.computing());
    assert_eq!(stored.info().counts[2..5], [2, 0, 0]);
    let epoch = stored.ingress(Default::default()).unwrap().epoch();
    drop(stored);
    assert!(
        Accounting::start_mining(
            db.clone(),
            limits,
            Default::default(),
            Default::default(),
            configured(&importer)
        )
        .is_err()
    );
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .epoch(),
        epoch
    );
    let next = self::owner(&db, &importer, Default::default());
    // start_mining resumes Compute synchronously, before allocating a new epoch.
    check_first_reward(&db);
    let finished = db
        .stratum(Default::default())
        .unwrap()
        .rounds(Default::default())
        .unwrap()
        .unwrap();
    assert!(!finished.computing());
    assert_eq!(finished.next, pending.next + 1);
    assert!(next.shutdown().failure.is_none());
    stop(importer);
}
