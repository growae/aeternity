use super::*;
use ae_mempool::pool::accounts::{Limits, Strategy};
use ae_tx::{SignedTransaction, Spend, Transaction, TransactionLeaf};
use ae_types::{Id, IdTag};

fn transaction(
    owner: ae_types::AccountPublicKey,
    nonce: u64,
) -> Arc<SignedTransaction<Transaction>> {
    let mut tx = Spend::new(owner, Id::new(IdTag::Account, [9; 32])).unwrap();
    tx.set_nonce(nonce.into());
    tx.set_fee(1_000_000_000_000u64.into());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Spend(tx)),
        vec![],
    ))
}
#[test]
fn account_pool_queries_are_complete_ordered_and_read_only_across_both_tables() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let signed = tx();
    let owner = signed.transaction().view().origin().unwrap();
    client
        .push(signed.clone(), Event::Created, false)
        .unwrap()
        .wait()
        .unwrap();
    client
        .candidate(1_000_000, BTreeSet::new(), Default::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(client.info().pool.visited, 1);
    for nonce in [5, 3, 0] {
        client
            .force_push(transaction(owner, nonce))
            .unwrap()
            .wait()
            .unwrap();
    }
    let revision = db.pool_view().unwrap().revision;
    let list = client
        .pending_account(owner, Limits::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        list.value
            .iter()
            .map(|t| t.transaction().view().nonce().to_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 3, 5]
    );
    assert_eq!(
        client
            .account_nonce(owner, 0.into(), Strategy::Max, Limits::default())
            .unwrap()
            .wait()
            .unwrap()
            .value,
        6.into()
    );
    assert_eq!(
        client
            .account_nonce(owner, 0.into(), Strategy::Continuity, Limits::default())
            .unwrap()
            .wait()
            .unwrap()
            .value,
        2.into()
    );
    assert_eq!(db.pool_view().unwrap().revision, revision);
    for limits in [
        Limits {
            items: 3,
            ..Default::default()
        },
        Limits {
            bytes: 1,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client.pending_account(owner, limits).unwrap().wait(),
            Err(Error::Service(crate::pool::Failure::Policy(
                policy::Error::ResourceLimit
            )))
        ));
    }
    assert!(matches!(
        client
            .account_nonce(
                owner,
                0.into(),
                Strategy::Continuity,
                Limits {
                    items: 3,
                    ..Default::default()
                }
            )
            .unwrap()
            .wait(),
        Err(Error::Service(crate::pool::Failure::Policy(
            policy::Error::ResourceLimit
        )))
    ));
    drop(list);
    assert_eq!(client.account_query_usage(), (0, 0));
    worker.shutdown().unwrap();
}

#[test]
fn account_response_credits_survive_receipt_retirement_and_bound_queued_or_abandoned_work() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = Worker::spawn(
        db.clone(),
        NetworkProfile::mainnet(),
        config(),
        Settings {
            requests: 2,
            ..settings()
        },
    )
    .unwrap();
    let client = worker.client();
    until(&client, |i| i.background_runs > 0 && !i.in_flight);
    let owner = tx().transaction().view().origin().unwrap();
    let one = client
        .pending_account(owner, Limits::default())
        .unwrap()
        .wait()
        .unwrap();
    let two = client
        .pending_account(owner, Limits::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(client.account_query_usage().0, 2);
    assert!(matches!(
        client.pending_account(owner, Limits::default()),
        Err(Error::Saturated)
    ));
    drop(two);
    assert_eq!(client.account_query_usage().0, 1);
    let (release, gated) = gate(&client);
    let pending = client.pending_account(owner, Limits::default()).unwrap();
    assert!(matches!(
        pending.wait_timeout(Duration::from_millis(1)),
        Err(Error::Timeout)
    ));
    drop(pending);
    assert_eq!(client.account_query_usage().0, 2);
    let done = client.info().completed;
    release.send(()).unwrap();
    gated.wait().unwrap();
    until(&client, |i| i.completed >= done + 2);
    // The abandoned ticket eventually releases its reply and reservation.
    for _ in 0..100 {
        if client.account_query_usage().0 == 1 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(client.account_query_usage().0, 1);
    worker.shutdown().unwrap();
    assert_eq!(client.account_query_usage().0, 1);
    assert!(matches!(
        client.pending_account(owner, Limits::default()),
        Err(Error::Stopped)
    ));
    let replacement = spawn(db);
    let current = replacement.client();
    assert!(!client.same_worker(&current));
    assert_eq!(current.account_query_usage(), (0, 0));
    let next = current
        .pending_account(owner, Limits::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(current.account_query_usage().0, 1);
    drop(one);
    assert_eq!(client.account_query_usage(), (0, 0));
    assert_eq!(current.account_query_usage().0, 1);
    drop(next);
    replacement.shutdown().unwrap();
}
