use super::*;
use ae_storage::pinning_children as journal;
async fn originals() -> Vec<journal::Record> {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |_| {});
    prefix(&node, &mut c, &mut vec![], 9).await;
    let records = node
        .db
        .pinning_children()
        .unwrap()
        .records(Default::default())
        .unwrap();
    assert_eq!(records.len(), 2);
    shutdown(c);
    records
}
fn records(node: &Node) -> Vec<journal::Record> {
    node.db
        .pinning_children()
        .unwrap()
        .records(Default::default())
        .unwrap()
}
pub(super) async fn import_until_failure(node: &Node, c: &mut Conductor<Otp26>) -> pinning::Error {
    for block in node.blocks().into_iter().skip(5).take(4) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        let revision = node.db.view().unwrap().status().revision;
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            let status = c.pinning().unwrap().status();
            if let Some(error) = status.failure {
                return error;
            }
            if status.cursor.revision() >= revision {
                break;
            }
            assert!(Instant::now() < end, "{status:?}");
            thread::sleep(Duration::from_millis(1));
        }
    }
    panic!("expected durable child failure")
}
#[tokio::test]
async fn retained_child_receipts_and_exact_pool_observations_avoid_fresh_signing_or_admission() {
    let seed = originals().await;
    for mode in ["submitted", "rejected", "observed", "missing"] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut writer = journal::Writer::start(node.db.clone(), Default::default()).unwrap();
        for original in &seed {
            let mut record = original.clone();
            record.receipt = None;
            writer.reserve(record.clone()).unwrap();
            match mode {
                "submitted" => {
                    writer
                        .record_receipt(&record, original.receipt.clone().unwrap())
                        .unwrap();
                }
                "rejected" => {
                    writer
                        .record_receipt(
                            &record,
                            journal::Receipt::Rejected("retained policy rejection".into()),
                        )
                        .unwrap();
                }
                _ => (),
            }
        }
        drop(writer);
        let mut c = configured(&node, &wallet, |_| {});
        let mut reports = vec![];
        prefix(&node, &mut c, &mut reports, 5).await;
        if mode == "observed" {
            // In-order original signed operations were admitted, but the agent
            // lost both replies. A different nonce would create a third spend.
            let mut ordered = seed.iter().map(|r| r.signed().unwrap()).collect::<Vec<_>>();
            ordered.sort_by_key(|tx| tx.transaction().view().nonce());
            for tx in ordered {
                c.pool()
                    .push(Arc::new(tx), ae_mempool::pool::Event::Received, false)
                    .unwrap()
                    .wait()
                    .unwrap();
            }
        }
        if mode == "missing" {
            assert!(matches!(
                import_until_failure(&node, &mut c).await,
                pinning::Error::ChildUnresolved(_)
            ));
            assert!(
                c.pool()
                    .peek(100, None, None)
                    .unwrap()
                    .wait()
                    .unwrap()
                    .is_empty()
            );
            assert!(records(&node).iter().all(|r| r.receipt.is_none()));
            let exit = shutdown(c).pinning.take().unwrap().unwrap();
            assert!(
                exit.outputs
                    .iter()
                    .chain(exit.retained.iter())
                    .flat_map(|r| &r.effects)
                    .any(|e| matches!(
                        e,
                        pinning::Effect::Child {
                            result: pinning::ChildResult::Unresolved,
                            ..
                        }
                    ))
            );
        } else {
            for block in node.blocks().into_iter().skip(5).take(4) {
                within(
                    c.importer()
                        .import_block(block.encode().unwrap(), 1_000_000)
                        .unwrap()
                        .wait(),
                )
                .await
                .unwrap();
                settled(
                    &mut c,
                    &mut reports,
                    node.db.view().unwrap().status().revision,
                );
            }
            let recovered = reports
                .iter()
                .flat_map(|r| &r.effects)
                .filter_map(|e| match e {
                    pinning::Effect::Child {
                        transaction,
                        result: pinning::ChildResult::Recovered(receipt),
                        ..
                    } => Some((transaction, receipt)),
                    pinning::Effect::Child { .. } => panic!("fresh child admission on recovery"),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(recovered.len(), 2);
            for (tx, receipt) in recovered {
                assert!(seed.iter().any(|r| r.transaction == tx.encode()));
                assert!(match mode {
                    "submitted" => matches!(receipt, journal::Receipt::Submitted(_)),
                    "rejected" => matches!(receipt, journal::Receipt::Rejected(_)),
                    _ => matches!(
                        receipt,
                        journal::Receipt::Observed {
                            location: ae_mempool::pool::Location::Pool,
                            ..
                        }
                    ),
                });
            }
            let pool = c.pool().peek(100, None, None).unwrap().wait().unwrap();
            assert_eq!(pool.len(), if mode == "observed" { 2 } else { 0 });
            assert_eq!(records(&node).len(), 2);
            shutdown(c);
        }
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn journal_quotas_stop_child_admission_without_dropping_prior_intents() {
    for full in [false, true] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |cfg| {
            let limits = &mut cfg
                .parent
                .pins
                .as_mut()
                .unwrap()
                .agent
                .as_mut()
                .unwrap()
                .children;
            if full {
                limits.records = 1;
            } else {
                limits.bytes = 1;
            }
        });
        prefix(&node, &mut c, &mut vec![], 5).await;
        assert!(matches!(
            import_until_failure(&node, &mut c).await,
            pinning::Error::Children(journal::Failure::Storage(
                ae_chain::repository::Error::ResourceLimit
            ))
        ));
        let kept = records(&node);
        assert_eq!(kept.len(), usize::from(full));
        let pool = c.pool().peek(100, None, None).unwrap().wait().unwrap();
        assert_eq!(pool.len(), usize::from(full));
        if full {
            assert_eq!(pool[0].encode(), kept[0].transaction);
            assert!(matches!(
                kept[0].receipt,
                Some(journal::Receipt::Submitted(_))
            ));
        }
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        shutdown(c);
    }
}

#[path = "children/recovery.rs"]
mod recovery;
