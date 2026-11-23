use super::*;

#[tokio::test]
async fn live_pinning_and_replacement_factories_use_restored_pool_generations() {
    for index in [0, 2, 4] {
        for restart_agent in [false, true] {
            let node = Node::new(index);
            let wallet = Wallet::new();
            let mut c = configured(&node, &wallet, |_| {});
            prefix(&node, &mut c, &mut vec![], 5).await;
            let old_pool = c.pool();
            let old_agent = c.pinning().unwrap();
            c.request_pool_restart(1).unwrap();
            if restart_agent {
                old_agent.request_stop();
            }
            super::super::pool_supervision::replaced(&mut c, 2);
            let mut reports = Vec::new();
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
            let children = children(&reports);
            assert_eq!(children.len(), 2);
            let records = node
                .db
                .pinning_children()
                .unwrap()
                .records(Default::default())
                .unwrap();
            assert_eq!(records.len(), 2);
            assert!(records.iter().all(|r| r.receipt.is_some()));
            assert!(old_pool.info().joined && c.pool().info().submitted > 0);
            assert_eq!(c.status().consensus.attempts, u64::from(restart_agent));
            assert_eq!(c.status().pool.restart_attempts, 1);
            assert_eq!(wallet.posted.lock().unwrap().len(), 1);
            assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
            assert!(shutdown(c).pool.unwrap().joined);
        }
    }
}
