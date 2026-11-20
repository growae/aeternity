use super::*;
use crate::pool::tests::{Temp, blocks, config, database, prepare, tx};
use ae_mempool::pool::Event;
use ae_protocol::NetworkProfile;

#[test]
fn sync_snapshot_uses_durable_membership_and_get_preserves_retained_records() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service.push(t.clone(), Event::Created, false).unwrap();
    service
        .candidate(1_000_000, &BTreeSet::new(), Default::default(), || 0)
        .unwrap();
    service.force_push(t.clone()).unwrap();
    assert_eq!(
        (service.inspection().active, service.inspection().visited),
        (1, 1)
    );
    let view = db.pool_view().unwrap();
    let snapshot = view.sync_snapshot(Default::default()).unwrap();
    assert_eq!(snapshot.tree.len(), 1);
    assert_eq!(snapshot.revision, view.revision());
    assert_eq!(snapshot.chain, view.chain().status());
    let hashes = vec![
        vec![],
        vec![1],
        vec![0; 32],
        t.hash().0.to_vec(),
        t.hash().0.to_vec(),
    ];
    assert_eq!(
        view.sync_get(&hashes, Default::default()).unwrap(),
        vec![t.clone(), t.clone()]
    );
    service.delete(t.hash()).unwrap();
    assert!(
        db.pool_view()
            .unwrap()
            .sync_snapshot(Default::default())
            .unwrap()
            .tree
            .is_empty()
    );
    assert_eq!(
        db.pool_view()
            .unwrap()
            .sync_get(&hashes, Default::default())
            .unwrap(),
        vec![t.clone(), t.clone()]
    );
    assert_eq!(
        view.sync_snapshot(Default::default()).unwrap().tree.root(),
        snapshot.tree.root()
    );
    for block in blocks().iter().take(2) {
        db.commit(prepare(&db, block)).unwrap();
    }
    service.force_push(t.clone()).unwrap();
    assert_eq!(service.inspection().active, 1);
    let canonical = db.pool_view().unwrap();
    assert!(
        canonical
            .sync_snapshot(Default::default())
            .unwrap()
            .tree
            .is_empty()
    );
    assert_eq!(
        canonical.sync_get(&hashes, Default::default()).unwrap(),
        vec![t.clone(), t.clone()]
    );
    assert!(matches!(
        canonical.location(t.hash()).unwrap(),
        Location::Block(_)
    ));
}

#[test]
fn sync_queries_fail_atomically_at_limits_and_keep_the_pinned_view() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service.push(t.clone(), Event::Created, false).unwrap();
    let view = db.pool_view().unwrap();
    assert!(matches!(
        view.sync_snapshot(protocol::Limits {
            entries: 0,
            ..Default::default()
        }),
        Err(Failure::Repository(Error::ResourceLimit))
    ));
    assert!(matches!(
        view.sync_snapshot(protocol::Limits {
            tree_nodes: 0,
            ..Default::default()
        }),
        Err(Failure::Protocol(protocol::Error::ResourceLimit))
    ));
    let hashes = vec![t.hash().0.to_vec(), t.hash().0.to_vec()];
    let bytes = t.encode().len();
    assert_eq!(
        view.sync_get(
            &hashes,
            GetLimits {
                requests: 1,
                wire_bytes: usize::MAX
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        view.sync_get(
            &hashes,
            GetLimits {
                requests: 2,
                wire_bytes: bytes * 2 - 1
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        view.sync_get(
            &hashes,
            GetLimits {
                requests: 2,
                wire_bytes: bytes * 2
            }
        )
        .unwrap(),
        vec![t.clone(), t]
    );
    assert_eq!(
        view.sync_snapshot(Default::default()).unwrap().tree.len(),
        1
    );
}
