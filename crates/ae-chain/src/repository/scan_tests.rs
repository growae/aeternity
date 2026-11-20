use super::*;
use ae_trie::{Nibbles, Trie, Value, scan::PageLimits};

#[test]
fn snapshot_scan_covers_all_eight_roots_and_reports_missing_cache_nodes() {
    let mut db = Memory::default();
    let mut nodes = BTreeMap::new();
    let roots: Vec<_> = (1..=8)
        .map(|n| {
            let mut tree = Trie::default();
            tree.put(&Nibbles::from_bytes(&[n]), Value::Bytes(vec![n; 64]))
                .unwrap();
            nodes.extend(tree.store().nodes().iter().map(|(h, b)| (*h, b.clone())));
            tree.root_hash().unwrap()
        })
        .collect();
    let roots = Roots {
        state: StateRoots {
            accounts: Some(roots[0]),
            calls: Some(roots[1]),
            channels: Some(roots[2]),
            contracts: Some(roots[3]),
            names: Some(roots[4]),
            oracles: Some(roots[5]),
        },
        names: Some(roots[6]),
        oracles: Some(roots[7]),
    };
    // This synthetic admitted header tests storage binding, not PoW validity.
    let Header::Key(mut header) =
        ae_state::genesis::header(ae_state::genesis::Network::Uat).unwrap()
    else {
        panic!()
    };
    header.common.state_hash = roots.state.hash().0;
    let block = Block::Key(header);
    let hash = block.hash().unwrap();
    let identity = Identity::new(&NetworkProfile::uat(), hash, Hash256::ZERO);
    let record = Record {
        roots: roots.clone(),
        difficulty: 0.into(),
        fees: 0.into(),
        fraud: false,
        fork: hash,
    };
    let mut writes = Writes::new();
    writes.insert(IDENTITY.to_vec(), Some(identity.encode()));
    writes.insert(
        STATUS.to_vec(),
        Some(
            Status {
                revision: 1,
                tip: Some(hash),
            }
            .encode(),
        ),
    );
    for (h, b) in &nodes {
        writes.insert(key(NODES, h.0), Some(b.clone()));
    }
    store_block(&mut writes, &block, &record, &ReadConfig::default()).unwrap();
    db.apply(writes);
    let view = View::load(db.clone()).unwrap();
    let mut scan = view.scan_state(hash, Default::default()).unwrap();
    assert_eq!(scan.block(), hash);
    assert_eq!(scan.revision(), 1);
    assert_eq!(scan.progress().pending, 8);
    assert!(
        view.scan_state(
            hash,
            ae_trie::scan::Limits {
                pending: 7,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(matches!(
        view.scan_state(Hash256::ZERO, Default::default()),
        Err(Error::Missing(_))
    ));
    let first = scan
        .next_page(PageLimits {
            nodes: 2,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.len(), 2);
    let cache = roots.oracles.unwrap();
    db.apply(BTreeMap::from([(key(NODES, cache.0), None)]));
    let mut broken = View::load(db.clone())
        .unwrap()
        .scan_state(hash, Default::default())
        .unwrap();
    let before = broken.progress();
    assert!(
        matches!(broken.next_page(PageLimits::default()), Err(Error::Trie(ae_trie::Error::MissingNode(h))) if h == cache)
    );
    assert_eq!(broken.progress(), before);
    // The earlier scan owns an immutable view, including the now-missing cache.
    let mut actual = first;
    actual.extend(scan.next_page(PageLimits::default()).unwrap());
    assert_eq!(
        actual.iter().map(|n| n.hash).collect::<Vec<_>>(),
        roots.array().into_iter().flatten().collect::<Vec<_>>()
    );
    assert!(scan.is_complete());
    assert!(scan.next_page(PageLimits::default()).unwrap().is_empty());
    for node in actual {
        assert_eq!(nodes.get(&node.hash), Some(&node.bytes));
    }
    let mut wrong = record;
    wrong.roots.state.accounts = None;
    db.apply(BTreeMap::from([(
        key(RECORDS, hash.0),
        Some(wrong.encode()),
    )]));
    assert!(matches!(
        View::load(db).unwrap().scan_state(hash, Default::default()),
        Err(Error::Corrupt("state binding"))
    ));
}
