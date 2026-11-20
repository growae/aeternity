use ae_crypto::blake2b_256;
use ae_trie::{
    Error, MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value,
    scan::{Cursor, Limits, NodeBytes, PageLimits},
};
use ae_types::Hash256;
use serde_json::Value as Json;
use std::cell::Cell;

fn key(text: &str) -> Nibbles {
    Nibbles::new(
        text.chars()
            .map(|c| c.to_digit(16).unwrap() as u8)
            .collect(),
    )
    .unwrap()
}
fn hash(text: &str) -> Option<Hash256> {
    (!text.is_empty()).then(|| Hash256::try_from(hex::decode(text).unwrap().as_slice()).unwrap())
}
fn tree() -> Trie {
    let mut tree = Trie::default();
    for n in 0..16 {
        tree.put(&Nibbles::new(vec![n]).unwrap(), Value::Bytes(vec![n; 64]))
            .unwrap();
    }
    tree
}
#[test]
fn original_hash_order_and_node_bytes_survive_paging_and_checkpoint_resume() {
    let data: Json =
        serde_json::from_str(include_str!("fixtures/erlang_reachability.json")).unwrap();
    let mut mutations = 0;
    let mut visits = 0;
    for case in data["cases"].as_array().unwrap() {
        let mut tree = Trie::default();
        for row in case.as_array().unwrap() {
            let k = key(row["key"].as_str().unwrap());
            match row["op"].as_str().unwrap() {
                "put" => tree
                    .put(
                        &k,
                        Value::decode(&hex::decode(row["value"].as_str().unwrap()).unwrap())
                            .unwrap(),
                    )
                    .unwrap(),
                "delete" => tree.delete(&k).unwrap(),
                _ => panic!(),
            }
            assert_eq!(tree.root_hash(), hash(row["root"].as_str().unwrap()));
            let expected: Vec<_> = row["visits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| NodeBytes {
                    hash: hash(node["hash"].as_str().unwrap()).unwrap(),
                    bytes: hex::decode(node["rlp"].as_str().unwrap()).unwrap(),
                })
                .collect();
            for size in [1, 7, 100] {
                let limits = Limits::default();
                let roots = [tree.root_hash(), tree.root_hash()].into_iter().flatten();
                let mut cursor = Cursor::new(roots, limits).unwrap();
                let mut actual = Vec::new();
                while !cursor.is_complete() {
                    let page = cursor
                        .page(
                            tree.store(),
                            PageLimits {
                                nodes: size,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    assert!(!page.nodes.is_empty() && page.nodes.len() <= size);
                    actual.extend(page.nodes);
                    cursor = Cursor::decode(&page.next.encode(), limits).unwrap();
                    assert_eq!(cursor, page.next);
                }
                assert_eq!(cursor.progress().nodes, expected.len() as u64);
                assert_eq!(
                    cursor.progress().bytes,
                    expected.iter().map(|n| n.bytes.len() as u64).sum::<u64>()
                );
                assert_eq!(actual, expected, "mutation {mutations}, page {size}");
            }
            visits += expected.len();
            mutations += 1;
        }
    }
    println!("Reachability comparisons: {mutations} mutations, {visits} visits, three page sizes");
    assert!(mutations > 300 && visits > 10000);
}

struct Faulty<'a> {
    store: &'a MemoryStore,
    target: Hash256,
    fault: u8,
}
impl NodeReader for Faulty<'_> {
    fn verify_node_hashes(&self) -> bool {
        false
    }
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
        if hash == &self.target {
            match self.fault {
                0 => Ok(None),
                1 => Ok(Some(vec![0])),
                _ => Err(Error::Backend("injected read failure".into())),
            }
        } else {
            self.store.node(hash)
        }
    }
}
#[test]
fn missing_corrupt_and_failed_reads_abort_without_consuming_the_cursor() {
    let tree = tree();
    let cursor = Cursor::new(tree.root_hash(), Limits::default()).unwrap();
    let all = cursor.page(tree.store(), PageLimits::default()).unwrap();
    let target = all.nodes[3].hash;
    let checkpoint = cursor.encode();
    for fault in 0..3 {
        let failed = cursor
            .page(
                &Faulty {
                    store: tree.store(),
                    target,
                    fault,
                },
                PageLimits::default(),
            )
            .unwrap_err();
        match fault {
            0 => assert_eq!(failed, Error::MissingNode(target)),
            1 => assert_eq!(failed, Error::HashMismatch(target)),
            _ => assert!(matches!(failed, Error::Backend(_))),
        }
        assert_eq!(cursor.encode(), checkpoint);
    }
    assert_eq!(
        cursor
            .page(tree.store(), PageLimits::default())
            .unwrap()
            .nodes,
        all.nodes
    );
}

#[test]
fn page_and_cumulative_budgets_reject_without_skipping_work() {
    let tree = tree();
    let root = tree.root_hash().unwrap();
    let cursor = Cursor::new([root], Limits::default()).unwrap();
    let all = cursor.page(tree.store(), PageLimits::default()).unwrap();
    let first_bytes = all.nodes[0].bytes.len();
    let page = cursor
        .page(
            tree.store(),
            PageLimits {
                nodes: 100,
                bytes: first_bytes,
            },
        )
        .unwrap();
    assert_eq!(page.nodes.len(), 1);
    assert!(!page.next.is_complete());
    assert!(matches!(
        cursor.page(
            tree.store(),
            PageLimits {
                nodes: 100,
                bytes: first_bytes - 1
            }
        ),
        Err(Error::ResourceLimit)
    ));
    for limits in [
        Limits {
            pending: 1,
            ..Default::default()
        },
        Limits {
            nodes: 1,
            ..Default::default()
        },
        Limits {
            bytes: first_bytes as u64,
            ..Default::default()
        },
    ] {
        let cursor = Cursor::new([root], limits).unwrap();
        assert!(matches!(
            cursor.page(tree.store(), PageLimits::default()),
            Err(Error::ResourceLimit)
        ));
    }
    let limits = Limits {
        nodes: all.next.progress().nodes,
        bytes: all.next.progress().bytes,
        ..Default::default()
    };
    assert!(
        Cursor::new([root], limits)
            .unwrap()
            .page(tree.store(), PageLimits::default())
            .unwrap()
            .next
            .is_complete()
    );
    assert!(
        Cursor::new(
            [root, root],
            Limits {
                pending: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    for page in [
        PageLimits { nodes: 0, bytes: 1 },
        PageLimits { nodes: 1, bytes: 0 },
    ] {
        assert!(matches!(
            cursor.page(tree.store(), page),
            Err(Error::ResourceLimit)
        ));
    }
    let empty = Cursor::new(
        [],
        Limits {
            pending: 0,
            nodes: 0,
            bytes: 0,
        },
    )
    .unwrap();
    assert!(
        empty
            .page(tree.store(), PageLimits::default())
            .unwrap()
            .next
            .is_complete()
    );
}

#[test]
fn checkpoint_decoding_is_strict_and_bounded() {
    let limits = Limits::default();
    let cursor = Cursor::new([Hash256([1; 32]), Hash256([2; 32])], limits).unwrap();
    let bytes = cursor.encode();
    assert_eq!(Cursor::decode(&bytes, limits).unwrap(), cursor);
    assert!(
        Cursor::decode(
            &bytes,
            Limits {
                pending: 1,
                ..limits
            }
        )
        .is_err()
    );
    for end in 0..bytes.len() {
        assert!(Cursor::decode(&bytes[..end], limits).is_err());
    }
    let Value::List(fields) = Value::decode(&bytes).unwrap() else {
        panic!()
    };
    for (index, value) in [
        (0, Value::Bytes(vec![2])),
        (1, Value::Bytes(vec![0])),
        (1, Value::Bytes(vec![1; 9])),
        (1, Value::Bytes(u64::MAX.to_be_bytes().to_vec())),
        (2, Value::Bytes(u64::MAX.to_be_bytes().to_vec())),
        (3, Value::List(vec![Value::Bytes(vec![0; 31])])),
        (3, Value::Bytes(vec![])),
    ] {
        let mut broken = fields.clone();
        broken[index] = value;
        assert!(Cursor::decode(&Value::List(broken).encode(), limits).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(Cursor::decode(&trailing, limits).is_err());
}

#[test]
fn inline_nodes_are_checked_and_values_are_never_followed_as_references() {
    let mut tree = Trie::default();
    for k in ["12340", "12341"] {
        tree.put(&key(k), Value::Bytes(vec![1])).unwrap();
    }
    let page = Cursor::new(tree.root_hash(), Limits::default())
        .unwrap()
        .page(tree.store(), PageLimits::default())
        .unwrap();
    assert_eq!(page.nodes.len(), 2); // extension and its forced-hash tiny branch
    assert!(page.nodes[1].bytes.len() < 32);
    let dangling = Value::List(vec![
        Value::Bytes(vec![0; 32]),
        Value::List(vec![Value::Bytes(vec![7; 32])]),
    ]);
    tree.put(&key("abcd"), dangling).unwrap();
    assert!(
        Cursor::new(tree.root_hash(), Limits::default())
            .unwrap()
            .page(tree.store(), PageLimits::default())
            .unwrap()
            .next
            .is_complete()
    );
    let mut fields = vec![Value::empty(); 17];
    fields[0] = Value::Bytes(vec![0]); // malformed inline child inside authenticated parent
    let raw = Value::List(fields).encode();
    let h = blake2b_256(&raw);
    let mut store = MemoryStore::default();
    store.put_node(h, raw).unwrap();
    assert!(matches!(
        Cursor::new([h], Limits::default())
            .unwrap()
            .page(&store, PageLimits::default()),
        Err(Error::InvalidNode)
    ));
}

#[test]
fn many_pages_retain_only_a_bounded_frontier_and_read_each_visit_once() {
    struct Counting {
        store: MemoryStore,
        reads: Cell<u64>,
    }
    impl NodeReader for Counting {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, Error> {
            self.reads.set(self.reads.get() + 1);
            self.store.node(h)
        }
    }
    let mut tree = Trie::default();
    for i in 0u32..2048 {
        tree.put(
            &Nibbles::from_bytes(&i.to_be_bytes()),
            Value::Bytes(vec![(i % 256) as u8; 40]),
        )
        .unwrap();
    }
    let mut cursor = Cursor::new(
        tree.root_hash(),
        Limits {
            pending: 64,
            ..Default::default()
        },
    )
    .unwrap();
    let store = Counting {
        store: tree.into_store(),
        reads: Cell::new(0),
    };
    let mut high_water = 0;
    while !cursor.is_complete() {
        let page = cursor
            .page(
                &store,
                PageLimits {
                    nodes: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(page.nodes.len(), 1);
        cursor = page.next;
        high_water = high_water.max(cursor.progress().pending);
    }
    assert!(cursor.progress().nodes > 2048);
    assert!(high_water < 64);
    assert_eq!(store.reads.get(), cursor.progress().nodes);
}
