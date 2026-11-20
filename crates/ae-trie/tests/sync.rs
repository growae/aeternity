use ae_trie::{
    Nibbles, Trie, Value,
    sync::{Budget, Presence, Unfold},
};
use serde_json::{Value as J, json};
fn nibbles(s: &str) -> Nibbles {
    Nibbles::new(s.chars().map(|c| c.to_digit(16).unwrap() as u8).collect()).unwrap()
}
fn path(p: &Nibbles) -> String {
    p.as_slice()
        .iter()
        .map(|n| char::from_digit(u32::from(*n), 16).unwrap())
        .collect()
}
fn external(u: Unfold) -> J {
    match u {
        Unfold::Node { path: p, node } => {
            json!({"kind":"node","path":path(&p),"node":hex::encode(node)})
        }
        Unfold::Leaf(p) => json!({"kind":"leaf","path":path(&p)}),
    }
}
fn fixture() -> J {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-mempool/tests/fixtures/erlang_pool_sync.json"
    )))
    .unwrap()
}
#[test]
fn original_unfold_presence_and_ordered_subtrees_match() {
    let data = fixture();
    for (i, row) in data["trees"].as_array().unwrap().iter().enumerate() {
        let mut tree = Trie::default();
        for item in row["inputs"].as_array().unwrap() {
            tree.put(
                &nibbles(item["key"].as_str().unwrap()),
                Value::decode(&hex::decode(item["value"].as_str().unwrap()).unwrap()).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(
            tree.root_hash().map_or(String::new(), |h| hex::encode(h.0)),
            row["root"]
        );
        for (j, q) in row["queries"].as_array().unwrap().iter().enumerate() {
            let p = nibbles(q["input"]["path"].as_str().unwrap());
            let node = hex::decode(q["input"]["node"].as_str().unwrap()).unwrap();
            let unfolded = match tree.unfold(&p, &node, &mut Budget::new(Default::default())) {
                Ok(us) => {
                    json!({"ok":true,"value":us.into_iter().map(external).collect::<Vec<_>>()})
                }
                Err(_) => json!({"ok":false}),
            };
            assert_eq!(unfolded, q["unfold"], "tree {i} query {j} {}", q["input"]);
            let present = match tree.has_node(&p, &node, &mut Budget::new(Default::default())) {
                Ok(p) => {
                    json!({"ok":true,"value":match p {Presence::Yes=>"yes",Presence::No=>"no",Presence::Maybe=>"maybe"}})
                }
                Err(_) => json!({"ok":false}),
            };
            assert_eq!(present, q["presence"], "tree {i} query {j} {}", q["input"]);
        }
        for q in row["subtrees"].as_array().unwrap() {
            let p = nibbles(q["path"].as_str().unwrap());
            if p.as_slice().len() == 64 {
                continue;
            } // Pool wrapper has an unconditional full-key shortcut.
            let actual = match tree.sync_keys(&p, 64, &mut Budget::new(Default::default())) {
                Ok(keys) => {
                    json!({"ok":true,"value":keys.iter().map(|k|json!({"kind":"key","key":path(k)})).collect::<Vec<_>>()})
                }
                Err(_) => json!({"ok":false}),
            };
            assert_eq!(actual, q["result"], "subtree {i} {p:?}");
        }
    }
}

#[test]
fn permissive_advertised_leaves_do_not_change_state_decoding_or_hide_backend_failures() {
    use ae_trie::{Error, MemoryStore, NodeReader, NodeWriter};
    use std::{cell::Cell, rc::Rc};
    let raw = vec![0xc2, 0x20, 0x80];
    let hash = ae_crypto::blake2b_256(&raw);
    let mut store = MemoryStore::default();
    store.put_node(hash, raw.clone()).unwrap();
    assert!(matches!(
        Trie::from_root(Some(hash), store),
        Err(Error::InvalidNode)
    ));
    let empty = Trie::default();
    assert_eq!(
        empty
            .has_node(&nibbles(""), &raw, &mut Budget::new(Default::default()))
            .unwrap(),
        Presence::Yes
    );
    let mut base = Trie::default();
    base.put(&nibbles("1234"), Value::List(vec![])).unwrap();
    let root = base.root_hash();
    struct Failing {
        store: MemoryStore,
        mode: Rc<Cell<u8>>,
    }
    impl NodeReader for Failing {
        fn node(&self, hash: &ae_types::Hash256) -> Result<Option<Vec<u8>>, Error> {
            match self.mode.get() {
                1 => Err(Error::Backend("offline".into())),
                2 => Ok(Some(vec![0])),
                _ => self.store.node(hash),
            }
        }
    }
    let mode = Rc::new(Cell::new(0));
    let tree = Trie::from_root(
        root,
        Failing {
            store: base.into_store(),
            mode: mode.clone(),
        },
    )
    .unwrap();
    for failure in [1, 2] {
        mode.set(failure);
        let error = tree
            .has_node(
                &nibbles("1"),
                &[99; 32],
                &mut Budget::new(Default::default()),
            )
            .unwrap_err();
        assert!(matches!(error, Error::Backend(_) | Error::HashMismatch(_)));
    }
    mode.set(0);
    assert_eq!(
        tree.has_node(
            &nibbles("1"),
            &[99; 32],
            &mut Budget::new(Default::default())
        )
        .unwrap(),
        Presence::Maybe
    );
    assert_eq!(tree.root_hash(), root);
    assert_eq!(
        tree.has_node(
            &nibbles("1"),
            &[99; 32],
            &mut Budget::new(ae_trie::sync::Limits {
                nodes: 0,
                ..Default::default()
            })
        ),
        Err(Error::ResourceLimit)
    );
}
