use ae_mempool::sync::{self, Error, Limits, Snapshot, Unfold};
use ae_trie::Nibbles;
use ae_types::Hash256;
use serde_json::{Value as J, json};
fn data() -> J {
    serde_json::from_str(include_str!("fixtures/erlang_pool_sync.json")).unwrap()
}
fn path(s: &str) -> Nibbles {
    Nibbles::new(s.chars().map(|c| c.to_digit(16).unwrap() as u8).collect()).unwrap()
}
fn text(p: &Nibbles) -> String {
    p.as_slice()
        .iter()
        .map(|n| char::from_digit(u32::from(*n), 16).unwrap())
        .collect()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn hash(j: &J) -> Hash256 {
    Hash256::try_from(bytes(j).as_slice()).unwrap()
}
fn unfold(j: &J) -> Unfold {
    match j["kind"].as_str().unwrap() {
        "node" => Unfold::Node {
            path: path(j["path"].as_str().unwrap()),
            node: bytes(&j["node"]),
        },
        "leaf" => Unfold::Leaf(path(j["path"].as_str().unwrap())),
        "subtree" => Unfold::Subtree(path(j["path"].as_str().unwrap())),
        "key" => Unfold::Key(bytes(&j["key"])),
        other => panic!("{other}"),
    }
}
fn external(u: &Unfold) -> J {
    match u {
        Unfold::Node { path, node } => {
            json!({"kind":"node","path":text(path),"node":hex::encode(node)})
        }
        Unfold::Leaf(p) => json!({"kind":"leaf","path":text(p)}),
        Unfold::Subtree(p) => json!({"kind":"subtree","path":text(p)}),
        Unfold::Key(k) => json!({"kind":"key","key":hex::encode(k)}),
    }
}
#[test]
fn original_unfold_codecs_accept_and_reject_the_same_wire_inputs() {
    let d = data();
    let limits = Limits::default();
    for r in d["codec"]["roundtrip"].as_array().unwrap() {
        let u = unfold(&r["input"]);
        let encoded = bytes(&r["encoded"]);
        assert_eq!(u.encode(limits).unwrap(), encoded);
        assert_eq!(Unfold::decode(&encoded, limits).unwrap(), u);
    }
    for r in d["codec"]["decode"].as_array().unwrap() {
        let result = match Unfold::decode(&bytes(&r["encoded"]), limits) {
            Ok(u) => json!({"ok":true,"value":external(&u)}),
            Err(_) => json!({"ok":false}),
        };
        assert_eq!(result, r["result"], "{}", r["encoded"]);
    }
}
#[test]
fn original_two_sided_exchanges_match_every_request_response_and_missing_key() {
    let d = data();
    let limits = Limits::default();
    for (i, row) in d["exchanges"].as_array().unwrap().iter().enumerate() {
        let local =
            Snapshot::build(row["local"].as_array().unwrap().iter().map(hash), limits).unwrap();
        let remote =
            Snapshot::build(row["remote"].as_array().unwrap().iter().map(hash), limits).unwrap();
        let mut request = vec![local.initial()];
        let mut gets = vec![];
        for step in row["steps"].as_array().unwrap() {
            let wire = sync::encode_batch(&request, limits).unwrap();
            assert_eq!(
                json!(wire.iter().map(hex::encode).collect::<Vec<_>>()),
                step["request"],
                "exchange {i}"
            );
            let response = remote.respond_wire(&wire, limits).unwrap();
            assert_eq!(
                json!(response.iter().map(hex::encode).collect::<Vec<_>>()),
                step["response"],
                "exchange {i}"
            );
            let analysis = local
                .analyze(&sync::decode_batch(&response, limits).unwrap(), limits)
                .unwrap();
            assert_eq!(
                json!(analysis.unfold.iter().map(external).collect::<Vec<_>>()),
                step["next"]
            );
            assert_eq!(
                json!(analysis.get.iter().map(text).collect::<Vec<_>>()),
                step["gets"]
            );
            request = analysis.unfold;
            gets.extend(analysis.get);
        }
        assert!(request.is_empty());
        assert_eq!(
            json!(gets.iter().map(text).collect::<Vec<_>>()),
            row["gets"]
        );
        let mut expected: Vec<_> = row["remote"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|h| !row["local"].as_array().unwrap().contains(h))
            .map(|h| h.as_str().unwrap().to_string())
            .collect();
        let mut actual: Vec<_> = gets.iter().map(text).collect();
        expected.sort();
        actual.sort();
        assert_eq!(actual, expected);
    }
}
#[test]
fn original_analysis_preserves_duplicates_and_permissive_keys_and_leaf_paths() {
    let tree = Snapshot::build(
        [ae_crypto::blake2b_256(&1u32.to_be_bytes())],
        Default::default(),
    )
    .unwrap();
    for row in data()["analyze"].as_array().unwrap() {
        let input: Vec<_> = row["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(unfold)
            .collect();
        let actual = match tree.analyze(&input, Default::default()) {
            Ok(a) => {
                json!({"ok":true,"value":{"next":a.unfold.iter().map(external).collect::<Vec<_>>(),"gets":a.get.iter().map(text).collect::<Vec<_>>()}})
            }
            Err(_) => json!({"ok":false}),
        };
        assert_eq!(actual, row["result"]);
    }
    let absent = Hash256([99; 32]);
    assert_eq!(
        tree.respond(
            &[Unfold::Subtree(Nibbles::from_bytes(&absent.0))],
            Default::default()
        )
        .unwrap(),
        vec![Unfold::Key(absent.0.to_vec())]
    );
}
#[test]
fn resource_failures_preserve_snapshots_and_larger_retries_match() {
    let hashes: Vec<_> = (0u32..32)
        .map(|n| ae_crypto::blake2b_256(&n.to_be_bytes()))
        .collect();
    for limits in [
        Limits {
            entries: 1,
            ..Default::default()
        },
        Limits {
            tree_nodes: 0,
            ..Default::default()
        },
        Limits {
            tree_bytes: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            Snapshot::build(hashes.clone(), limits).unwrap_err(),
            Error::ResourceLimit
        );
    }
    let limits = Limits::default();
    let tree = Snapshot::build(hashes.clone(), limits).unwrap();
    let root = tree.root();
    assert_eq!(tree.len(), 32);
    assert!(!tree.is_empty());
    let request = vec![Unfold::Subtree(path(""))];
    let expected = tree.respond(&request, limits).unwrap();
    assert_eq!(expected.len(), 32);
    for limited in [
        Limits { items: 0, ..limits },
        Limits {
            wire_bytes: 1,
            ..limits
        },
        Limits {
            work: ae_trie::sync::Limits {
                nodes: 0,
                ..limits.work
            },
            ..limits
        },
        Limits {
            work: ae_trie::sync::Limits {
                outputs: 1,
                ..limits.work
            },
            ..limits
        },
        Limits {
            work: ae_trie::sync::Limits {
                bytes: 1,
                ..limits.work
            },
            ..limits
        },
        Limits {
            work: ae_trie::sync::Limits {
                path_nibbles: 1,
                ..limits.work
            },
            ..limits
        },
        Limits {
            work: ae_trie::sync::Limits {
                pending: 0,
                ..limits.work
            },
            ..limits
        },
    ] {
        assert_eq!(tree.respond(&request, limited), Err(Error::ResourceLimit));
        assert_eq!(tree.root(), root);
        assert_eq!(tree.respond(&request, limits).unwrap(), expected);
    }
    let wire = sync::encode_batch(&expected, limits).unwrap();
    let size: usize = wire.iter().map(Vec::len).sum();
    assert_eq!(
        sync::decode_batch(
            &wire,
            Limits {
                wire_bytes: size - 1,
                ..limits
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        sync::encode_batch(
            &expected,
            Limits {
                wire_bytes: size - 1,
                ..limits
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        sync::decode_batch(
            &wire,
            Limits {
                wire_bytes: size,
                ..limits
            }
        )
        .unwrap(),
        expected
    );
    let duplicate = Snapshot::build([hashes[0], hashes[0]], limits).unwrap();
    assert_eq!(duplicate.len(), 1);
    assert_eq!(
        duplicate.root(),
        Snapshot::build([hashes[0]], limits).unwrap().root()
    );
}
