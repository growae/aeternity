// Original aeu_mp_trees / aeser_rlp at fa2a6d7 / a1204df, run with OTP 26.2.5.21.
// Hash primitive substituted with Python hashlib BLAKE2b-256; trie code unchanged.
use ae_trie::{IteratorOptions, MemoryStore, Nibbles, NodeWriter, Trie, Value, verify_proof};
use ae_types::Hash256;
use serde_json::Value as Json;

fn nibbles(text: &str) -> Nibbles {
    Nibbles::new(
        text.chars()
            .map(|c| c.to_digit(16).unwrap() as u8)
            .collect(),
    )
    .unwrap()
}
fn value(text: &str) -> Value {
    Value::decode(&hex::decode(text).unwrap()).unwrap()
}
fn root(text: &str) -> Option<Hash256> {
    if text.is_empty() {
        None
    } else {
        Some(Hash256::try_from(hex::decode(text).unwrap().as_slice()).unwrap())
    }
}

fn expected_items(items: &Json) -> Vec<(Nibbles, Value)> {
    items
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                nibbles(row[0].as_str().unwrap()),
                value(row[1].as_str().unwrap()),
            )
        })
        .collect()
}

#[test]
fn every_reference_mutation_root_lookup_and_proof_matches() {
    let cases: Vec<Vec<Json>> =
        serde_json::from_str(include_str!("fixtures/erlang_trie.json")).unwrap();
    let mut steps = 0;
    for (case, operations) in cases.iter().enumerate() {
        let mut tree = Trie::default();
        for (index, step) in operations.iter().enumerate() {
            let key = nibbles(step["key"].as_str().unwrap());
            match step["op"].as_str().unwrap() {
                "put" => tree
                    .put(&key, value(step["value"].as_str().unwrap()))
                    .unwrap(),
                "delete" => tree.delete(&key).unwrap(),
                _ => panic!("unknown fixture operation"),
            }
            assert_eq!(
                tree.root_hash(),
                root(step["root"].as_str().unwrap()),
                "case {case}, step {index}"
            );
            for query in step["queries"].as_array().unwrap() {
                let key = nibbles(query["key"].as_str().unwrap());
                let expected = value(query["value"].as_str().unwrap());
                let expected = (!expected.is_empty()).then_some(expected);
                let (actual, actual_proof) = tree.lookup_with_proof(&key).unwrap();
                assert_eq!(actual, expected, "case {case}, step {index}, query {key:?}");
                let mut reference_proof = MemoryStore::default();
                for node in query["proof"].as_array().unwrap() {
                    let hash = root(node["hash"].as_str().unwrap()).unwrap();
                    let bytes = hex::decode(node["rlp"].as_str().unwrap()).unwrap();
                    reference_proof.put_node(hash, bytes).unwrap();
                }
                assert_eq!(
                    actual_proof, reference_proof,
                    "proof node bytes: case {case}, step {index}"
                );
                verify_proof(tree.root_hash(), &key, expected.as_ref(), reference_proof).unwrap();
                let after = tree
                    .iter_with_options(IteratorOptions {
                        after: Some(key.clone()),
                        ..IteratorOptions::default()
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    after,
                    expected_items(&query["after"]),
                    "ordered seek: case {case}, step {index}"
                );
                let subtree = tree.read_only_subtree(&key);
                if query["subtree"].is_null() {
                    assert!(
                        subtree.is_err(),
                        "absent subtree: case {case}, step {index}"
                    );
                } else {
                    let subtree = subtree.unwrap();
                    assert_eq!(
                        subtree.root_hash(),
                        root(query["subtree"]["root"].as_str().unwrap())
                    );
                    assert_eq!(
                        subtree.iter().collect::<Result<Vec<_>, _>>().unwrap(),
                        expected_items(&query["subtree"]["items"])
                    );
                }
            }
            steps += 1;
        }
        assert_eq!(tree.root_hash(), None);
    }
    assert!(steps > 300);
}
