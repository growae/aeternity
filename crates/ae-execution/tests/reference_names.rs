use ae_execution::{Event, names::prune};
use ae_protocol::{Protocol, names as rules};
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    names::{Auction, Commitment, Name, Names, hashing, pointers_valid_at, sanitize_pointers},
};
use ae_trie::{NodeReader, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use serde_json::{Value as Json, json};

fn fixture() -> Json {
    serde_json::from_str(include_str!("fixtures/erlang_names.json")).unwrap()
}
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn key(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
fn hash(v: &Json) -> [u8; 32] {
    bytes(v).try_into().unwrap()
}
fn protocol(v: &Json) -> Protocol {
    (v.as_u64().unwrap() as u32).try_into().unwrap()
}
fn root(h: Option<Hash256>) -> Json {
    h.map_or(Json::Null, |h| json!(hex::encode(h.0)))
}
fn entries<S: NodeReader>(tree: &Trie<S>) -> Json {
    json!(
        tree.iter()
            .map(|item| {
                let (k, v) = item.unwrap();
                let key = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect::<Vec<_>>();
                let Value::Bytes(v) = v else {
                    panic!("state value must be bytes")
                };
                json!({"key":hex::encode(key), "encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
#[test]
fn naming_expiry_actions_cache_accounts_events_and_state_roots_match_erlang() {
    let data = fixture();
    let mut steps = 0;
    for trace in data["traces"].as_array().unwrap() {
        let mut names = Names::default();
        let mut accounts = Accounts::default();
        if !trace["initial_account"].is_null() {
            accounts
                .enter(
                    &Account::deserialize(
                        AccountPublicKey::ZERO,
                        &bytes(&trace["initial_account"]),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let p = protocol(&trace["protocol"]);
        for (i, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
            let mut events = vec![];
            match step["op"].as_str().unwrap() {
                "enter_name" => names
                    .enter_name(
                        &Name::deserialize(hash(&step["key"]), &bytes(&step["encoded"])).unwrap(),
                    )
                    .unwrap(),
                "enter_commitment" => names
                    .insert_commitment(
                        &Commitment::deserialize(hash(&step["key"]), &bytes(&step["encoded"]))
                            .unwrap(),
                    )
                    .unwrap(),
                "enter_name_auction" => names
                    .enter_auction(
                        &Auction::deserialize(
                            bytes(&step["key"]).try_into().unwrap(),
                            &bytes(&step["encoded"]),
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                "delete_name" => names.delete_name(hash(&step["key"])).unwrap(),
                "delete_commitment" => names.delete_commitment(hash(&step["key"])).unwrap(),
                "delete_name_auction" => names
                    .delete_auction(bytes(&step["key"]).try_into().unwrap())
                    .unwrap(),
                "prune" => {
                    events = prune(
                        &mut accounts,
                        &mut names,
                        step["height"].as_u64().unwrap(),
                        p,
                    )
                    .unwrap()
                }
                _ => unreachable!(),
            }
            assert_eq!(
                root(names.root_hash()),
                step["names_root"],
                "{p:?} step {i}"
            );
            assert_eq!(
                root(names.cache_root_hash()),
                step["cache_root"],
                "{p:?} step {i}"
            );
            assert_eq!(entries(names.tree()), step["names"], "{p:?} step {i}");
            assert_eq!(entries(names.cache()), step["cache"], "{p:?} step {i}");
            assert_eq!(entries(accounts.tree()), step["accounts"], "{p:?} step {i}");
            let state = StateRoots {
                accounts: accounts.root_hash(),
                names: names.root_hash(),
                ..Default::default()
            };
            assert_eq!(
                hex::encode(state.hash().0),
                step["root"].as_str().unwrap(),
                "{p:?} step {i}"
            );
            let expected = step["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    assert_eq!(hash(&e["account"]), [0; 32]);
                    assert_eq!(e["info"], "Chain.lock");
                    Event::NameFeeLocked {
                        amount: number(&e["amount"]),
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(events, expected, "{p:?} step {i}");
            let expected_auctions = step["names"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| bytes(&r["key"]).len() > 32)
                .count();
            assert_eq!(
                names
                    .auctions()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
                    .len(),
                expected_auctions
            );
            steps += 1;
        }
    }
    assert_eq!(steps, 480);
}
#[test]
fn name_hashes_governance_objects_and_large_expiry_keys_match_erlang() {
    let data = fixture();
    assert_eq!(data["hashes"].as_array().unwrap().len(), 35);
    for row in data["hashes"].as_array().unwrap() {
        let name = bytes(&row["name"]);
        assert_eq!(hashing::name_hash(&name).0, hash(&row["hash"]));
        let commitment = hashing::commitment_hash(&name, &number(&row["salt"]));
        if row.get("error").is_some() {
            assert_eq!(commitment, Err(ae_state::names::Error::IllegalSalt));
        } else {
            assert_eq!(commitment.unwrap().0, hash(&row["commitment"]));
        }
    }
    assert_eq!(data["policies"].as_array().unwrap().len(), 960);
    for row in data["policies"].as_array().unwrap() {
        let (p, n, o) = (
            protocol(&row["protocol"]),
            row["size"].as_u64().unwrap() as usize,
            row["override"].as_u64(),
        );
        assert_eq!(
            rules::claim_fee(n.try_into().unwrap(), p),
            number(&row["fee"])
        );
        assert_eq!(
            rules::bid_timeout(n, p, o),
            row["timeout"].as_u64().unwrap()
        );
        assert_eq!(
            rules::bid_extension(n, p, o),
            row["extension"].as_u64().unwrap()
        );
        assert_eq!(
            rules::max_expiration(p),
            row["expiration"].as_u64().unwrap()
        );
    }
    for row in data["objects"].as_array().unwrap() {
        let height = number(&row["height"]);
        let commitment =
            Commitment::new(key(1), AccountPublicKey(key(2)), 7.into(), height.clone());
        assert_eq!(commitment.serialize().unwrap(), bytes(&row["commitment"]));
        assert_eq!(
            Commitment::deserialize(key(1), &bytes(&row["commitment"])).unwrap(),
            commitment
        );
        let auction = Auction::new(
            key(3),
            AccountPublicKey(key(4)),
            Unsigned::from_decimal("123456789012345678901234567890").unwrap(),
            7.into(),
            height.clone(),
        );
        assert_eq!(auction.serialize().unwrap(), bytes(&row["auction"]));
        assert_eq!(
            Auction::deserialize(auction.hash(), &bytes(&row["auction"])).unwrap(),
            auction
        );
        let extended = auction.extend(
            AccountPublicKey(key(5)),
            Unsigned::from_decimal("9876543210123456789876543210").unwrap(),
            3.into(),
            height.add(&1.into()),
        );
        assert_eq!(extended.serialize().unwrap(), bytes(&row["extended"]));
        let mut names = Names::default();
        names.insert_commitment(&commitment).unwrap();
        names.enter_auction(&auction).unwrap();
        let cache = entries(names.cache());
        assert_eq!(cache[0]["key"], row["commitment_cache"]);
        assert_eq!(cache[1]["key"], row["auction_cache"]);
        // Pruning parses the first large key even when it is not yet expired.
        assert!(names.prune(1, Protocol::Ceres).unwrap().is_empty());
    }
}
#[test]
fn pointer_sanitization_and_historical_admission_match_reference() {
    for row in fixture()["pointers"].as_array().unwrap() {
        let name = Name::deserialize(key(0), &bytes(&row["name"])).unwrap();
        let mut sanitized = name.clone();
        sanitized.pointers = sanitize_pointers(&name.pointers);
        assert_eq!(sanitized.serialize().unwrap(), bytes(&row["sanitized"]));
        let clean = sanitized.pointers == name.pointers;
        let raw = row["raw"].as_bool().unwrap();
        for p in 1..=8 {
            assert_eq!(
                pointers_valid_at(&name.pointers, p.try_into().unwrap()),
                !((p < 6 && raw) || (p >= 5 && !clean))
            );
        }
    }
    assert!(rules::valid_overbid(&105.into(), &100.into()));
    assert!(!rules::valid_overbid(&104.into(), &100.into()));
    assert!(!rules::valid_overbid(&99.into(), &100.into()));
    let huge = Unsigned::from_decimal("10000000000000000000000000000000000000000000").unwrap();
    assert!(rules::valid_overbid(&huge.mul_u64(105), &huge.mul_u64(100)));
}
