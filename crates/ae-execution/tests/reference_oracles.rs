use ae_execution::{Error, Event, oracles};
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    oracles::{self as state, Oracle, Oracles, Query, QueryFilter, Ttl},
};
use ae_trie::{NodeReader, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};
use serde_json::{Value as Json, json};

fn corpus() -> Json {
    serde_json::from_str(include_str!("fixtures/erlang_oracles.json")).unwrap()
}
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn hash(v: &Json) -> [u8; 32] {
    bytes(v).try_into().unwrap()
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn small(v: &Json) -> u64 {
    v.as_str().unwrap().parse().unwrap()
}
fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn entries<S: NodeReader>(t: &Trie<S>) -> Json {
    json!(
        t.iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                let k = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect::<Vec<_>>();
                let Value::Bytes(v) = v else {
                    panic!("state value")
                };
                json!({"key":hex::encode(k),"encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
fn root(r: Option<Hash256>) -> Json {
    r.map_or(Json::Null, |h| json!(hex::encode(h.0)))
}
fn snapshot<S: NodeReader, O: NodeReader, C: NodeReader>(
    accounts: &Accounts<S>,
    oracles: &Oracles<O, C>,
) -> Json {
    json!({"root":hex::encode(StateRoots { accounts:accounts.root_hash(),oracles:oracles.root_hash(),..Default::default() }.hash().0),
        "accounts":entries(accounts.tree()),"oracles":entries(oracles.tree()),"cache":entries(oracles.cache()),
        "oracle_root":root(oracles.root_hash()),"cache_root":root(oracles.cache_root_hash())})
}
fn initial(v: &Json) -> (Accounts, Oracles) {
    let mut accounts = Accounts::default();
    for a in v["accounts"].as_array().unwrap() {
        accounts
            .enter(
                &Account::deserialize(AccountPublicKey(hash(&a["key"])), &bytes(&a["encoded"]))
                    .unwrap(),
            )
            .unwrap();
    }
    let state = Oracles::default();
    assert_eq!(snapshot(&accounts, &state), *v);
    (accounts, state)
}
fn response(v: &Json) -> Option<Vec<u8>> {
    if v == "none" {
        None
    } else {
        Some(bytes(&v["binary"]))
    }
}
fn apply(a: &[Json], accounts: &mut Accounts, ot: &mut Oracles) -> Result<Vec<Event>, Error> {
    match a[0].as_str().unwrap() {
        "oracle" => {
            let id = small(&a[1]);
            ot.insert_oracle(&Oracle {
                public_key: key(id),
                query_format: [b"query".as_slice(), &[id as u8]].concat(),
                response_format: [b"response".as_slice(), &[id as u8]].concat(),
                query_fee: id.into(),
                expires_by: number(&a[2]),
                abi_version: (id % 4).into(),
            })?;
        }
        "query" => {
            let id = small(&a[2]);
            let mut q = Query::new(
                Id::new(IdTag::Oracle, key(small(&a[1]))),
                AccountPublicKey(key(small(&a[3]))),
                id.into(),
                [b"query".as_slice(), &[id as u8]].concat(),
                number(&a[6]),
                number(&a[4]),
                number(&a[5]),
            )?;
            q.id = key(id);
            q.response = response(&a[7]);
            ot.insert_query(&q)?;
        }
        "extend" => {
            let mut o = ot.oracle(key(small(&a[1])))?.unwrap();
            o.expires_by = number(&a[2]);
            ot.enter_oracle(&o)?;
        }
        "respond" => {
            let mut q = ot.query(key(small(&a[1])), key(small(&a[2])))?.unwrap();
            q.add_response(&number(&a[3]), response(&a[4]));
            ot.enter_query(&q)?;
        }
        "duplicate_oracle" => {
            let o = ot.oracle(key(small(&a[1])))?.unwrap();
            ot.insert_oracle(&o)?;
        }
        "duplicate_query" => {
            let q = ot.query(key(small(&a[1])), key(small(&a[2])))?.unwrap();
            ot.insert_query(&q)?;
        }
        "prune" => return oracles::prune(accounts, ot, small(&a[1])),
        "list" => {}
        _ => panic!("unknown action"),
    }
    Ok(vec![])
}
fn listings(ot: &Oracles, expected: &Json) {
    for r in expected["oracles"].as_array().unwrap() {
        let actual = ot
            .oracles(
                (!r["from"].is_null()).then(|| hash(&r["from"])),
                r["limit"].as_u64().unwrap() as usize,
            )
            .unwrap();
        assert_eq!(
            json!(
                actual
                    .iter()
                    .map(|o| hex::encode(o.public_key))
                    .collect::<Vec<_>>()
            ),
            r["result"]
        );
    }
    for r in expected["queries"].as_array().unwrap() {
        let f = match r["filter"].as_str().unwrap() {
            "all" => QueryFilter::All,
            "open" => QueryFilter::Open,
            "closed" => QueryFilter::Closed,
            _ => unreachable!(),
        };
        let actual = ot
            .queries(
                hash(&r["oracle"]),
                (!r["from"].is_null()).then(|| hash(&r["from"])),
                f,
                r["limit"].as_u64().unwrap() as usize,
            )
            .unwrap();
        assert_eq!(json!(actual.iter().map(|q|json!({"id":hex::encode(q.id),"encoded":hex::encode(q.serialize().unwrap())})).collect::<Vec<_>>()),r["result"],"{r}");
    }
    for r in expected["ids"].as_array().unwrap() {
        assert_eq!(
            json!(
                ot.query_ids(hash(&r["oracle"]))
                    .unwrap()
                    .iter()
                    .map(hex::encode)
                    .collect::<Vec<_>>()
            ),
            r["ids"]
        );
    }
}
#[test]
fn expiry_refunds_stale_entries_event_order_and_listings_match_original_erlang() {
    let corpus = corpus();
    let (mut steps, mut failures) = (0, 0);
    for trace in corpus["traces"].as_array().unwrap() {
        let (mut accounts, mut ot) = initial(&trace["initial"]);
        for row in trace["steps"].as_array().unwrap() {
            let a = row["action"].as_array().unwrap();
            let label = format!("{} {a:?}", trace["label"]);
            assert_eq!(snapshot(&accounts, &ot), row["before"], "{label}");
            match apply(a, &mut accounts, &mut ot) {
                Ok(events) => {
                    assert!(
                        row.get("after").is_some(),
                        "{label}: expected {}",
                        row["error"]
                    );
                    assert_eq!(snapshot(&accounts, &ot), row["after"], "{label}");
                    let expected = row["events"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|e| {
                            assert_eq!(e["info"], "Oracle.refund");
                            Event::OracleRefund {
                                account: AccountPublicKey(hash(&e["account"])),
                                amount: number(&e["amount"]),
                            }
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(events, expected, "{label}");
                    if let Some(l) = row.get("listings") {
                        listings(&ot, l);
                    }
                }
                Err(e) => {
                    let (reason, class) = match e {
                        Error::ReferenceException { reason, class } => (reason, class),
                        Error::OracleRefundAccountMissing(_) => ("account_disappeared", "error"),
                        Error::Oracle(state::Error::AlreadyPresent) => ("already_present", "error"),
                        _ => panic!("{label}: {e:?}"),
                    };
                    assert_eq!(row["error"], reason, "{label}");
                    assert_eq!(row["exception"], class, "{label}");
                    assert_eq!(snapshot(&accounts, &ot), row["before"], "{label}");
                    failures += 1;
                }
            }
            steps += 1;
            ot = Oracles::from_roots(
                ot.root_hash(),
                ot.cache_root_hash(),
                ot.tree().store().clone(),
                ot.cache().store().clone(),
            )
            .unwrap();
        }
    }
    assert_eq!((steps, failures), (69, 6));
}
#[test]
fn oracle_objects_query_hashes_and_ttls_match_original_without_narrowing() {
    let corpus = corpus();
    for r in corpus["objects"].as_array().unwrap() {
        let n = number(&r["number"]);
        let o = Oracle {
            public_key: key(1),
            query_format: vec![],
            response_format: vec![255],
            query_fee: n.clone(),
            expires_by: n.clone(),
            abi_version: n.clone(),
        };
        assert_eq!(o.serialize().unwrap(), bytes(&r["oracle"]));
        assert_eq!(
            Oracle::deserialize(key(1), &bytes(&r["oracle"])).unwrap(),
            o
        );
        let tag = if r["tag"] == "name" {
            IdTag::Name
        } else {
            IdTag::Oracle
        };
        let mut q = Query::new(
            Id::new(tag, key(2)),
            AccountPublicKey(key(21)),
            n.clone(),
            vec![0, 255],
            n.clone(),
            n.clone(),
            n.add(&1.into()),
        )
        .unwrap();
        if n.as_bytes().last().unwrap() & 1 == 1 {
            q.response = Some(vec![]);
        }
        assert_eq!(q.id, hash(&r["id"]));
        assert_eq!(q.serialize().unwrap(), bytes(&r["query"]));
        assert_eq!(Query::deserialize(q.id, &bytes(&r["query"])).unwrap(), q);
        let mut ot = Oracles::default();
        ot.insert_oracle(&o).unwrap();
        q.oracle.tag = IdTag::Oracle;
        ot.insert_query(&q).unwrap();
        assert_eq!(snapshot(&Accounts::default(), &ot), r["state"]);
    }
    for r in corpus["hashes"].as_array().unwrap() {
        assert_eq!(
            state::query_id(
                AccountPublicKey(hash(&r["sender"])),
                &number(&r["nonce"]),
                hash(&r["oracle"])
            )
            .0,
            hash(&r["id"])
        );
        assert_eq!(
            state::ga_query_id(hash(&r["sender"]), hash(&r["oracle"])).0,
            hash(&r["ga"])
        );
    }
    for r in corpus["ttls"].as_array().unwrap() {
        let ttl = if r["kind"] == "delta" {
            Ttl::Delta(number(&r["value"]))
        } else {
            Ttl::Block(number(&r["value"]))
        };
        for (field, actual) in [
            ("delta", ttl.delta(&number(&r["height"]))),
            ("expiry", ttl.expiry(&number(&r["height"]))),
        ] {
            if r[field].is_string() {
                assert_eq!(actual.unwrap(), number(&r[field]));
            } else {
                assert_eq!(actual, Err(ae_protocol::oracles::TtlError));
            }
        }
    }
    for r in corpus["decoding"].as_array().unwrap() {
        let raw = bytes(&r["raw"]);
        let canonical = if r["module"] == "aeo_oracles" {
            Oracle::deserialize(key(100), &raw).and_then(|o| o.serialize())
        } else {
            Query::deserialize(key(100), &raw).and_then(|q| q.serialize())
        };
        if r["accepted"] == true {
            assert_eq!(canonical.unwrap(), bytes(&r["canonical"]), "{r}");
        } else {
            assert!(canonical.is_err(), "{r}");
        }
    }
}

#[test]
fn oracle_refund_failures_roll_back_each_backend_and_nested_transactions() {
    use ae_trie::{MemoryStore, NodeWriter};
    use std::{cell::Cell, collections::BTreeSet, rc::Rc};
    struct FaultStore {
        inner: MemoryStore,
        remaining: Rc<Cell<usize>>,
        failed: Rc<Cell<Option<&'static str>>>,
        label: &'static str,
    }
    impl NodeReader for FaultStore {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.inner.node(h)
        }
    }
    impl NodeWriter for FaultStore {
        fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            if self.remaining.get() == 0 {
                self.failed.set(Some(self.label));
                return Err(ae_trie::Error::Backend("injected write failure".into()));
            }
            self.remaining.set(self.remaining.get() - 1);
            self.inner.put_node(h, b)
        }
    }
    let corpus = corpus();
    let (mut failure_count, mut successes) = (0, 0);
    let mut backends = BTreeSet::new();
    for trace in corpus["traces"].as_array().unwrap() {
        let (mut accounts, mut ot) = initial(&trace["initial"]);
        for row in trace["steps"].as_array().unwrap() {
            let a = row["action"].as_array().unwrap();
            if a[0] == "prune" && row.get("after").is_some() {
                for budget in 0..64 {
                    let remaining = Rc::new(Cell::new(budget));
                    let failed = Rc::new(Cell::new(None));
                    let store = |inner, label| FaultStore {
                        inner,
                        label,
                        remaining: remaining.clone(),
                        failed: failed.clone(),
                    };
                    let mut ac = Accounts::from_root(
                        accounts.root_hash(),
                        store(accounts.tree().store().clone(), "accounts"),
                    )
                    .unwrap();
                    let mut state = Oracles::from_roots(
                        ot.root_hash(),
                        ot.cache_root_hash(),
                        store(ot.tree().store().clone(), "oracles"),
                        store(ot.cache().store().clone(), "expiry"),
                    )
                    .unwrap();
                    match oracles::prune(&mut ac, &mut state, small(&a[1])) {
                        Ok(_) => {
                            successes += 1;
                            assert_eq!(snapshot(&ac, &state), row["after"]);
                        }
                        Err(_) => {
                            failure_count += 1;
                            backends.insert(failed.get().expect("backend failure"));
                            assert_eq!(snapshot(&ac, &state), row["before"]);
                        }
                    }
                }
                // A successful inner prune must still roll back if its enclosing
                // block/VM transaction later fails.
                let before = snapshot(&accounts, &ot);
                let result: Result<(), Error> = accounts.transaction(|ac| {
                    ot.transaction(|state| {
                        oracles::prune(ac, state, small(&a[1]))?;
                        Err(Error::InsufficientFunds)
                    })
                });
                assert_eq!(result, Err(Error::InsufficientFunds));
                assert_eq!(snapshot(&accounts, &ot), before);
            }
            let _ = apply(a, &mut accounts, &mut ot);
        }
    }
    assert!(failure_count > 20 && successes > 100);
    assert_eq!(backends, BTreeSet::from(["accounts", "oracles", "expiry"]));
}
