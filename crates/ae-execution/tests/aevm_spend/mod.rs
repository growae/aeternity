use super::*;
use ae_execution::{
    Context, Event, Origin,
    aevm_chain::{self, EventContext, Host, State},
};
use ae_protocol::Protocol;
use ae_state::Trees;
use ae_trie::{MemoryStore, Trie};
use ae_tx::CanonicalTransaction;
use ae_types::{Id, IdTag, Unsigned};
use ae_vm_aevm::{
    execution::{Budget, Limits},
    runtime::{Fault, Native},
};
use num_bigint::{BigInt, BigUint};
use std::sync::{Arc, LazyLock};
static SPENDS: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_chain_spends.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
mod invocations;
mod oracle;
fn field<'a>(v: &'a J, k: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == json!(["atom", k]))
        .unwrap()[1]
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn map(v: Vec<(&str, J)>) -> J {
    let mut p: Vec<_> = v.into_iter().map(|(k, v)| json!([atom(k), v])).collect();
    p.sort_by_key(J::to_string);
    json!(["map", p])
}
fn logical(mut v: J) -> J {
    if let Some(a) = v.as_array_mut() {
        for x in a.iter_mut() {
            *x = logical(x.take());
        }
        if a.first() == Some(&json!("map")) {
            a[1].as_array_mut().unwrap().sort_by_key(J::to_string);
        }
    }
    v
}
fn trees_from(snap: &J) -> Trees {
    let mut accounts = Accounts::default();
    for a in field(snap, "accounts")[1].as_array().unwrap() {
        let k = AccountPublicKey(bytes(field(a, "key")).try_into().unwrap());
        let a = Account::deserialize(k, &bytes(field(a, "encoded"))).unwrap();
        accounts.enter(&a).unwrap();
    }
    Trees {
        accounts,
        contracts: Contracts::default(),
        oracles: Oracles::default(),
        names: Default::default(),
        calls: Default::default(),
        channels: Trie::new(MemoryStore::default()),
    }
}
fn context(row: &J) -> Context {
    let mut c = Context::transaction(
        50,
        Protocol::try_from(row["protocol"].as_u64().unwrap_or(4) as u32).unwrap(),
    );
    c.origin = Origin::Contract;
    c.dry_run = row["case_name"] == "ga_dry";
    if row["case_name"] == "ga_auth" {
        c.ga_authenticated.insert(AccountPublicKey(Word::from(1).0));
        c.ga_nonces
            .insert(AccountPublicKey(Word::from(1).0), Word::from(42).0.to_vec());
    }
    c
}
fn state(row: &J) -> State {
    let inner = if row["mode"] == "offchain" {
        let r = SPENDS["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["mode"] == "onchain"
                    && r["vm"] == row["vm"]
                    && r["protocol"] == row["protocol"]
                    && r["case_name"] == row["case_name"]
            })
            .unwrap();
        Some(Arc::new(trees_from(&r["before"])))
    } else {
        None
    };
    State {
        trees: trees_from(&row["before"]),
        onchain: inner,
        account: AccountPublicKey(Word::from(1).0),
        vm_version: ae_vm_aevm::opcode::Version::from_id(row["vm"].as_u64().unwrap() as u32)
            .unwrap(),
        context: Arc::new(context(row)),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    }
}
fn unsigned(n: &Unsigned) -> BigInt {
    BigUint::from_bytes_be(n.as_bytes()).into()
}
fn snapshot<A: ae_trie::NodeReader, C: ae_trie::NodeReader>(s: &State<A, C>) -> J {
    snapshot_keys(s, &[1, 2, 3])
}
fn snapshot_keys<A: ae_trie::NodeReader, C: ae_trie::NodeReader>(
    s: &State<A, C>,
    keys: &[u64],
) -> J {
    let accounts = keys
        .iter()
        .copied()
        .filter_map(|n| {
            s.trees
                .accounts
                .lookup(AccountPublicKey(Word::from(n).0))
                .unwrap()
        })
        .map(|a| {
            map(vec![
                ("key", bin(a.public_key().0)),
                ("encoded", bin(a.serialize().unwrap())),
            ])
        })
        .collect();
    let events = s
        .events
        .iter()
        .map(|e| {
            let (name, info) = match &e.event {
                Event::Spend { from, to, amount } => (
                    tuple(vec![
                        atom("spend"),
                        tuple(vec![bin(from.0), bin(to.0), int(unsigned(amount))]),
                    ]),
                    b"Spend.amount".as_slice(),
                ),
                Event::FeeDebit { account, amount } => (
                    tuple(vec![
                        atom("delta"),
                        tuple(vec![bin(account.0), int(-unsigned(amount))]),
                    ]),
                    b"Chain.fee".as_slice(),
                ),
                e => panic!("{e:?}"),
            };
            let (kind, hash) = match &e.context {
                EventContext::None => (atom("non_tx"), bin([])),
                EventContext::Transaction { kind, hash } => {
                    assert_eq!(*kind, ae_tx::TransactionKind::Spend);
                    (atom("spend_tx"), bin(hash.0))
                }
            };
            tuple(vec![
                name,
                map(vec![("info", bin(info)), ("type", kind), ("tx_hash", hash)]),
            ])
        })
        .collect();
    map(vec![
        ("root", bin(s.trees.roots().hash().0)),
        ("accounts", list(accounts)),
        ("events", list(events)),
    ])
}
#[test]
fn actual_spend_construction_application_events_and_roots_match_original() {
    let rows = SPENDS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1024);
    let host = Host::default();
    let mut counts = [0; 3];
    for row in rows {
        let s = state(row);
        let before = snapshot(&s);
        assert_eq!(before, logical(row["before"].clone()));
        let mut budget = Budget::new(Limits::default());
        let value: BigInt = row["amount"].as_str().unwrap().parse().unwrap();
        let id = Id::new(
            IdTag::Account,
            Word::from(row["recipient"].as_u64().unwrap()).0,
        );
        let prepared = host.prepare(id, &value, &s, &mut budget);
        let r = &row["result"];
        if r["kind"] == "native" {
            let native = match &r["reason"] {
                v if v == &atom("function_clause") => Native::ChainException {
                    class: "error",
                    reason: "function_clause",
                },
                v if v == &tuple(vec![atom("badmatch"), atom("none")]) => Native::BadMatchNone,
                _ => panic!("{row}"),
            };
            assert_eq!(prepared, Err(Fault::Native(native)), "{row}");
            counts[2] += 1;
        } else {
            let tx = prepared.unwrap().unwrap();
            let expected = &r["value"][1];
            assert_eq!(tx.encode(), bytes(&expected[0]), "{row}");
            let outcome = host.apply(&tx, &s, &mut budget).unwrap();
            let expected = &expected[1][1];
            if expected[0] == atom("ok") {
                let next = outcome.unwrap();
                assert_eq!(snapshot(&next), logical(expected[1].clone()), "{row}");
                assert_eq!(
                    next.onchain.as_ref().map(|t| bin(t.roots().hash().0)),
                    s.onchain.as_ref().map(|t| bin(t.roots().hash().0))
                );
                counts[0] += 1;
            } else {
                assert_eq!(
                    outcome.err(),
                    Some(expected[1][1].as_str().unwrap().to_string()),
                    "{row}"
                );
                counts[1] += 1;
            }
        }
        assert_eq!(snapshot(&s), before);
    }
    assert_eq!(counts, [320, 544, 160]);
}
