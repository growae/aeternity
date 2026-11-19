// Unchanged original aesc_channels/aesc_state_tree at fa2a6d7.
use ae_protocol::Protocol;
use ae_state::{
    accounts::{Account, Authorization},
    channels::{Channel, Channels, Delegates, Error, Nonce, Role, channel_id},
};
use ae_trie::{MemoryStore, NodeReader, NodeWriter};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};
use serde_json::{Value as J, json};
use std::sync::LazyLock;
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J = serde_json::from_str(include_str!("fixtures/erlang_channel_state.json")).unwrap();
    assert_eq!(p["format"], "interned-json-v1");
    fn expand(v: &J, pool: &[J], bound: usize) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                let i = m["$ref"].as_u64().unwrap() as usize;
                assert!(i < bound);
                expand(&pool[i], pool, i)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), expand(v, pool, bound)))
                    .collect(),
            ),
            J::Array(a) => J::Array(a.iter().map(|v| expand(v, pool, bound)).collect()),
            _ => v.clone(),
        }
    }
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
});
fn rows(j: &J) -> &[J] {
    j.as_array().unwrap()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn key(j: &J) -> [u8; 32] {
    bytes(j).try_into().unwrap()
}
fn small(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}
fn num(j: &J) -> Unsigned {
    Unsigned::from_decimal(j.as_str().unwrap()).unwrap()
}
fn dec(n: &Unsigned) -> String {
    let mut n = n.clone();
    let mut d = vec![];
    loop {
        let (q, r) = n.div_rem_u64(10).unwrap();
        d.push(char::from(b'0' + r as u8));
        n = q;
        if n == 0.into() {
            break;
        }
    }
    d.into_iter().rev().collect()
}
fn auth(a: Option<&Authorization>) -> J {
    a.map(|a|json!({"function":hex::encode(&a.function),"contract":hex::encode(a.contract.to_bytes())})).unwrap_or(J::Null)
}
fn ids() -> [Id; 4] {
    [
        Id::new(IdTag::Account, small(1)),
        Id::new(IdTag::Account, small(2)),
        Id::new(IdTag::Account, small(3)),
        Id::new(IdTag::Contract, small(1)),
    ]
}
fn project(c: &Channel) -> J {
    let roles = [None, Some(Role::Initiator), Some(Role::Responder)];
    json!({"key":hex::encode(c.public_key),"initiator":hex::encode(c.initiator.0),"responder":hex::encode(c.responder.0),
        "amount":dec(&c.channel_amount),"initiator_amount":dec(&c.initiator_amount),"responder_amount":dec(&c.responder_amount),"reserve":dec(&c.channel_reserve),
        "initiator_auth":auth(c.initiator_auth.as_ref()),"responder_auth":auth(c.responder_auth.as_ref()),"hash":hex::encode(&c.state_hash),"round":dec(&c.round),"solo_round":dec(&c.solo_round),
        "lock_period":dec(&c.lock_period),"locked_until":dec(&c.locked_until),"version":c.object_version,"active":c.is_active(),"closing":c.is_solo_closing(),"forced":c.is_last_state_forced(),
        "closed":([0,168299,168300,168309,168310,168311].into_iter().map(|h|c.is_solo_closed(&h.into())).chain([c.is_solo_closed(&Unsigned::from_decimal("1606938044258990275541962092341162602522202993782792835301376").unwrap())]).collect::<Vec<_>>()),
        "delegates":roles.map(|r|c.delegate_ids(r).ok().map(|v|v.iter().map(|id|hex::encode(id.to_bytes())).collect::<Vec<_>>())),
        "delegate_keys":roles.map(|r|c.delegate_pubkeys(r).ok().map(|v|v.iter().map(|k|hex::encode(k.0)).collect::<Vec<_>>())),
        "roles":ids().map(|id| match c.role(id) { Some(Role::Initiator)=>"initiator",Some(Role::Responder)=>"responder",None=>"none" }),
        "auth_for":ids().map(|id|match c.authorization(id) { Ok(a)=>json!({"ok":auth(a)}),Err(Error::NotParticipant)=>json!({"error":"id_is_not_a_participant"}),e=>panic!("{e:?}") }),
        "auth_keys":ids().map(|id|hex::encode(c.auth_store_key(id).0)),"encoded":c.serialize().ok().map(hex::encode),"client":c.for_client().ok() })
}
fn account(n: u64, ga: bool) -> Account {
    let mut a = Account::new(AccountPublicKey(small(n)), 1000.into());
    if ga {
        a.attach_authorization(Id::new(IdTag::Contract, small(n + 10)), small(n + 20))
            .unwrap();
    }
    a
}
fn delegates(p: u32) -> Delegates {
    let ids = |ns: &[u64]| {
        ns.iter()
            .map(|n| Id::new(IdTag::Account, small(*n)))
            .collect()
    };
    if p >= 5 {
        Delegates::Separate {
            initiator: ids(&[3, 3]),
            responder: ids(&[4, 3]),
        }
    } else {
        Delegates::Shared(ids(&[3, 4, 3]))
    }
}
fn new(p: u32, a: u64, n: Nonce<'_>, hash: Vec<u8>) -> Channel {
    Channel::new(
        &account(1, a & 1 == 1),
        123.into(),
        &account(2, a & 2 == 2),
        456.into(),
        7.into(),
        delegates(p),
        hash,
        10.into(),
        n,
        Protocol::try_from(p).unwrap(),
        1.into(),
    )
    .unwrap()
}
#[test]
fn channel_construction_hashes_and_client_fields_match_all_historical_formats() {
    assert_eq!(rows(&DATA["channels"]).len(), 480);
    for r in rows(&DATA["channels"]) {
        let n = &r["nonce"];
        let number = n["number"].as_str().map(|_| num(&n["number"]));
        let nonce = match &number {
            Some(n) => Nonce::Number(n),
            None => Nonce::Hash(key(&n["hash"])),
        };
        let c = new(
            r["protocol"].as_u64().unwrap() as u32,
            r["auth"].as_u64().unwrap(),
            nonce,
            bytes(&r["hash"]),
        );
        assert_eq!(project(&c), r["channel"], "constructor {r}");
    }
    for r in rows(&DATA["constructors"]) {
        let p = r["protocol"].as_u64().unwrap() as u32;
        let wrong = r["wrong_delegates"].as_bool().unwrap();
        let pow = |n: usize| {
            let mut b = vec![0; n / 8 + 1];
            b[0] = 1 << (n % 8);
            Unsigned::from_field_bytes(&b).unwrap()
        };
        let c = Channel::new(
            &account(1, true),
            pow(200),
            &account(if r["same"].as_bool().unwrap() { 1 } else { 2 }, true),
            pow(201),
            pow(202),
            delegates(if wrong { if p >= 5 { 1 } else { 5 } } else { p }),
            small(0).to_vec(),
            pow(203),
            Nonce::Number(&pow(400)),
            Protocol::try_from(p).unwrap(),
            pow(204),
        );
        if r["result"].is_null() {
            assert_eq!(c, Err(Error::InvalidDelegates));
        } else {
            assert_eq!(project(&c.unwrap()), r["result"]);
        }
    }
    for r in rows(&DATA["hashes"]) {
        let n = &r["nonce"];
        let number = n["number"].as_str().map(|_| num(&n["number"]));
        let nonce = match &number {
            Some(n) => Nonce::Number(n),
            None => Nonce::Hash(key(&n["hash"])),
        };
        assert_eq!(
            hex::encode(
                channel_id(
                    AccountPublicKey(small(1)),
                    nonce,
                    AccountPublicKey(small(2))
                )
                .0
            ),
            r["result"]
        );
    }
}
#[test]
fn channel_decoding_retains_version_specific_validation_and_canonical_bytes() {
    assert_eq!(rows(&DATA["decode"]).len(), 215);
    for r in rows(&DATA["decode"]) {
        let got = Channel::deserialize(small(9), &bytes(&r["bytes"]));
        if r["result"].is_null() {
            assert!(got.is_err(), "{}", r["bytes"]);
        } else {
            assert_eq!(project(&got.unwrap()), r["result"], "{}", r["bytes"]);
        }
    }
}
fn step(c: &mut Channel, op: &J, height: &Unsigned) {
    let n = |i: usize| op[i].as_u64().unwrap();
    match op[0].as_str().unwrap() {
        "force" => c.force_progress(
            small(n(3)).to_vec(),
            n(2).into(),
            [300.into(), 279.into()],
            height,
            op[1].as_bool().unwrap(),
        ),
        "close" => c.close_solo(
            [300.into(), 279.into()],
            height,
            op[1]
                .as_bool()
                .unwrap()
                .then(|| (n(2).into(), small(n(3)).to_vec())),
        ),
        "snapshot" => c.snapshot(n(1).into(), small(n(2)).to_vec()),
        "deposit" => c.deposit(&n(1).into(), n(2).into(), small(n(3)).to_vec()),
        "withdraw" => c
            .withdraw(&n(1).into(), n(2).into(), small(n(3)).to_vec())
            .unwrap(),
        "delegates" => c.set_delegates(
            &[AccountPublicKey(small(8)); 2],
            &[AccountPublicKey(small(9))],
        ),
        _ => panic!(),
    }
}
#[test]
fn channel_dispute_round_and_lock_transitions_match_height_boundary() {
    assert_eq!(rows(&DATA["traces"]).len(), 240);
    for r in rows(&DATA["traces"]) {
        let p = r["protocol"].as_u64().unwrap() as u32;
        let a = r["auth"].as_u64().unwrap();
        let h = num(&r["height"]);
        let mut c = new(p, a, Nonce::Number(&1.into()), small(0).to_vec());
        c.lock_period = r["lock"].as_u64().unwrap().into();
        if r["closing"].as_bool().unwrap() {
            c.close_solo([300.into(), 279.into()], &h, None);
        }
        assert_eq!(project(&c), r["initial"]);
        for s in rows(&r["steps"]) {
            step(&mut c, &s["op"], &h);
            assert_eq!(
                project(&c),
                s["channel"],
                "p={p} auth={a} height={h:?} op={}",
                s["op"]
            );
        }
    }
}
fn tree_op<S: NodeWriter>(t: &mut Channels<S>, r: &J) -> Result<(), Error> {
    match r["op"].as_str().unwrap() {
        "enter" => t.enter(
            &Channel::deserialize(key(&r["input"]["key"]), &bytes(&r["input"]["encoded"])).unwrap(),
        ),
        "delete" => t.delete(key(&r["input"])),
        _ => panic!(),
    }
}
fn tree_project<S: NodeReader>(t: &Channels<S>) -> J {
    let mut entries = t
        .tree()
        .iter()
        .map(|r| {
            let (k, v) = r.unwrap();
            let k = k
                .as_slice()
                .chunks_exact(2)
                .map(|p| (p[0] << 4) | p[1])
                .collect::<Vec<_>>();
            let ae_trie::Value::Bytes(v) = v else {
                panic!()
            };
            json!([hex::encode(k), hex::encode(v)])
        })
        .collect::<Vec<_>>();
    entries.reverse();
    let keys = [
        new(1, 0, Nonce::Number(&1.into()), small(1).to_vec()).public_key,
        new(3, 1, Nonce::Number(&2.into()), small(3).to_vec()).public_key,
        new(5, 3, Nonce::Number(&3.into()), small(5).to_vec()).public_key,
        small(99),
    ];
    json!({"entries":entries,"root":t.root_hash().map(|h|hex::encode(h.0)),"encoded":hex::encode(t.to_binary_without_backend().unwrap()),"lookups":keys.map(|k|t.lookup(k).unwrap().map(|c|project(&c)))})
}
#[test]
fn channel_tree_roots_import_export_and_replacement_match_original() {
    let mut t = Channels::default();
    for r in rows(&DATA["tree"]) {
        tree_op(&mut t, r).unwrap();
        assert_eq!(tree_project(&t), r["state"]);
        let imported = Channels::from_binary_without_backend(
            &t.to_binary_without_backend().unwrap(),
            MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(tree_project(&imported), r["state"]);
    }
    for r in rows(&DATA["imports"]) {
        let got =
            Channels::from_binary_without_backend(&bytes(&r["bytes"]), MemoryStore::default());
        if r["result"].is_null() {
            assert!(got.is_err());
        } else {
            let t = got.unwrap();
            assert_eq!(
                json!({"encoded":hex::encode(t.to_binary_without_backend().unwrap()),"root":t.root_hash().map(|h|hex::encode(h.0))}),
                r["result"]
            );
        }
    }
}
#[test]
fn channel_storage_failures_keep_roots_unchanged_and_can_be_retried() {
    use std::{cell::Cell, rc::Rc};
    #[derive(Clone)]
    struct Store {
        inner: MemoryStore,
        count: Rc<Cell<usize>>,
        fail: Rc<Cell<Option<usize>>>,
    }
    impl Store {
        fn tick(&self) -> Result<(), ae_trie::Error> {
            let n = self.count.get();
            self.count.set(n + 1);
            if self.fail.get() == Some(n) {
                Err(ae_trie::Error::Backend("channel injected failure".into()))
            } else {
                Ok(())
            }
        }
    }
    impl NodeReader for Store {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.tick()?;
            self.inner.node(h)
        }
    }
    impl NodeWriter for Store {
        fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            self.tick()?;
            self.inner.put_node(h, b)
        }
    }
    let mut base = Channels::default();
    let mut failures = 0;
    for r in rows(&DATA["tree"]) {
        let build = || {
            let store = Store {
                inner: base.tree().store().clone(),
                count: Rc::new(Cell::new(0)),
                fail: Rc::new(Cell::new(None)),
            };
            let count = store.count.clone();
            let fail = store.fail.clone();
            let t = Channels::from_root(base.root_hash(), store).unwrap();
            count.set(0);
            (t, count, fail)
        };
        let (mut success, count, _) = build();
        tree_op(&mut success, r).unwrap();
        let operations = count.get();
        for n in 0..operations {
            let (mut t, count, fail) = build();
            let before = t.root_hash();
            fail.set(Some(n));
            let e = tree_op(&mut t, r).unwrap_err();
            assert_eq!(
                e,
                Error::Tree(ae_trie::Error::Backend("channel injected failure".into()))
            );
            assert_eq!(t.root_hash(), before);
            fail.set(None);
            count.set(0);
            tree_op(&mut t, r).unwrap();
            assert_eq!(tree_project(&t), r["state"]);
            failures += 1;
        }
        tree_op(&mut base, r).unwrap();
    }
    let mut read_failures = 0;
    for op in 0..3 {
        let build = || {
            let store = Store {
                inner: base.tree().store().clone(),
                count: Rc::new(Cell::new(0)),
                fail: Rc::new(Cell::new(None)),
            };
            let count = store.count.clone();
            let fail = store.fail.clone();
            let t = Channels::from_root(base.root_hash(), store).unwrap();
            count.set(0);
            (t, count, fail)
        };
        let read = |t: &Channels<Store>| -> Result<J, Error> {
            match op {
                0 => Ok(t
                    .lookup(new(1, 0, Nonce::Number(&1.into()), small(1).to_vec()).public_key)?
                    .map(|c| project(&c))
                    .unwrap_or(J::Null)),
                1 => Ok(t.lookup(small(99))?.map(|c| project(&c)).unwrap_or(J::Null)),
                _ => Ok(json!(hex::encode(t.to_binary_without_backend()?))),
            }
        };
        let (t, count, _) = build();
        let expected = read(&t).unwrap();
        let operations = count.get();
        assert!(operations > 0);
        for n in 0..operations {
            let (t, _, fail) = build();
            let before = t.root_hash();
            fail.set(Some(n));
            assert_eq!(
                read(&t).unwrap_err(),
                Error::Tree(ae_trie::Error::Backend("channel injected failure".into()))
            );
            assert_eq!(t.root_hash(), before);
            fail.set(None);
            assert_eq!(read(&t).unwrap(), expected);
            read_failures += 1;
        }
    }
    assert!(read_failures > 0);
    eprintln!("channel read failure prefixes: {read_failures}");
    assert!(failures > 0);
    eprintln!("channel mutation failure prefixes: {failures}");
    let mut c = new(5, 3, Nonce::Number(&1.into()), small(0).to_vec());
    let before = c.clone();
    assert_eq!(
        c.withdraw(&1000.into(), 2.into(), vec![]),
        Err(Error::AmountUnderflow)
    );
    assert_eq!(c, before);
}
