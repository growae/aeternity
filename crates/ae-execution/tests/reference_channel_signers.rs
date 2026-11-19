use ae_execution::transactions::{self, SignatureError, SignatureLimits};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::{
    accounts::Account,
    channels::{Channel, Channels, Delegates, Nonce},
};
use ae_tx::{CanonicalTransaction, Transaction};
use ae_types::AccountPublicKey;
use serde_json::{Value as J, json};
use std::sync::LazyLock;
fn unpack(text: &str) -> J {
    fn u(v: &J, pool: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                u(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            J::Object(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), u(v, pool))).collect()),
            J::Array(a) => J::Array(a.iter().map(|v| u(v, pool)).collect()),
            _ => v.clone(),
        }
    }
    let v: J = serde_json::from_str(text).unwrap();
    if v["format"] == "interned-json-v1" {
        u(&v["root"], v["values"].as_array().unwrap())
    } else {
        v
    }
}
static DATA: LazyLock<J> = LazyLock::new(|| {
    unpack(include_str!(
        "../../ae-tx/tests/fixtures/erlang_channel_wire.json"
    ))
});
fn key(n: u8) -> AccountPublicKey {
    let mut b = [0; 32];
    b[31] = n;
    AccountPublicKey(b)
}
fn channels() -> Channels {
    let c = Channel::new(
        &Account::new(key(1), 100.into()),
        100.into(),
        &Account::new(key(2), 200.into()),
        200.into(),
        7.into(),
        Delegates::Separate {
            initiator: vec![],
            responder: vec![],
        },
        vec![0; 32],
        10.into(),
        Nonce::Number(&1.into()),
        Protocol::Iris,
        1.into(),
    )
    .unwrap();
    let mut t = Channels::default();
    t.enter(&c).unwrap();
    t
}
fn outcome(r: Result<Vec<AccountPublicKey>, SignatureError>) -> J {
    let value = match r {
        Ok(keys) => {
            json!({"tuple":[{"atom":"ok"},{"list":keys.iter().map(|k|json!({"binary":hex::encode(k.0)})).collect::<Vec<_>>()}]})
        }
        Err(SignatureError::ChannelNotFound) => {
            json!({"tuple":[{"atom":"error"},{"atom":"channel_not_found"}]})
        }
        Err(SignatureError::ChannelDoesNotExist) => {
            json!({"tuple":[{"atom":"error"},{"atom":"channel_does_not_exist"}]})
        }
        Err(SignatureError::InnerIncorrectlySigned) => {
            json!({"tuple":[{"atom":"error"},{"atom":"inner_tx_incorrectly_signed"}]})
        }
        Err(e) => panic!("unexpected signer result {e:?}"),
    };
    json!({"ok":value})
}
#[test]
fn channel_and_wrapper_signer_callbacks_match_original_state_lookups() {
    let available = channels();
    let empty = Channels::default();
    for r in DATA["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .chain(DATA["wrappers"].as_array().unwrap())
    {
        if !r["result"]["ok"].is_object() {
            continue;
        }
        let t = Transaction::decode(&hex::decode(r["input"].as_str().unwrap()).unwrap()).unwrap();
        for (trees, expected) in [&empty, &available]
            .into_iter()
            .zip(r["result"]["ok"]["signers"].as_array().unwrap())
        {
            assert_eq!(
                outcome(transactions::signer_keys(
                    t.view(),
                    &NetworkProfile::mainnet(),
                    SignatureLimits::default(),
                    trees
                )),
                *expected,
                "{}",
                r["label"]
            );
        }
    }
}

fn crypto_key(n: u8) -> AccountPublicKey {
    ae_crypto::Ed25519SecretKey::from_seed(&key(n).0).public_key()
}
fn crypto_channels(same: bool) -> Channels {
    let c = Channel::new(
        &Account::new(crypto_key(1), 100.into()),
        100.into(),
        &Account::new(crypto_key(if same { 1 } else { 2 }), 200.into()),
        200.into(),
        7.into(),
        Delegates::Separate {
            initiator: vec![],
            responder: vec![],
        },
        vec![0; 32],
        10.into(),
        Nonce::Number(&1.into()),
        Protocol::Iris,
        1.into(),
    )
    .unwrap();
    let mut t = Channels::default();
    t.enter(&c).unwrap();
    t
}
fn verification(r: Result<(), SignatureError>) -> J {
    match r {
        Ok(()) => json!({"ok":{"atom":"ok"}}),
        Err(SignatureError::CheckFailed) => {
            json!({"ok":{"tuple":[{"atom":"error"},{"atom":"signature_check_failed"}]}})
        }
        Err(SignatureError::VerificationFailed) => {
            json!({"ok":{"tuple":[{"atom":"error"},{"atom":"signature_verification_failed"}]}})
        }
        Err(e) => panic!("unexpected verification {e:?}"),
    }
}
#[test]
fn channel_signatures_match_original_network_protocol_wrappers_and_authentication() {
    let data: J = unpack(include_str!("fixtures/erlang_channel_signatures.json"));
    let available = crypto_channels(false);
    let same = crypto_channels(true);
    let empty = Channels::default();
    for (index, r) in data["runs"].as_array().unwrap().iter().enumerate() {
        let s = ae_tx::SignedTransaction::<Transaction>::decode(
            &hex::decode(r["signed"]["binary"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        let tx = s.transaction().view();
        let protocol = Protocol::try_from(r["protocol"].as_u64().unwrap() as u32).unwrap();
        let channels = if r["same"] == true { &same } else { &available };
        for (trees, expected) in [&empty, channels]
            .into_iter()
            .zip(r["strict"].as_array().unwrap())
        {
            assert_eq!(
                verification(transactions::verify_with_channel_state(
                    tx,
                    s.signatures(),
                    &NetworkProfile::mainnet(),
                    protocol,
                    false,
                    SignatureLimits::default(),
                    trees
                )),
                *expected,
                "row {index} strict"
            );
        }
        for e in r["contextual"].as_array().unwrap() {
            let mut context = ae_execution::Context::transaction(50, protocol);
            context.payer = e["sponsored"].as_bool().unwrap().then(|| crypto_key(3));
            let authenticated: Vec<_> = e["authenticated"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| crypto_key(n.as_u64().unwrap() as u8))
                .collect();
            context.ga_authenticated = authenticated.iter().copied().collect();
            if context.ga_authenticated.len() == authenticated.len() {
                assert_eq!(
                    verification(transactions::verify_with_trees(
                        tx,
                        s.signatures(),
                        &NetworkProfile::mainnet(),
                        &context,
                        SignatureLimits::default(),
                        channels
                    )),
                    e["result"]
                );
            }
            assert_eq!(
                verification(transactions::verify_with_authenticated_ids(
                    tx,
                    s.signatures(),
                    &NetworkProfile::mainnet(),
                    &context,
                    &authenticated,
                    SignatureLimits::default(),
                    channels
                )),
                e["result"],
                "row {index} auth {} sponsored {}",
                e["authenticated"],
                e["sponsored"]
            );
        }
    }
}

#[test]
fn channel_signer_backend_failures_and_resource_exhaustion_are_retryable() {
    use ae_trie::{MemoryStore, NodeReader};
    use ae_types::Hash256;
    use std::{cell::Cell, rc::Rc};
    #[derive(Clone)]
    struct Reader {
        store: MemoryStore,
        calls: Rc<Cell<usize>>,
        fail: Rc<Cell<Option<usize>>>,
    }
    impl NodeReader for Reader {
        fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            let n = self.calls.get();
            self.calls.set(n + 1);
            if self.fail.get() == Some(n) {
                return Err(ae_trie::Error::Backend("channel fault".into()));
            }
            self.store.node(hash)
        }
    }
    let mut available = channels();
    for n in 2..34 {
        let c = Channel::new(
            &Account::new(key(1), 100.into()),
            100.into(),
            &Account::new(key(2), 200.into()),
            200.into(),
            7.into(),
            Delegates::Separate {
                initiator: vec![],
                responder: vec![],
            },
            vec![0; 32],
            10.into(),
            Nonce::Number(&n.into()),
            Protocol::Iris,
            1.into(),
        )
        .unwrap();
        available.enter(&c).unwrap();
    }
    let calls = Rc::new(Cell::new(0));
    let fail = Rc::new(Cell::new(None));
    let root = available.root_hash();
    let view = Channels::from_root(
        root,
        Reader {
            store: available.tree().store().clone(),
            calls: calls.clone(),
            fail: fail.clone(),
        },
    )
    .unwrap();
    let mut checked = 0;
    for r in DATA["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .chain(DATA["wrappers"].as_array().unwrap())
    {
        if !r["result"]["ok"].is_object() {
            continue;
        }
        let t = Transaction::decode(&hex::decode(r["input"].as_str().unwrap()).unwrap()).unwrap();
        if t.leaf().kind() != ae_tx::TransactionKind::ChannelDeposit {
            continue;
        }
        calls.set(0);
        let expected = transactions::signer_keys(
            t.view(),
            &NetworkProfile::mainnet(),
            SignatureLimits::default(),
            &view,
        );
        let count = calls.get();
        for n in 0..count {
            calls.set(0);
            fail.set(Some(n));
            let e = transactions::signer_keys(
                t.view(),
                &NetworkProfile::mainnet(),
                SignatureLimits::default(),
                &view,
            )
            .unwrap_err();
            assert!(matches!(e, SignatureError::Channel(_)) && e.is_operational());
            calls.set(0);
            let e = transactions::verify_with_channel_state(
                t.view(),
                &[],
                &NetworkProfile::mainnet(),
                Protocol::Iris,
                false,
                SignatureLimits::default(),
                &view,
            )
            .unwrap_err();
            assert!(e.is_operational());
            calls.set(0);
            fail.set(None);
            assert_eq!(
                transactions::signer_keys(
                    t.view(),
                    &NetworkProfile::mainnet(),
                    SignatureLimits::default(),
                    &view
                ),
                expected
            );
            assert_eq!(view.root_hash(), root);
            checked += 1;
        }
    }
    assert!(checked > 20);
    println!("Channel signer backend failure prefixes: {checked}");
    let data = unpack(include_str!("fixtures/erlang_channel_signatures.json"));
    for r in data["runs"].as_array().unwrap().iter().filter(|r| {
        r["protocol"] == 5
            && r["mode"] == "plain"
            && r["inner_mode"] == "inner"
            && r["shape"] == "payer"
    }) {
        let s = ae_tx::SignedTransaction::<Transaction>::decode(
            &hex::decode(r["signed"]["binary"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        let cs = crypto_channels(false);
        for limits in [
            SignatureLimits {
                max_layers: 0,
                ..Default::default()
            },
            SignatureLimits {
                max_encoded_bytes: 0,
                ..Default::default()
            },
        ] {
            assert_eq!(
                transactions::verify_with_channel_state(
                    s.transaction().view(),
                    s.signatures(),
                    &NetworkProfile::mainnet(),
                    Protocol::Iris,
                    false,
                    limits,
                    &cs
                ),
                Err(SignatureError::ResourceLimit)
            );
        }
        transactions::verify_with_channel_state(
            s.transaction().view(),
            s.signatures(),
            &NetworkProfile::mainnet(),
            Protocol::Iris,
            false,
            SignatureLimits::default(),
            &cs,
        )
        .unwrap();
    }
}
