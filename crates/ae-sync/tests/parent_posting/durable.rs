use super::*;
use ae_chain::repository::Identity;
use ae_storage::{Database, parent_posts as journal};
use ae_sync::parent_pins::posting::Journal;
use ae_types::Hash256;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
struct Store {
    path: PathBuf,
    db: Option<Arc<Database>>,
}
fn identity() -> Identity {
    Identity::new(
        &ae_protocol::NetworkProfile::mainnet(),
        ae_state::genesis::header(ae_state::genesis::Network::Mainnet)
            .unwrap()
            .hash()
            .unwrap(),
        Hash256::ZERO,
    )
}
impl Store {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ae-parent-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let db = Arc::new(Database::open(&path, identity()).unwrap());
        Self { path, db: Some(db) }
    }
    fn db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }
    fn reopen(&mut self) {
        self.db.take();
        self.db = Some(Arc::new(Database::open(&self.path, identity()).unwrap()));
    }
    fn records(&self) -> Vec<journal::Record> {
        self.db()
            .parent_posts()
            .unwrap()
            .records(Default::default())
            .unwrap()
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        self.db.take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn service(
    db: Arc<Database>,
    nodes: Vec<Node>,
    limits: journal::Limits,
    signing: Option<Signing>,
) -> Service {
    Service::spawn_journalled(
        nodes[0].chain(),
        nodes,
        config(),
        signing,
        Journal {
            database: db,
            network: b"parent_network".to_vec(),
            limits,
        },
    )
    .unwrap()
}
fn first_two(l: &TcpListener, hash: Option<&str>) -> Vec<u8> {
    let mut nonce = accept(l);
    assert!(read_request(&mut nonce).0.contains("/next-nonce "));
    reply(&mut nonce, 200, &json!({"next_nonce":1}));
    drop(nonce);
    let mut broadcast = accept(l);
    let (head, body) = read_request(&mut broadcast);
    assert!(head.starts_with("POST /v3/transactions "));
    let (ae_codec::api::ApiType::Transaction, tx) =
        ae_codec::api::decode(body["tx"].as_str().unwrap()).unwrap()
    else {
        panic!()
    };
    if let Some(hash) = hash {
        reply(&mut broadcast, 200, &json!({"tx_hash":hash}));
    }
    tx
}
#[test]
fn original_ae_and_wallet_callbacks_preserve_bytes_with_durable_intents() {
    let mut stores = vec![];
    original_callbacks(|nodes, config, signing| {
        let store = Store::new();
        let result = Service::spawn_journalled(
            nodes[0].chain(),
            nodes,
            config,
            signing,
            Journal {
                database: store.db(),
                network: b"parent_network".to_vec(),
                limits: Default::default(),
            },
        )
        .unwrap();
        assert!(result.client().durable_posts());
        stores.push(store);
        result
    });
    let data: J = serde_json::from_str(include_str!("../fixtures/hc_post.json")).unwrap();
    for (store, row) in stores.iter_mut().zip(data["rows"].as_array().unwrap()) {
        store.reopen();
        let records = store.records();
        let sent = row["requests"].as_array().unwrap().last().is_some_and(|r| {
            String::from_utf8(bytes(&r["url"]))
                .unwrap()
                .ends_with("/v3/transactions")
                || r.get("body")
                    .is_some_and(|v| term(v)["method"] == "sendrawtransaction")
        });
        assert_eq!(records.len(), usize::from(sent), "{}", row["name"]);
        if sent {
            let record = &records[0];
            assert_eq!(record.response, row["result"].get("hash").map(bytes));
            assert!(!record.endpoint.contains(&b'@'));
            if row["chain"] == "doge" {
                assert_eq!(record.transaction, b"signed");
            }
        }
    }
}
#[test]
fn recovered_unknown_blocks_changed_endpoints_fees_and_keys_until_explicit_response() {
    let mut store = Store::new();
    let l = listener();
    let endpoint = Node::new(
        Chain::Aeternity,
        &format!("http://{}", l.local_addr().unwrap()),
        Some((b"user".to_vec(), b"synthetic secret".to_vec())),
    )
    .unwrap();
    let s = service(
        store.db(),
        vec![endpoint.clone()],
        Default::default(),
        Some(signing()),
    );
    let old = s.client();
    let thread = thread::spawn(move || {
        let tx = first_two(&l, None);
        (l, tx)
    });
    let outcome = wait(&mut old.post(post()).unwrap());
    let Outcome::Unknown { submission, .. } = outcome else {
        panic!("{outcome:?}")
    };
    s.shutdown().unwrap();
    let (l, tx) = thread.join().unwrap();
    assert_eq!(tx, submission.transaction.as_ref());
    let original = store.records().pop().unwrap();
    assert_eq!(original.transaction, tx);
    assert!(original.response.is_none());
    assert_eq!(original.endpoint, endpoint.origin().as_str().as_bytes());
    store.reopen();
    let changed = listener();
    let replacement = service(
        store.db(),
        vec![node(&changed, Chain::Aeternity)],
        Default::default(),
        None,
    );
    let mut p = post();
    p.fee = 99.into();
    p.amount = 45.into();
    assert_eq!(
        wait(&mut replacement.client().post(p).unwrap()),
        Outcome::Unknown {
            submission: submission.clone(),
            reason: Failure::Recovered
        }
    );
    replacement.shutdown().unwrap();
    assert!(matches!(old.post(post()), Err(Error::Closed)));
    assert!(l.accept().is_err() && changed.accept().is_err());
    let mut writer = journal::Writer::start(store.db(), Default::default()).unwrap();
    let current = writer.get(&original.intent).unwrap().unwrap();
    writer
        .record_response(&current, b"observed-hash".to_vec())
        .unwrap();
    drop(writer);
    store.reopen();
    let replacement = service(
        store.db(),
        vec![node(&changed, Chain::Aeternity)],
        Default::default(),
        None,
    );
    assert_eq!(
        wait(&mut replacement.client().post(post()).unwrap()),
        Outcome::Submitted {
            hash: b"observed-hash".to_vec(),
            submission
        }
    );
    replacement.shutdown().unwrap();
    assert!(changed.accept().is_err());
}
#[test]
fn durable_intent_exists_before_http_and_duplicate_queue_never_broadcasts_twice() {
    let store = Store::new();
    let l = listener();
    let db = store.db();
    let s = service(
        store.db(),
        vec![node(&l, Chain::Aeternity)],
        Default::default(),
        Some(signing()),
    );
    let thread = thread::spawn(move || {
        let mut nonce = accept(&l);
        read_request(&mut nonce);
        reply(&mut nonce, 200, &json!({"next_nonce":1}));
        drop(nonce);
        let mut broadcast = accept(&l);
        let (_, body) = read_request(&mut broadcast);
        let (_, tx) = ae_codec::api::decode(body["tx"].as_str().unwrap()).unwrap();
        let records = db
            .parent_posts()
            .unwrap()
            .records(Default::default())
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].transaction, tx);
        assert!(records[0].response.is_none());
        reply(&mut broadcast, 200, &json!({"tx_hash":"accepted"}));
        l
    });
    let mut requests: Vec<_> = (0..8).map(|_| s.client().post(post()).unwrap()).collect();
    let outcomes: Vec<_> = requests.iter_mut().map(wait).collect();
    assert!(outcomes.iter().all(|o| o == &outcomes[0]));
    assert!(matches!(&outcomes[0],Outcome::Submitted {hash,..}if hash==b"accepted"));
    s.shutdown().unwrap();
    let l = thread.join().unwrap();
    assert!(l.accept().is_err());
    assert_eq!(
        store.records()[0].response.as_deref(),
        Some(b"accepted".as_slice())
    );
}
#[test]
fn exhausted_intent_storage_prevents_http_broadcast() {
    let store = Store::new();
    let l = listener();
    let mut w = journal::Writer::start(store.db(), Default::default()).unwrap();
    w.reserve(journal::Record {
        intent: journal::Intent {
            chain: journal::Chain::Aeternity,
            network: b"parent_network".to_vec(),
            account: AccountPublicKey([1; 32]),
            payload: vec![1],
        },
        endpoint: b"http://old.example/".to_vec(),
        transaction: vec![1],
        response: None,
    })
    .unwrap();
    drop(w);
    let s = service(
        store.db(),
        vec![node(&l, Chain::Aeternity)],
        journal::Limits {
            records: 1,
            ..Default::default()
        },
        Some(signing()),
    );
    let thread = thread::spawn(move || {
        let mut nonce = accept(&l);
        read_request(&mut nonce);
        reply(&mut nonce, 200, &json!({"next_nonce":1}));
        l
    });
    assert!(matches!(
        wait(&mut s.client().post(post()).unwrap()),
        Outcome::NotSubmitted(Failure::Journal(journal::Failure::Storage(
            ae_chain::repository::Error::ResourceLimit
        )))
    ));
    s.shutdown().unwrap();
    let l = thread.join().unwrap();
    assert!(l.accept().is_err());
    assert_eq!(store.records().len(), 1);
}
#[test]
fn response_storage_failure_preserves_unknown_and_recovery_does_not_repost() {
    let mut store = Store::new();
    let l = listener();
    let endpoint = node(&l, Chain::Aeternity);
    let s = service(
        store.db(),
        vec![endpoint.clone()],
        journal::Limits {
            bytes: 2048,
            ..Default::default()
        },
        Some(signing()),
    );
    let thread = thread::spawn(move || {
        let tx = first_two(&l, Some(&"h".repeat(3000)));
        (l, tx)
    });
    let outcome = wait(&mut s.client().post(post()).unwrap());
    assert!(matches!(
        outcome,
        Outcome::Unknown {
            reason: Failure::Journal(journal::Failure::Storage(
                ae_chain::repository::Error::ResourceLimit
            )),
            ..
        }
    ));
    s.shutdown().unwrap();
    let (l, tx) = thread.join().unwrap();
    let record = store.records().pop().unwrap();
    assert_eq!(record.transaction, tx);
    assert!(record.response.is_none());
    store.reopen();
    let s = service(
        store.db(),
        vec![endpoint],
        Default::default(),
        Some(signing()),
    );
    assert!(matches!(
        wait(&mut s.client().post(post()).unwrap()),
        Outcome::Unknown {
            reason: Failure::Recovered,
            ..
        }
    ));
    s.shutdown().unwrap();
    assert!(l.accept().is_err());
}
#[path = "durable/recovery.rs"]
mod recovery;
