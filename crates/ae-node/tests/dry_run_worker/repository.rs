use super::*;
use ae_chain::{dry_run::Selector, repository as repo};
use ae_codec::headers::{CommonHeader, KeyHeader};
use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_storage::{Database, snapshots};
use ae_types::Hash256;
use repo::Read;
use std::{path::PathBuf, sync::Arc};

struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn real_repository_reader_and_database_live_through_retained_worker_result() {
    let path =
        Temp(std::env::temp_dir().join(format!("ae-dry-run-executable-{}", std::process::id())));
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "spend_success" && r["input"]["protocol"] == 6)
        .unwrap();
    let trees = ae_state::Trees::deserialize_from_binary(
        &bytes(&row["seed"]["initial"]),
        ae_trie::MemoryStore::default(),
        ae_trie::MemoryStore::default(),
        Default::default(),
    )
    .unwrap();
    let roots = repo::Roots::of(&trees);
    // This is a tiny custom genesis for lifetime/isolation checks. It does not
    // claim to be an independently validated mainnet block or execution vector.
    let network = NetworkProfile::custom(
        b"ae_worker_snapshot".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Ceres,
        }],
    )
    .unwrap();
    let header = KeyHeader {
        common: CommonHeader {
            version: 6,
            height: 0,
            previous_hash: [0; 32],
            previous_key_hash: [0; 32],
            state_hash: roots.state.hash().0,
        },
        miner: [2; 32],
        beneficiary: [3; 32],
        target: 0x1f012345,
        seal: [0; 42],
        nonce: 0,
        time: 0,
        info: Some([0; 4]),
        hole: false,
        end_of_epoch: false,
    };
    let hash = ae_codec::headers::Header::Key(header.clone())
        .hash()
        .unwrap();
    let identity = repo::Identity::new(&network, hash, Hash256::ZERO);
    let database = Arc::new(Database::open(&path.0, identity.clone()).unwrap());
    let update = repo::ledger::bootstrap(
        &database.view().unwrap(),
        &network,
        &header,
        &trees,
        Default::default(),
    )
    .unwrap();
    database.commit(update).unwrap();
    let before = database
        .view()
        .unwrap()
        .source()
        .prefix(&[], 10000)
        .unwrap();
    let snapshots = snapshots::Owner::start(
        database.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            maximum_node_bytes: 1 << 20,
            ..Default::default()
        },
    )
    .unwrap();
    let captured = snapshots.view().unwrap();
    let snapshot = repo::execution::snapshot(
        captured.value.clone(),
        &identity,
        &network,
        Default::default(),
    )
    .unwrap();
    let view = snapshot.dry_run_view(&Selector::Top).unwrap();
    assert_eq!(repo::Roots::of(&view.trees), roots);
    let expected = ae_execution::dry_run::setup(
        (*view.trees).clone(),
        &view.context,
        &[],
        &Default::default(),
    )
    .unwrap();
    let expected_roots = repo::Roots::of(expected.trees());
    drop(expected);
    let weak = Arc::downgrade(&database);
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let config = ae_execution::transactions::Config::primitives(&network);
    let ticket = worker::submit(
        &client,
        p,
        view,
        worker::Request {
            inputs: vec![],
            funding: vec![],
            options: Default::default(),
        },
        parameters(&config, None),
        Timeout::After(Duration::from_secs(10)),
    )
    .unwrap();
    drop((snapshot, captured, snapshots, database));
    assert!(weak.upgrade().is_some());
    let retained = ticket.wait_retained().await.unwrap();
    assert!(weak.upgrade().is_some());
    let worker::Reply::Done(result) =
        worker::decode_reply(retained.value.result.as_ref().unwrap()).unwrap()
    else {
        panic!()
    };
    assert_eq!(result.roots, expected_roots);
    assert_eq!(repo::Roots::of(&retained.value.broker.view.trees), roots);
    assert_eq!(client.available().0, 0);
    drop(retained);
    assert!(weak.upgrade().is_none());
    assert_eq!(client.available().0, 1);
    owner.shutdown().await;
    let reopened = Database::open(&path.0, identity).unwrap();
    assert_eq!(
        reopened
            .view()
            .unwrap()
            .source()
            .prefix(&[], 10000)
            .unwrap(),
        before
    );
}
