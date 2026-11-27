use super::*;
use ae_chain::repository as repo;
use ae_codec::headers::{CommonHeader, Header, KeyHeader};
use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_storage::{Database, snapshots};
use repo::Read;
struct Temp(std::path::PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn durable_contract_pipeline_keeps_one_snapshot_until_response_release_and_never_writes() {
    let path =
        Temp(std::env::temp_dir().join(format!("ae-dry-run-pipeline-{}", std::process::id())));
    let row = selected_row();
    let trees = ae_state::Trees::deserialize_from_binary(
        &bytes(&row["seed"]["initial"]),
        ae_trie::MemoryStore::default(),
        ae_trie::MemoryStore::default(),
        Default::default(),
    )
    .unwrap();
    let roots = repo::Roots::of(&trees);
    let network = Arc::new(
        NetworkProfile::custom(
            b"ae_dry_run_pipeline".to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::Ceres,
            }],
        )
        .unwrap(),
    );
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
    let hash = Header::Key(header.clone()).hash().unwrap();
    let identity = repo::Identity::new(&network, hash, Hash256::ZERO);
    let db = Arc::new(Database::open(&path.0, identity.clone()).unwrap());
    db.commit(
        repo::ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &header,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let before = db.view().unwrap().source().prefix(&[], 10000).unwrap();
    let snapshots = snapshots::Owner::start(
        db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            maximum_node_bytes: 1 << 20,
            ..Default::default()
        },
    )
    .unwrap();
    let weak = Arc::downgrade(&db);
    let c = config();
    let (client, owner) = pool(&c);
    let captured_identity = identity.clone();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            let captured = snapshots.view().map_err(|_| repo::Error::ResourceLimit)?;
            let snapshot = repo::execution::snapshot(
                captured.value.clone(),
                &captured_identity,
                &network,
                Default::default(),
            )?;
            let source = pipeline::repository::Repository::new(snapshot, network.clone(), 1024)?;
            Ok(configured(&FIXTURE["profile"], |base| {
                capture_with(
                    source,
                    row,
                    &FIXTURE["profile"],
                    &ae_execution::transactions::Config {
                        network: &network,
                        ..*base
                    },
                )
            }))
        })
        .unwrap();
    drop(db);
    let mut held = ticket.wait_retained().await.unwrap();
    let response = held.value.response.as_ref().unwrap();
    assert_eq!(response.status, 200);
    // This local custom genesis is height zero. Reuse the original contract
    // result/gas/nonce/return/event fixture, adjusting only that known height.
    // It is not an independently captured full durable-chain dry-run root.
    let mut expected = row["result"]["response"]["body"].clone();
    for result in expected["results"].as_array_mut().unwrap() {
        result["call_obj"]["height"] = 0.into();
    }
    assert_eq!(
        serde_json::from_slice::<J>(&response.body).unwrap(),
        expected
    );
    assert_eq!(
        repo::Roots::of(&held.value.selected.as_ref().unwrap().trees),
        roots
    );
    let snapshot = held.value.captured.as_ref().unwrap().source.snapshot();
    assert_eq!(
        snapshot.repository().source().prefix(&[], 10000).unwrap(),
        before
    );
    assert_eq!(client.available().0, 0);
    assert!(weak.upgrade().is_some());
    // Selected views must independently retain their reader even if the
    // preparation source is discarded before a response is sent.
    held.value.captured = None;
    assert!(weak.upgrade().is_some());
    drop(held);
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
