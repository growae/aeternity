use super::*;
use ae_sync::chain::{Config, Direction, Download, Error, Importer, Outcome};

async fn receive(
    importer: &Importer,
    app: &App,
    client: &net::Client,
    remote: &net::Client,
    events: &mut net::Events,
    key: &ae_chain::block::Block,
    micros: &[ae_chain::block::Block],
) -> Download {
    let hash = key.hash().unwrap();
    let download = importer.download(&app.network, client.clone(), hash, Direction::Backward);
    let serve = async {
        let request = generation_request(events).await;
        assert_eq!(
            request.message().fields()["hash"],
            Value::Binary(hash.0.to_vec())
        );
        remote
            .send(
                &Message::response_ok(
                    &common::import::message(key, micros, Direction::Backward),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap()
            .await
            .unwrap();
    };
    let (download, ()) = within(async { tokio::join!(download, serve) }).await;
    download.unwrap()
}
#[tokio::test]
async fn staged_out_of_order_receipts_keep_quota_until_ordered_real_import() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let _ = response(&remote, &ping(&node, 1, false)).await;
    let client = admitted_client(&app, remote_peer()).await;
    let limits = message::Limits::default();
    let weight = limits.body_bytes + limits.work_bytes;
    let importer = download_importer(
        &node,
        Config {
            jobs: 2,
            bytes: weight * 2,
            ..Default::default()
        },
    );
    let blocks = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
    assert!(matches!(blocks[1], ae_chain::block::Block::Micro(_)));
    assert!(matches!(blocks[2], ae_chain::block::Block::Key(_)));
    let before = node.db.view().unwrap().status();
    // Key 2 and its preceding micro arrive before key 1. Buffering either is
    // not import, admission or progress in the actual database.
    let second = receive(
        &importer,
        &app,
        &client,
        &remote,
        &mut events,
        &blocks[2],
        &blocks[1..2],
    )
    .await;
    assert_eq!(second.requested(), blocks[2].hash().unwrap());
    assert_eq!(second.direction(), Direction::Backward);
    assert_eq!(second.peer(), client.peer());
    assert_eq!(second.generation(), client.generation());
    assert_eq!(importer.available(), (1, weight));
    assert_eq!(node.db.view().unwrap().status(), before);
    let first = receive(
        &importer,
        &app,
        &client,
        &remote,
        &mut events,
        &blocks[0],
        &[],
    )
    .await;
    assert_eq!(importer.available(), (0, 0));
    assert_eq!(node.db.view().unwrap().status(), before);
    let failure = importer
        .download(
            &app.network,
            client.clone(),
            blocks[0].hash().unwrap(),
            Direction::Backward,
        )
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::Busy);
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), generation_request(&mut events))
            .await
            .is_err()
    );
    assert!(matches!(
        importer.import(
            common::import::packet(&blocks[0], &[], Direction::Backward),
            blocks[0].hash().unwrap(),
            Direction::Backward,
            u64::MAX - 600_000
        ),
        Err(Error::Busy)
    ));
    let first = importer
        .import_download(&app.network, first, u64::MAX - 600_000)
        .await
        .unwrap();
    assert_eq!(first.accepted.len(), 1);
    assert!(matches!(
        first.accepted[0].outcome,
        Outcome::Committed { .. }
    ));
    assert_eq!(importer.available(), (1, weight));
    let second = importer
        .import_download(&app.network, second, u64::MAX - 600_000)
        .await
        .unwrap();
    assert_eq!(second.accepted.len(), 2);
    assert!(
        second
            .accepted
            .iter()
            .all(|a| matches!(a.outcome, Outcome::Committed { .. }))
    );
    assert_eq!(importer.available(), (2, weight * 2));
    let view = node.db.view().unwrap();
    assert_eq!(view.status().tip, Some(blocks[2].hash().unwrap()));
    for block in &blocks[..3] {
        assert_eq!(view.require_block(block.hash().unwrap()).unwrap(), *block);
    }
    drop(view);
    importer.stop();
    importer.join().await;
    remote.shutdown();
    within(driver).await.unwrap();
    stop(app).await;
}
#[tokio::test]
async fn buffered_receipts_refund_on_drop_foreign_importer_stop_and_stale_driver() {
    for mode in ["drop", "foreign", "stop", "stale", "owner_drop"] {
        let node = Node::new();
        let app = start(&node, config(), Default::default()).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let _ = response(&remote, &ping(&node, 1, false)).await;
        let client = admitted_client(&app, remote_peer()).await;
        let cfg = Config {
            jobs: 1,
            ..Default::default()
        };
        let available = (cfg.jobs, cfg.bytes);
        let importer = download_importer(&node, cfg);
        let before = node.db.view().unwrap().status();
        let blocks = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
        let download = receive(
            &importer,
            &app,
            &client,
            &remote,
            &mut events,
            &blocks[0],
            &[],
        )
        .await;
        assert_eq!(importer.available().0, 0);
        match mode {
            "drop" => drop(download),
            "foreign" => {
                // Same database and rules do not transfer this owner's quota.
                let foreign = download_importer(&node, Config::default());
                let error = foreign
                    .import_download(&app.network, download, u64::MAX - 600_000)
                    .await
                    .unwrap_err();
                assert_eq!(error.error, Error::Configuration);
                assert!(error.progress.unwrap().accepted.is_empty());
                foreign.stop();
                foreign.join().await;
            }
            "stop" => {
                importer.stop();
                importer.join().await;
                let error = importer
                    .import_download(&app.network, download, u64::MAX - 600_000)
                    .await
                    .unwrap_err();
                assert_eq!(error.error, Error::Closed);
                assert!(error.progress.unwrap().accepted.is_empty());
            }
            "stale" => {
                remote.shutdown();
                within(client.monitor().wait()).await;
                let error = importer
                    .import_download(&app.network, download, u64::MAX - 600_000)
                    .await
                    .unwrap_err();
                assert_eq!(error.error, Error::Stale);
                assert!(error.progress.unwrap().accepted.is_empty());
            }
            "owner_drop" => {
                importer.stop();
                importer.join().await;
                drop(importer);
                drop(download);
                assert_eq!(node.db.view().unwrap().status(), before);
                remote.shutdown();
                within(driver).await.unwrap();
                stop(app).await;
                continue;
            }
            _ => unreachable!(),
        }
        assert_eq!(importer.available(), available);
        assert_eq!(node.db.view().unwrap().status(), before);
        importer.stop();
        importer.join().await;
        remote.shutdown();
        within(driver).await.unwrap();
        stop(app).await;
    }
}
#[tokio::test]
async fn download_timeout_has_a_known_empty_prefix_and_joins_the_cancelled_driver() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let _ = response(&remote, &ping(&node, 1, false)).await;
    let client = admitted_client(&app, remote_peer()).await;
    let cfg = Config {
        jobs: 1,
        timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let available = (cfg.jobs, cfg.bytes);
    let importer = download_importer(&node, cfg);
    let before = node.db.view().unwrap().status();
    let hash = common::ledger_fixtures::blocks(&NetworkProfile::mainnet())[0]
        .hash()
        .unwrap();
    let mut download =
        Box::pin(importer.download(&app.network, client.clone(), hash, Direction::Backward));
    tokio::select! {result=&mut download=>panic!("response not supplied: {result:?}"),_=generation_request(&mut events)=>()}
    let failure = within(download).await.unwrap_err();
    assert_eq!(failure.error, Error::Timeout);
    assert!(failure.progress.unwrap().accepted.is_empty());
    within(client.monitor().wait()).await;
    within(driver).await.unwrap();
    assert_eq!(importer.available(), available);
    assert_eq!(node.db.view().unwrap().status(), before);
    importer.stop();
    importer.join().await;
    stop(app).await;
}
