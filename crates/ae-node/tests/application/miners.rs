use super::*;
use ae_node::{application, configuration};
use std::sync::Arc;

#[tokio::test(flavor = "current_thread")]
async fn application_selects_captured_miner_instances_without_starting_external_work() {
    let lab = Lab::new("pow_cuckoo", true);
    let rows = json!([
        ["unstarted-a", "", false, 2, null, "aecuckoo"],
        [
            "unstarted-b",
            "--test",
            true,
            3,
            [1, 3, 1],
            "aecuckooprebuilt"
        ]
    ]);
    for (miners, user, expected) in [
        (rows, json!({}), 4),
        (json!([]), json!({}), 0),
        (json!("unused-invalid-list"), json!({"miners": []}), 0),
    ] {
        let mut cfg = lab.config();
        cfg["mining"]["cuckoo"] = user;
        let cfg = configuration::Configuration::from_value(cfg).unwrap();
        let mut context =
            configuration::resolution::Context::new(configuration::resolution::Directories {
                home: lab.0.to_str().unwrap().into(),
                data: lab.0.join("resources").to_str().unwrap().into(),
                log: lab.0.join("log").to_str().unwrap().into(),
            })
            .unwrap();
        context
            .set("aecore".into(), "aec_mining".into(), json!([15, miners]))
            .unwrap();
        let mut options = application::Options::new(
            lab.0.clone(),
            lab.0.join("chain"),
            ae_node::http::DryRunConfig::new(
                PathBuf::from(env!("CARGO_BIN_EXE_ae-dry-run-worker")),
                lab.0.join("dry-run"),
            ),
        );
        options.miner_directories = ae_sync::chain::pow::configuration::Directories {
            cuckoo: Some(lab.0.clone()),
            prebuilt: Some(lab.0.clone()),
        };
        let prepared = application::Prepared::load(
            &cfg,
            Arc::new(context),
            &configuration::execution::Specification::from_value(&profile::default_value())
                .unwrap(),
            options,
        )
        .unwrap();
        assert_eq!(
            prepared
                .configured()
                .chain()
                .settings()
                .cuckoo_edge_bits()
                .get(),
            15
        );
        let running = prepared.launch().unwrap();
        let handle = running.handle();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            while handle.try_output().is_some() {}
            let status = handle.status();
            assert!(status.running && status.failure.is_none(), "{status:?}");
            if let Some(generation) = handle
                .pow()
                .and_then(|p| p.production())
                .and_then(|p| p.current())
            {
                let status = generation.producer.status();
                if let Some(mining) = status.mining
                    && let Some(candidate) = &mining.candidate
                {
                    assert!(!status.enabled);
                    assert_eq!(candidate.header.common.height, 1);
                    assert_eq!(candidate.edge_bits.get(), 15);
                    assert_eq!(mining.workers.len(), expected);
                    assert_eq!(mining.active, 0);
                    assert!(mining.preparing.is_none());
                    for worker in mining.workers {
                        assert!(worker.running && worker.accepting);
                        assert_eq!(worker.submitted, 0);
                        assert!(
                            worker.active.is_none()
                                && worker.external.is_none()
                                && worker.failure.is_none()
                        );
                    }
                    break;
                }
            }
            assert!(Instant::now() < deadline, "application miner workers");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        drop(handle);
    }
}
