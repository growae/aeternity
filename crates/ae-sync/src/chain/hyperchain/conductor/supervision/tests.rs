use super::*;
use ae_hyperchain::parent_connector::fetch::{BackendError, Context, Query, Remote};
use std::sync::atomic::{AtomicBool, Ordering};
#[test]
fn failed_or_panicked_connector_construction_consumes_shared_budget_and_keeps_old_route_closed() {
    struct Backend;
    impl ae_hyperchain::parent_connector::fetch::Backend for Backend {
        fn fetch(
            &self,
            _: usize,
            _: &Query,
            _: &[u8; 16],
            _: &Context,
        ) -> Result<Remote, BackendError> {
            Ok(Remote::NotFound)
        }
    }
    for panic in [false, true] {
        let mailbox = cache::Mailbox::new(
            parent_cache::Cache::new(
                parent_cache::Config {
                    child_start_height: 0.into(),
                    max_size: 20.into(),
                    retry_interval: 1000.into(),
                    finality: 2.into(),
                },
                parent_cache::ChildTop {
                    hash: ae_types::Hash256::ZERO,
                    height: 0.into(),
                },
            ),
            Default::default(),
        )
        .unwrap();
        let (service, mut factory) = ConnectorFactory::start(
            Arc::new(Backend),
            0,
            [0; 16],
            mailbox.sink(),
            connector::Config {
                fetch_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let old = service.client();
        let original = factory.spawn;
        let fail = AtomicBool::new(true);
        factory.spawn = Box::new(move || {
            if fail.swap(false, Ordering::AcqRel) {
                assert!(!panic, "controlled connector constructor panic");
                Err(std::io::Error::other(
                    "controlled connector construction error",
                ))
            } else {
                original()
            }
        });
        old.request_stop();
        let end = Instant::now() + std::time::Duration::from_secs(5);
        while !service.is_finished() {
            assert!(Instant::now() < end);
            std::thread::yield_now();
        }
        let mut supervisor: Supervisor<ae_vm_fate::maps::order::Otp26> =
            Supervisor::new(None, factory, 8);
        let mut child = Some(service);
        let now = Instant::now();
        supervisor.poll(&mut None, &mut child, now).unwrap();
        assert!(child.is_none());
        assert_eq!(supervisor.status().attempts, 1);
        assert_eq!(supervisor.status().connector_generation, 1);
        assert!(supervisor.status().connector_restarting);
        assert_eq!(
            supervisor.connector().check_top(),
            Err(connector::Error::Closed)
        );
        supervisor.poll(&mut None, &mut child, now).unwrap();
        assert!(child.is_some());
        assert_eq!(supervisor.status().attempts, 2);
        assert_eq!(supervisor.status().connector_generation, 2);
        assert!(!supervisor.status().restarting);
        assert_eq!(old.check_top(), Err(connector::Error::Closed));
        let exit = supervisor.finish();
        assert!(matches!(
            exit.notices.as_slice(),
            [
                Notice::ConnectorRetired { generation: 1, .. },
                Notice::ConnectorStartFailed { attempt: 1, .. },
                Notice::ConnectorRestarted { generation: 2 }
            ]
        ));
        let native: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/erlang_connector_supervision.json"
        ))
        .unwrap();
        let row = native["lifecycle"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["kind"] == "failed_restart")
            .unwrap();
        assert_eq!(row["siblings_unchanged"], true);
        assert_eq!(
            row["trace"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["event"] == "started" && e["child"] == "connector")
                .count() as u64,
            exit.status.connector_generation
        );
        child.take().unwrap().shutdown().unwrap();
    }
}

#[tokio::test]
async fn http_backend_owner_joins_cleanup_in_async_shutdown_gaps_and_failed_start() {
    for fail in [false, true] {
        let node = crate::parent_blocks::Node::new(
            crate::parent_blocks::Chain::Aeternity,
            "http://127.0.0.1:1",
            None,
        )
        .unwrap();
        let backend =
            Arc::new(crate::parent_blocks::Backend::new(vec![node], Default::default()).unwrap());
        let weak = Arc::downgrade(&backend);
        let mailbox = cache::Mailbox::new(
            parent_cache::Cache::new(
                parent_cache::Config {
                    child_start_height: 0.into(),
                    max_size: 20.into(),
                    retry_interval: 1000.into(),
                    finality: 2.into(),
                },
                parent_cache::ChildTop {
                    hash: ae_types::Hash256::ZERO,
                    height: 0.into(),
                },
            ),
            Default::default(),
        )
        .unwrap();
        let result = ConnectorFactory::start(
            backend,
            1,
            [0; 16],
            mailbox.sink(),
            connector::Config {
                command_capacity: if fail { 0 } else { 4 },
                fetch_interval: None,
                ..Default::default()
            },
        );
        if fail {
            assert!(result.is_err());
        } else {
            let (service, factory) = result.unwrap();
            service.shutdown().unwrap();
            assert!(weak.upgrade().is_some());
            // No connector worker remains to own the last HTTP runtime.
            drop(factory);
        }
        assert!(weak.upgrade().is_none());
    }
}
