use super::*;

pub struct RetiredTop {
    pub record: Arc<publication::Payload>,
    pub result: Result<parent_cache::Reply, cache::Error>,
}
impl std::fmt::Debug for RetiredTop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetiredTop")
            .field("revision", &self.record.change().revision)
            .field("result", &self.result)
            .finish()
    }
}
#[derive(Debug)]
pub struct CacheExit {
    pub child: cache::Retired,
    pub delivery: Option<delivery::RetiredCacheCall>,
    pub top: Option<RetiredTop>,
}
pub(in super::super) struct CacheInputs<T, O> {
    pub database: Arc<Database>,
    pub config: parent_cache::Config,
    pub runtime: cache::Config,
    pub targets: T,
    pub order: O,
    pub connector: cache::ConnectorLink,
    pub route: cache::Route,
}
pub(in super::super) struct CacheFactory {
    spawn: Box<dyn Fn() -> Result<(cache::Runtime, u64), Error> + Send + Sync>,
    pub(super) route: cache::Route,
    client: cache::Client,
    revision: u64,
}
impl CacheFactory {
    pub(in super::super) fn new<T: cache::Targets, O: MapOrder + Clone + Send + Sync + 'static>(
        input: CacheInputs<T, O>,
        client: cache::Client,
        revision: u64,
    ) -> Self {
        let targets = std::sync::Mutex::new(input.targets);
        let spawn = Box::new(move || {
            let view = input
                .database
                .view()
                .map_err(|e| Error::Importer(e.into()))?;
            let status = view.status();
            let hash = status.tip.ok_or(Error::Configuration)?;
            let block = view
                .require_block(hash)
                .map_err(|e| Error::Importer(e.into()))?;
            let top = parent_cache::ChildTop {
                hash,
                height: block.header().common().height.into(),
            };
            let revision = status.revision;
            drop(view);
            let mailbox = cache::Mailbox::new(
                parent_cache::Cache::new(input.config.clone(), top),
                input.runtime,
            )
            .map_err(|e| Error::Parent(e.to_string()))?;
            let child = mailbox
                .spawn(
                    targets.lock().expect("cache target factory").clone(),
                    input.connector.clone(),
                    input.order.clone(),
                )
                .map_err(|e| Error::Parent(e.to_string()))?;
            Ok((child, revision))
        });
        Self {
            spawn,
            route: input.route,
            client,
            revision,
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Supervisor<O> {
    pub(in super::super) fn parent(&self) -> cache::Client {
        self.cache.as_ref().expect("cache factory").client.clone()
    }
    pub(in super::super) fn cache_revision(&self) -> u64 {
        self.cache.as_ref().expect("cache factory").revision
    }
    pub(in super::super) fn poll_cache(
        &mut self,
        child: &mut Option<cache::Runtime>,
        now: Instant,
        settle: impl FnOnce(&cache::Client, &mut CacheExit) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.cache.is_none() {
            return Ok(());
        }
        let origin = *self.origin.get_or_insert(now);
        if let Some(service) = child.as_ref() {
            self.status.cache_restarting = service.replacement_pending();
        }
        if self.notices.len() >= self.limit {
            return Ok(());
        }
        if let Some(service) = child
            && service.is_finished()
            && service.replacement_pending()
        {
            let factory = self.cache.as_mut().expect("cache factory");
            factory.route.replace(None);
            let old = factory.client.clone();
            let retired = child.take().expect("finished cache").retire();
            let mut exit = CacheExit {
                child: retired,
                delivery: None,
                top: None,
            };
            self.status.cache_restarting = true;
            let settled = settle(&old, &mut exit);
            self.notices.push_back(Notice::CacheRetired {
                generation: self.status.cache_generation,
                child: Box::new(exit),
            });
            settled?;
        }
        if child.is_some() || !self.status.cache_restarting || self.notices.len() >= self.limit {
            return Ok(());
        }
        self.record(now.saturating_duration_since(origin).as_secs())?;
        let factory = self.cache.as_mut().expect("cache factory");
        let replacement =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (factory.spawn)()))
                .unwrap_or_else(|_| Err(Error::Parent("parent cache constructor panicked".into())));
        match replacement {
            Ok((service, revision)) => {
                self.status.cache_generation = self
                    .status
                    .cache_generation
                    .checked_add(1)
                    .ok_or(Error::Configuration)?;
                factory.client = service.client();
                factory.revision = revision;
                factory.route.replace(Some(factory.client.clone()));
                *child = Some(service);
                self.status.cache_restarting = false;
                self.notices.push_back(Notice::CacheRestarted {
                    generation: self.status.cache_generation,
                    revision,
                });
            }
            Err(error) => self.notices.push_back(Notice::CacheStartFailed {
                attempt: self.status.attempts,
                error,
            }),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod order {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../ae-vm-fate/tests/support/otp_order.rs"
        ));
    }
    use ae_hyperchain::parent_connector::fetch::{BackendError, Context, Query, Remote};
    use std::sync::atomic::{AtomicBool, Ordering};
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
    #[derive(Clone)]
    struct Targets;
    impl cache::Targets for Targets {
        fn targets(
            &mut self,
            _: &ae_types::Unsigned,
        ) -> Result<Vec<num_bigint::BigInt>, cache::TargetError> {
            Ok(vec![1.into(), 6.into()])
        }
    }
    fn mailbox() -> cache::Mailbox {
        cache::Mailbox::new(
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
        .unwrap()
    }
    #[test]
    fn cache_start_errors_and_panics_count_without_publishing_a_replacement() {
        for panic in [false, true] {
            let mailbox = mailbox();
            let old = mailbox.client();
            let route = cache::Route::new(old.clone());
            let (connector, connector_factory) = ConnectorFactory::start(
                Arc::new(Backend),
                0,
                [0; 16],
                route.clone(),
                connector::Config {
                    fetch_interval: None,
                    ..Default::default()
                },
            )
            .unwrap();
            let link = connector_factory.link();
            let fixture: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../../tests/fixtures/erlang_cache_supervision.json"
            ))
            .unwrap();
            let order = order::profile(&fixture["profile"]);
            let service = mailbox.spawn(Targets, link.clone(), order.clone()).unwrap();
            let failed = AtomicBool::new(false);
            let factory_order = order.clone();
            let spawn = Box::new(move || {
                if !failed.swap(true, Ordering::AcqRel) {
                    assert!(!panic, "controlled cache constructor panic");
                    return Err(Error::Parent("controlled cache constructor failure".into()));
                }
                Ok((
                    self::mailbox()
                        .spawn(Targets, link.clone(), factory_order.clone())
                        .unwrap(),
                    7,
                ))
            });
            let factory = CacheFactory {
                spawn,
                route: route.clone(),
                client: old.clone(),
                revision: 1,
            };
            let mut supervisor: Supervisor<ae_vm_fate::maps::order::Otp26> =
                Supervisor::new(None, connector_factory, Some(factory), 8);
            old.request_stop();
            let end = Instant::now() + std::time::Duration::from_secs(5);
            while !service.is_finished() {
                assert!(Instant::now() < end);
                std::thread::yield_now();
            }
            let now = Instant::now();
            let mut child = Some(service);
            supervisor
                .poll_cache(&mut child, now, |_, _| Ok(()))
                .unwrap();
            assert!(child.is_none() && route.current().is_none());
            assert!(supervisor.parent().same_runtime(&old));
            assert_eq!(supervisor.status().attempts, 1);
            assert_eq!(supervisor.cache_revision(), 1);
            supervisor
                .poll_cache(&mut child, now, |_, _| panic!("already retired"))
                .unwrap();
            assert_eq!(supervisor.status().attempts, 2);
            assert_eq!(supervisor.status().cache_generation, 2);
            assert_eq!(supervisor.cache_revision(), 7);
            assert!(!route.current().unwrap().same_runtime(&old));
            assert!(old.is_closed());
            let report = supervisor.finish();
            assert!(route.current().is_none());
            assert!(matches!(
                report.notices.as_slice(),
                [
                    Notice::CacheRetired { generation: 1, .. },
                    Notice::CacheStartFailed { attempt: 1, .. },
                    Notice::CacheRestarted {
                        generation: 2,
                        revision: 7
                    }
                ]
            ));
            child.unwrap().shutdown().unwrap();
            connector.shutdown().unwrap();
        }
    }
}
