use super::super::Error as SyncError;
use super::*;

#[derive(PartialEq)]
enum Key {
    Policy,
    View,
}
enum Update {
    Policy(resilience::Config, resilience::Snapshot),
    View(Point, Option<u64>),
}
pub(in crate::chain::sync) struct Owner<O: MapOrder + Send + Sync + 'static> {
    context: Arc<Context<O>>,
    work: Tasks<Key, Result<Update, SyncError>>,
    policy_busy: bool,
    view_busy: bool,
    refresh: bool,
    initialized: bool,
    wants_synced: bool,
    status: Status,
}
impl<O: MapOrder + Send + Sync + 'static> Owner<O> {
    pub fn new(context: Arc<Context<O>>) -> Self {
        Self {
            context,
            work: Tasks::new(),
            policy_busy: false,
            view_busy: false,
            refresh: true,
            initialized: false,
            wants_synced: false,
            status: Status::default(),
        }
    }
    pub fn ready(&self) -> bool {
        self.initialized
    }
    pub fn status(&self) -> Status {
        let mut status = self.status.clone();
        status.pending = self.work.len() > 0 || !self.initialized;
        status
    }
    pub fn observe(&mut self, tasks: Vec<Task>, top_target: u64, transitions: u64) {
        self.status.observe(tasks, top_target);
        self.status.syncing_transitions = transitions;
    }
    fn progress(&mut self) {
        self.status.progress = progress(
            &self.status.tasks,
            self.status.top_target,
            self.status.selected.map_or(0, |p| p.height),
        );
    }
    pub fn refresh(&mut self) {
        self.refresh = true;
    }
    pub fn suppress_generation(&mut self) -> bool {
        if self.status.last_generation_in_sync {
            self.status.suppressed_generations =
                self.status.suppressed_generations.saturating_add(1);
            true
        } else {
            false
        }
    }
    pub fn generation_fetched(&mut self) {
        self.status.last_generation_in_sync = true;
    }
    pub fn done(&mut self, peer: PeerPublicKey, generation: net::Generation, reason: DoneReason) {
        self.status.peer_completions = self.status.peer_completions.saturating_add(1);
        self.status.last_peer_done = Some(PeerDone {
            peer,
            generation,
            reason,
        });
        self.wants_synced = true;
    }
    pub fn start(&mut self, config: resilience::Config) {
        if !self.policy_busy {
            let event = if !self.initialized {
                Some(resilience::Event::Start)
            } else if self.wants_synced
                && self.status.policy.is_some_and(|p| !p.state.synchronized())
            {
                Some(resilience::Event::Synced)
            } else if self.status.policy_config != Some(config) {
                Some(resilience::Event::ConfigChanged)
            } else {
                None
            };
            if let Some(event) = event {
                let context = self.context.clone();
                self.work.insert(Key::Policy, async move {
                    let snapshot = context
                        .importer
                        .policy(event, config)
                        .await
                        .map_err(SyncError::Policy)?;
                    Ok(Update::Policy(config, snapshot))
                });
                self.policy_busy = true;
            }
        }
        if self.initialized && self.refresh && !self.view_busy {
            self.refresh = false;
            self.view_busy = true;
            let context = self.context.clone();
            self.work.insert(Key::View, async move {
                context
                    .read_when_available(|db| {
                        let view = db.view()?;
                        let hash = view.status().tip.ok_or(SyncError::NoProgress)?;
                        let point = Point {
                            height: view.require_block(hash)?.header().common().height,
                            hash,
                        };
                        Ok(Update::View(point, view.finalized_height()?))
                    })
                    .await
            });
        }
    }
    pub async fn next(&mut self) -> Result<(), SyncError> {
        let (key, result) = self.work.next().await;
        match key {
            Key::Policy => self.policy_busy = false,
            Key::View => self.view_busy = false,
        }
        match result? {
            Update::Policy(config, snapshot) => {
                self.initialized = true;
                self.status.policy = Some(snapshot);
                self.status.policy_config = Some(config);
                self.refresh = true;
            }
            Update::View(point, finalized) => {
                self.status.selected = Some(point);
                self.status.finalized_height = finalized;
                self.progress();
            }
        }
        Ok(())
    }
    pub fn cancel(&mut self) {
        self.work.clear();
        self.policy_busy = false;
        self.view_busy = false;
        self.refresh = false;
        self.status.pending = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chain,
        test_support::{Node, ledger_fixtures as fixtures, within},
    };
    use ae_p2p::noise;
    use ae_protocol::NetworkProfile;

    #[tokio::test]
    async fn lifecycle_view_waits_for_peer_capacity_and_cancellation_refunds_its_slot() {
        let node = Node::new();
        let profile = NetworkProfile::mainnet();
        let importer = chain::Importer::new(
            node.db.clone(),
            fixtures::execution(&profile),
            fixtures::rules(node.db.view().unwrap().identity().genesis),
            Default::default(),
        )
        .unwrap();
        let mut settings = network::Config {
            listen: None,
            ..Default::default()
        };
        settings.startup.include_default_peers = false;
        settings.registry.policy.maximum_outbound = 0;
        settings.registry.policy.maximum_probes = 0;
        let (network, _events, _pool, network_service) = network::create(
            node.db.clone(),
            node.client(),
            Arc::new(noise::Identity::from_secret([1; 32])),
            profile,
            settings,
        )
        .await
        .unwrap();
        let config = Config {
            read_jobs: 2,
            read_bytes: 80 << 20,
            timeout: std::time::Duration::from_millis(250),
            ..Default::default()
        };
        let reads = blocking::Pool::new(1, config.read_jobs, config.read_bytes).unwrap();
        let (wake, _) = watch::channel(());
        let context = Arc::new(Context {
            importer: Arc::new(importer),
            network: network.clone(),
            config,
            reads,
            stopped: AtomicBool::new(false),
            wake,
            shared: Mutex::new(shared::Book::new()),
            headers: Mutex::new(BTreeMap::new()),
        });
        let held = context.reads.reserve(context.config.read_bytes).unwrap();
        let mut owner = Owner::new(context.clone());
        owner.start(context.config.resilience);
        within(owner.next()).await.unwrap();
        assert!(owner.ready());
        owner.start(context.config.resilience);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(600), owner.next())
                .await
                .is_err()
        );
        assert!(owner.status().pending);
        assert!(owner.status().selected.is_none());
        drop(held);
        within(owner.next()).await.unwrap();
        assert_eq!(owner.status().selected.unwrap().height, 0);
        let held = context.reads.reserve(context.config.read_bytes).unwrap();
        owner.refresh();
        owner.start(context.config.resilience);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(600), owner.next())
                .await
                .is_err()
        );
        assert_eq!(context.reads.available(), (0, 0));
        owner.cancel();
        assert_eq!(context.reads.available(), (1, 0));
        drop(held);
        assert_eq!(context.reads.available(), (2, context.config.read_bytes));
        context.reads.stop();
        context.reads.join().await;
        context.importer.stop();
        context.importer.join().await;
        network.shutdown();
        assert!(network_service.run().await.status.joined);
    }
}
