use super::*;
struct Inputs<O> {
    importer: chain::Importer<O>,
    snapshots: snapshots::Owner,
    pool: pool::Client,
    keys: keys::Client,
    generator: generator::Config<O>,
    producer: producer::Config,
    mining: Option<producer::mining::Config>,
    clocks: Arc<dyn Clocks>,
    execution: Option<Arc<Owned<O>>>,
    commands: Option<chain::on_demand::Config>,
}
pub(super) struct Factory<O>(Mutex<Inputs<O>>);
impl<O: MapOrder + Clone + Send + Sync + 'static> Factory<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: &Config<O>,
        clocks: Arc<dyn Clocks>,
        execution: Option<Arc<Owned<O>>>,
        commands: Option<chain::on_demand::Config>,
    ) -> Self {
        Self(Mutex::new(Inputs {
            importer: importer.borrowed(),
            snapshots,
            pool,
            keys,
            generator: config.generator.clone(),
            producer: config.producer,
            mining: config.mining.clone(),
            clocks,
            execution,
            commands,
        }))
    }
}
struct ProducerClock(Box<dyn producer::Clock>);
impl producer::Clock for ProducerClock {
    fn now(&self) -> Result<(u64, Instant), producer::Error> {
        self.0.now()
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> supervisor::Factory for Factory<O> {
    type Generator = generator::Worker<O>;
    type Conductor = producer::Worker;
    type Error = StartError;
    fn generator(&self, generation: u64) -> Result<Self::Generator, StartError> {
        let (db, snapshots, pool, config, clocks, execution) = {
            let i = lock(&self.0);
            (
                i.importer.database().clone(),
                i.snapshots.clone(),
                i.pool.clone(),
                i.generator.clone(),
                i.clocks.clone(),
                i.execution.clone(),
            )
        };
        match execution {
            Some(execution) => generator::Worker::spawn_owned(
                db,
                snapshots,
                pool,
                config,
                clocks.generator(generation),
                execution,
            ),
            None => {
                generator::Worker::spawn(db, snapshots, pool, config, clocks.generator(generation))
            }
        }
        .map_err(StartError::Generator)
    }
    fn conductor(
        &self,
        generation: u64,
        generator: &Self::Generator,
    ) -> Result<Self::Conductor, StartError> {
        let (importer, keys, mut config, mining, snapshots, clocks, pool, commands) = {
            let i = lock(&self.0);
            (
                i.importer.borrowed(),
                i.keys.clone(),
                i.producer,
                i.mining.clone(),
                i.snapshots.clone(),
                i.clocks.clone(),
                i.pool.clone(),
                i.commands.clone(),
            )
        };
        // Activation happens only after a complete pair is published, under
        // the same lock as group desired-state changes and shutdown admission.
        config.enabled = false;
        match (mining, commands) {
            (Some(mining), Some(commands)) => producer::Worker::spawn_with_commands(
                &importer,
                generator.client(),
                keys,
                config,
                ProducerClock(clocks.producer(generation)),
                producer::mining::Inputs {
                    snapshots: snapshots.clone(),
                    config: mining,
                    clock: clocks.preparation(generation),
                },
                producer::CommandInputs {
                    snapshots,
                    pool,
                    config: commands,
                    clock: clocks.commands(generation),
                },
            ),
            (Some(mining), None) => producer::Worker::spawn_with_mining(
                &importer,
                generator.client(),
                keys,
                config,
                ProducerClock(clocks.producer(generation)),
                producer::mining::Inputs {
                    snapshots,
                    config: mining,
                    clock: clocks.preparation(generation),
                },
            ),
            (None, None) => producer::Worker::spawn(
                &importer,
                generator.client(),
                keys,
                config,
                ProducerClock(clocks.producer(generation)),
            ),
            (None, Some(_)) => Err(producer::Error::Binding),
        }
        .map_err(StartError::Producer)
    }
}
