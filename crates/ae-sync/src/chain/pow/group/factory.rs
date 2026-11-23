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
}
pub(super) struct Factory<O>(Mutex<Inputs<O>>);
impl<O: MapOrder + Clone + Send + Sync + 'static> Factory<O> {
    pub fn new(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: &Config<O>,
        clocks: Arc<dyn Clocks>,
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
        let (db, snapshots, pool, config, clocks) = {
            let i = lock(&self.0);
            (
                i.importer.database().clone(),
                i.snapshots.clone(),
                i.pool.clone(),
                i.generator.clone(),
                i.clocks.clone(),
            )
        };
        generator::Worker::spawn(db, snapshots, pool, config, clocks.generator(generation))
            .map_err(StartError::Generator)
    }
    fn conductor(
        &self,
        generation: u64,
        generator: &Self::Generator,
    ) -> Result<Self::Conductor, StartError> {
        let (importer, keys, mut config, mining, snapshots, clocks) = {
            let i = lock(&self.0);
            (
                i.importer.borrowed(),
                i.keys.clone(),
                i.producer,
                i.mining.clone(),
                i.snapshots.clone(),
                i.clocks.clone(),
            )
        };
        // Activation happens only after a complete pair is published, under
        // the same lock as group desired-state changes and shutdown admission.
        config.enabled = false;
        match mining {
            Some(mining) => producer::Worker::spawn_with_mining(
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
            None => producer::Worker::spawn(
                &importer,
                generator.client(),
                keys,
                config,
                ProducerClock(clocks.producer(generation)),
            ),
        }
        .map_err(StartError::Producer)
    }
}
