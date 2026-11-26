use crate::publication::Kind;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counter {
    BlocksMined,
    MiningRetries,
    MicroCandidateExpired,
    PingFailure,
    PingSuccess,
    PoolGarbageCollected,
    PoolOriginGarbageCollected,
    PoolPushError,
    PoolPushIgnore,
    PoolPushIllegal,
    PoolPush,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gauge {
    Height,
    PeersBlocked,
    PeersCount,
    PeersErrored,
    PeersInbound,
    PeersOutbound,
    PeersUnverified,
    PeersVerified,
    SyncProgress,
    KeyInfo,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {
    Integer(u64),
    Float(f64),
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Event {
    Counter(Counter, u64),
    Gauge(Gauge, Value),
    BlockCreated,
    TopChanged,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Insert(Kind),
    MicroTransactions,
}

// One runtime insertion may retry its transaction. A native exception after
// strict transaction success (notably deep-fee reads) leaves the outer timer
// unfinished even though the strict-transaction success was already observed.
pub struct Insertion {
    publisher: Publisher,
    observation: Observation,
    exception: std::cell::Cell<bool>,
    missing_context: std::cell::Cell<bool>,
}
impl Insertion {
    pub fn new(publisher: &Publisher, kind: Kind) -> Self {
        Self {
            publisher: publisher.clone(),
            observation: publisher.start(Stage::Insert(kind)),
            exception: std::cell::Cell::new(false),
            missing_context: std::cell::Cell::new(false),
        }
    }
    pub fn finish(self, success: bool) {
        if !self.exception.get() {
            self.observation.finish(success);
        }
    }
    pub fn missing_context(&self) -> bool {
        self.missing_context.get()
    }
}
// Native context construction returns illegal_orphan for missing headers.
// Missing state/records after that boundary are storage failures instead.
pub(crate) fn context<T>(
    insertion: Option<&Insertion>,
    result: Result<T, crate::repository::Error>,
) -> Result<T, crate::repository::Error> {
    if matches!(&result, Err(crate::repository::Error::Missing(_)))
        && let Some(insertion) = insertion
    {
        insertion.missing_context.set(true);
    }
    result
}
pub(crate) struct Micro<'a> {
    insertion: Option<&'a Insertion>,
    observation: Option<Observation>,
    succeeded: bool,
}
impl<'a> Micro<'a> {
    pub fn new(insertion: Option<&'a Insertion>) -> Self {
        Self {
            insertion,
            observation: insertion.map(|i| i.publisher.start(Stage::MicroTransactions)),
            succeeded: false,
        }
    }
    pub fn completed<A, C>(
        &mut self,
        result: &Result<
            ae_execution::transactions::batch::Receipt<A, C>,
            ae_execution::transactions::batch::Error<A, C>,
        >,
    ) {
        self.succeeded = result.is_ok();
        if let Some(observation) = self.observation.take()
            && !result.as_ref().is_err_and(|e| e.failure.is_operational())
        {
            observation.finish(self.succeeded);
        }
    }
    pub fn finish<T, E>(self, result: Result<T, E>) -> Result<T, E> {
        if self.succeeded
            && result.is_err()
            && let Some(insertion) = self.insertion
        {
            insertion.exception.set(true);
        }
        result
    }
}

// Observation callbacks cannot supply consensus inputs or change a result.
// Time and metric storage belong to the operational implementation of Sink.
pub trait Sink: Send + Sync {
    // Producers may hold operational locks. Implementations must be bounded,
    // must not wait for producers, and must not reenter storage or execution.
    fn event(&self, event: Event);
    fn start(&self, stage: Stage) -> Option<Box<dyn Span>>;
}
pub trait Span: Send {
    fn finish(self: Box<Self>, success: bool);
}
struct Registration {
    sink: Arc<dyn Sink>,
    accepting: AtomicBool,
}
#[derive(Default)]
struct Shared {
    current: Mutex<Weak<Registration>>,
    panics: AtomicU64,
}
#[derive(Clone, Default)]
pub struct Publisher(Arc<Shared>);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    AlreadyBound,
}
pub struct Binding {
    registration: Arc<Registration>,
}
impl Drop for Binding {
    fn drop(&mut self) {
        self.registration.accepting.store(false, Ordering::Release);
    }
}
pub struct Observation {
    span: Option<Box<dyn Span>>,
    registration: Option<Arc<Registration>>,
    shared: Arc<Shared>,
}
impl Observation {
    pub fn finish(mut self, success: bool) {
        if let Some(span) = self.span.take() {
            self.shared.contain(|| span.finish(success));
        }
        self.registration.take();
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        if let Some(span) = self.span.take() {
            self.shared.contain(|| drop(span));
        }
    }
}
impl Shared {
    fn contain<T>(&self, action: impl FnOnce() -> T) -> Option<T> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
            Ok(value) => Some(value),
            Err(_) => {
                let _ = self
                    .panics
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                        Some(n.saturating_add(1))
                    });
                None
            }
        }
    }
}
impl Publisher {
    pub fn bind(&self, sink: Arc<dyn Sink>) -> Result<Binding, Error> {
        let mut current = self.0.current.lock().unwrap_or_else(|e| e.into_inner());
        if current.upgrade().is_some() {
            return Err(Error::AlreadyBound);
        }
        let registration = Arc::new(Registration {
            sink,
            accepting: AtomicBool::new(true),
        });
        *current = Arc::downgrade(&registration);
        Ok(Binding { registration })
    }
    fn current(&self) -> Option<Arc<Registration>> {
        self.0
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .filter(|r| r.accepting.load(Ordering::Acquire))
    }
    pub fn event(&self, event: Event) {
        if let Some(registration) = self.current() {
            self.0.contain(|| registration.sink.event(event));
        }
    }
    pub fn counter(&self, counter: Counter, value: u64) {
        self.event(Event::Counter(counter, value));
    }
    pub fn gauge(&self, gauge: Gauge, value: Value) {
        self.event(Event::Gauge(gauge, value));
    }
    pub fn start(&self, stage: Stage) -> Observation {
        let registration = self.current();
        let span = registration
            .as_ref()
            .and_then(|r| self.0.contain(|| r.sink.start(stage)).flatten());
        Observation {
            span,
            registration,
            shared: self.0.clone(),
        }
    }
    pub fn panics(&self) -> u64 {
        self.0.panics.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Events(Arc<Mutex<Vec<bool>>>);
    impl Sink for Events {
        fn event(&self, _: Event) {
            self.0.lock().unwrap().push(true);
        }
        fn start(&self, _: Stage) -> Option<Box<dyn Span>> {
            Some(Box::new(Events(self.0.clone())))
        }
    }
    impl Span for Events {
        fn finish(self: Box<Self>, success: bool) {
            self.0.lock().unwrap().push(success);
        }
    }
    #[test]
    fn observations_keep_the_exact_binding_until_completion_and_close_new_admission() {
        let publisher = Publisher::default();
        let events = Arc::new(Mutex::new(vec![]));
        let sink: Arc<dyn Sink> = Arc::new(Events(events.clone()));
        let binding = publisher.bind(sink.clone()).unwrap();
        let span = publisher.start(Stage::MicroTransactions);
        drop(binding);
        publisher.counter(Counter::BlocksMined, 1);
        assert!(matches!(
            publisher.bind(sink.clone()),
            Err(Error::AlreadyBound)
        ));
        span.finish(false);
        assert_eq!(*events.lock().unwrap(), vec![false]);
        let binding = publisher.bind(sink).unwrap();
        publisher.counter(Counter::BlocksMined, 1);
        drop(publisher.start(Stage::MicroTransactions));
        assert_eq!(*events.lock().unwrap(), vec![false, true]);
        drop(binding);
    }
    struct Panics;
    impl Sink for Panics {
        fn event(&self, _: Event) {
            panic!("observation event panic");
        }
        fn start(&self, _: Stage) -> Option<Box<dyn Span>> {
            panic!("observation start panic");
        }
    }
    #[test]
    fn late_failure_preserves_completed_strict_sample_and_suppresses_insertion_sample() {
        use ae_execution::transactions::{self, batch};
        let publisher = Publisher::default();
        let events = Arc::new(Mutex::new(vec![]));
        let _binding = publisher.bind(Arc::new(Events(events.clone()))).unwrap();
        let insertion = Insertion::new(&publisher, Kind::Micro);
        let mut micro = Micro::new(Some(&insertion));
        let network = ae_protocol::NetworkProfile::mainnet();
        let mut trees: ae_state::Trees = ae_state::Trees {
            accounts: Default::default(),
            contracts: Default::default(),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let result = batch::apply_micro_observed(
            &[],
            &mut trees,
            &ae_execution::Context::transaction(1, ae_protocol::Protocol::Roma),
            &transactions::Config::primitives(&network),
            6_000_000.into(),
            0.into(),
            |result| micro.completed(result),
        );
        assert!(result.is_ok());
        // The actual late-read failure and atomic roots are covered by the
        // batch fixture test; this verifies the outer observer's phase routing.
        assert_eq!(
            micro.finish(Err::<(), _>("late fee failure")),
            Err("late fee failure")
        );
        insertion.finish(false);
        assert_eq!(*events.lock().unwrap(), vec![true]);
    }
    #[test]
    fn failed_observation_callbacks_do_not_escape_into_the_producer() {
        let publisher = Publisher::default();
        let _binding = publisher.bind(Arc::new(Panics)).unwrap();
        publisher.event(Event::TopChanged);
        publisher.start(Stage::Insert(Kind::Key)).finish(true);
        assert_eq!(publisher.panics(), 2);
    }
}
