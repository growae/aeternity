use ae_chain::instrumentation::{self, Counter, Event, Sink};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
pub struct Expiry(AtomicU64);
impl Sink for Expiry {
    fn event(&self, event: Event) {
        if let Event::Counter(Counter::MicroCandidateExpired, value) = event {
            assert_eq!(value, 1);
            self.0.fetch_add(value, Ordering::SeqCst);
        }
    }
    fn start(&self, _: instrumentation::Stage) -> Option<Box<dyn instrumentation::Span>> {
        None
    }
}
impl Expiry {
    pub fn bind(db: &ae_storage::Database) -> (Arc<Self>, instrumentation::Binding) {
        let sink = Arc::new(Self::default());
        let binding = db.instrumentation().bind(sink.clone()).unwrap();
        (sink, binding)
    }
    pub fn count(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
