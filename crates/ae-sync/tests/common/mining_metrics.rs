use ae_chain::instrumentation::{self, Counter, Event, Gauge, Publisher, Sink};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct MiningMetrics(Mutex<Vec<Event>>);
impl Sink for MiningMetrics {
    fn event(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }
    fn start(&self, _: instrumentation::Stage) -> Option<Box<dyn instrumentation::Span>> {
        None
    }
}
impl MiningMetrics {
    pub fn bind(publisher: &Publisher) -> (Arc<Self>, instrumentation::Binding) {
        let sink = Arc::new(Self::default());
        let binding = publisher.bind(sink.clone()).unwrap();
        (sink, binding)
    }
    pub fn counts(&self) -> (u64, u64) {
        let mut counts = (0, 0);
        for event in self.0.lock().unwrap().iter() {
            match event {
                Event::Counter(Counter::BlocksMined, n) => counts.0 += n,
                Event::Counter(Counter::MiningRetries, n) => counts.1 += n,
                _ => (),
            }
        }
        counts
    }
    pub fn key_info(&self) -> Vec<instrumentation::Value> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Event::Gauge(Gauge::KeyInfo, value) => Some(*value),
                _ => None,
            })
            .collect()
    }
}
