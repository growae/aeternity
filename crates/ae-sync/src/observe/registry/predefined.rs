use super::*;
use ae_chain::instrumentation::{self, Counter, Event, Gauge, Stage};
use ae_chain::publication::Kind as BlockKind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// aecore/priv/exometer_predefined.script at fa2a6d7.
// Copyright (c) Aeternity developers. ISC: LICENSE.
const COUNTERS: [(Counter, &str); 11] = [
    (Counter::BlocksMined, "mining.blocks_mined"),
    (Counter::MiningRetries, "mining.retries"),
    (
        Counter::MicroCandidateExpired,
        "mining.micro_candidate_expired",
    ),
    (Counter::PingFailure, "peers.ping.failure"),
    (Counter::PingSuccess, "peers.ping.success"),
    (Counter::PoolGarbageCollected, "tx_pool.gced"),
    (Counter::PoolOriginGarbageCollected, "tx_pool.origin_gced"),
    (Counter::PoolPushError, "tx_pool.push.error"),
    (Counter::PoolPushIgnore, "tx_pool.push.ignore"),
    (Counter::PoolPushIllegal, "tx_pool.push.illegal"),
    (Counter::PoolPush, "tx_pool.push"),
];
const GAUGES: [(Gauge, &str); 10] = [
    (Gauge::Height, "chain.height"),
    (Gauge::PeersBlocked, "peers.blocked"),
    (Gauge::PeersCount, "peers.count"),
    (Gauge::PeersErrored, "peers.errored"),
    (Gauge::PeersInbound, "peers.inbound"),
    (Gauge::PeersOutbound, "peers.outbound"),
    (Gauge::PeersUnverified, "peers.unverified"),
    (Gauge::PeersVerified, "peers.verified"),
    (Gauge::SyncProgress, "sync.progress"),
    (Gauge::KeyInfo, "blocks.key.info"),
];
const HISTOGRAMS: [(&str, u64); 8] = [
    ("chain.top_change.interval", 60_000),
    ("mining.interval", 60_000),
    ("blocks.micro.txs_execution_time.success", 600_000),
    ("blocks.micro.txs_execution_time.error", 600_000),
    ("blocks.micro.insert_execution_time.success", 600_000),
    ("blocks.micro.insert_execution_time.error", 600_000),
    ("blocks.key.insert_execution_time.success", 14_400_000),
    ("blocks.key.insert_execution_time.error", 14_400_000),
];
pub fn name(suffix: &str) -> Name {
    ["ae", "epoch", "aecore"]
        .into_iter()
        .chain(suffix.split('.'))
        .map(Part::atom)
        .collect()
}
pub fn entries() -> Vec<Entry> {
    COUNTERS
        .iter()
        .map(|(_, suffix)| (*suffix, "counter"))
        .chain(GAUGES.iter().map(|(_, suffix)| (*suffix, "gauge")))
        .chain(HISTOGRAMS.iter().map(|(suffix, _)| (*suffix, "histogram")))
        .map(|(suffix, kind)| Entry {
            name: name(suffix),
            kind: kind.into(),
            enabled: true,
        })
        .collect()
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Info {
    pub rejected: u64,
    pub last_error: Option<Error>,
}
#[derive(Default)]
struct Health {
    rejected: AtomicU64,
    last_error: Mutex<Option<Error>>,
}
impl Health {
    fn record(&self, result: Result<(), Error>) {
        if let Err(error) = result {
            let _ = self
                .rejected
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_add(1))
                });
            *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
        }
    }
}
#[derive(Default)]
struct Times {
    mining: Option<i128>,
    top: Option<i128>,
}
// One application lifetime, shared across connection/reporter generations.
// Histogram admission is bounded; producers never await a probe reply.
pub struct Feed {
    store: Arc<Store>,
    times: Mutex<Times>,
    clock: Arc<dyn Clock>,
    health: Arc<Health>,
}
impl Feed {
    pub fn install(store: Arc<Store>) -> Result<Arc<Self>, Error> {
        let mut created = vec![];
        let result = (|| {
            for (_, suffix) in COUNTERS {
                let n = name(suffix);
                store.create(&n, Kind::Counter, true)?;
                created.push(n);
            }
            for (_, suffix) in GAUGES {
                let n = name(suffix);
                store.create(&n, Kind::Gauge, true)?;
                created.push(n);
            }
            for (suffix, time_span_ms) in HISTOGRAMS {
                let n = name(suffix);
                store.create_histogram(
                    &n,
                    &HistogramOptions {
                        config: histogram::Config {
                            time_span_ms,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )?;
                created.push(n);
            }
            Ok(())
        })();
        if let Err(error) = result {
            for n in created {
                let _ = store.remove(&n);
            }
            return Err(error);
        }
        Ok(Arc::new(Self {
            clock: store.clock.clone(),
            store,
            times: Mutex::new(Times::default()),
            health: Arc::new(Health::default()),
        }))
    }
    pub fn info(&self) -> Info {
        Info {
            rejected: self.health.rejected.load(Ordering::Relaxed),
            last_error: self
                .health
                .last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
    pub fn publish(&self, snapshot: &super::super::metrics::Snapshot) -> Result<(), StoreError> {
        self.store.publish_frame(snapshot, true)
    }
    fn update(&self, suffix: &str, value: Value) {
        self.health.record(self.store.update(&name(suffix), &value));
    }
    fn interval(&self, mining: bool) {
        let mut times = self.times.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.clock.now().unix_microseconds;
        let previous = if mining {
            &mut times.mining
        } else {
            &mut times.top
        };
        if let Some(last) = previous.replace(now) {
            let elapsed = now.saturating_sub(last).max(0) / 1000;
            self.update(
                if mining {
                    "mining.interval"
                } else {
                    "chain.top_change.interval"
                },
                Value::Integer(elapsed.into()),
            );
        }
    }
}
impl instrumentation::Sink for Feed {
    fn event(&self, event: Event) {
        match event {
            Event::Counter(counter, n) => self.update(
                COUNTERS
                    .iter()
                    .find(|(c, _)| *c == counter)
                    .expect("all counters")
                    .1,
                Value::Integer(n.into()),
            ),
            Event::Gauge(gauge, value) => self.update(
                GAUGES
                    .iter()
                    .find(|(g, _)| *g == gauge)
                    .expect("all gauges")
                    .1,
                match value {
                    instrumentation::Value::Integer(n) => Value::Integer(n.into()),
                    instrumentation::Value::Float(n) => Value::Float(n),
                },
            ),
            Event::BlockCreated => self.interval(true),
            Event::TopChanged => self.interval(false),
        }
    }
    fn start(&self, stage: Stage) -> Option<Box<dyn instrumentation::Span>> {
        Some(Box::new(Timing {
            store: self.store.clone(),
            health: self.health.clone(),
            stage,
            start: Instant::now(),
        }))
    }
}
struct Timing {
    store: Arc<Store>,
    health: Arc<Health>,
    stage: Stage,
    start: Instant,
}
impl instrumentation::Span for Timing {
    fn finish(self: Box<Self>, success: bool) {
        let suffix = match (self.stage, success) {
            (Stage::MicroTransactions, true) => HISTOGRAMS[2].0,
            (Stage::MicroTransactions, false) => HISTOGRAMS[3].0,
            (Stage::Insert(BlockKind::Micro), true) => HISTOGRAMS[4].0,
            (Stage::Insert(BlockKind::Micro), false) => HISTOGRAMS[5].0,
            (Stage::Insert(BlockKind::Key), true) => HISTOGRAMS[6].0,
            (Stage::Insert(BlockKind::Key), false) => HISTOGRAMS[7].0,
        };
        self.health.record(self.store.update(
            &name(suffix),
            &Value::Integer(self.start.elapsed().as_micros().into()),
        ));
    }
}
// Only these names are owned by both the native catalog and the sampled
// observer. Ping values are cumulative snapshots, never counter increments.
pub(super) fn sampled_kind(n: &Name) -> Option<Kind> {
    if GAUGES[..9].iter().any(|(_, suffix)| *n == name(suffix)) {
        Some(Kind::Gauge)
    } else if *n == name("peers.ping.success") || *n == name("peers.ping.failure") {
        Some(Kind::Counter)
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
