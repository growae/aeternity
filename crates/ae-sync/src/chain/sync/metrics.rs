// aec_sync progress callback and status logging, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: LICENSE.
use super::*;
use crate::events::{self, Channel, Cursor, Feed};
use ae_chain::instrumentation::{Gauge, Value as MetricValue};
use rand::Rng;
use std::future::Future;
use tokio::time::Instant;

const PERIOD: Duration = Duration::from_secs(30);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub task: lifecycle::Task,
    pub pool: usize,
    pub got: usize,
    pub adding: usize,
    // Oldest complete chunks first, final entry is the pending accumulator.
    pub pending: Vec<usize>,
    pub workers: usize,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Log {
    Progress(f64),
    Task(Task),
}
impl Log {
    pub fn text(&self) -> String {
        match self {
            Self::Progress(n) => format!("Sync progress: {n:.4}%"),
            Self::Task(t) => {
                let pending = match t.pending.split_last() {
                    None => "none".into(),
                    Some((last, chunks)) => format!(
                        "[{}] + {last}",
                        chunks
                            .iter()
                            .map(usize::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                };
                format!(
                    "SyncTask ({}): Pool: size {} (got {}), Add: size {}, Pending: {}, Workers: {}",
                    t.task.id, t.pool, t.got, t.adding, pending, t.workers
                )
            }
        }
    }
}
impl events::Event for Log {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(match self {
            Self::Progress(_) => 0,
            Self::Task(t) => t
                .pending
                .capacity()
                .saturating_mul(std::mem::size_of::<usize>()),
        })
    }
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub ticks: u64,
    pub gauges: u64,
    pub logs: u64,
    pub dropped_logs: u64,
    pub failures: u64,
    pub last: Option<f64>,
    pub next: Option<Instant>,
}
pub(super) struct Snapshot {
    pub tasks: Vec<Task>,
    pub top_target: u64,
    pub logging: bool,
}
struct Attempt {
    value: Option<f64>,
    logs: u64,
    dropped: u64,
    result: Result<(), Error>,
}
// Keep the callback's already emitted prefix on failure. The log's progress
// deliberately performs a second fresh height read; a sampled/cached value
// changes native behavior during concurrent chain advancement.
async fn callback<F: Future<Output = Result<u64, Error>>>(
    snapshot: Snapshot,
    mut height: impl FnMut() -> F,
    mut gauge: impl FnMut(f64),
    mut log: impl FnMut(Log) -> Result<bool, Error>,
) -> Attempt {
    let tasks: Vec<_> = snapshot.tasks.iter().map(|t| t.task).collect();
    let active = tasks.iter().any(|t| !t.suspect);
    let mut attempt = Attempt {
        value: None,
        logs: 0,
        dropped: 0,
        result: Ok(()),
    };
    attempt.result = async {
        let selected = if active { height().await? } else { 0 };
        let value = lifecycle::progress(&tasks, snapshot.top_target, selected)
            .map_err(Error::Progress)?
            .percent;
        gauge(value);
        attempt.value = Some(value);
        if snapshot.logging {
            let selected = if active { height().await? } else { 0 };
            let percent = lifecycle::progress(&tasks, snapshot.top_target, selected)
                .map_err(Error::Progress)?
                .percent;
            for event in std::iter::once(Log::Progress(percent))
                .chain(snapshot.tasks.into_iter().map(Log::Task))
            {
                if log(event)? {
                    attempt.logs += 1;
                } else {
                    attempt.dropped += 1;
                }
            }
        }
        Ok(())
    }
    .await;
    attempt
}
pub(super) struct Owner {
    channel: Arc<Channel<Log>>,
    tasks: Tasks<(), Attempt>,
    status: Status,
}
impl Owner {
    pub fn new(config: events::Config) -> Result<(Self, Feed<Log>, Cursor), Error> {
        let (channel, feed) = events::channel(config).map_err(Error::Events)?;
        let origin = feed.cursor();
        let jitter = rand::thread_rng().gen_range(1..=1000);
        Ok((
            Self {
                channel: Arc::new(channel),
                tasks: Tasks::new(),
                status: Status {
                    next: Some(Instant::now() + Duration::from_millis(jitter)),
                    ..Default::default()
                },
            },
            feed,
            origin,
        ))
    }
    pub fn status(&self) -> Status {
        self.status.clone()
    }
    pub async fn next<O: MapOrder + Send + Sync + 'static>(
        &mut self,
        context: Arc<Context<O>>,
    ) -> Result<(), Error> {
        let channel = self.channel.clone();
        self.next_with(move || async move {
            let snapshot = context
                .shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .metrics();
            callback(
                snapshot,
                || {
                    context.read_when_available(|db| {
                        let view = db.view()?;
                        Ok(view
                            .require_block(
                                view.status().tip.ok_or(repository::Error::Uninitialized)?,
                            )?
                            .header()
                            .common()
                            .height)
                    })
                },
                |value| {
                    context
                        .importer
                        .database()
                        .instrumentation()
                        .gauge(Gauge::SyncProgress, MetricValue::Float(value))
                },
                |event| {
                    match channel.publish(event) {
                        Ok(_) => Ok(true),
                        // Diagnostic backpressure must not reject chain work.
                        Err(events::Error::EventTooLarge) => Ok(false),
                        Err(e) => Err(Error::Events(e)),
                    }
                },
            )
            .await
        })
        .await
    }
    async fn next_with<F: Future<Output = Attempt> + Send + 'static>(
        &mut self,
        callback: impl FnOnce() -> F + Send + 'static,
    ) -> Result<(), Error> {
        if self.tasks.len() == 0 {
            let Some(deadline) = self.status.next else {
                return std::future::pending().await;
            };
            self.tasks.insert((), async move {
                tokio::time::sleep_until(deadline).await;
                callback().await
            });
        }
        let (_, attempt) = self.tasks.next().await;
        self.status.ticks = self.status.ticks.saturating_add(1);
        if let Some(value) = attempt.value {
            self.status.gauges = self.status.gauges.saturating_add(1);
            self.status.last = Some(value);
        }
        self.status.logs = self.status.logs.saturating_add(attempt.logs);
        self.status.dropped_logs = self.status.dropped_logs.saturating_add(attempt.dropped);
        self.status.failures = self
            .status
            .failures
            .saturating_add(u64::from(attempt.result.is_err()));
        self.status.next = attempt.result.is_ok().then(|| Instant::now() + PERIOD);
        attempt.result
    }
    pub fn cancel(&mut self) {
        self.tasks.clear();
        self.status.next = None;
        self.channel.close();
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.cancel();
    }
}
#[cfg(test)]
mod tests;
