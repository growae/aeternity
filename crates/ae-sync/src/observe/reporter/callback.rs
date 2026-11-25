use super::*;
use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex},
    thread,
};
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub joined: bool,
    pub pending: usize,
    pub retained_bytes: usize,
    pub submitted: u64,
    pub reports: u64,
    pub dropped: u64,
    pub last_error: Option<Error>,
}
struct Batch {
    samples: Vec<Sample>,
    extra: report::Extra,
    bytes: usize,
}
struct State {
    info: Info,
    queue: VecDeque<Batch>,
    closing: bool,
    abort: bool,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}
pub(super) struct Worker {
    shared: Arc<Shared>,
    thread: Option<thread::JoinHandle<()>>,
    limits: Limits,
}
impl Worker {
    pub fn spawn(
        options: report::Options,
        output: statsd::Client,
        limits: Limits,
        parent: Arc<service::Shared>,
    ) -> Result<Self, Error> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                info: Info {
                    running: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
                closing: false,
                abort: false,
            }),
            wake: Condvar::new(),
        });
        let state = shared.clone();
        let thread = thread::Builder::new()
            .name("ae-metrics-reporter".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&state, options, output, limits)
                }));
                let mut s = state.lock();
                s.info.running = false;
                match result {
                    Err(_) => s.info.last_error = Some(Error::Panicked),
                    Ok(Err(e)) => s.info.last_error = Some(e),
                    _ => (),
                }
                s.info.dropped = s.info.dropped.saturating_add(s.queue.len() as u64);
                s.queue.clear();
                s.info.pending = 0;
                s.info.retained_bytes = 0;
                drop(s);
                parent.wake();
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            shared,
            thread: Some(thread),
            limits,
        })
    }
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub fn submit(&self, samples: Vec<Sample>, extra: report::Extra) -> Result<(), Error> {
        let bytes = self.limits.batch(&samples, &extra)?;
        let mut s = self.shared.lock();
        if !s.info.running || s.closing {
            return Err(Error::Closed);
        }
        if s.info.pending >= self.limits.callback_records
            || bytes
                > self
                    .limits
                    .callback_bytes
                    .saturating_sub(s.info.retained_bytes)
        {
            return Err(Error::Busy);
        }
        s.queue.push_back(Batch {
            samples: samples.clone(),
            extra: extra.clone(),
            bytes,
        });
        s.info.pending += 1;
        s.info.retained_bytes += bytes;
        s.info.submitted = s.info.submitted.saturating_add(1);
        self.shared.wake.notify_one();
        Ok(())
    }
    pub fn stop(&mut self, abort: bool) {
        {
            let mut s = self.shared.lock();
            s.closing = true;
            s.abort |= abort;
            self.shared.wake.notify_one();
        }
        if let Some(join) = self.thread.take() {
            let _ = join.join();
        }
        self.shared.lock().info.joined = true;
    }
}
fn run(
    shared: &Shared,
    options: report::Options,
    output: statsd::Client,
    limits: Limits,
) -> Result<(), Error> {
    loop {
        let batch = {
            let mut s = shared.lock();
            while s.queue.is_empty() && !s.closing {
                s = shared.wake.wait(s).unwrap_or_else(|e| e.into_inner());
            }
            if s.abort {
                return Ok(());
            }
            match s.queue.pop_front() {
                Some(b) => b,
                None => return Ok(()),
            }
        };
        let mut reports = 0u64;
        let mut dropped = 0u64;
        for sample in batch.samples {
            for (dp, value) in sample.values {
                let event = options
                    .event(
                        Key {
                            name: sample.metric.clone(),
                            datapoint: dp,
                        },
                        value,
                        batch.extra.clone(),
                        limits.report,
                    )
                    .map_err(Error::Report)?;
                if output.report(event).is_ok() {
                    reports = reports.saturating_add(1);
                } else {
                    dropped = dropped.saturating_add(1);
                }
            }
        }
        let mut s = shared.lock();
        s.info.pending -= 1;
        s.info.retained_bytes -= batch.bytes;
        s.info.reports = s.info.reports.saturating_add(reports);
        s.info.dropped = s.info.dropped.saturating_add(dropped);
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop(true);
    }
}
