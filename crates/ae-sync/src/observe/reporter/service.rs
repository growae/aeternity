use super::*;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Condvar, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Instant,
};
use tokio::sync::{oneshot, watch};

struct Credit {
    shared: Weak<Shared>,
    bytes: AtomicUsize,
}
impl Credit {
    fn grow(&self, bytes: usize) -> Result<(), Error> {
        let shared = self.shared.upgrade().ok_or(Error::Closed)?;
        let mut q = shared.lock();
        if bytes
            > shared
                .limits
                .pending_bytes
                .saturating_sub(q.info.pending_bytes)
        {
            return Err(Error::Busy);
        }
        q.info.pending_bytes += bytes;
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let mut q = shared.lock();
            q.info.pending -= 1;
            q.info.pending_bytes -= self.bytes.load(Ordering::Relaxed);
            drop(q);
            shared.changed.send_replace(());
        }
    }
}
pub struct Ticket<T> {
    receiver: oneshot::Receiver<Result<T, Error>>,
    _credit: Arc<Credit>,
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Closed)))
    }
}
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.receiver.blocking_recv().unwrap_or(Err(Error::Closed))
    }
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub joined: bool,
    pub pending: usize,
    pub pending_bytes: usize,
    pub reporters: usize,
    pub subscriptions: usize,
    pub retained_bytes: usize,
    pub catalog_bytes: usize,
    pub timer_fires: u64,
    pub source_errors: u64,
    pub callback_drops: u64,
    pub completed: u64,
    pub last_error: Option<Error>,
}
#[derive(Debug, Clone)]
pub struct ReporterInfo {
    pub name: String,
    pub module: String,
    pub enabled: bool,
    pub generation: u64,
    pub alive: bool,
    pub previous_joined: bool,
    pub restart_attempts: u64,
    pub intervals: Vec<IntervalInfo>,
    pub last_error: Option<Error>,
}
#[derive(Debug, Clone)]
pub struct IntervalInfo {
    pub interval: NamedInterval,
    pub timer: Option<u64>,
    pub armed: bool,
}
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub key: Subscription,
    pub interval: Interval,
    pub timer: Option<u64>,
    pub armed: bool,
}
type Completion = Box<dyn FnOnce() + Send>;
type Action = Box<dyn FnOnce(&mut Engine) -> Completion + Send>;
struct Command {
    admitted: Instant,
    _credit: Arc<Credit>,
    action: Action,
}
struct Queue {
    commands: VecDeque<Command>,
    info: Info,
    closing: bool,
    woken: bool,
}
pub(super) struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
    changed: watch::Sender<()>,
    done: watch::Sender<bool>,
    limits: Limits,
    clock: Arc<dyn Clock>,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn wake(&self) {
        self.lock().woken = true;
        self.wake.notify_one();
    }
}
#[derive(Clone)]
pub struct Signal(Weak<Shared>);
impl Signal {
    pub fn notify(&self) {
        if let Some(s) = self.0.upgrade() {
            s.wake();
        }
    }
    pub fn closed(&self) -> bool {
        self.0.upgrade().is_none_or(|s| !s.lock().info.running)
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    updates: watch::Receiver<()>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
    pub fn shutdown(&self) {
        let mut q = self.shared.lock();
        q.closing = true;
        self.shared.wake.notify_one();
    }
    fn call<T: Send + 'static>(
        &self,
        bytes: usize,
        action: impl FnOnce(&mut Engine) -> Result<T, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.call_sized(bytes, action, |_| Ok(0))
    }
    fn call_sized<T: Send + 'static>(
        &self,
        bytes: usize,
        action: impl FnOnce(&mut Engine) -> Result<T, Error> + Send + 'static,
        result_bytes: impl FnOnce(&T) -> Result<usize, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let admitted = self.shared.clock.now().monotonic;
        let mut q = self.shared.lock();
        if q.closing || !q.info.running {
            return Err(Error::Closed);
        }
        if q.info.pending >= self.shared.limits.requests
            || bytes
                > self
                    .shared
                    .limits
                    .pending_bytes
                    .saturating_sub(q.info.pending_bytes)
        {
            return Err(Error::Busy);
        }
        let (reply, receiver) = oneshot::channel();
        let credit = Arc::new(Credit {
            shared: Arc::downgrade(&self.shared),
            bytes: AtomicUsize::new(bytes),
        });
        let result_credit = credit.clone();
        q.commands.push_back(Command {
            admitted,
            _credit: credit.clone(),
            action: Box::new(move |engine| {
                let result = action(engine).and_then(|result| {
                    result_credit.grow(result_bytes(&result)?)?;
                    Ok(result)
                });
                Box::new(move || {
                    let _ = reply.send(result);
                })
            }),
        });
        q.info.pending += 1;
        q.info.pending_bytes += bytes;
        self.shared.wake.notify_one();
        Ok(Ticket {
            receiver,
            _credit: credit,
        })
    }
    pub fn add(&self, config: Config) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.config(&config)?;
        let config = config.clone();
        self.call(bytes, move |s| s.add(config))
    }
    pub fn subscribe_application(&self, name: String) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.atom(&name)?;
        self.call(bytes, move |s| {
            s.reporters
                .get_mut(&name)
                .ok_or(Error::NotFound)?
                .application = true;
            s.refresh(true)
        })
    }
    pub fn remove(&self, name: String) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.atom(&name)?;
        let name = name.clone();
        self.call(bytes, move |s| s.remove(&name))
    }
    pub fn enable(&self, name: String, enabled: bool) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.atom(&name)?;
        let name = name.clone();
        self.call(bytes, move |s| s.enable(&name, enabled))
    }
    pub fn subscribe(&self, key: Subscription, interval: Interval) -> Result<Ticket<()>, Error> {
        let bytes = self
            .shared
            .limits
            .subscription(&key)?
            .saturating_add(self.shared.limits.interval(&interval)?);
        let key = key.clone();
        let interval = interval.clone();
        self.call(bytes, move |s| s.subscribe(key, interval))
    }
    // The native convenience API omits retry from the ordered-set key and
    // therefore cannot find a subscription admitted by subscribe/4..6.
    pub fn unsubscribe_native(&self, key: Subscription) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.subscription(&key)?;
        self.call(bytes, move |_| Err(Error::NotFound))
    }
    pub fn unsubscribe(&self, key: Subscription) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.subscription(&key)?;
        let key = key.clone();
        self.call(bytes, move |s| s.unsubscribe(&key))
    }
    pub fn unsubscribe_all(&self, reporter: String, metric: Selector) -> Result<Ticket<()>, Error> {
        let key = Subscription {
            reporter,
            metric,
            points: Points::default_points(),
            retry: true,
            extra: report::Extra::Undefined,
        };
        let bytes = self.shared.limits.subscription(&key)?;
        let key = key.clone();
        self.call(bytes, move |s| {
            s.subs
                .retain(|sub| sub.key.reporter != key.reporter || sub.key.metric != key.metric);
            Ok(())
        })
    }
    pub fn set_interval(
        &self,
        reporter: String,
        name: String,
        period: Period,
        delay_ms: Option<u64>,
    ) -> Result<Ticket<()>, Error> {
        let i = NamedInterval {
            name,
            period,
            delay_ms: delay_ms.unwrap_or(0),
        };
        let bytes = self
            .shared
            .limits
            .atom(&reporter)?
            .saturating_add(self.shared.limits.named(&i)?);
        let reporter = reporter.clone();
        self.call(bytes, move |s| {
            s.set_interval(&reporter, i, delay_ms.is_some())
        })
    }
    pub fn delete_interval(&self, reporter: String, name: String) -> Result<Ticket<()>, Error> {
        let bytes = self
            .shared
            .limits
            .atom(&reporter)?
            .saturating_add(self.shared.limits.atom(&name)?);
        let reporter = reporter.clone();
        let name = name.clone();
        self.call(bytes, move |s| {
            let r = s.reporters.get_mut(&reporter).ok_or(Error::NotFound)?;
            let i = r
                .intervals
                .iter()
                .position(|i| i.spec.name == name)
                .ok_or(Error::NotFound)?;
            r.intervals.remove(i);
            Ok(())
        })
    }
    pub fn restart_intervals(&self, reporter: String) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.atom(&reporter)?;
        let reporter = reporter.clone();
        self.call(bytes, move |s| s.restart_intervals(&reporter))
    }
    pub fn trigger(&self, reporter: String, name: String) -> Result<Ticket<()>, Error> {
        let bytes = self
            .shared
            .limits
            .atom(&reporter)?
            .saturating_add(self.shared.limits.atom(&name)?);
        let reporter = reporter.clone();
        let name = name.clone();
        self.call(bytes, move |s| {
            s.report_batch(&reporter, &name);
            Ok(())
        })
    }
    pub fn reporters(&self) -> Result<Ticket<Vec<ReporterInfo>>, Error> {
        self.call_sized(
            64,
            |s| Ok(s.reporters.values().map(Reporter::info).collect()),
            |rows: &Vec<ReporterInfo>| {
                Ok(rows
                    .iter()
                    .map(|r| {
                        256 + r.name.len()
                            + r.module.len()
                            + r.intervals
                                .iter()
                                .map(|i| 128 + i.interval.name.len())
                                .sum::<usize>()
                    })
                    .sum())
            },
        )
    }
    pub fn subscriptions(&self, name: String) -> Result<Ticket<Vec<SubscriptionInfo>>, Error> {
        let limits = self.shared.limits;
        let bytes = limits.atom(&name)?;
        let name = name.clone();
        self.call_sized(
            bytes,
            move |s| {
                Ok(s.subs
                    .iter()
                    .rev()
                    .filter(|sub| sub.key.reporter == name)
                    .map(|s| SubscriptionInfo {
                        key: s.key.clone(),
                        interval: s.interval.clone(),
                        timer: s.timer,
                        armed: s.due.is_some(),
                    })
                    .collect())
            },
            move |rows: &Vec<SubscriptionInfo>| {
                rows.iter().try_fold(32usize, |n, s| {
                    Ok(n.saturating_add(limits.subscription(&s.key)?)
                        .saturating_add(limits.interval(&s.interval)?))
                })
            },
        )
    }
    pub fn restart(&self, name: String, generation: u64) -> Result<Ticket<()>, Error> {
        let bytes = self.shared.limits.atom(&name)?;
        let name = name.clone();
        self.call(bytes, move |s| {
            let r = s.reporters.get_mut(&name).ok_or(Error::NotFound)?;
            if r.generation != generation {
                return Err(Error::Stale);
            }
            if let Some(worker) = &mut r.worker {
                worker.stop(true);
            }
            s.reap();
            Ok(())
        })
    }
}
pub struct Owner {
    client: Client,
    thread: Option<thread::JoinHandle<()>>,
}
impl Owner {
    pub fn spawn(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
    ) -> Result<Self, Error> {
        Self::spawn_with_clock(limits, source, output, Arc::new(SystemClock))
    }
    pub fn spawn_with_clock(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        limits.validate()?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                commands: VecDeque::new(),
                info: Info {
                    running: true,
                    ..Default::default()
                },
                closing: false,
                woken: false,
            }),
            wake: Condvar::new(),
            changed: watch::channel(()).0,
            done: watch::channel(false).0,
            limits,
            clock,
        });
        let client = Client {
            updates: shared.changed.subscribe(),
            shared: shared.clone(),
        };
        source.watch(Signal(Arc::downgrade(&shared)))?;
        let state = shared.clone();
        let thread = thread::Builder::new()
            .name("ae-metrics-report-owner".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(state.clone(), source, output)
                }));
                let discarded = {
                    let mut q = state.lock();
                    q.info.running = false;
                    q.closing = true;
                    let discarded = std::mem::take(&mut q.commands);
                    if result.is_err() {
                        q.info.last_error = Some(Error::Panicked);
                    }
                    discarded
                };
                drop(discarded);
                state.done.send_replace(true);
                state.changed.send_replace(());
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            client,
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub async fn finish(&mut self) {
        self.client.shutdown();
        let mut done = self.client.shared.done.subscribe();
        while !*done.borrow_and_update() {
            let _ = done.changed().await;
        }
        self.join();
    }
    fn join(&mut self) {
        if let Some(join) = self.thread.take() {
            let _ = join.join();
        }
        self.client.shared.lock().info.joined = true;
        self.client.shared.changed.send_replace(());
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.client.shutdown();
        self.join();
    }
}
struct Timer {
    spec: NamedInterval,
    timer: Option<u64>,
    due: Option<Instant>,
    expected_us: i128,
    delay: bool,
}
struct Subscriber {
    key: Subscription,
    interval: Interval,
    timer: Option<u64>,
    due: Option<Instant>,
    expected_us: i128,
    bytes: usize,
}
struct Reporter {
    application: bool,
    config: Config,
    worker: Option<callback::Worker>,
    generation: u64,
    previous_joined: bool,
    attempts: u64,
    history: VecDeque<i128>,
    intervals: Vec<Timer>,
    bytes: usize,
    last_error: Option<Error>,
}
impl Reporter {
    fn info(&self) -> ReporterInfo {
        ReporterInfo {
            name: self.config.name.clone(),
            module: self.config.module.clone(),
            enabled: self.config.enabled,
            generation: self.generation,
            alive: self.worker.as_ref().is_some_and(|w| w.info().running),
            previous_joined: self.previous_joined,
            restart_attempts: self.attempts,
            intervals: self
                .intervals
                .iter()
                .map(|i| IntervalInfo {
                    interval: i.spec.clone(),
                    timer: i.timer,
                    armed: i.due.is_some(),
                })
                .collect(),
            last_error: self.last_error.clone(),
        }
    }
}
struct Engine {
    shared: Arc<Shared>,
    source: Arc<dyn Source>,
    output: statsd::Client,
    reporters: BTreeMap<String, Reporter>,
    subs: Vec<Subscriber>,
    timer: u64,
    catalog_revision: Option<u64>,
    catalog: BTreeMap<Name, u64>,
    info: Info,
}
impl Engine {
    fn refresh(&mut self, force: bool) -> Result<(), Error> {
        let Some(catalog) =
            self.source
                .catalog(if force { None } else { self.catalog_revision })?
        else {
            return Ok(());
        };
        if catalog.entries.len() > self.shared.limits.subscriptions {
            return Err(Error::Limit);
        }
        let mut bytes = 0usize;
        for (entry, _) in &catalog.entries {
            bytes = bytes
                .saturating_add(
                    self.shared
                        .limits
                        .report
                        .name(&entry.name)
                        .map_err(Error::Report)?,
                )
                .saturating_add(self.shared.limits.atom(&entry.kind)?);
        }
        if bytes > self.shared.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let names = self
            .reporters
            .iter()
            .filter(|(_, r)| r.application)
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        for (entry, version) in &catalog.entries {
            if self.catalog.get(&entry.name) != Some(version) {
                let ids = self
                    .subs
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| {
                        s.key.metric == Selector::Metric(entry.name.clone())
                            && self
                                .reporters
                                .get(&s.key.reporter)
                                .is_some_and(|r| r.config.enabled)
                    })
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                for i in ids {
                    if let Interval::Milliseconds(ms) = self.subs[i].interval {
                        let (timer, due, expected_us) = self.schedule(ms, None)?;
                        self.subs[i].timer = Some(timer);
                        self.subs[i].due = Some(due);
                        self.subs[i].expected_us = expected_us;
                    }
                }
            }
            if entry.enabled
                && entry
                    .name
                    .starts_with(&[Part::atom("ae"), Part::atom("epoch")])
            {
                for name in &names {
                    self.subscribe(
                        Subscription {
                            reporter: name.clone(),
                            metric: Selector::Metric(entry.name.clone()),
                            points: if matches!(entry.kind.as_str(), "counter" | "gauge") {
                                Points::Many(vec![Part::atom("value")])
                            } else {
                                Points::default_points()
                            },
                            retry: true,
                            extra: report::Extra::Undefined,
                        },
                        Interval::Named("default".into()),
                    )?;
                }
            }
        }
        self.info.catalog_bytes = bytes;
        self.catalog_revision = Some(catalog.revision);
        self.catalog = catalog
            .entries
            .into_iter()
            .map(|(entry, version)| (entry.name, version))
            .collect();
        Ok(())
    }
    fn timer(&mut self) -> Result<u64, Error> {
        self.timer = self.timer.checked_add(1).ok_or(Error::Sequence)?;
        Ok(self.timer)
    }
    fn schedule(&mut self, ms: u64, base: Option<i128>) -> Result<(u64, Instant, i128), Error> {
        let now = self.shared.clock.now();
        let (remaining, base) = base
            .map(|base| timing::adjust(ms, base, now.unix_microseconds))
            .unwrap_or((ms, now.unix_microseconds));
        let expected = timing::next_fire(base, ms);
        Ok((
            self.timer()?,
            now.monotonic + Duration::from_millis(remaining),
            expected,
        ))
    }
    fn arm(&mut self, spec: NamedInterval) -> Result<Timer, Error> {
        let (delay, ms) = match spec.period {
            Period::Manual => {
                return Ok(Timer {
                    spec,
                    timer: None,
                    due: None,
                    expected_us: 0,
                    delay: false,
                });
            }
            Period::Milliseconds(ms) => (
                spec.delay_ms != 0,
                if spec.delay_ms == 0 {
                    ms
                } else {
                    spec.delay_ms
                },
            ),
        };
        let (timer, due, expected_us) = self.schedule(ms, None)?;
        Ok(Timer {
            spec,
            timer: Some(timer),
            due: Some(due),
            expected_us,
            delay,
        })
    }
    fn retained(&self) -> usize {
        self.reporters
            .values()
            .map(|r| {
                r.bytes
                    + r.intervals
                        .iter()
                        .map(|i| 64 + i.spec.name.len())
                        .sum::<usize>()
            })
            .sum::<usize>()
            + self.subs.iter().map(|s| s.bytes).sum::<usize>()
    }
    fn room(&self, extra: usize) -> Result<(), Error> {
        if extra
            > self
                .shared
                .limits
                .retained_bytes
                .saturating_sub(self.retained())
        {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
    fn add(&mut self, config: Config) -> Result<(), Error> {
        if self.reporters.contains_key(&config.name) {
            return Err(Error::AlreadyRunning);
        }
        if self.reporters.len() >= self.shared.limits.reporters {
            return Err(Error::Limit);
        }
        let bytes = self.shared.limits.config(&config)?;
        self.room(bytes.saturating_mul(2))?;
        let worker = Some(callback::Worker::spawn(
            config.options.clone(),
            self.output.clone(),
            self.shared.limits,
            self.shared.clone(),
        )?);
        let mut intervals = vec![];
        for i in config.intervals.clone() {
            intervals.push(self.arm(i)?);
        }
        self.reporters.insert(
            config.name.clone(),
            Reporter {
                application: false,
                config,
                worker,
                generation: 1,
                previous_joined: false,
                attempts: 0,
                history: VecDeque::new(),
                intervals,
                bytes,
                last_error: None,
            },
        );
        Ok(())
    }
    fn remove(&mut self, name: &str) -> Result<(), Error> {
        let mut r = self.reporters.remove(name).ok_or(Error::NotFound)?;
        if let Some(w) = &mut r.worker {
            w.stop(false);
        }
        self.subs.retain(|s| s.key.reporter != name);
        Ok(())
    }
    fn enable(&mut self, name: &str, enabled: bool) -> Result<(), Error> {
        let r = self.reporters.get_mut(name).ok_or(Error::NotFound)?;
        if r.config.enabled == enabled {
            return Ok(());
        }
        if !enabled {
            if let Some(mut w) = r.worker.take() {
                w.stop(false);
                r.previous_joined = true;
            }
            r.config.enabled = false;
            for s in self.subs.iter_mut().filter(|s| s.key.reporter == name) {
                s.due = None;
                s.timer = None;
            }
            return Ok(());
        }
        let generation = r.generation.checked_add(1).ok_or(Error::Sequence)?;
        if let Some(mut previous) = r.worker.take() {
            previous.stop(false);
            r.previous_joined = true;
        }
        let worker = callback::Worker::spawn(
            r.config.options.clone(),
            self.output.clone(),
            self.shared.limits,
            self.shared.clone(),
        )?;
        r.worker = Some(worker);
        r.generation = generation;
        r.config.enabled = true;
        self.resubscribe(name)
    }
    fn resubscribe(&mut self, name: &str) -> Result<(), Error> {
        let ids = self
            .subs
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                s.key.reporter == name && matches!(s.interval, Interval::Milliseconds(_))
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        for i in ids {
            let Interval::Milliseconds(ms) = self.subs[i].interval else {
                unreachable!()
            };
            let (timer, due, expected_us) = self.schedule(ms, None)?;
            self.subs[i].timer = Some(timer);
            self.subs[i].due = Some(due);
            self.subs[i].expected_us = expected_us;
        }
        Ok(())
    }
    fn subscribe(&mut self, key: Subscription, interval: Interval) -> Result<(), Error> {
        let enabled = self
            .reporters
            .get(&key.reporter)
            .ok_or(Error::UnknownReporter)?
            .config
            .enabled;
        if !self.source.valid(&key.metric, &key.points)? && !key.retry {
            return Err(Error::NotFound);
        }
        let position = match self
            .subs
            .binary_search_by(|s| super::order::compare(&s.key, &key))
        {
            Ok(_) => return Ok(()),
            Err(i) => i,
        };
        if self.subs.len() >= self.shared.limits.subscriptions {
            return Err(Error::Limit);
        }
        let bytes = self
            .shared
            .limits
            .subscription(&key)?
            .saturating_add(self.shared.limits.interval(&interval)?);
        self.room(bytes)?;
        let (timer, due, expected_us) = match interval {
            Interval::Milliseconds(ms) if enabled => {
                let (timer, due, expected) = self.schedule(ms, None)?;
                (Some(timer), Some(due), expected)
            }
            _ => (None, None, 0),
        };
        self.subs.insert(
            position,
            Subscriber {
                key,
                interval,
                timer,
                due,
                expected_us,
                bytes,
            },
        );
        Ok(())
    }
    fn unsubscribe(&mut self, key: &Subscription) -> Result<(), Error> {
        let i = self
            .subs
            .iter()
            .position(|s| super::order::compare(&s.key, key).is_eq())
            .ok_or(Error::NotFound)?;
        self.subs.remove(i);
        Ok(())
    }
    fn set_interval(
        &mut self,
        name: &str,
        mut spec: NamedInterval,
        replace_delay: bool,
    ) -> Result<(), Error> {
        let r = self.reporters.get(name).ok_or(Error::NotFound)?;
        let old = r.intervals.iter().position(|i| i.spec.name == spec.name);
        if old.is_none() && r.intervals.len() >= self.shared.limits.intervals {
            return Err(Error::Limit);
        }
        self.room(self.shared.limits.named(&spec)?)?;
        if let Some(i) = old
            && !replace_delay
        {
            spec.delay_ms = r.intervals[i].spec.delay_ms;
        }
        let timer = if spec.period == Period::Manual {
            Timer {
                timer: old.and_then(|i| r.intervals[i].timer),
                spec,
                due: None,
                expected_us: 0,
                delay: false,
            }
        } else {
            self.arm(spec)?
        };
        let r = self.reporters.get_mut(name).expect("checked reporter");
        if let Some(i) = old {
            r.intervals[i] = timer;
        } else {
            r.intervals.push(timer);
        }
        Ok(())
    }
    fn restart_intervals(&mut self, name: &str) -> Result<(), Error> {
        let intervals = self
            .reporters
            .get(name)
            .ok_or(Error::NotFound)?
            .intervals
            .iter()
            .map(|i| (i.spec.clone(), i.timer))
            .collect::<Vec<_>>();
        let mut timers = vec![];
        for (spec, old) in intervals {
            let mut t = self.arm(spec)?;
            if t.spec.period == Period::Manual {
                t.timer = old;
            }
            timers.push(t);
        }
        self.reporters
            .get_mut(name)
            .expect("checked reporter")
            .intervals = timers;
        Ok(())
    }
    fn report_one(&mut self, index: usize) -> bool {
        let sub = &self.subs[index];
        let key = sub.key.clone();
        let values = match self.source.read(&key.metric, &key.points) {
            Ok(v) => v,
            Err(e) => {
                self.info.source_errors = self.info.source_errors.saturating_add(1);
                self.info.last_error = Some(e);
                return key.retry;
            }
        };
        if values.is_empty() {
            return key.retry;
        }
        if let Some(worker) = self
            .reporters
            .get(&key.reporter)
            .and_then(|r| r.worker.as_ref())
            && let Err(error) = worker.submit(values, key.extra)
        {
            self.info.callback_drops = self.info.callback_drops.saturating_add(1);
            self.info.last_error = Some(error);
        }
        true
    }
    fn report_batch(&mut self, name: &str, interval: &str) {
        let base = self.shared.clock.now().unix_microseconds;
        self.report_batch_at(name, interval, base);
    }
    fn report_batch_at(&mut self, name: &str, interval: &str, base: i128) {
        if !self.reporters.get(name).is_some_and(|r| r.config.enabled) {
            return;
        }
        let ids = self
            .subs
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                s.key.reporter == name && matches!(&s.interval,Interval::Named(n)if n==interval)
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        for i in ids {
            self.report_one(i);
        }
        let period = self
            .reporters
            .get(name)
            .and_then(|r| r.intervals.iter().find(|i| i.spec.name == interval))
            .map(|i| i.spec.period);
        if let Some(Period::Milliseconds(ms)) = period {
            match self.schedule(ms, Some(base)) {
                Ok((timer, due, expected_us)) => {
                    if let Some(r) = self.reporters.get_mut(name)
                        && let Some(i) = r.intervals.iter_mut().find(|i| i.spec.name == interval)
                    {
                        i.timer = Some(timer);
                        i.due = Some(due);
                        i.expected_us = expected_us;
                        i.delay = false;
                    }
                }
                Err(e) => self.info.last_error = Some(e),
            }
        }
    }
    fn reap(&mut self) {
        let dead = self
            .reporters
            .iter()
            .filter(|(_, r)| r.worker.as_ref().is_some_and(|w| !w.info().running))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in dead {
            let r = self.reporters.get_mut(&name).expect("known child");
            let mut old = r.worker.take().expect("finished child");
            old.stop(true);
            r.previous_joined = true;
            r.last_error = old.info().last_error;
            r.attempts = r.attempts.saturating_add(1);
            let now = self.shared.clock.now().unix_microseconds / 1000;
            let mut history = r.history.clone();
            history.push_front(now);
            history.truncate(10);
            let exhausted = timing::exhausted(&history);
            if exhausted {
                let module = r.config.module.clone();
                let _ = self.remove(&module);
                continue;
            }
            r.history = history;
            match r
                .generation
                .checked_add(1)
                .ok_or(Error::Sequence)
                .and_then(|generation| {
                    callback::Worker::spawn(
                        r.config.options.clone(),
                        self.output.clone(),
                        self.shared.limits,
                        self.shared.clone(),
                    )
                    .map(|w| (generation, w))
                }) {
                Ok((generation, w)) => {
                    r.generation = generation;
                    r.worker = Some(w);
                    if let Err(e) = self.resubscribe(&name) {
                        self.info.last_error = Some(e);
                    }
                }
                Err(e) => r.last_error = Some(e),
            }
        }
    }
    fn next_due(&self) -> Option<Instant> {
        self.reporters
            .values()
            .flat_map(|r| r.intervals.iter().filter_map(|i| i.due))
            .chain(self.subs.iter().filter_map(|s| s.due))
            .min()
    }
    fn fire(&mut self) {
        let now = self.shared.clock.now().monotonic;
        let due = self
            .reporters
            .iter()
            .flat_map(|(name, r)| {
                r.intervals
                    .iter()
                    .filter(move |i| i.due.is_some_and(|t| t <= now))
                    .map(move |i| (name.clone(), i.spec.name.clone(), i.delay, i.expected_us))
            })
            .take(self.shared.limits.requests)
            .collect::<Vec<_>>();
        for (name, interval, delay, base) in due {
            self.info.timer_fires = self.info.timer_fires.saturating_add(1);
            let r = self.reporters.get_mut(&name).expect("timer owner");
            let i = r
                .intervals
                .iter_mut()
                .find(|i| i.spec.name == interval)
                .expect("timer slot");
            i.due = None;
            if !r.config.enabled {
                continue;
            }
            if delay {
                let period = i.spec.period;
                if let Period::Milliseconds(ms) = period {
                    match self.schedule(ms, None) {
                        Ok((timer, due, expected_us)) => {
                            let i = self
                                .reporters
                                .get_mut(&name)
                                .expect("timer owner")
                                .intervals
                                .iter_mut()
                                .find(|i| i.spec.name == interval)
                                .expect("timer slot");
                            i.delay = false;
                            i.timer = Some(timer);
                            i.due = Some(due);
                            i.expected_us = expected_us;
                        }
                        Err(e) => self.info.last_error = Some(e),
                    }
                }
            } else {
                self.report_batch_at(&name, &interval, base);
            }
        }
        let due = self
            .subs
            .iter()
            .enumerate()
            .filter(|(_, s)| s.due.is_some_and(|t| t <= now))
            .map(|(i, _)| i)
            .take(self.shared.limits.requests)
            .collect::<Vec<_>>();
        for i in due {
            self.info.timer_fires = self.info.timer_fires.saturating_add(1);
            let base = self.subs[i].expected_us;
            self.subs[i].due = None;
            if !self
                .reporters
                .get(&self.subs[i].key.reporter)
                .is_some_and(|r| r.config.enabled)
            {
                continue;
            }
            if self.report_one(i) {
                let Interval::Milliseconds(ms) = self.subs[i].interval else {
                    continue;
                };
                match self.schedule(ms, Some(base)) {
                    Ok((timer, due, expected_us)) => {
                        self.subs[i].timer = Some(timer);
                        self.subs[i].due = Some(due);
                        self.subs[i].expected_us = expected_us;
                    }
                    Err(e) => self.info.last_error = Some(e),
                }
            }
        }
    }
    fn publish(&self) {
        let mut q = self.shared.lock();
        q.info.reporters = self.reporters.len();
        q.info.subscriptions = self.subs.len();
        q.info.retained_bytes = self.retained();
        q.info.catalog_bytes = self.info.catalog_bytes;
        q.info.timer_fires = self.info.timer_fires;
        q.info.source_errors = self.info.source_errors;
        q.info.callback_drops = self.info.callback_drops;
        q.info.last_error = self.info.last_error.clone();
        drop(q);
        self.shared.changed.send_replace(());
    }
}
fn run(shared: Arc<Shared>, source: Arc<dyn Source>, output: statsd::Client) {
    let mut engine = Engine {
        shared: shared.clone(),
        source,
        output,
        reporters: BTreeMap::new(),
        subs: vec![],
        timer: 0,
        catalog_revision: None,
        catalog: BTreeMap::new(),
        info: Default::default(),
    };
    let mut previous_timer_turn = false;
    loop {
        if let Err(error) = engine.refresh(false) {
            engine.info.source_errors = engine.info.source_errors.saturating_add(1);
            engine.info.last_error = Some(error);
        }
        engine.reap();
        engine.publish();
        let command = {
            let mut q = shared.lock();
            loop {
                if q.closing {
                    return;
                }
                let next = engine.next_due();
                if q.commands.front().is_some_and(|c| {
                    next.is_none_or(|due| {
                        c.admitted < due || (previous_timer_turn && c.admitted == due)
                    })
                }) {
                    break q.commands.pop_front();
                }
                if next.is_some_and(|due| due <= shared.clock.now().monotonic) {
                    break None;
                }
                if q.woken {
                    q.woken = false;
                    break None;
                }
                q = if let Some(due) = engine.next_due() {
                    let (q, result) = shared
                        .wake
                        .wait_timeout(
                            q,
                            due.saturating_duration_since(shared.clock.now().monotonic),
                        )
                        .unwrap_or_else(|e| e.into_inner());
                    if result.timed_out() {
                        break None;
                    }
                    q
                } else {
                    shared.wake.wait(q).unwrap_or_else(|e| e.into_inner())
                };
            }
        };
        if let Some(command) = command {
            previous_timer_turn = false;
            let complete = (command.action)(&mut engine);
            {
                let mut q = shared.lock();
                q.info.completed = q.info.completed.saturating_add(1);
            }
            engine.publish();
            complete();
        } else {
            engine.fire();
            previous_timer_turn = true;
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        for reporter in self.reporters.values_mut() {
            if let Some(worker) = &mut reporter.worker {
                worker.stop(thread::panicking());
            }
        }
    }
}
