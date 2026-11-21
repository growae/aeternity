use super::*;
use crate::tasks::Tasks;
use ae_p2p::discovery::Peer;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
};
use tokio::{net::TcpListener, time::Instant};

mod admission;
mod operations;

type PoolFuture = Pin<Box<dyn Future<Output = pool::Status> + Send>>;
enum Resolution {
    Discovery,
    Dial { peer: Peer, probe: bool },
}
enum Completion {
    Dns(resolver::Completion),
    Socket(transport::Event),
    Ping {
        token: policy::Token,
        outcome: Option<PingResult>,
    },
}
struct Admitted {
    client: net::Client,
    connection: Connection,
}
pub struct Exit {
    pub status: Status,
    pub registry: Result<registry::Service, registry::worker::Error>,
    pub pool: Option<pool::Status>,
}
pub struct Service {
    config: Config,
    sockets: transport::Sockets,
    pool: pool::Handle,
    pool_run: Option<PoolFuture>,
    pool_exit: Option<pool::Status>,
    listener: Option<TcpListener>,
    resolver: resolver::Resolver,
    resolutions: BTreeMap<policy::Token, Resolution>,
    ready_dials: BTreeMap<policy::Token, (Peer, bool)>,
    dial_retry: Instant,
    dns: Tasks<policy::Token, resolver::Completion>,
    ping_jobs: Tasks<policy::Token, Option<PingResult>>,
    ping_tokens: BTreeMap<policy::Token, net::Client>,
    admitted: BTreeMap<policy::Token, Admitted>,
    provisional: BTreeMap<policy::Token, Admitted>,
    rejected_admissions: std::collections::BTreeSet<policy::Token>,
    pending: VecDeque<Completion>,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    stopped: watch::Receiver<bool>,
    published: watch::Sender<Status>,
    current: Status,
    origin: Instant,
    effect_retry: Instant,
    startup_retry: Instant,
    frame: Option<registry::worker::Frame>,
    stopping: bool,
    client: registry::worker::Client,
    // Last: abrupt drop closes owned connections before a synchronous store
    // thread join could wait for an executing database operation.
    worker: registry::worker::Worker,
}
impl Service {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        config: Config,
        worker: registry::worker::Worker,
        resolver: resolver::Resolver,
        sockets: transport::Sockets,
        pool: pool::Handle,
        pool_service: pool::Service,
        commands: mpsc::Receiver<Command>,
        events: mpsc::Sender<Event>,
        stopped: watch::Receiver<bool>,
        published: watch::Sender<Status>,
        current: Status,
    ) -> Self {
        let now = Instant::now();
        let client = worker.client();
        Self {
            config,
            sockets,
            pool,
            pool_run: Some(Box::pin(pool_service.run())),
            pool_exit: None,
            listener: None,
            resolver,
            resolutions: BTreeMap::new(),
            ready_dials: BTreeMap::new(),
            dial_retry: now,
            dns: Tasks::new(),
            ping_jobs: Tasks::new(),
            ping_tokens: BTreeMap::new(),
            admitted: BTreeMap::new(),
            provisional: BTreeMap::new(),
            rejected_admissions: std::collections::BTreeSet::new(),
            pending: VecDeque::new(),
            commands,
            events,
            stopped,
            published,
            current,
            origin: now,
            effect_retry: now,
            startup_retry: now,
            frame: None,
            stopping: false,
            client,
            worker,
        }
    }
    fn now(&self) -> Result<u64, Error> {
        u64::try_from(self.origin.elapsed().as_millis()).map_err(|_| Error::TimeOverflow)
    }
    fn publish(&mut self) {
        self.current.sockets = self.sockets.occupied();
        self.current.resolutions = self.resolutions.len() + self.ready_dials.len();
        self.current.pings = self.ping_tokens.len();
        self.published.send_replace(self.current.clone());
    }
    fn begin_stop(&mut self) {
        self.stopping = true;
        self.current.ready = false;
        self.listener = None;
        self.current.listening = None;
        self.sockets.stop();
        self.resolver.stop();
        self.pool.shutdown();
    }
    fn fail(&mut self, error: Error) {
        if self.current.failure.is_none() {
            self.current.failure = Some(error);
        }
        self.begin_stop();
    }
    fn pool_finished(&mut self, status: pool::Status) {
        self.pool_run = None;
        self.pool_exit = Some(status);
        if !self.stopping {
            self.fail(Error::Pool(pool::Error::Closed));
        }
    }
    // Drive owned network work while a serialized registry or attachment reply
    // is pending. Completion buffering is bounded and never executes another
    // policy mutation behind an acknowledgement or a storage recovery.
    async fn wait<T>(&mut self, future: impl Future<Output = T>) -> T {
        tokio::pin!(future);
        loop {
            let room = self.pending.len() < self.config.completions;
            tokio::select! {
                result = &mut future => return result,
                status = poll_pool(&mut self.pool_run) => self.pool_finished(status),
                _ = self.stopped.changed(), if !self.stopping => self.begin_stop(),
                _ = self.events.closed(), if !self.stopping => self.begin_stop(),
                event = self.sockets.next(), if room && self.sockets.occupied() != 0 => {
                    if let Some(event) = event { self.pending.push_back(Completion::Socket(event)); }
                },
                (_, result) = self.dns.next(), if room && !self.stopping => self.pending.push_back(Completion::Dns(result)),
                (token, outcome) = self.ping_jobs.next(), if room && !self.stopping => self.pending.push_back(Completion::Ping { token, outcome }),
            }
        }
    }
    async fn refresh(&mut self) -> Result<(), Error> {
        let ticket = self.client.frame()?;
        let frame = self.wait(ticket).await?;
        self.current.policy = Some(frame.inspection.clone());
        self.current.startup = frame.startup;
        self.frame = Some(frame);
        Ok(())
    }
    pub async fn run(mut self) -> Exit {
        if let Err(error) = self.run_inner().await {
            self.fail(error);
        }
        self.begin_stop();
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            command.finish(Err(Error::Closed));
        }
        self.pending.clear();
        self.dns.clear();
        self.resolutions.clear();
        self.ready_dials.clear();
        self.ping_jobs.clear();
        self.ping_tokens.clear();
        self.admitted.clear();
        self.provisional.clear();
        self.rejected_admissions.clear();
        if !self.current.policy.as_ref().is_some_and(|p| p.closed) {
            if let Err(error) = self.apply(policy::Command::Shutdown).await {
                if self.current.failure.is_none() {
                    self.current.failure = Some(error);
                }
            } else if let Err(error) = self.refresh().await
                && self.current.failure.is_none()
            {
                self.current.failure = Some(error);
            }
        }
        self.pending.clear();
        while self.pool_run.is_some() {
            tokio::select! {
                status = poll_pool(&mut self.pool_run) => self.pool_finished(status),
                _ = self.sockets.next(), if self.sockets.occupied() != 0 => (),
            }
        }
        while self.sockets.occupied() != 0 {
            let _ = self.sockets.next().await;
        }
        self.resolver.join().await;
        self.worker.stop();
        let registry = self.worker.join().await;
        self.current.running = false;
        self.current.joined = true;
        self.publish();
        Exit {
            status: self.current.clone(),
            registry,
            pool: self.pool_exit.take(),
        }
    }
    async fn run_inner(&mut self) -> Result<(), Error> {
        if *self.stopped.borrow() {
            return Ok(());
        }
        if let Some(address) = self.config.listen {
            let listener = TcpListener::bind(address).await?;
            let std::net::SocketAddr::V4(address) = listener.local_addr()? else {
                return Err(Error::Configuration);
            };
            self.current.listening = Some(address);
            self.listener = Some(listener);
        }
        let ticket = self.client.configure_startup(
            self.now()?,
            self.config.startup.clone(),
            self.config.startup_limits,
        )?;
        self.wait(ticket).await?;
        self.refresh().await?;
        self.current.ready = true;
        loop {
            if self.stopping || *self.stopped.borrow() {
                return Ok(());
            }
            let mut progress = false;
            if let Some(completion) = self.pending.pop_front() {
                self.completed(completion).await?;
                progress = true;
            }
            if self.stopping {
                return Ok(());
            }
            if let Some(timer) = self.frame.as_ref().and_then(|f| f.timer)
                && timer.deadline <= self.now()?
            {
                self.apply(policy::Command::Timeout(timer.token)).await?;
                self.refresh().await?;
                progress = true;
            }
            if Instant::now() >= self.effect_retry {
                progress |= self.effect().await?;
            }
            if self.stopping {
                return Ok(());
            }
            if Instant::now() >= self.dial_retry {
                progress |= self.dial_one().await?;
            }
            if self.stopping {
                return Ok(());
            }
            if Instant::now() >= self.startup_retry
                && self
                    .frame
                    .as_ref()
                    .and_then(|f| f.startup)
                    .is_some_and(|s| s.queued != 0)
            {
                let ticket = self
                    .client
                    .advance_startup(self.now()?, self.config.startup_limits.maximum_step)?;
                let status = self.wait(ticket).await?;
                self.startup_retry = Instant::now() + self.config.retry_interval;
                self.refresh().await?;
                progress |= !status.backpressured;
            }
            match self.commands.try_recv() {
                Ok(command) => {
                    self.command(command).await;
                    progress = true;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                Err(mpsc::error::TryRecvError::Empty) => (),
            }
            self.publish();
            if self.stopping {
                return Ok(());
            }
            let deadline = self.deadline()?;
            let room = self.pending.len() < self.config.completions;
            tokio::select! {
                _ = self.stopped.changed() => return Ok(()),
                _ = self.events.closed() => return Ok(()),
                status = poll_pool(&mut self.pool_run) => self.pool_finished(status),
                command = self.commands.recv() => match command {
                    Some(command) => self.command(command).await,
                    None => return Ok(()),
                },
                event = self.sockets.next(), if room && self.sockets.occupied() != 0 => {
                    if let Some(event) = event { self.pending.push_back(Completion::Socket(event)); }
                },
                (_, result) = self.dns.next(), if room => self.pending.push_back(Completion::Dns(result)),
                (token, outcome) = self.ping_jobs.next(), if room => self.pending.push_back(Completion::Ping { token, outcome }),
                socket = accept(&self.listener), if room => {
                    let (socket, address) = socket?;
                    self.inbound(socket, address).await?;
                },
                _ = sleep(deadline) => (),
                _ = tokio::task::yield_now(), if progress || !self.pending.is_empty() => (),
            }
        }
    }
    fn deadline(&self) -> Result<Option<Instant>, Error> {
        let mut deadline = self
            .frame
            .as_ref()
            .and_then(|f| f.timer)
            .map(|t| {
                self.origin
                    .checked_add(Duration::from_millis(t.deadline))
                    .ok_or(Error::TimeOverflow)
            })
            .transpose()?;
        let frame = self.frame.as_ref();
        for next in [
            frame
                .and_then(|f| f.delivery.as_ref())
                .map(|_| self.effect_retry),
            frame
                .and_then(|f| f.startup)
                .filter(|s| s.queued != 0)
                .map(|_| self.startup_retry),
            (!self.ready_dials.is_empty()).then_some(self.dial_retry),
        ]
        .into_iter()
        .flatten()
        {
            deadline = Some(deadline.map_or(next, |old| old.min(next)));
        }
        Ok(deadline)
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.begin_stop();
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            command.finish(Err(Error::Closed));
        }
        self.pending.clear();
        self.dns.clear();
        self.ping_jobs.clear();
        // Close every owned driver before joining any blocking pool on drop.
        self.pool_run.take();
        self.worker.stop();
        if self.current.running {
            self.current.running = false;
            if self.current.failure.is_none() {
                self.current.failure = Some(Error::Closed);
            }
            self.publish();
        }
    }
}
async fn poll_pool(pool: &mut Option<PoolFuture>) -> pool::Status {
    match pool {
        Some(future) => future.await,
        None => std::future::pending().await,
    }
}
async fn accept(
    listener: &Option<TcpListener>,
) -> Result<(tokio::net::TcpStream, std::net::SocketAddr), std::io::Error> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}
async fn sleep(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
