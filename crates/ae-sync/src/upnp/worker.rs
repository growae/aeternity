use super::*;
use rand::Rng;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub network: Settings,
    pub system_gateways: bool,
    pub ports: PortsSource,
    pub listener: Option<ListenerSource>,
    pub requests: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            network: Settings::default(),
            system_gateways: true,
            ports: PortsSource::new(Ports {
                internal: 3015,
                external: 3015,
            })
            .expect("default ports"),
            listener: None,
            requests: 16,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        self.network.validate()?;
        if self.requests == 0 || self.requests > 1024 {
            return Err(Error::Configuration);
        }
        self.ports.get().validate()
    }
    fn network(&self, listener: Option<super::listener::BoundListener>) -> Result<Network, Error> {
        let mut settings = self.network.clone();
        if let Some(listener) = listener
            && !listener.address.ip().is_unspecified()
        {
            if !settings.bind.is_unspecified() && settings.bind != *listener.address.ip() {
                return Err(Error::Configuration);
            }
            settings.bind = *listener.address.ip();
        }
        if self.system_gateways {
            Network::from_system(settings)
        } else {
            Network::new(settings)
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub active: bool,
    pub retiring: bool,
    pub preparing: bool,
    pub prepared: bool,
    pub pending: usize,
    pub attempts: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub retirements: u64,
    pub retirement_completions: u64,
    pub mapping: Option<Mapping>,
    pub retained_mappings: usize,
    pub uncertain_mappings: usize,
    pub retiring_mappings: usize,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.0.blocking_recv().map_err(|_| Error::Disconnected)?
    }
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Disconnected)))
    }
}
struct Shared {
    info: Mutex<Info>,
    stop: watch::Sender<bool>,
    limit: usize,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Info> {
        self.info.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        self.lock().accepting = false;
        self.stop.send_replace(true);
    }
}
enum Command {
    Refresh(oneshot::Sender<Result<u64, Error>>),
    #[cfg(test)]
    Fail,
    #[cfg(test)]
    Pause(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ),
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::Sender<oneshot::Sender<Result<(), Error>>>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.lock().clone()
    }
    pub fn refresh(&self) -> Result<Ticket<u64>, Error> {
        let mut info = self.shared.lock();
        if !info.accepting {
            return Err(Error::Closed);
        }
        if info.pending >= self.shared.limit {
            return Err(Error::Busy);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .try_send(Command::Refresh(reply))
            .map_err(|_| Error::Busy)?;
        info.pending += 1;
        Ok(Ticket(result))
    }
    // A separate reserved slot makes application cleanup independent of the
    // administrative refresh queue. Once accepted, this incarnation quiesces.
    // Dropping the ticket does not cancel the accepted cleanup.
    pub fn prepare_stop(&self) -> Result<Ticket<()>, Error> {
        let mut info = self.shared.lock();
        if !info.accepting {
            return Err(Error::Closed);
        }
        let (reply, result) = oneshot::channel();
        self.cleanup.try_send(reply).map_err(|_| Error::Busy)?;
        info.accepting = false;
        info.preparing = true;
        Ok(Ticket(result))
    }
    #[cfg(test)]
    pub(crate) fn fail(&self) {
        self.commands.try_send(Command::Fail).unwrap();
    }
    #[cfg(test)]
    pub(super) fn pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (release, resume) = std::sync::mpsc::sync_channel(1);
        self.commands
            .try_send(Command::Pause(entered, resume))
            .unwrap();
        (waiting, release)
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        let delay = Duration::from_millis(rand::thread_rng().gen_range(1..=1000));
        Self::with_timing(config, delay, RENEWAL_INTERVAL)
    }
    pub(super) fn with_timing(
        config: Config,
        initial: Duration,
        renewal: Duration,
    ) -> Result<Self, Error> {
        config.validate()?;
        if renewal.is_zero()
            || Instant::now().checked_add(initial).is_none()
            || Instant::now().checked_add(renewal).is_none()
        {
            return Err(Error::Configuration);
        }
        let claim = config.ports.claim()?;
        let source = config.ports.clone();
        let (stop, stopped) = watch::channel(false);
        let shared = Arc::new(Shared {
            info: Mutex::new(Info {
                running: true,
                accepting: true,
                retained_mappings: source.retained_mappings(),
                uncertain_mappings: source.uncertain_mappings(),
                retiring_mappings: source.retiring_mappings(),
                ..Default::default()
            }),
            stop,
            limit: config.requests,
        });
        let (commands, receiver) = mpsc::channel(config.requests);
        let (cleanup, cleanups) = mpsc::channel(1);
        let client = Client {
            shared: shared.clone(),
            commands,
            cleanup,
        };
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("ae-port-mapping".into())
            .spawn(move || {
                let _claim = claim;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(r) => r,
                        Err(_) => {
                            let _ = ready.send(Err(Error::Spawn));
                            return Err(Error::Spawn);
                        }
                    };
                    let driver = match run(
                        config,
                        shared.clone(),
                        receiver,
                        cleanups,
                        stopped,
                        initial,
                        renewal,
                    ) {
                        Ok(driver) => driver,
                        Err(error) => {
                            let _ = ready.send(Err(error.clone()));
                            return Err(error);
                        }
                    };
                    let _ = ready.send(Ok(()));
                    runtime.block_on(driver);
                    Ok(())
                }));
                let mut info = shared.lock();
                info.running = false;
                info.accepting = false;
                info.active = false;
                info.retiring = false;
                info.preparing = false;
                info.pending = 0;
                info.retained_mappings = source.retained_mappings();
                info.uncertain_mappings = source.uncertain_mappings();
                info.retiring_mappings = source.retiring_mappings();
                info.failure = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e),
                    Err(_) => Some(Error::Panicked),
                };
            })
            .map_err(|_| Error::Spawn)?;
        match started.recv() {
            Ok(Ok(())) => Ok(Self {
                client,
                thread: Some(thread),
            }),
            other => {
                let _ = thread.join();
                Err(other.ok().and_then(Result::err).unwrap_or(Error::Panicked))
            }
        }
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn info(&self) -> Info {
        self.client.info()
    }
    pub fn request_shutdown(&self) {
        self.client.shared.stop();
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Panicked)?;
        }
        let mut info = self.client.shared.lock();
        info.joined = true;
        info.failure.clone().map_or(Ok(()), Err)
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.join()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.join();
    }
}

enum Outcome {
    Mapped {
        mapping: Mapping,
        listener: Option<super::listener::BoundListener>,
        began: std::time::Instant,
    },
    Retired,
}
async fn cancel(attempts: &mut JoinSet<Result<Outcome, Error>>, shared: &Shared) {
    let occupied = !attempts.is_empty();
    attempts.abort_all();
    while attempts.join_next().await.is_some() {}
    let mut info = shared.lock();
    info.active = false;
    info.retiring = false;
    if occupied {
        info.cancelled = info.cancelled.saturating_add(1);
    }
}
async fn start(
    attempts: &mut JoinSet<Result<Outcome, Error>>,
    shared: &Shared,
    config: &Config,
    advertisement: Option<&super::listener::AdvertisementLease>,
) -> Result<u64, Error> {
    if let Some(advertisement) = advertisement {
        advertisement.clear();
    }
    let listener = config.listener.as_ref().and_then(ListenerSource::current);
    if config.listener.is_some() && listener.is_none() {
        return Err(Error::NotReady);
    }
    cancel(attempts, shared).await;
    shared.lock().mapping = None;
    let config = config.clone();
    // Start before discovery/cleanup: a slow response must not extend a lease.
    let began = std::time::Instant::now();
    attempts.spawn(async move {
        timeout(MAPPING_TIMEOUT, async {
            let network = config.network(listener)?;
            let gateway = network.discover().await?;
            let configured = listener
                .map(|bound| bound.ports())
                .unwrap_or_else(|| config.ports.get());
            timeout(
                CLEANUP_TIMEOUT,
                config.ports.retire_except(&gateway, configured),
            )
            .await
            .map_err(|_| Error::Timeout)??;
            if listener.is_some()
                && config.listener.as_ref().and_then(ListenerSource::current) != listener
            {
                return Err(Error::NotReady);
            }
            config
                .ports
                .add_ports(network, gateway, configured)
                .await
                .map(|mapping| Outcome::Mapped {
                    mapping,
                    listener,
                    began,
                })
        })
        .await
        .map_err(|_| Error::Timeout)?
    });
    let mut info = shared.lock();
    info.active = true;
    info.attempts = info.attempts.saturating_add(1);
    Ok(info.attempts)
}
async fn retire(attempts: &mut JoinSet<Result<Outcome, Error>>, shared: &Shared, config: &Config) {
    cancel(attempts, shared).await;
    if config.ports.retained_mappings() == 0 {
        return;
    }
    let config = config.clone();
    attempts.spawn(async move { prepare(&config).await.map(|()| Outcome::Retired) });
    let mut info = shared.lock();
    info.mapping = None;
    info.active = true;
    info.retiring = true;
    info.retirements = info.retirements.saturating_add(1);
}
async fn prepare(config: &Config) -> Result<(), Error> {
    timeout(CLEANUP_TIMEOUT, config.ports.cleanup())
        .await
        .map_err(|_| Error::Timeout)?
}
fn run(
    config: Config,
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
    mut cleanups: mpsc::Receiver<oneshot::Sender<Result<(), Error>>>,
    mut stopped: watch::Receiver<bool>,
    initial: Duration,
    renewal: Duration,
) -> Result<impl Future<Output = ()>, Error> {
    let advertisement = config
        .listener
        .as_ref()
        .map(ListenerSource::advertise)
        .transpose()?;
    // Capture notifications before acknowledging startup. Publications between
    // construction and the first poll must not be mistaken for initial state.
    let mut listener = config.listener.as_ref().map(ListenerSource::subscribe);
    Ok(async move {
        let mut attempts = JoinSet::new();
        let mut next = Instant::now() + initial;
        let mut prepared = false;
        loop {
            {
                let mut info = shared.lock();
                info.retained_mappings = config.ports.retained_mappings();
                info.uncertain_mappings = config.ports.uncertain_mappings();
                info.retiring_mappings = config.ports.retiring_mappings();
            }
            if *stopped.borrow() || stopped.has_changed().is_err() {
                break;
            }
            tokio::select! {
                biased;
                _=stopped.changed()=>break,
                cleanup=cleanups.recv(),if !prepared=>{
                    let Some(reply)=cleanup else {break;};
                    if let Some(advertisement) = &advertisement { advertisement.clear(); }
                    cancel(&mut attempts,&shared).await;
                    let result=tokio::select! {
                        biased;
                        _=stopped.changed()=>Err(Error::Closed),
                        result=prepare(&config)=>result,
                    };
                    {let mut info=shared.lock();info.preparing=false;info.prepared=true;info.mapping=None;info.last_error=result.as_ref().err().cloned();}
                    prepared=true;
                    let _=reply.send(result);
                },
                _=async { match &mut listener { Some(source)=>{let _=source.changed().await;},None=>std::future::pending().await } },if !prepared=>{
                    cancel(&mut attempts,&shared).await;
                    shared.lock().mapping=None;
                    if start(&mut attempts,&shared,&config,advertisement.as_ref()).await == Err(Error::NotReady) {
                        retire(&mut attempts,&shared,&config).await;
                    }
                    next=Instant::now()+renewal;
                },
                _=sleep_until(next),if !prepared=>{
                    if start(&mut attempts,&shared,&config,advertisement.as_ref()).await == Err(Error::NotReady) {
                        retire(&mut attempts,&shared,&config).await;
                    }
                    next=Instant::now()+renewal;
                },
                result=attempts.join_next(),if !attempts.is_empty()=>{
                    let result=result.expect("active mapping attempt").unwrap_or(Err(Error::Panicked));
                    let mut info=shared.lock();info.active=false;
                    if info.retiring {info.retirement_completions=info.retirement_completions.saturating_add(1);} else {info.completed=info.completed.saturating_add(1);}
                    info.retiring=false;
                    match result {Ok(Outcome::Mapped { mapping, listener, began })=>{
                        if let (Some(advertisement), Some(bound)) = (&advertisement, listener)
                            && !advertisement.confirm(bound, &mapping, began)
                        { info.mapping=None;info.last_error=Some(Error::NotReady); }
                        else {info.mapping=Some(mapping);info.last_error=None;}
                    },Ok(Outcome::Retired)=>{info.mapping=None;info.last_error=None;},Err(e)=>info.last_error=Some(e)}
                },
                command=commands.recv()=>match command {
                    None=>break,
                    Some(Command::Refresh(reply))=>{
                        let result=if prepared {Err(Error::Closed)} else {let result=start(&mut attempts,&shared,&config,advertisement.as_ref()).await;next=Instant::now()+renewal;result};
                        shared.lock().pending-=1;let _=reply.send(result);
                    },
                    #[cfg(test)]
                    Some(Command::Fail)=>panic!("injected port mapping owner failure"),
                    #[cfg(test)]
                    Some(Command::Pause(entered,release))=>{let _=entered.send(());let _=release.recv();},
                },
            }
        }
        if let Some(advertisement) = &advertisement {
            advertisement.clear();
        }
        cancel(&mut attempts, &shared).await;
        commands.close();
        cleanups.close();
        while let Ok(command) = commands.try_recv() {
            match command {
                Command::Refresh(reply) => {
                    let _ = reply.send(Err(Error::Closed));
                }
                #[cfg(test)]
                Command::Fail => {}
                #[cfg(test)]
                Command::Pause(_, _) => {}
            }
        }
        while let Ok(reply) = cleanups.try_recv() {
            let _ = reply.send(Err(Error::Closed));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn listener_publication_before_first_poll_starts_discovery_without_waiting_for_timer() {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = match socket.local_addr().unwrap() {
            std::net::SocketAddr::V4(address) => address,
            _ => unreachable!(),
        };
        let gate = ListenerSource::default();
        let lease = gate.acquire().unwrap();
        let config = Config {
            listener: Some(gate),
            system_gateways: false,
            network: Settings {
                bind: Ipv4Addr::LOCALHOST,
                ssdp: address,
                ..Settings::default()
            },
            ..Config::default()
        };
        let (stop, stopped) = watch::channel(false);
        let shared = Arc::new(Shared {
            info: Mutex::new(Info::default()),
            stop,
            limit: 1,
        });
        let (_commands, commands) = mpsc::channel(1);
        let (_cleanups, cleanups) = mpsc::channel(1);
        let driver = run(
            config,
            shared.clone(),
            commands,
            cleanups,
            stopped,
            Duration::from_secs(60),
            RENEWAL_INTERVAL,
        )
        .unwrap();
        // This is the constructor-ready/first-poll interval that used to lose
        // the notification. A long initial timer makes the regression explicit.
        lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 7000);
        let driver = tokio::spawn(driver);
        let mut bytes = [0; 4096];
        let observed = timeout(Duration::from_secs(2), socket.recv_from(&mut bytes)).await;
        shared.stop();
        timeout(Duration::from_secs(2), driver)
            .await
            .unwrap()
            .unwrap();
        let (n, _) = observed.unwrap().unwrap();
        assert!(
            std::str::from_utf8(&bytes[..n])
                .unwrap()
                .starts_with("M-SEARCH ")
        );
        assert_eq!(shared.lock().attempts, 1);
        assert!(!shared.lock().active);
        assert_eq!(shared.lock().cancelled, 1);
    }
}
