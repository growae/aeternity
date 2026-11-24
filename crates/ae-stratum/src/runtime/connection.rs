use super::*;
use num_bigint::BigInt;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::{Instant, sleep_until},
};

#[derive(Default)]
struct Timer {
    active: Option<(u64, Instant)>,
    queued: usize,
}
impl Timer {
    fn collect(&mut self, now: Instant) {
        if self.active.is_some_and(|(_, deadline)| deadline <= now) {
            self.active = None;
            self.queued += 1;
        }
    }
    fn cancel(&mut self, id: u64, now: Instant) {
        self.collect(now);
        if self.active.is_some_and(|(active, _)| active == id) {
            self.active = None;
        }
    }
    fn schedule(&mut self, id: u64, after_ms: u64, now: Instant) {
        self.collect(now);
        self.active = Some((id, now + Duration::from_millis(after_ms)));
    }
    fn take(&mut self) -> bool {
        self.collect(Instant::now());
        if self.queued == 0 {
            false
        } else {
            self.queued -= 1;
            true
        }
    }
}
struct Cancellation {
    stop: watch::Receiver<bool>,
    close: watch::Receiver<bool>,
    services: watch::Receiver<()>,
    lease: services::Connection,
}
impl Cancellation {
    fn check(&self) -> Result<(), ExitReason> {
        if *self.stop.borrow() {
            return Err(ExitReason::Shutdown);
        }
        if *self.close.borrow() {
            return Err(ExitReason::Administrative);
        }
        if self.lease.is_closed() {
            return Err(ExitReason::Services(services::Error::Closed));
        }
        Ok(())
    }
    async fn wait<T>(
        &mut self,
        future: impl Future<Output = T>,
        deadline: Instant,
    ) -> Result<T, ExitReason> {
        tokio::pin!(future);
        loop {
            self.check()?;
            tokio::select! {
                biased;
                _ = self.stop.changed() => {},
                _ = self.close.changed() => {},
                _ = sleep_until(deadline) => return Err(ExitReason::OperationTimeout),
                value = &mut future => return Ok(value),
                _ = self.services.changed() => {},
            }
        }
    }
}
struct Guard {
    lease: services::Connection,
    shared: Arc<Shared>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.lease.close();
        self.shared
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.lease.id());
    }
}
struct WaitingPool(Arc<Shared>);
impl Drop for WaitingPool {
    fn drop(&mut self) {
        self.0.waiting_pool.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Driver {
    session: session::Session,
    timer: Timer,
    cancel: Cancellation,
    shared: Arc<Shared>,
    limits: Limits,
    peer: SocketAddr,
    read_bytes: u64,
    written_bytes: u64,
    unsent_bytes: usize,
    pending_pool: Vec<share::Effect>,
}
impl Driver {
    fn snapshot(&self) -> Result<Lease<Snapshot>, Error> {
        // Conservative bounded charge includes every retained share/error,
        // largest allowed integer fields, job identifiers and container costs.
        let bytes = 16384
            + self.session.jobs().map_or(0, |jobs| {
                jobs.iter()
                    .map(|j| 32768 + j.retained_bytes())
                    .sum::<usize>()
            });
        let credit = self.shared.snapshots.reserve(bytes)?;
        Ok(Lease::new(
            Snapshot {
                id: self.cancel.lease.id(),
                peer: self.peer,
                session: self.session.clone(),
                read_bytes: self.read_bytes,
                written_bytes: self.written_bytes,
            },
            credit,
        ))
    }
    fn command(&mut self, command: Command) {
        match command {
            Command::Update(u, reply) => {
                let _ = reply.send(self.session.set(u).map_err(Error::Session));
            }
            Command::Status(reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.snapshot());
                }
            }
        }
    }
    async fn service<T>(
        &mut self,
        mut f: impl FnMut(&services::Connection) -> Result<T, services::Error>,
    ) -> Result<T, ExitReason> {
        let deadline = Instant::now() + self.limits.operation_timeout;
        loop {
            self.cancel.check()?;
            match f(&self.cancel.lease) {
                Ok(v) => return Ok(v),
                Err(services::Error::Busy) => {
                    // Busy is definitely unaccepted. A short retry also covers
                    // lock contention with read-only service queries.
                    self.cancel
                        .wait(tokio::time::sleep(Duration::from_millis(2)), deadline)
                        .await?;
                }
                Err(e) => return Err(ExitReason::Services(e)),
            }
        }
    }
    async fn publish_pool(&mut self) -> Result<(), ExitReason> {
        if self.pending_pool.is_empty() {
            return Ok(());
        }
        let deadline = Instant::now() + self.limits.operation_timeout;
        let mut changes = self.shared.outbox.budget.changed.subscribe();
        let mut waiting = None;
        loop {
            let mut output = Some(Output::Pool {
                connection: self.cancel.lease.id(),
                effects: std::mem::take(&mut self.pending_pool),
            });
            match self.shared.outbox.push(&mut output) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let Some(Output::Pool { effects, .. }) = output else {
                        unreachable!("unaccepted pool output");
                    };
                    self.pending_pool = effects;
                    if e != Error::Busy {
                        return Err(ExitReason::OutputLimit);
                    }
                }
            }
            if waiting.is_none() {
                self.shared.waiting_pool.fetch_add(1, Ordering::AcqRel);
                waiting = Some(WaitingPool(self.shared.clone()));
            }
            let _ = self.cancel.wait(changes.changed(), deadline).await?;
        }
    }
    async fn step<S: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mut event: session::Event,
    ) -> Result<(), ExitReason> {
        loop {
            self.cancel.check()?;
            let now = BigInt::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| ExitReason::Clock)?
                    .as_millis(),
            );
            let config = self.shared.config.borrow().clone();
            let step = self
                .session
                .handle(
                    event,
                    &session::Context {
                        config: &config,
                        now: &now,
                    },
                )
                .map_err(ExitReason::Engine)?;
            if let session::Action::Send(bytes) = &step.action {
                self.unsent_bytes = bytes.len();
            }
            for effect in step.effects {
                match effect {
                    session::Effect::ScheduleTimer { timer, after_ms } => {
                        self.timer.schedule(timer.id, after_ms, Instant::now())
                    }
                    session::Effect::CancelTimer { id } => self.timer.cancel(id, Instant::now()),
                    session::Effect::Enqueue(chain) => {
                        self.service(|c| c.enqueue(chain.clone())).await?
                    }
                    session::Effect::Pool(effect) => self.pending_pool.push(effect),
                }
            }
            self.publish_pool().await?;
            match step.action {
                session::Action::None => return Ok(()),
                session::Action::Stop(reason) => return Err(ExitReason::Session(reason)),
                session::Action::Command(command) => {
                    event = self.service(|c| c.execute(command.clone())).await?
                }
                session::Action::Send(bytes) => {
                    self.unsent_bytes = bytes.len();
                    let deadline = Instant::now() + self.limits.operation_timeout;
                    let mut offset = 0;
                    while offset < bytes.len() {
                        let n = self
                            .cancel
                            .wait(stream.write(&bytes[offset..]), deadline)
                            .await?
                            .map_err(|e| ExitReason::Io(e.kind()))?;
                        if n == 0 {
                            return Err(ExitReason::Io(std::io::ErrorKind::WriteZero));
                        }
                        offset += n;
                        self.unsent_bytes -= n;
                        self.written_bytes = self.written_bytes.saturating_add(n as u64);
                    }
                    return Ok(());
                }
            }
        }
    }
    async fn drive<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mut commands: mpsc::Receiver<Command>,
    ) -> Result<(), ExitReason> {
        self.step(stream, session::Event::Init).await?;
        let mut frames = json::Frames::new(json::Limits {
            bytes: self.limits.frame_bytes,
            ..Default::default()
        })
        .expect("validated frame limit");
        let mut buffer = vec![0; self.limits.read_bytes];
        let (mut at, mut end) = (0, 0);
        let mut chain_turn = true;
        loop {
            self.cancel.check()?;
            if self.timer.take() {
                self.step(stream, session::Event::Timeout).await?;
                continue;
            }
            if let Ok(command) = commands.try_recv() {
                self.command(command);
            }
            let mut service_busy = false;
            if chain_turn || at == end {
                match self.cancel.lease.next_notification() {
                    Ok(Some(event)) => {
                        self.step(stream, session::Event::Chain((*event).clone()))
                            .await?;
                        chain_turn = false;
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(None) => {}
                    Err(services::Error::Busy) => service_busy = true,
                    Err(e) => return Err(ExitReason::Services(e)),
                }
            }
            if at < end {
                let mut input = &buffer[at..end];
                let frame = frames
                    .next_raw(&mut input)
                    .map_err(|_| ExitReason::FrameLimit)?;
                at = end - input.len();
                if let Some(frame) = frame {
                    self.step(stream, session::Event::Data(frame)).await?;
                    chain_turn = true;
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            let deadline = self
                .timer
                .active
                .map_or_else(|| Instant::now() + Duration::from_secs(86_400), |(_, t)| t);
            tokio::select! {
                _ = self.cancel.stop.changed() => {},
                _ = self.cancel.close.changed() => {},
                _ = self.cancel.services.changed() => {},
                _ = sleep_until(deadline) => {},
                // Read-only lock contention produces no service notification.
                _ = tokio::time::sleep(Duration::from_millis(2)), if service_busy => {},
                command = commands.recv() => {
                    if let Some(command) = command { self.command(command); }
                },
                read = stream.read(&mut buffer) => {
                    let n = read.map_err(|e| ExitReason::Io(e.kind()))?;
                    if n == 0 {
                        return Err(if frames.finish().is_ok() { ExitReason::PeerClosed } else { ExitReason::IncompleteFrame });
                    }
                    self.read_bytes = self.read_bytes.saturating_add(n as u64);
                    at = 0;
                    end = n;
                    chain_turn = true;
                }
            }
        }
    }
}
// Catch each poll while keeping the driver (including unhanded pool effects)
// outside the unwinding future. Connection cleanup also has a Drop guard.
struct Catch<F>(Pin<Box<F>>);
impl<F: Future> Future for Catch<F> {
    type Output = Result<F::Output, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.0.as_mut().poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}
pub(super) async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    peer: SocketAddr,
    lease: services::Connection,
    shared: Arc<Shared>,
    limits: Limits,
    commands: mpsc::Receiver<Command>,
    close: watch::Receiver<bool>,
) -> Exit {
    let id = lease.id();
    let guard = Guard {
        lease: lease.clone(),
        shared: shared.clone(),
    };
    let cancel = Cancellation {
        stop: shared.stop.subscribe(),
        close,
        services: lease.changes(),
        lease,
    };
    let mut driver = Driver {
        session: session::Session::new(),
        timer: Timer::default(),
        cancel,
        shared,
        limits,
        peer,
        read_bytes: 0,
        written_bytes: 0,
        unsent_bytes: 0,
        pending_pool: Vec::new(),
    };
    let reason = match Catch(Box::pin(driver.drive(&mut stream, commands))).await {
        Ok(Err(reason)) => reason,
        Ok(Ok(())) => unreachable!("driver ends with a reason"),
        Err(()) => ExitReason::Panicked,
    };
    drop(stream);
    drop(guard);
    Exit {
        id,
        peer,
        reason,
        last_phase: driver.session.phase(),
        read_bytes: driver.read_bytes,
        written_bytes: driver.written_bytes,
        unsent_bytes: driver.unsent_bytes,
        pending_pool: driver.pending_pool,
    }
}

#[cfg(test)]
mod timer_tests {
    use super::*;
    #[test]
    fn delivered_timeout_survives_cancel_and_replacement() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.schedule(1, 10, now);
        timer.cancel(1, now + Duration::from_millis(10));
        timer.schedule(2, 100, now + Duration::from_millis(10));
        assert_eq!(timer.queued, 1);
        assert_eq!(timer.active.unwrap().0, 2);
        timer.cancel(1, now + Duration::from_millis(20));
        assert_eq!(timer.active.unwrap().0, 2);
        timer.cancel(2, now + Duration::from_millis(20));
        assert_eq!(timer.queued, 1);
        assert!(timer.active.is_none());
    }
}

#[cfg(test)]
mod failure_tests {
    use super::super::tests::{authorize, config, configure, native, owner, subscribe};
    use super::*;
    use tokio::io::{DuplexStream, ReadBuf};
    struct Wire {
        stream: DuplexStream,
        bytes: usize,
        cut: usize,
        mode: u8,
    }
    impl AsyncRead for Wire {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(cx, out)
        }
    }
    impl AsyncWrite for Wire {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.bytes >= self.cut {
                return match self.mode {
                    0 => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
                    1 => Poll::Pending,
                    _ => panic!("injected write panic"),
                };
            }
            let n = bytes.len().min(3).min(self.cut - self.bytes);
            let result = Pin::new(&mut self.stream).poll_write(cx, &bytes[..n]);
            if let Poll::Ready(Ok(n)) = result {
                self.bytes += n;
            }
            result
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
    #[test]
    fn partial_send_failure_stall_and_panic_cleanup_after_registration() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for (mode, partial) in [(0, 0), (0, 7), (1, 0), (2, 0)] {
            let mut cfg = config();
            cfg.limits.operation_timeout = Duration::from_millis(30);
            let owner = owner(cfg.clone());
            let shared = owner.client().shared;
            let lease = shared.services.connect().unwrap();
            let (tx, rx) = mpsc::channel(1);
            let (close, crx) = watch::channel(false);
            let expected = native("send_failure_after_registration").pop().unwrap();
            let cut = expected.len() + partial;
            let exit = runtime.block_on(async {
                let (server, mut peer) = tokio::io::duplex(4096);
                let wire = Wire {
                    stream: server,
                    bytes: 0,
                    cut,
                    mode,
                };
                let run = run(
                    wire,
                    "127.0.0.1:1".parse().unwrap(),
                    lease,
                    shared.clone(),
                    cfg.limits,
                    rx,
                    crx,
                );
                let reader = async {
                    peer.write_all(&[subscribe(), authorize("one")].concat())
                        .await
                        .unwrap();
                    let mut bytes = Vec::new();
                    peer.read_to_end(&mut bytes).await.unwrap();
                    bytes
                };
                let (exit, bytes) = tokio::join!(run, reader);
                assert_eq!(bytes.len(), cut);
                assert_eq!(&bytes[..expected.len()], expected.as_slice());
                exit
            });
            assert_eq!(
                exit.reason,
                match mode {
                    0 => ExitReason::Io(std::io::ErrorKind::BrokenPipe),
                    1 => ExitReason::OperationTimeout,
                    _ => ExitReason::Panicked,
                }
            );
            assert_eq!(exit.last_phase, session::Phase::Authorized);
            assert_eq!(exit.written_bytes, cut as u64);
            assert!(exit.unsent_bytes > 0);
            assert_eq!(shared.services.info().accounts, 0);
            assert_eq!(shared.services.info().nonces, 0);
            drop((tx, close));
            owner.shutdown();
        }
    }
    #[test]
    fn bounded_admin_mailbox_and_close_interrupt_a_stalled_write() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let cfg = config();
        let owner = owner(cfg.clone());
        let shared = owner.client().shared;
        let lease = shared.services.connect().unwrap();
        let (tx, rx) = mpsc::channel(1);
        let (close, crx) = watch::channel(false);
        let control = Connection {
            id: lease.id(),
            peer: "127.0.0.1:1".parse().unwrap(),
            commands: tx,
            close,
        };
        runtime.block_on(async {
            let (server, mut peer) = tokio::io::duplex(4096);
            let cut = native("send_failure_after_registration")[0].len();
            let task = tokio::spawn(run(
                Wire {
                    stream: server,
                    bytes: 0,
                    cut,
                    mode: 1,
                },
                control.peer,
                lease,
                shared.clone(),
                cfg.limits,
                rx,
                crx,
            ));
            peer.write_all(&[subscribe(), authorize("one")].concat())
                .await
                .unwrap();
            let mut bytes = vec![0; cut];
            peer.read_exact(&mut bytes).await.unwrap();
            while shared.services.info().accounts == 0 {
                tokio::task::yield_now().await;
            }
            let queued = control.status().unwrap();
            assert!(matches!(control.status(), Err(Error::Busy)));
            control.close();
            let exit = task.await.unwrap();
            assert_eq!(exit.reason, ExitReason::Administrative);
            assert!(queued.await.is_err());
            assert!(control.is_closed());
            assert_eq!(shared.services.info().nonces, 0);
        });
        owner.shutdown();
    }
    #[test]
    fn driver_retains_queued_timeout_across_configure_and_authorize_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for authorized in [false, true] {
            let cfg = config();
            let owner = owner(cfg.clone());
            let shared = owner.client().shared;
            let lease = shared.services.connect().unwrap();
            let (_close, close) = watch::channel(false);
            let mut d = Driver {
                session: session::Session::new(),
                timer: Timer::default(),
                cancel: Cancellation {
                    stop: shared.stop.subscribe(),
                    close,
                    services: lease.changes(),
                    lease: lease.clone(),
                },
                shared,
                limits: cfg.limits,
                peer: "127.0.0.1:1".parse().unwrap(),
                read_bytes: 0,
                written_bytes: 0,
                unsent_bytes: 0,
                pending_pool: Vec::new(),
            };
            runtime.block_on(async {
                let (mut server, mut peer) = tokio::io::duplex(4096);
                d.step(&mut server, session::Event::Init).await.unwrap();
                if authorized {
                    d.step(&mut server, session::Event::Data(subscribe()))
                        .await
                        .unwrap();
                }
                d.timer.active.as_mut().unwrap().1 = Instant::now();
                d.step(
                    &mut server,
                    session::Event::Data(if authorized {
                        authorize("one")
                    } else {
                        configure()
                    }),
                )
                .await
                .unwrap();
                assert!(d.timer.take());
                let result = d.step(&mut server, session::Event::Timeout).await;
                if authorized {
                    result.unwrap();
                    assert_eq!(d.session.phase(), session::Phase::Authorized);
                    let event = lease.next_notification().unwrap().unwrap();
                    d.step(&mut server, session::Event::Chain((*event).clone()))
                        .await
                        .unwrap();
                } else {
                    assert_eq!(
                        result,
                        Err(ExitReason::Session(session::StopReason::Timeout))
                    );
                }
                drop(server);
                let mut bytes = Vec::new();
                peer.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(
                    bytes,
                    native(if authorized {
                        "queued_timeout_after_cancel"
                    } else {
                        "queued_timeout_after_configure"
                    })
                    .concat()
                );
            });
            lease.close();
            assert_eq!(owner.shutdown().services.nonces, 0);
        }
    }
}
