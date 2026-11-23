use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Driver {
    handle: Handle,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    generation: u64,
}
#[derive(Debug)]
pub struct DriverReport {
    pub generation: u64,
    pub exit: thread::Result<()>,
    pub failure: Option<Error>,
    pub pending: Option<Observed>,
}
impl DriverReport {
    pub fn poll(&mut self) {
        if let Some(pending) = &mut self.pending {
            pending.poll();
        }
    }
}
struct ExitGuard(Arc<Shared>);
impl Drop for ExitGuard {
    fn drop(&mut self) {
        let mut c = lock(&self.0.control);
        c.driver_exited = true;
        if thread::panicking() {
            c.driver_failure.get_or_insert(Error::DriverPanicked);
        }
        self.0.wake.notify_all();
    }
}
impl Driver {
    pub fn spawn(handle: Handle, clock: impl Clock, poll: Duration) -> Result<Self, String> {
        if poll.is_zero() || poll > Duration::from_secs(1) {
            return Err("invalid driver poll interval".into());
        }
        let shared = handle.0.clone();
        let mut c = lock(&shared.control);
        let generation = c.status.session_generation;
        if c.stop
            || c.quiescing
            || c.joined
            || c.status.failure.is_some()
            || c.status.session_replacing
            || c.driver_lease
            || generation <= c.driver_generation
            || c.driver_work.is_some()
        {
            return Err("production session is not available for a fresh driver".into());
        }
        let previous = (
            c.driver_generation,
            c.driver_exited,
            c.driver_failure.take(),
        );
        c.driver_lease = true;
        c.driver_generation = generation;
        c.driver_exited = false;
        let stop = Arc::new(AtomicBool::new(false));
        let moved_stop = stop.clone();
        let moved = shared.clone();
        let thread = match thread::Builder::new()
            .name("ae-hc-production".into())
            .spawn(move || {
                let _guard = ExitGuard(moved.clone());
                let client = SessionClient {
                    shared: moved.clone(),
                    generation,
                };
                let running = || {
                    let c = lock(&moved.control);
                    !moved_stop.load(Ordering::Acquire) && !c.stop && !c.quiescing
                };
                let pause = || {
                    let c = lock(&moved.control);
                    if !moved_stop.load(Ordering::Acquire) && !c.stop && !c.quiescing {
                        drop(
                            moved
                                .wake
                                .wait_timeout(c, poll)
                                .unwrap_or_else(|e| e.into_inner()),
                        );
                    }
                };
                while running() && !lock(&moved.control).ready {
                    pause();
                }
                while running() {
                    let (wall, now) = match clock.now() {
                        Ok(time) => time,
                        Err(error) => {
                            lock(&moved.control).driver_failure = Some(error);
                            return;
                        }
                    };
                    // Clock evaluation may block. A stop during that call must not
                    // admit another advance when the clock eventually returns.
                    if !running() {
                        return;
                    }
                    let ticket = match client.submit(Command::Advance(wall, now)) {
                        Ok(ticket) => ticket,
                        Err(RequestError::Busy | RequestError::Retiring) => {
                            pause();
                            continue;
                        }
                        Err(_) => return,
                    };
                    let result = loop {
                        if !running() {
                            return;
                        }
                        if let Some(result) = ticket.wait_timeout(poll) {
                            break result;
                        }
                    };
                    // Clear only the exact observed request. Never poll a receipt
                    // while holding the admission mutex: unwind closes admission
                    // before publishing Unknown in the opposite direction.
                    {
                        let mut c = lock(&moved.control);
                        if c.driver_work
                            .as_ref()
                            .is_some_and(|t| t.sequence() == ticket.sequence())
                        {
                            c.driver_work = None;
                        }
                    }
                    drop(ticket);
                    match result {
                        Ok(Reply::Advanced(_)) => (),
                        _ => return,
                    }
                    pause();
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                c.driver_lease = false;
                (c.driver_generation, c.driver_exited, c.driver_failure) = previous;
                return Err(error.to_string());
            }
        };
        drop(c);
        Ok(Self {
            handle,
            stop,
            thread: Some(thread),
            generation,
        })
    }
    pub fn request_shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        self.handle.0.wake.notify_all();
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    fn finish(&mut self) -> DriverReport {
        self.request_shutdown();
        let exit = self.thread.take().expect("production driver").join();
        let (pending, failure) = {
            let mut c = lock(&self.handle.0.control);
            c.driver_lease = false;
            (c.driver_work.take(), c.driver_failure.clone())
        };
        DriverReport {
            generation: self.generation,
            exit,
            failure,
            pending: pending.map(Observed::from),
        }
    }
    pub fn shutdown(mut self) -> DriverReport {
        self.finish()
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let _ = self.finish();
        }
    }
}
impl crate::production_group::Child for Driver {
    type Exit = DriverReport;
    fn is_finished(&self) -> bool {
        self.is_finished()
    }
    fn shutdown(self) -> thread::Result<Self::Exit> {
        Ok(self.shutdown())
    }
}
