use super::{Config, Control};
use crate::child_process::{Child, Error, Termination};
use std::{
    io,
    process::ExitStatus,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Exited,
    Control(super::super::Error),
    Aborted,
}
#[derive(Debug)]
pub struct Observation {
    pub pid: u32,
    pub status: ExitStatus,
    pub cause: Cause,
    pub termination: Option<Termination>,
}
pub(super) struct Monitor {
    stop: Arc<AtomicBool>,
    pub finished: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<Observation, Error>>>,
}
impl Monitor {
    pub fn start(mut child: Child, control: Control, config: &Config) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let abort = stop.clone();
        let finished = done.clone();
        let poll = config.poll;
        #[cfg(test)]
        let reaped = config.reaped.clone();
        let thread = thread::Builder::new()
            .name("ae-dry-run-child".into())
            .spawn(move || {
                struct Finished(Arc<AtomicBool>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release)
                    }
                }
                let _finished = Finished(finished);
                let cause = loop {
                    if child.exited()? {
                        break Cause::Exited;
                    }
                    if let Err(e) = control.check() {
                        break Cause::Control(e);
                    }
                    if abort.load(Ordering::Acquire) {
                        break Cause::Aborted;
                    }
                    thread::sleep(poll);
                };
                let pid = child.id();
                let status = child.finish()?;
                #[cfg(test)]
                if let Some(tx) = reaped {
                    let _ = tx.send(pid);
                }
                Ok(Observation {
                    pid,
                    status,
                    cause,
                    termination: child.termination(),
                })
            })?;
        Ok(Self {
            stop,
            finished: done,
            thread: Some(thread),
        })
    }
    pub fn join(mut self, abort: bool) -> Result<Observation, super::MonitorError> {
        if abort {
            self.stop.store(true, Ordering::Release)
        }
        self.thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| super::MonitorError::Panicked)?
            .map_err(super::MonitorError::Process)
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
