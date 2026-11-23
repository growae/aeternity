use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    Running,
    WorkStopped(coordinator::Error),
    Stopped,
}
#[derive(Debug, Clone)]
pub struct Info {
    pub health: Health,
    pub running: bool,
    pub stopping: bool,
    pub outstanding: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submission {
    Stream(u64),
    Caller,
}
impl ReplyTo {
    pub(super) fn submission(&self) -> Submission {
        match self {
            Self::Stream(sequence) => Submission::Stream(*sequence),
            Self::Caller { .. } => Submission::Caller,
        }
    }
}
#[derive(Debug)]
pub struct Retired {
    pub info: Info,
    pub state: Option<coordinator::Snapshot>,
    pub result: Result<(), String>,
    // Channel output precedes the interrupted send and coordinator outbox.
    pub outputs: Vec<Output>,
    pub queued: Vec<Submission>,
    // A callback panicked without an observed reply. It must not be replayed.
    pub interrupted: Option<Submission>,
}
pub(super) struct WorkerExit {
    pub(super) state: coordinator::Snapshot,
    pub(super) outputs: Vec<Output>,
    pub(super) queued: Vec<Submission>,
    pub(super) interrupted: Option<Submission>,
    pub(super) panic: Option<Box<dyn std::any::Any + Send>>,
}
#[derive(Default)]
pub(super) struct Pending {
    pub(super) output: Option<Output>,
    pub(super) command: Option<Submission>,
}
impl Pending {
    pub(super) fn send(
        &mut self,
        output: Output,
        sender: &SyncSender<Output>,
        stop: &AtomicBool,
    ) -> bool {
        self.output = Some(output);
        loop {
            if stop.load(Ordering::Acquire) {
                return false;
            }
            match sender.try_send(self.output.take().expect("pending output")) {
                Ok(()) => return true,
                Err(TrySendError::Full(output)) => self.output = Some(output),
                Err(TrySendError::Disconnected(output)) => {
                    self.output = Some(output);
                    return false;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}
impl<A, C> Client<A, C> {
    pub fn info(&self) -> Info {
        Info {
            health: self.control.health.lock().expect("EOE health").clone(),
            running: self.control.alive.load(Ordering::Acquire),
            stopping: self.control.stop.load(Ordering::Acquire),
            outstanding: self.outstanding(),
        }
    }
    pub fn replacement_pending(&self) -> bool {
        self.is_closed() && !matches!(self.info().health, Health::WorkStopped(_))
    }
}
impl<A, C> Runtime<A, C> {
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn replacement_pending(&self) -> bool {
        self.client().replacement_pending()
    }
    // Stop-aware output sends let retirement join without dropping the
    // receiver or losing the worker's current output under backpressure.
    pub fn retire(mut self) -> Retired {
        let client = self.client();
        client.request_stop();
        self.sender.take();
        let exit = self.worker.take().expect("live EOE worker").join();
        let mut outputs = self
            .receiver
            .take()
            .expect("EOE receiver")
            .try_iter()
            .collect::<Vec<_>>();
        match exit {
            Ok(exit) => {
                outputs.extend(exit.outputs);
                Retired {
                    info: client.info(),
                    state: Some(exit.state),
                    result: if exit.panic.is_some() {
                        Err("EOE worker panicked".into())
                    } else {
                        Ok(())
                    },
                    outputs,
                    queued: exit.queued,
                    interrupted: exit.interrupted,
                }
            }
            Err(_) => Retired {
                info: client.info(),
                state: None,
                result: Err("EOE worker cleanup panicked".into()),
                outputs,
                queued: vec![],
                interrupted: None,
            },
        }
    }
}
