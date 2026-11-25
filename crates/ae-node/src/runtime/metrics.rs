use super::{Exit, Handle, Status};
use ae_sync::observe::{self, statsd};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tokio::sync::{oneshot, watch};

#[derive(Clone, Default)]
struct State {
    done: bool,
    error: Option<observe::Error>,
}
#[derive(Clone)]
pub(super) struct View {
    pub client: statsd::Client,
    closing: Arc<AtomicBool>,
    stopping: watch::Sender<bool>,
    state: watch::Receiver<State>,
}
impl View {
    pub fn stop(&self) {
        self.closing.store(true, Ordering::Release);
        self.stopping.send_replace(true);
    }
    pub fn apply(&self, mut status: Status) -> Status {
        let info = self.client.info();
        let state = self.state.borrow();
        status.ready &= !self.closing.load(Ordering::Acquire) && (info.ready || !info.running);
        status.joined &= info.joined && state.done;
        status.running |= !state.done;
        if let Some(error) = &state.error {
            status.metrics_error = Some(error.clone());
        }
        status.metrics = Some(info);
        status
    }
    pub fn done(&self) -> bool {
        self.state.borrow().done
    }
    pub async fn changed(&mut self) {
        tokio::select! { _ = self.client.changed() => (), _ = self.state.changed() => () }
    }
    pub fn exit(&self, exit: Exit) -> Exit {
        Exit::Application {
            inner: Box::new(exit),
            metrics: self.client.info(),
            error: self.state.borrow().error.clone(),
        }
    }
}
pub(super) struct Owner {
    view: View,
    parent: Option<Handle>,
    abandon: watch::Sender<bool>,
    bind: Option<oneshot::Sender<Handle>>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Owner {
    pub fn prepare(settings: statsd::Settings) -> Result<Self, observe::Error> {
        let pipeline = settings.pipeline.clone();
        let worker = statsd::supervision::Owner::prepare(settings)?;
        let client = worker.client();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| observe::Error::Io(e.kind()))?;
        let (bind, parent) = oneshot::channel();
        let (abandon, abandoned) = watch::channel(false);
        let (state, view) = watch::channel(State::default());
        let closing = Arc::new(AtomicBool::new(false));
        let stopped = closing.clone();
        let (stopping, requested) = watch::channel(false);
        let thread = thread::Builder::new()
            .name("ae-application-metrics".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(run(
                        worker, parent, &state, stopped, abandoned, requested, pipeline,
                    ))
                }));
                state.send_modify(|s| {
                    if result.is_err() {
                        s.error = Some(observe::Error::Io(std::io::ErrorKind::Other));
                    }
                    s.done = true;
                });
            })
            .map_err(|e| observe::Error::Io(e.kind()))?;
        Ok(Self {
            view: View {
                client,
                closing,
                stopping,
                state: view,
            },
            parent: None,
            abandon,
            bind: Some(bind),
            thread: Some(thread),
        })
    }
    pub fn view(&self) -> View {
        self.view.clone()
    }
    pub fn bind(&mut self, mut parent: Handle) {
        // The monitor observes the concrete family root, never its own view.
        parent.metrics = None;
        self.parent = Some(parent.clone());
        let _ = self
            .bind
            .take()
            .expect("single application owner binding")
            .send(parent);
    }
    pub async fn finish(&mut self) {
        while !self.view.done() {
            let _ = self.view.state.changed().await;
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.view.stop();
        if let Some(parent) = &self.parent {
            parent.request_shutdown();
        }
        self.bind.take();
        self.abandon.send_replace(true);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
async fn run(
    mut worker: statsd::supervision::Owner,
    parent: oneshot::Receiver<Handle>,
    state: &watch::Sender<State>,
    closing: Arc<AtomicBool>,
    mut abandoned: watch::Receiver<bool>,
    mut stopping: watch::Receiver<bool>,
    pipeline: Option<observe::pipeline::Config>,
) {
    let Ok(parent) = parent.await else {
        worker.finish().await;
        return;
    };
    let mut parent = parent;
    let mut activated = false;
    let mut prepared_stop = false;
    loop {
        let root = parent.status();
        if !root.running || *abandoned.borrow() {
            break;
        }
        if root.ready && !activated && !closing.load(Ordering::Acquire) {
            if let Some(config) = &pipeline {
                let routing = if let Some(pow) = parent.pow() {
                    pow.workers().and_then(|w| w.bindings().routing)
                } else {
                    parent
                        .hyperchain()
                        .and_then(|h| h.workers())
                        .and_then(|w| w.bindings().routing)
                };
                config.set_routing(routing.map(observe::pipeline::Routing::Managed));
            }
            worker.activate();
            activated = true;
        }
        if closing.load(Ordering::Acquire) && !prepared_stop {
            prepared_stop = true;
            if activated && let Some(input) = worker.client().input() {
                let result = match input.prepare_stop() {
                    Ok(ticket) => {
                        tokio::time::timeout(std::time::Duration::from_secs(5), ticket.wait())
                            .await
                            .map_err(|_| observe::Error::Timeout)
                            .and_then(|r| r.map_err(observe::Error::Pipeline))
                    }
                    Err(observe::pipeline::Error::Closed) => Ok(()),
                    Err(e) => Err(observe::Error::Pipeline(e)),
                };
                if let Err(error) = result {
                    state.send_modify(|s| s.error = Some(error));
                }
            }
            // Keep routing and all family workers alive through the input /
            // filter acknowledgement. Final logger retirement joins resources.
            parent.request_shutdown();
            worker.client().request_shutdown();
        }
        worker.poll().await;
        let error = worker.handle().status().last_error;
        if error.is_some() && state.borrow().error != error {
            // Logger startup/recovery is independent of the family root.
            // The supervision handle also exposes an exhausted parent boundary.
            state.send_modify(|s| s.error = error);
        }
        tokio::select! { _ = abandoned.changed() => break, _ = parent.changed() => (), _ = worker.changed() => (), _ = stopping.changed(), if !prepared_stop => () }
    }
    worker.finish().await;
}
