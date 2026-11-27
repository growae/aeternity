use super::*;

// Bind every socket before accepting requests. All listeners share one dry-run
// admission owner, and retirement joins its work after cancelling connections.
pub(crate) struct Running {
    handle: Handle,
    task: Option<JoinHandle<Status>>,
}
impl Running {
    pub async fn start(parent: runtime::Handle, config: Config) -> Result<Self, Error> {
        Self::start_group(parent, vec![config], None).await
    }
    pub async fn start_group(
        parent: runtime::Handle,
        configs: Vec<Config>,
        dry_run: Option<DryRunConfig>,
    ) -> Result<Self, Error> {
        let parent = parent.without_http();
        if !parent.status().running {
            return Err(Error::Closed);
        }
        if configs.is_empty() || configs.len() > 4 {
            return Err(Error::Configuration);
        }
        for (i, config) in configs.iter().enumerate() {
            if configs[..i].iter().any(|c| c.target == config.target)
                || dry_run
                    .as_ref()
                    .is_some_and(|d| config.body_bytes > d.pipeline.body_bytes)
            {
                return Err(Error::Configuration);
            }
            config.validate()?;
        }
        let mut bound = Vec::with_capacity(configs.len());
        for config in configs {
            bound.push(BoundListener::bind(config).await?);
        }
        let dry_run = dry_run
            .map(|config| {
                dry_run::service::Owner::start(parent.information.dry_run.clone(), config)
            })
            .transpose()?;
        let service = dry_run.as_ref().map(dry_run::service::Owner::service);
        let dry_run = dry_run.map(dry_run::service::Owner::into_worker);
        let listeners: Vec<_> = bound
            .into_iter()
            .map(|b| b.start(parent.clone(), service.clone()))
            .collect();
        let handles: Arc<[Handle]> = listeners.iter().map(Listener::handle).collect();
        let (publish, status) = watch::channel(aggregate(&handles));
        let (stop, stopping) = watch::channel(false);
        let handle = Handle {
            stop,
            status,
            listeners: handles,
        };
        let task = tokio::spawn(run_group(
            move || parent.request_shutdown(),
            listeners,
            dry_run,
            stopping,
            publish,
        ));
        Ok(Self {
            handle,
            task: Some(task),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn listener_handles(&self) -> Vec<Handle> {
        self.handle.listeners.to_vec()
    }
    pub async fn finish(mut self) -> Status {
        self.handle.request_shutdown();
        match self.task.take().expect("HTTP group owner").await {
            Ok(status) => status,
            Err(_) => {
                let mut status = self.handle.status();
                status.running = false;
                status.joined = false;
                status.failure = Some(Error::Task);
                status
            }
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.handle.request_shutdown();
    }
}
fn aggregate(handles: &[Handle]) -> Status {
    let mut state = aggregate_states(handles.iter().map(Handle::status));
    state.joined = false;
    state
}
fn aggregate_states(mut states: impl Iterator<Item = Status>) -> Status {
    let mut out = states.next().expect("nonempty listener group");
    for status in states {
        out.running &= status.running;
        out.joined &= status.joined;
        out.active = out.active.saturating_add(status.active);
        out.accepted = out.accepted.saturating_add(status.accepted);
        out.completed = out.completed.saturating_add(status.completed);
        out.cancelled = out.cancelled.saturating_add(status.cancelled);
        if out.failure.is_none() {
            out.failure = status.failure;
        }
    }
    out
}
async fn run_group(
    stop_parent: impl FnOnce(),
    listeners: Vec<Listener>,
    dry_run: Option<ae_sync::dry_run::Worker>,
    mut stopping: watch::Receiver<bool>,
    publish: watch::Sender<Status>,
) -> Status {
    loop {
        let mut state = aggregate_states(listeners.iter().map(|l| l.handle.status()));
        // Listener completion is only one part of group retirement. In-flight
        // worker callbacks may still own their state and admission credits.
        state.joined = false;
        let ended = !state.running
            || listeners
                .iter()
                .any(|l| l.task.as_ref().is_none_or(JoinHandle::is_finished));
        publish.send_replace(state);
        if *stopping.borrow() || ended {
            break;
        }
        tokio::select! {
            _ = stopping.changed() => (),
            _ = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
    }
    for listener in &listeners {
        listener.handle.request_shutdown();
    }
    if let Some(owner) = &dry_run {
        owner.stop();
    }
    let mut states = Vec::with_capacity(listeners.len());
    for listener in listeners {
        states.push(listener.finish().await);
    }
    if let Some(owner) = dry_run {
        owner.shutdown().await;
    }
    let state = aggregate_states(states.into_iter());
    if state.failure.is_some() {
        stop_parent();
    }
    publish.send_replace(state.clone());
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Condvar, Mutex, mpsc};
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    #[tokio::test]
    async fn completed_listeners_do_not_publish_joined_before_shared_work_ends() {
        let (client, worker) = ae_sync::dry_run::create(ae_sync::dry_run::Config {
            threads: 1,
            requests: 1,
            bytes: 1,
        })
        .unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(gate.clone());
        let (entered, ready) = mpsc::sync_channel(1);
        let ticket = client
            .submit(1, ae_sync::dry_run::Timeout::Unbounded, move |_| {
                entered.send(()).unwrap();
                drop(
                    gate.1
                        .wait_while(gate.0.lock().unwrap(), |released| !*released)
                        .unwrap(),
                );
            })
            .unwrap();
        ready.recv_timeout(Duration::from_secs(3)).unwrap();
        let state = Status {
            address: ([127, 0, 0, 1], 0).into(),
            running: false,
            joined: true,
            active: 0,
            accepted: 1,
            completed: 0,
            cancelled: 1,
            failure: None,
        };
        let (_listener_publish, status) = watch::channel(state.clone());
        let (stop, _) = watch::channel(false);
        let listener = Listener {
            handle: Handle {
                stop,
                status,
                listeners: Arc::default(),
            },
            task: Some(tokio::spawn(async move { state })),
        };
        let initial = aggregate(&[listener.handle()]);
        assert!(!initial.joined);
        let (publish, observed) = watch::channel(initial);
        let (_stop, stopping) = watch::channel(false);
        let task = tokio::spawn(run_group(
            || (),
            vec![listener],
            Some(worker),
            stopping,
            publish,
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    client.reserve(1, ae_sync::dry_run::Timeout::Unbounded),
                    Err(ae_sync::dry_run::Error::Stopped)
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(!observed.borrow().joined);
        assert!(!task.is_finished());
        assert_eq!(client.available().0, 0);
        drop(release);
        let terminal = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.joined && observed.borrow().joined);
        assert!(matches!(
            ticket.wait().await,
            Err(ae_sync::dry_run::Error::Stopped)
        ));
        assert_eq!(client.available().0, 1);
    }
}
