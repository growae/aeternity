use super::{Error, Timeout};
use std::time::Instant;
use tokio::sync::watch;

#[derive(Clone, Copy)]
enum State {
    Dormant,
    Active(Option<Instant>),
    Finished,
    Expired,
}
#[derive(Clone)]
pub(super) struct Clock(watch::Sender<State>);
impl Clock {
    pub fn new() -> Self {
        Self(watch::channel(State::Dormant).0)
    }
    pub fn start(&self, timeout: Timeout) -> Result<(), Error> {
        let deadline = match timeout {
            Timeout::Unbounded => None,
            Timeout::After(d) => Some(Instant::now().checked_add(d).ok_or(Error::Configuration)?),
        };
        let mut result = Err(Error::Configuration);
        self.0.send_if_modified(|state| {
            if !matches!(state, State::Dormant) {
                return false;
            }
            *state = State::Active(deadline);
            result = Ok(());
            true
        });
        result
    }
    pub fn check(&self) -> Result<(), Error> {
        self.0.send_if_modified(|state| {
            if matches!(state,State::Active(Some(d)) if Instant::now()>=*d) {
                *state = State::Expired;
                true
            } else {
                false
            }
        });
        if matches!(*self.0.borrow(), State::Expired) {
            Err(Error::NativeTimeout)
        } else {
            Ok(())
        }
    }
    pub fn finish(&self) -> Result<(), Error> {
        let mut result = Err(Error::Configuration);
        self.0.send_if_modified(|state| match *state {
            State::Active(deadline) => {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    *state = State::Expired;
                    result = Err(Error::NativeTimeout);
                } else {
                    *state = State::Finished;
                    result = Ok(());
                }
                true
            }
            State::Expired => {
                result = Err(Error::NativeTimeout);
                false
            }
            _ => false,
        });
        result
    }
    pub fn end(&self) {
        // finish() is atomic with expiry and harmlessly rejects inactive or
        // already completed phases. Ignore that rejection for job teardown.
        let _ = self.finish();
    }
    pub async fn expired(&self) -> Error {
        let mut changed = self.0.subscribe();
        loop {
            if self.check().is_err() {
                return Error::NativeTimeout;
            }
            let state = *changed.borrow_and_update();
            match state {
                State::Expired => return Error::NativeTimeout,
                State::Finished => std::future::pending::<()>().await,
                State::Active(Some(deadline)) => {
                    tokio::select! {
                        biased;
                        r=changed.changed()=>if r.is_err(){return Error::Configuration},
                        _=tokio::time::sleep_until(deadline.into())=>{},
                    }
                }
                _ => {
                    if changed.changed().await.is_err() {
                        return Error::Configuration;
                    }
                }
            }
        }
    }
}
