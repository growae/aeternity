use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
struct State<T> {
    result: Option<Result<T, Error>>,
    closed: bool,
    waker: Option<Waker>,
}
struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
}
impl<T> Shared<T> {
    fn lock(&self) -> std::sync::MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}
pub(super) struct Sender<T> {
    shared: Arc<Shared<T>>,
}
pub struct Ticket<T> {
    shared: Arc<Shared<T>>,
}
pub(super) fn channel<T>() -> (Sender<T>, Ticket<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            result: None,
            closed: false,
            waker: None,
        }),
        changed: Condvar::new(),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Ticket { shared },
    )
}
impl<T> Sender<T> {
    pub(super) fn send(self, result: Result<T, Error>) {
        let wake = {
            let mut state = self.shared.lock();
            state.result = Some(result);
            state.closed = true;
            self.shared.changed.notify_all();
            state.waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.shared.lock();
            state.closed = true;
            self.shared.changed.notify_all();
            state.waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}
impl<T> Ticket<T> {
    pub fn wait(self) -> Result<T, Error> {
        let mut state = self
            .shared
            .changed
            .wait_while(self.shared.lock(), |s| s.result.is_none() && !s.closed)
            .unwrap_or_else(|e| e.into_inner());
        state.result.take().unwrap_or(Err(Error::Disconnected))
    }
    // Timeout observes the request, never cancels it. The same ticket can be
    // waited on again or awaited. Dropping it leaves accepted work running.
    pub fn wait_timeout(&self, timeout: Duration) -> Result<T, Error> {
        let (mut state, _) = self
            .shared
            .changed
            .wait_timeout_while(self.shared.lock(), timeout, |s| {
                s.result.is_none() && !s.closed
            })
            .unwrap_or_else(|e| e.into_inner());
        state.result.take().unwrap_or_else(|| {
            Err(if state.closed {
                Error::Disconnected
            } else {
                Error::Timeout
            })
        })
    }
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.lock();
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        if state.closed {
            return Poll::Ready(Err(Error::Disconnected));
        }
        if state
            .waker
            .as_ref()
            .is_none_or(|w| !w.will_wake(context.waker()))
        {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}
