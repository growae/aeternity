pub use ae_p2p::listener::Config;
use ae_p2p::listener::Listener as Policy;
use std::{
    future::poll_fn,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex, MutexGuard},
    task::{Poll, Waker},
};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub active: usize,
    pub acceptors: usize,
    pub maximum_connections: usize,
    pub sleeping: usize,
    pub closed: bool,
}
struct State {
    policy: Policy<u64>,
    next: u64,
    closed: bool,
    waker: Option<Waker>,
}
struct Shared(Mutex<State>);
impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
// Held through the actual transport terminal monitor, including graceful
// close. Pending-attempt cancellation drops it with that attempt's socket.
pub(crate) struct Permit {
    shared: Arc<Shared>,
    id: u64,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.shared.lock();
            state.policy.closed(&self.id);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
pub(super) struct Accepted {
    pub socket: TcpStream,
    pub address: SocketAddr,
    pub permit: Permit,
}
pub(super) struct Listener {
    socket: Option<TcpListener>,
    shared: Arc<Shared>,
}
impl Listener {
    pub async fn bind(address: SocketAddrV4, config: Config) -> Result<Self, super::Error> {
        let policy = Policy::new(config).map_err(|_| super::Error::Configuration)?;
        Ok(Self {
            socket: Some(TcpListener::bind(address).await?),
            shared: Arc::new(Shared(Mutex::new(State {
                policy,
                next: 0,
                closed: false,
                waker: None,
            }))),
        })
    }
    pub fn address(&self) -> Result<SocketAddrV4, super::Error> {
        match self
            .socket
            .as_ref()
            .ok_or(super::Error::Closed)?
            .local_addr()?
        {
            SocketAddr::V4(address) => Ok(address),
            _ => Err(super::Error::Configuration),
        }
    }
    pub fn status(&self) -> Status {
        let state = self.shared.lock();
        let config = state.policy.config();
        Status {
            active: state.policy.active(),
            acceptors: config.acceptors,
            maximum_connections: config.maximum_connections,
            sleeping: state.policy.sleeping().len(),
            closed: state.closed,
        }
    }
    pub fn close(&mut self) {
        self.socket = None;
        let waker = {
            let mut state = self.shared.lock();
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    pub async fn accept(&self) -> Result<Accepted, super::Error> {
        poll_fn(|cx| {
            let mut state = self.shared.lock();
            if state.closed {
                return Poll::Pending;
            }
            let Some(acceptor) = state.policy.available() else {
                state.waker = Some(cx.waker().clone());
                return Poll::Pending;
            };
            let Some(socket) = &self.socket else {
                return Poll::Pending;
            };
            match socket.poll_accept(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
                Poll::Ready(Ok((socket, address))) => {
                    let id = state.next;
                    let Some(next) = id.checked_add(1) else {
                        return Poll::Ready(Err(super::Error::Configuration));
                    };
                    state.next = next;
                    if state.policy.started(acceptor, id).is_err() {
                        return Poll::Ready(Err(super::Error::Configuration));
                    }
                    Poll::Ready(Ok(Accepted {
                        socket,
                        address,
                        permit: Permit {
                            shared: self.shared.clone(),
                            id,
                        },
                    }))
                }
            }
        })
        .await
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.close();
    }
}
