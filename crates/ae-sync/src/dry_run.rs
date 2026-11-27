// Owned off-chain work, following aec_dry_run:run_bounded, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::blocking;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeout {
    Unbounded,
    After(Duration),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Stopped,
    Cancelled,
    Timeout,
    Panicked,
    Spawn,
}
impl From<blocking::Error> for Error {
    fn from(e: blocking::Error) -> Self {
        match e {
            blocking::Error::Configuration => Self::Configuration,
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Stopped => Self::Stopped,
            blocking::Error::Cancelled => Self::Cancelled,
            blocking::Error::Panicked => Self::Panicked,
            blocking::Error::Spawn => Self::Spawn,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub threads: usize,
    pub requests: usize,
    pub bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            threads: 2,
            requests: 8,
            bytes: 256 << 20,
        }
    }
}
#[derive(Clone)]
pub struct Client {
    pool: blocking::Pool,
    closed: Arc<AtomicBool>,
}
// This unique owner must outlive accepted work. Clients cannot detach workers
// when the owner exits; abrupt drop joins, even if a closure is slow to stop.
pub struct Worker {
    pool: blocking::Pool,
    closed: Arc<AtomicBool>,
}
pub fn create(config: Config) -> Result<(Client, Worker), Error> {
    let pool = blocking::Pool::named("ae-dry-run", config.threads, config.requests, config.bytes)?;
    let closed = Arc::new(AtomicBool::new(false));
    Ok((
        Client {
            pool: pool.clone(),
            closed: closed.clone(),
        },
        Worker { pool, closed },
    ))
}
// Cooperative checks belong at VM/work boundaries. A timeout cannot preempt
// one native operation; its reader and byte reservation remain held meanwhile.
#[derive(Clone)]
pub struct Control {
    cancel: blocking::Cancellation,
    closed: Arc<AtomicBool>,
    deadline: Option<Instant>,
}
impl Control {
    pub fn execution(&self) -> ae_types::work::Control {
        let source = self.clone();
        ae_types::work::Control::new(move || source.check().is_err())
    }
    pub fn check(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Error::Timeout);
        }
        if self.cancel.cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(())
    }
}
pub struct Reservation {
    reservation: blocking::Reservation,
    closed: Arc<AtomicBool>,
    deadline: Option<Instant>,
}
pub struct Ticket<T> {
    pending: blocking::Pending<Result<T, Error>>,
    deadline: Option<Instant>,
}
// Hold this receipt (or its credits) through rendering and socket completion.
// Its byte weight is a service reservation, not an allocator/RSS measurement.
pub struct Retained<T> {
    pub value: T,
    pub credits: blocking::Credits,
}
impl Client {
    pub fn available(&self) -> (usize, usize) {
        self.pool.available()
    }
    pub fn reserve(&self, bytes: usize, timeout: Timeout) -> Result<Reservation, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let deadline = match timeout {
            Timeout::Unbounded => None,
            Timeout::After(duration) => Some(
                Instant::now()
                    .checked_add(duration)
                    .ok_or(Error::Configuration)?,
            ),
        };
        Ok(Reservation {
            reservation: self.pool.reserve(bytes)?,
            closed: self.closed.clone(),
            deadline,
        })
    }
    pub fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        timeout: Timeout,
        operation: impl FnOnce(Control) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.reserve(bytes, timeout)?.submit(operation)
    }
}
impl Reservation {
    pub fn submit<T: Send + 'static>(
        self,
        operation: impl FnOnce(Control) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let Self {
            reservation,
            closed,
            deadline,
        } = self;
        if closed.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let pending = reservation.submit(move |cancel| {
            let control = Control {
                cancel,
                closed,
                deadline,
            };
            control.check()?;
            let result = operation(control.clone());
            control.check()?;
            Ok(result)
        })?;
        Ok(Ticket { pending, deadline })
    }
}
impl<T> Ticket<T> {
    pub fn cancel(&self) {
        self.pending.cancel();
    }
    pub async fn wait_retained(self) -> Result<Retained<T>, Error> {
        let held = self.pending.retained();
        tokio::pin!(held);
        let response = match self.deadline {
            None => held.await?,
            Some(deadline) => {
                // A result completed before the deadline remains valid even
                // if the caller only polls after the timer would have expired.
                tokio::select! {
                    biased;
                    response=&mut held=>response?,
                    _=tokio::time::sleep_until(deadline.into())=>return Err(Error::Timeout),
                }
            }
        };
        Ok(Retained {
            value: response.value?,
            credits: response.credits,
        })
    }
    pub async fn wait(self) -> Result<T, Error> {
        Ok(self.wait_retained().await?.value)
    }
}
impl Worker {
    pub fn stop(&self) {
        self.closed.store(true, Ordering::Release);
        self.pool.stop();
    }
    pub async fn shutdown(self) {
        self.stop();
        self.pool.join().await;
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop();
        self.pool.join_blocking();
    }
}

#[cfg(test)]
mod tests;

pub mod process;
