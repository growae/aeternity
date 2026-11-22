use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Configuration,
    Busy,
    Stopped,
    Cancelled,
    Panicked,
    Spawn,
}
#[derive(Clone)]
pub(crate) struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
type Job = Box<dyn FnOnce() + Send>;
struct Queue {
    stopped: bool,
    jobs: VecDeque<Job>,
}
struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
}
struct Inner {
    shared: Arc<Shared>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    byte_limit: usize,
}
impl Inner {
    fn stop(&self) {
        let mut queue = self.shared.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.stopped = true;
        queue.jobs.clear();
        self.slots.close();
        self.bytes.close();
        self.shared.wake.notify_all();
    }
    fn join(&self) {
        let threads = std::mem::take(&mut *self.threads.lock().unwrap_or_else(|e| e.into_inner()));
        for thread in threads {
            let _ = thread.join();
        }
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        // Cancellation cannot orphan a running database/cache operation. Normal
        // shutdown waits asynchronously before joining; abrupt owner drop may
        // block here until its already-running bounded operations return.
        self.stop();
        self.join();
    }
}
struct Response<T> {
    result: Result<T, Error>,
    _slot: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub(crate) struct Pending<T> {
    receiver: oneshot::Receiver<Response<T>>,
    cancel: Cancellation,
}
impl<T> Pending<T> {
    pub fn try_result(&mut self) -> Option<Result<T, Error>> {
        match self.receiver.try_recv() {
            Ok(response) => Some(response.result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(Error::Stopped)),
        }
    }
    pub fn cancel(&self) {
        self.cancel.0.store(true, Ordering::Release);
    }
}
impl<T> Future for Pending<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(response)) => Poll::Ready(response.result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Stopped)),
        }
    }
}
impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        self.cancel.0.store(true, Ordering::Release);
    }
}
#[derive(Clone)]
pub(crate) struct Pool(Arc<Inner>);
pub(crate) struct Reservation {
    pool: Pool,
    slot: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
}
impl Reservation {
    pub fn submit<T: Send + 'static>(
        self,
        operation: impl FnOnce(Cancellation) -> T + Send + 'static,
    ) -> Result<Pending<T>, Error> {
        self.pool.enqueue(self.slot, self.bytes, operation)
    }
}
impl Pool {
    pub fn new(threads: usize, jobs: usize, bytes: usize) -> Result<Self, Error> {
        Self::named("ae-sync-read", threads, jobs, bytes)
    }
    pub fn named(
        name: &'static str,
        threads: usize,
        jobs: usize,
        bytes: usize,
    ) -> Result<Self, Error> {
        let maximum = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        if !(1..=32).contains(&threads)
            || jobs == 0
            || jobs > maximum
            || bytes == 0
            || bytes > maximum
        {
            return Err(Error::Configuration);
        }
        let pool = Self(Arc::new(Inner {
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue {
                    stopped: false,
                    jobs: VecDeque::new(),
                }),
                wake: Condvar::new(),
            }),
            threads: Mutex::new(vec![]),
            slots: Arc::new(Semaphore::new(jobs)),
            bytes: Arc::new(Semaphore::new(bytes)),
            byte_limit: bytes,
        }));
        for index in 0..threads {
            let shared = pool.0.shared.clone();
            let thread = thread::Builder::new()
                .name(format!("{name}-{index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let queue = shared.queue.lock().unwrap_or_else(|e| e.into_inner());
                            let mut queue = shared
                                .wake
                                .wait_while(queue, |q| !q.stopped && q.jobs.is_empty())
                                .unwrap_or_else(|e| e.into_inner());
                            if queue.stopped {
                                break;
                            }
                            queue.jobs.pop_front().expect("nonempty queue")
                        };
                        job();
                    }
                })
                .map_err(|_| Error::Spawn)?;
            pool.0
                .threads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(thread);
        }
        Ok(pool)
    }
    pub fn available(&self) -> (usize, usize) {
        (
            self.0.slots.available_permits(),
            self.0.bytes.available_permits(),
        )
    }
    pub fn stopped(&self) -> bool {
        self.0
            .shared
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stopped
    }
    pub fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        operation: impl FnOnce(Cancellation) -> T + Send + 'static,
    ) -> Result<Pending<T>, Error> {
        self.reserve(bytes)?.submit(operation)
    }
    // A caller can charge work before acquiring input from an external peer.
    // Moving this reservation into the job does not release/reacquire capacity.
    pub fn reserve(&self, bytes: usize) -> Result<Reservation, Error> {
        if self.stopped() {
            return Err(Error::Stopped);
        }
        let slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let weight = u32::try_from(bytes.max(1)).map_err(|_| Error::Busy)?;
        let bytes = self
            .0
            .bytes
            .clone()
            .try_acquire_many_owned(weight)
            .map_err(|_| Error::Busy)?;
        Ok(Reservation {
            pool: self.clone(),
            slot,
            bytes,
        })
    }
    pub async fn submit_wait<T: Send + 'static>(
        &self,
        bytes: usize,
        operation: impl FnOnce(Cancellation) -> T + Send + 'static,
    ) -> Result<Pending<T>, Error> {
        self.reserve_wait(bytes).await?.submit(operation)
    }
    pub async fn reserve_wait(&self, bytes: usize) -> Result<Reservation, Error> {
        let weight = u32::try_from(bytes.max(1)).map_err(|_| Error::Busy)?;
        if bytes.max(1) > self.0.byte_limit {
            return Err(Error::Busy);
        }
        let slot = self
            .0
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Stopped)?;
        let bytes = self
            .0
            .bytes
            .clone()
            .acquire_many_owned(weight)
            .await
            .map_err(|_| Error::Stopped)?;
        Ok(Reservation {
            pool: self.clone(),
            slot,
            bytes,
        })
    }
    fn enqueue<T: Send + 'static>(
        &self,
        slot: OwnedSemaphorePermit,
        bytes: OwnedSemaphorePermit,
        operation: impl FnOnce(Cancellation) -> T + Send + 'static,
    ) -> Result<Pending<T>, Error> {
        let (sender, receiver) = oneshot::channel();
        let cancel = Cancellation(Arc::new(AtomicBool::new(false)));
        let token = cancel.clone();
        let job = Box::new(move || {
            let result = if token.cancelled() {
                Err(Error::Cancelled)
            } else {
                catch_unwind(AssertUnwindSafe(|| operation(token))).map_err(|_| Error::Panicked)
            };
            let _ = sender.send(Response {
                result,
                _slot: slot,
                _bytes: bytes,
            });
        });
        let mut queue = self
            .0
            .shared
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if queue.stopped {
            return Err(Error::Stopped);
        }
        queue.jobs.push_back(job);
        self.0.shared.wake.notify_one();
        Ok(Pending { receiver, cancel })
    }
    pub fn stop(&self) {
        self.0.stop();
    }
    pub async fn join(&self) {
        loop {
            if self
                .0
                .threads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .all(JoinHandle::is_finished)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        self.0.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn polling_and_explicit_cancellation_keep_unread_job_credits() {
        let pool = Pool::new(1, 2, 8).unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, observed) = oneshot::channel();
        let mut first = pool
            .submit(4, move |_| {
                started.send(()).unwrap();
                wait.recv().unwrap();
                7
            })
            .unwrap();
        observed.await.unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let flag = executed.clone();
        let mut second = pool
            .submit(4, move |_| flag.store(true, Ordering::Release))
            .unwrap();
        second.cancel();
        for _ in 0..3 {
            assert_eq!(first.try_result(), None);
            assert_eq!(second.try_result(), None);
            assert_eq!(pool.available(), (0, 0));
        }
        release.send(()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(result) = first.try_result() {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(result, Ok(7));
        assert_eq!(second.await, Err(Error::Cancelled));
        assert!(!executed.load(Ordering::Acquire));
        assert_eq!(pool.available(), (2, 8));
        pool.stop();
        pool.join().await;
    }
    #[tokio::test]
    async fn reservations_charge_capacity_before_work_and_transfer_through_unread_results() {
        let pool = Pool::new(1, 2, 8).unwrap();
        let first = pool.reserve(4).unwrap();
        assert_eq!(pool.available(), (1, 4));
        assert!(matches!(pool.reserve(5), Err(Error::Busy)));
        assert_eq!(pool.available(), (1, 4)); // Failed byte reservation returns its slot.
        let second = pool.reserve(4).unwrap();
        assert!(matches!(pool.submit(1, |_| 0), Err(Error::Busy)));
        let (done, observed) = oneshot::channel();
        let pending = first
            .submit(move |_| {
                done.send(()).unwrap();
                7
            })
            .unwrap();
        observed.await.unwrap();
        assert_eq!(pool.available(), (0, 0));
        assert_eq!(pending.await, Ok(7));
        assert_eq!(pool.available(), (1, 4));
        drop(second);
        assert_eq!(pool.available(), (2, 8));
        pool.stop();
        pool.join().await;
    }
    #[tokio::test]
    async fn stopping_a_pool_rejects_already_reserved_work_without_losing_capacity() {
        let pool = Pool::new(1, 1, 4).unwrap();
        let reservation = pool.reserve(4).unwrap();
        pool.stop();
        assert!(matches!(
            reservation.submit(|_| panic!("stopped work")),
            Err(Error::Stopped)
        ));
        assert_eq!(pool.available(), (1, 4));
        assert!(matches!(pool.reserve(4), Err(Error::Stopped)));
        pool.join().await;
    }
    #[tokio::test]
    async fn quotas_cover_unobserved_results_and_cancelled_queued_jobs_do_not_execute() {
        let pool = Pool::new(1, 1, 4).unwrap();
        let (started, observed) = oneshot::channel();
        let result = pool
            .submit(4, move |_| {
                let _ = started.send(());
                7
            })
            .unwrap();
        observed.await.unwrap();
        assert!(matches!(pool.submit(1, |_| 0), Err(Error::Busy)));
        assert_eq!(result.await, Ok(7));
        assert_eq!(pool.submit(4, |_| 8).unwrap().await, Ok(8));
        pool.stop();
        pool.join().await;
        let pool = Pool::new(1, 2, 8).unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, observed) = oneshot::channel();
        let running = pool
            .submit(4, move |_| {
                let _ = started.send(());
                wait.recv().unwrap();
                1
            })
            .unwrap();
        observed.await.unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let flag = executed.clone();
        let queued = pool
            .submit(4, move |_| flag.store(true, Ordering::Release))
            .unwrap();
        drop(queued);
        release.send(()).unwrap();
        assert_eq!(running.await, Ok(1));
        pool.stop();
        pool.join().await;
        assert!(!executed.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn abrupt_owner_drop_joins_running_jobs_and_contains_worker_panics() {
        let pool = Pool::new(1, 2, 8).unwrap();
        assert_eq!(
            pool.submit(1, |_| -> u8 { panic!("injected job failure") })
                .unwrap()
                .await,
            Err(Error::Panicked)
        );
        let (release, wait) = std::sync::mpsc::channel();
        let (started, observed) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let flag = completed.clone();
        let running = pool
            .submit(1, move |_| {
                let _ = started.send(());
                wait.recv().unwrap();
                flag.store(true, Ordering::Release);
                9
            })
            .unwrap();
        observed.await.unwrap();
        let releaser = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            release.send(()).unwrap();
        });
        drop(pool);
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(running.await, Ok(9));
        releaser.join().unwrap();
    }
    #[tokio::test]
    async fn asynchronous_capacity_wait_is_cancellable_and_oversize_never_waits() {
        let pool = Pool::new(1, 1, 4).unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, observed) = oneshot::channel();
        let running = pool
            .submit(4, move |_| {
                let _ = started.send(());
                wait.recv().unwrap();
                7
            })
            .unwrap();
        observed.await.unwrap();
        let executed = Arc::new(AtomicBool::new(false));
        let flag = executed.clone();
        let mut waiting =
            Box::pin(pool.submit_wait(4, move |_| flag.store(true, Ordering::Release)));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), waiting.as_mut())
                .await
                .is_err()
        );
        drop(waiting);
        release.send(()).unwrap();
        assert_eq!(running.await, Ok(7));
        assert_eq!(pool.submit_wait(4, |_| 8).await.unwrap().await, Ok(8));
        assert!(!executed.load(Ordering::Acquire));
        assert!(matches!(pool.submit_wait(5, |_| 0).await, Err(Error::Busy)));
        pool.stop();
        assert!(matches!(
            pool.submit_wait(1, |_| 0).await,
            Err(Error::Stopped)
        ));
        pool.join().await;
    }
}
