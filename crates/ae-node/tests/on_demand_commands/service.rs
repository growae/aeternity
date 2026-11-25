use super::*;
use ae_sync::chain::on_demand::service::{self, Client, Completion, Ticket, Worker};
use std::sync::{Mutex, mpsc};
use std::time::Instant;

fn wait(ticket: &Ticket) -> Arc<Completion> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(r) = ticket.try_result().unwrap() {
            return r;
        }
        assert!(Instant::now() < end, "command receipt deadline");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn start(node: &Node, config: service::Config) -> Worker {
    Worker::spawn(
        node.controller(runtime::Config::new(leader())).unwrap(),
        config,
    )
    .unwrap()
}
struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn open(&mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            let _ = s.send(());
        }
    }
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    Reverse,
    Panic,
}
struct Gate {
    entered: mpsc::SyncSender<()>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
    committed: Option<Arc<ae_storage::Database>>,
    calls: AtomicU64,
    fault: Fault,
}
impl Clock for Gate {
    fn unix_millis(&self) -> i128 {
        let n = self.calls.fetch_add(1, Ordering::Relaxed);
        if self
            .committed
            .as_ref()
            .is_none_or(|db| db.view().unwrap().status().revision >= 2)
            && let Some(recv) = self.release.lock().unwrap().take()
        {
            self.entered.send(()).unwrap();
            recv.recv().unwrap();
        }
        match self.fault {
            Fault::None => 1000 + i128::from(n) * 100,
            Fault::Reverse => {
                if n < 2 {
                    1_000_000
                } else {
                    0
                }
            }
            Fault::Panic if n >= 2 => panic!("controlled command clock failure"),
            Fault::Panic => 1000 + i128::from(n) * 100,
        }
    }
}
fn gated(node: &Node, after_commit: bool, fault: Fault) -> (Worker, mpsc::Receiver<()>, Release) {
    let (sent, entered) = mpsc::sync_channel(1);
    let (release, recv) = mpsc::channel();
    let clock = Arc::new(Gate {
        entered: sent,
        release: Mutex::new(Some(recv)),
        committed: after_commit.then(|| node.opened.database().clone()),
        calls: AtomicU64::new(0),
        fault,
    });
    let controller = node
        .with_clock(runtime::Config::new(leader()), clock)
        .unwrap();
    (
        Worker::spawn(controller, Default::default()).unwrap(),
        entered,
        Release(Some(release)),
    )
}
fn outputs(client: &Client) -> Vec<Arc<Completion>> {
    let mut out = vec![];
    while let Some(r) = client.front() {
        assert!(client.acknowledge(&r));
        out.push(r);
    }
    out
}

#[tokio::test]
async fn dropped_waiter_keeps_real_fifo_commands_and_transaction_receipts() {
    let node = Node::new();
    let tx = node.push(1);
    let (worker, entered, mut release) = gated(&node, false, Fault::None);
    let client = worker.client();
    let first = client.submit(Command::Key).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let waiter = tokio::spawn(first.wait());
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let micro = client.submit(Command::Micro).unwrap();
    let pair = client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Any,
        })
        .unwrap();
    let empty = client.submit(Command::EmptyPool).unwrap();
    release.open();
    micro
        .wait()
        .await
        .unwrap()
        .result
        .as_ref()
        .unwrap()
        .command
        .result
        .as_ref()
        .unwrap();
    pair.wait()
        .await
        .unwrap()
        .result
        .as_ref()
        .unwrap()
        .command
        .result
        .as_ref()
        .unwrap();
    empty
        .wait()
        .await
        .unwrap()
        .result
        .as_ref()
        .unwrap()
        .command
        .result
        .as_ref()
        .unwrap();
    let out = outputs(&client);
    assert_eq!(
        out.iter().map(|r| r.accepted.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    let admissions: Vec<_> = out
        .iter()
        .flat_map(|r| &r.result.as_ref().unwrap().admissions)
        .collect();
    assert_eq!(admissions.len(), 4);
    assert!(admissions.iter().all(|a| a.result.is_ok()));
    assert!(admissions.iter().any(
        |a| matches!(a.block.as_ref(),Block::Micro(m) if m.transactions.iter().any(|t|t.hash()==tx))
    ));
    let last = admissions.last().unwrap().block.hash().unwrap();
    assert_eq!(
        node.opened.database().view().unwrap().status().tip,
        Some(last)
    );
    let report = tokio::task::spawn_blocking(move || worker.shutdown())
        .await
        .unwrap();
    assert!(report.status.joined && report.is_settled());
    assert!(report.outputs.is_empty());
    assert_eq!(report.status.completed, 4);
    println!(
        "On-demand command service: dropped async waiter retains actual FIFO key/micro admissions and the funded transaction receipt"
    );
}

#[test]
fn bounded_credits_follow_retained_tickets_and_command_allocations() {
    let node = Node::new();
    let worker = start(
        &node,
        service::Config {
            requests: 1,
            command_bytes: 1024,
            ..Default::default()
        },
    );
    let client = worker.client();
    let rejected = client
        .submit(Command::Blocks {
            count: 0,
            kind: Kind::Unknown(Vec::with_capacity(2048)),
        })
        .err()
        .unwrap();
    assert_eq!(rejected.error, service::Error::ResourceLimit);
    assert!(
        matches!(rejected.command,Command::Blocks { kind:Kind::Unknown(k),.. } if k.capacity()>=2048)
    );
    assert_eq!(client.status().accepted, 0);
    let ticket = client.submit(Command::Key).unwrap();
    let result = wait(&ticket);
    assert!(result.result.as_ref().unwrap().command.result.is_ok());
    assert!(client.acknowledge(&result));
    assert_eq!(
        client.submit(Command::Key).err().unwrap().error,
        service::Error::Busy
    );
    drop(result);
    assert_eq!(
        client.submit(Command::Key).err().unwrap().error,
        service::Error::Busy
    );
    drop(ticket);
    assert_eq!(client.status().retained_requests, 0);
    let ticket = client.submit(Command::Key).unwrap();
    assert_eq!(ticket.accepted().id, 2);
    assert!(
        wait(&ticket)
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    let report = worker.shutdown();
    assert!(report.is_settled());
    assert_eq!(report.outputs.len(), 1);
    println!(
        "On-demand command service: queue credits survive acknowledgement until external receipts drop; oversized retained allocations do not consume sequence numbers"
    );
}

#[test]
fn nonblocking_receipt_reads_race_live_completion_without_losing_results() {
    let node = Node::new();
    let worker = start(&node, service::Config::default());
    let client = worker.client();
    let end = Instant::now() + Duration::from_secs(30);
    for id in 1..=20_000 {
        let ticket = client
            .submit(Command::Blocks {
                count: 0,
                kind: Kind::Key,
            })
            .unwrap();
        let result = loop {
            if let Some(result) = ticket.try_result().expect("accepted completion retained") {
                break result;
            }
            assert!(Instant::now() < end, "bounded live receipt race");
            std::hint::spin_loop();
        };
        assert_eq!(result.accepted.id, id);
        let command = result.result.as_ref().unwrap();
        assert!(command.command.result.is_ok());
        assert!(command.admissions.is_empty());
        assert!(client.acknowledge(&result));
    }
    let report = worker.shutdown();
    assert!(report.is_settled());
    assert_eq!(report.status.completed, 20_000);
    assert!(report.outputs.is_empty());
    println!(
        "On-demand command service: 20000 live nonblocking completion reads retain every accepted result through reply-channel closure"
    );
}

#[test]
fn shutdown_retains_committed_prefix_and_cancels_unstarted_commands() {
    let node = Node::new();
    let (worker, entered, mut release) = gated(&node, true, Fault::None);
    let client = worker.client();
    let active = client
        .submit(Command::Blocks {
            count: 3,
            kind: Kind::Key,
        })
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let queued = client.submit(Command::Key).unwrap();
    let queued2 = client.submit(Command::Micro).unwrap();
    assert_eq!(node.opened.database().view().unwrap().status().revision, 2);
    client.request_shutdown();
    assert_eq!(
        client.submit(Command::Key).err().unwrap().error,
        service::Error::Closed
    );
    release.open();
    let report = worker.shutdown();
    assert!(report.status.joined && report.is_settled());
    assert_eq!((report.status.completed, report.status.cancelled), (3, 2));
    let active = wait(&active);
    let active = active.result.as_ref().unwrap();
    assert_eq!(active.admissions.len(), 1);
    assert!(active.admissions[0].result.is_ok());
    assert_eq!(active.command.produced.len(), 1);
    assert!(matches!(
        active.command.result,
        Err(control::Error::Host(runtime::Error::Stopped))
    ));
    assert!(matches!(
        wait(&queued).result,
        Err(service::Error::Cancelled)
    ));
    assert!(matches!(
        wait(&queued2).result,
        Err(service::Error::Cancelled)
    ));
    assert_eq!(node.opened.database().view().unwrap().status().revision, 2);
    assert_eq!(
        report
            .outputs
            .iter()
            .map(|r| r.accepted.id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    println!(
        "On-demand command service: shutdown retains an actually committed command prefix and returns ordered cancellation receipts for unstarted commands"
    );
}

#[test]
fn implicit_drop_preserves_receipt_and_settled_release_allows_real_replacement() {
    let node = Node::new();
    let worker = start(&node, Default::default());
    let client = worker.client();
    let ticket = client.submit(Command::Key).unwrap();
    assert!(
        wait(&ticket)
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    drop(ticket);
    drop(worker);
    let report = client.take_dropped_report().unwrap();
    assert!(client.take_dropped_report().is_none());
    assert!(report.status.joined && report.is_settled());
    assert!(matches!(
        node.controller(runtime::Config::new(leader())),
        Err(runtime::Error::Snapshot(snapshots::Error::AlreadyRunning))
    ));
    let released = report.release().ok().unwrap();
    assert_eq!(released.outputs.len(), 1);
    let worker = start(&node, Default::default());
    let next = worker.client().submit(Command::Micro).unwrap();
    assert!(wait(&next).result.as_ref().unwrap().command.result.is_ok());
    assert_eq!(node.opened.database().view().unwrap().status().revision, 3);
    assert!(worker.shutdown().is_settled());
    println!(
        "On-demand command service: implicit drop joins and retains one receipt; settled release frees construction leases while observations stay readable"
    );
}

#[test]
fn admission_failure_and_clock_panic_fence_queued_commands_without_retry() {
    for fault in [Fault::Reverse, Fault::Panic] {
        let node = Node::new();
        let (worker, entered, mut release) = gated(&node, false, fault);
        let client = worker.client();
        let first = client.submit(Command::Key).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = client.submit(Command::Key).unwrap();
        release.open();
        let result = wait(&first);
        let r = result.result.as_ref().unwrap();
        assert!(r.recovery_required);
        match fault {
            Fault::Reverse => {
                assert_eq!(r.admissions.len(), 1);
                assert!(r.admissions[0].result.is_err());
            }
            Fault::Panic => {
                assert!(r.admissions.is_empty());
                assert!(matches!(
                    r.command.result,
                    Err(control::Error::Host(runtime::Error::Panicked))
                ));
            }
            Fault::None => unreachable!(),
        }
        assert!(matches!(
            wait(&second).result,
            Err(service::Error::Cancelled)
        ));
        assert_eq!(
            client.submit(Command::Key).err().unwrap().error,
            service::Error::Closed
        );
        let report = worker.shutdown();
        assert_eq!(
            report.status.failure,
            Some(service::Error::RecoveryRequired)
        );
        assert!(report.status.joined && report.is_settled());
        assert_eq!(report.outputs.len(), 2);
        assert_eq!(
            node.opened.database().view().unwrap().status().tip,
            Some(node.opened.identity().genesis)
        );
    }
    println!(
        "On-demand command service: actual failed admission and controlled runtime panic retain their journals, cancel queued requests and never replay effects"
    );
}

#[test]
fn invalid_service_configuration_returns_the_original_owned_controller() {
    let node = Node::new();
    let controller = node.controller(runtime::Config::new(leader())).unwrap();
    let rejected = Worker::spawn(
        controller,
        service::Config {
            requests: 0,
            ..Default::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(rejected.error, service::Error::Configuration);
    assert!(matches!(
        node.controller(runtime::Config::new(leader())),
        Err(runtime::Error::Snapshot(snapshots::Error::AlreadyRunning))
    ));
    let worker = Worker::spawn(rejected.controller, Default::default()).unwrap();
    let ticket = worker.client().submit(Command::Key).unwrap();
    assert!(
        wait(&ticket)
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    assert!(worker.shutdown().is_settled());
    let controller = node.controller(runtime::Config::new(leader())).unwrap();
    let stop = controller.stop_handle();
    let worker = Worker::spawn(controller, Default::default()).unwrap();
    stop.request();
    let end = Instant::now() + Duration::from_secs(5);
    while worker.client().status().running {
        assert!(
            Instant::now() < end,
            "external controller stop wakes the idle owner"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!worker.client().status().accepting);
    assert!(worker.shutdown().is_settled());
}
