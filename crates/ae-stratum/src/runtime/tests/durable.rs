use super::*;
use crate::ingress::Id;
use std::sync::atomic::AtomicU64;
type Request = (
    Id,
    Vec<share::Effect>,
    oneshot::Sender<Result<Id, pool::Error>>,
);
struct Controlled {
    requests: Mutex<VecDeque<Request>>,
    next: AtomicU64,
    busy: AtomicBool,
    closed: watch::Sender<bool>,
}
impl Controlled {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(VecDeque::new()),
            next: AtomicU64::new(1),
            busy: AtomicBool::new(false),
            closed: watch::channel(false).0,
        })
    }
}
impl pool::Sink for Controlled {
    fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
    fn submit(&self, effects: &[share::Effect]) -> Result<pool::Submission, pool::Error> {
        if self.busy.load(Ordering::Acquire) {
            return Err(pool::Error::Busy);
        }
        let id = Id {
            epoch: 7,
            sequence: self.next.fetch_add(1, Ordering::AcqRel),
        };
        let (reply, rx) = oneshot::channel();
        self.requests
            .lock()
            .unwrap()
            .push_back((id, effects.to_vec(), reply));
        Ok(pool::Submission {
            id,
            completion: Box::pin(async move { rx.await.unwrap_or(Err(pool::Error::Unknown)) }),
        })
    }
}
fn no_response(peer: &Peer) {
    peer.0
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(25)))
        .unwrap();
    let e = peer.0.get_ref().peek(&mut [0u8; 1]).unwrap_err();
    assert!(matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    peer.0
        .get_ref()
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
}
#[test]
fn miner_acknowledgement_waits_for_exact_receipt_and_unknown_exits_retain_identity_and_effects() {
    let row = corpus()
        .into_iter()
        .find(|r| r["mode"] == "integer")
        .unwrap();
    for mode in [
        "success",
        "reject",
        "wrong",
        "abandon",
        "timeout",
        "cancel",
        "sink_close",
    ] {
        let sink = Controlled::new();
        let (owner, client, mut peer, job, _) = proof_setup_pool(&row, 8, Some(sink.clone()));
        peer.send(&proof_request(&row, &job));
        until(|| !sink.requests.lock().unwrap().is_empty());
        no_response(&peer);
        assert_eq!(client.pending_pool_batches(), 1);
        let (id, effects, reply) = sink.requests.lock().unwrap().pop_front().unwrap();
        assert_eq!(
            id,
            Id {
                epoch: 7,
                sequence: 1
            }
        );
        assert_eq!(effects.len(), 2);
        match mode {
            "success" => reply.send(Ok(id)).unwrap(),
            "reject" => reply.send(Err(pool::Error::Rejected)).unwrap(),
            "wrong" => reply.send(Ok(Id { sequence: 2, ..id })).unwrap(),
            "abandon" => drop(reply),
            "cancel" => {
                client.connections()[0].close();
                drop(reply);
            }
            "sink_close" => {
                sink.closed.send_replace(true);
                drop(reply);
            }
            _ => {
                peer.eof();
                drop(reply);
            }
        }
        if mode == "success" {
            let response: Value = serde_json::from_slice(&peer.packets(1)[0]).unwrap();
            assert_eq!(response["result"], true);
            assert!(client.next_output().is_none());
            drop(peer);
        } else if mode != "timeout" {
            peer.eof();
            drop(peer);
        }
        until(|| client.connections().is_empty());
        assert_eq!(client.pending_pool_batches(), 0);
        let mut report = owner.shutdown();
        let output = client.next_output().or_else(|| report.outputs.pop());
        let exit = if let Some(output) = output {
            let Output::Closed(exit) = &*output else {
                panic!()
            };
            exit.clone()
        } else {
            report.unpublished_exits.pop().expect("retained exit")
        };
        if mode == "success" {
            assert!(exit.pending_pool.is_empty());
            assert_eq!(exit.pending_receipt, None);
        } else {
            assert_eq!(exit.pending_pool, effects);
            assert_eq!(exit.pending_receipt, Some(id));
            match mode {
                "reject" => assert_eq!(exit.reason, ExitReason::Pool(pool::Error::Rejected)),
                "wrong" => assert_eq!(exit.reason, ExitReason::Pool(pool::Error::Receipt)),
                "abandon" => assert_eq!(exit.reason, ExitReason::Pool(pool::Error::Unknown)),
                "timeout" => assert_eq!(exit.reason, ExitReason::OperationTimeout),
                _ => {}
            }
        }
    }
}
#[test]
fn unaccepted_sink_backpressure_retries_without_consuming_a_receipt_identity() {
    let row = corpus()
        .into_iter()
        .find(|r| r["mode"] == "integer")
        .unwrap();
    let sink = Controlled::new();
    sink.busy.store(true, Ordering::Release);
    let (owner, client, mut peer, job, _) = proof_setup_pool(&row, 8, Some(sink.clone()));
    peer.send(&proof_request(&row, &job));
    until(|| client.pending_pool_batches() == 1);
    no_response(&peer);
    assert!(sink.requests.lock().unwrap().is_empty());
    sink.busy.store(false, Ordering::Release);
    until(|| !sink.requests.lock().unwrap().is_empty());
    let (id, _, reply) = sink.requests.lock().unwrap().pop_front().unwrap();
    assert_eq!(id.sequence, 1);
    reply.send(Ok(id)).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&peer.packets(1)[0]).unwrap()["result"],
        true
    );
    drop(peer);
    owner.shutdown();
}
