use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn row() -> &'static J {
    FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["input"]["name"] == "call_success"
                && r["input"]["protocol"] == 6
                && r["operation"] == "DryRunTxs"
        })
        .unwrap()
}
type JobTicket = admission::Ticket<
    process::Completed<worker::Broker<ae_trie::MemoryStore, ae_trie::MemoryStore>>,
>;
fn ticket(
    client: &admission::Client,
    p: process::Config,
    control: Option<ae_types::work::Control>,
) -> (JobTicket, std::sync::Weak<View>) {
    timed_ticket(client, p, control, Timeout::After(Duration::from_secs(10)))
}
fn timed_ticket(
    client: &admission::Client,
    p: process::Config,
    control: Option<ae_types::work::Control>,
    timeout: Timeout,
) -> (JobTicket, std::sync::Weak<View>) {
    let row = row();
    let mut source = Backend::new(row);
    let wire = bytes(&row["raw"]);
    let parsed = schema::request(schema::Body::Complete(&wire), false, "", 5_000_000).unwrap();
    let gas: BigInt = 6_000_000.into();
    let keys: BTreeSet<_> = [
        "txs",
        "top",
        "accounts",
        "tx_events",
        "tx",
        "tx_hash",
        "call_req",
        "calldata",
        "contract",
        "amount",
        "gas",
        "caller",
        "nonce",
        "abi_version",
        "context",
        "stateful",
        "pub_key",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let order = otp_order::profile(&FIXTURE["profile"]);
    let config = Config {
        profile: Profile::Internal,
        gas_limit: &gas,
        existing_keys: &keys,
        order: &order,
        state: Default::default(),
        limits: Default::default(),
    };
    let request = dry_run::prepare(&parsed.input, &mut source, &config).unwrap();
    let view = source.view(request.top).unwrap();
    let weak = Arc::downgrade(&view);
    let options = ae_execution::dry_run::Options {
        control,
        ..Default::default()
    };
    let t = configured(&FIXTURE["profile"], |base| {
        worker::submit_http(
            client,
            p,
            view,
            request,
            options,
            parameters(base, None),
            timeout,
        )
    })
    .unwrap();
    (t, weak)
}
#[tokio::test]
async fn captured_control_cancels_real_fate_work_at_each_checkpoint_and_recovers() {
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let (t, _) = ticket(
        &client,
        p.clone(),
        Some(ae_types::work::Control::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
            false
        })),
    );
    let done = t.wait().await.unwrap();
    let worker::Reply::Done(expected) = worker::decode_reply(&done.result.unwrap()).unwrap() else {
        panic!()
    };
    drop(done.broker);
    let total = counter.load(Ordering::Relaxed);
    assert!(total > 20);
    let mut vm = false;
    for limit in 0..total {
        let c = Arc::new(AtomicUsize::new(0));
        let (t, weak) = ticket(
            &client,
            p.clone(),
            Some(ae_types::work::Control::new(move || {
                c.fetch_add(1, Ordering::Relaxed) >= limit
            })),
        );
        let kept = t.wait_retained().await.unwrap();
        let reply = worker::decode_reply(kept.value.result.as_ref().unwrap()).unwrap();
        let worker::Reply::Failed(f) = reply else {
            panic!("checkpoint {limit}/{total}: {reply:?}")
        };
        assert_eq!(f.stage, worker::Stage::Execution);
        assert!(matches!(
            f.kind,
            worker::Kind::Interrupted | worker::Kind::Preparation | worker::Kind::Operational
        ));
        vm |= f.diagnostic.contains("Contract(Interrupted)");
        assert!(weak.upgrade().is_some());
        assert_eq!(client.available().0, 0);
        drop(kept);
        assert!(weak.upgrade().is_none());
    }
    assert!(vm, "at least one interruption was inside FATE execution");
    let (t, _) = ticket(&client, p, None);
    let done = t.wait().await.unwrap();
    let worker::Reply::Done(retry) = worker::decode_reply(&done.result.unwrap()).unwrap() else {
        panic!()
    };
    assert_eq!(retry.roots, expected.roots);
    assert_eq!(retry.body, expected.body);
    println!("isolated control checkpoints={total}");
    owner.shutdown().await;
}
#[tokio::test]
async fn production_worker_heap_exits_and_small_output_limits_remain_operational() {
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    for source in [
        ae_worker_heap::Source::Native,
        ae_worker_heap::Source::Service,
    ] {
        let mut tiny = p.clone();
        tiny.heap = ae_worker_heap::Limit { bytes: 1, source };
        let (t, weak) = ticket(&client, tiny, None);
        let done = t.wait_retained().await.unwrap();
        let native = done
            .value
            .result
            .as_ref()
            .err()
            .and_then(dry_run::failure::process);
        assert_eq!(
            native,
            if source == ae_worker_heap::Source::Native {
                Some("dry-run failed: killed")
            } else {
                None
            }
        );
        let exit = match done.value.result {
            Err(process::Error::Exit(s)) => s.code().and_then(ae_worker_heap::Exit::from_code),
            ref r => panic!("startup cap: {r:?}"),
        };
        assert_eq!(
            exit,
            Some(if source == ae_worker_heap::Source::Native {
                ae_worker_heap::Exit::NativeHeap
            } else {
                ae_worker_heap::Exit::ServiceHeap
            })
        );
        drop(done);
        assert!(weak.upgrade().is_none());
    }
    let mut tiny = p.clone();
    tiny.output_bytes = 0;
    let (t, _) = ticket(&client, tiny, None);
    let done = t.wait().await.unwrap();
    assert!(matches!(done.result,Err(process::Error::Exit(s))if s.code()==Some(94)));
    let (t, _) = ticket(&client, p, None);
    let done = t.wait().await.unwrap();
    assert!(matches!(
        worker::decode_reply(&done.result.unwrap()).unwrap(),
        worker::Reply::Done(_)
    ));
    owner.shutdown().await;
}

#[tokio::test]
async fn native_timeout_response_keeps_blocked_callback_source_and_capacity_owned() {
    use std::sync::{Condvar, Mutex, mpsc};
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all()
        }
    }
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(gate.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let control = ae_types::work::Control::new(move || {
        let _ = tx.try_send(());
        drop(gate.1.wait_while(gate.0.lock().unwrap(), |v| !*v).unwrap());
        false
    });
    let (t, weak) = timed_ticket(
        &client,
        p.clone(),
        Some(control),
        Timeout::After(Duration::from_millis(500)),
    );
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let error = match t.wait_retained().await {
        Err(e) => e,
        Ok(_) => panic!("deadline must fire while callback is blocked"),
    };
    assert_eq!(error, admission::Error::Timeout);
    let response =
        dry_run::failure::body(dry_run::failure::admission(error).unwrap(), 1 << 20).unwrap();
    let native = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "deadline" && r["operation"] == "ProtectedDryRunTxs")
        .unwrap();
    assert_eq!(
        response,
        (400, native["result"]["response"]["body"].clone())
    );
    assert!(weak.upgrade().is_some());
    assert_eq!(client.available().0, 0);
    drop(release);
    tokio::time::timeout(Duration::from_secs(3), async {
        while client.available().0 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    assert!(weak.upgrade().is_none());
    let (t, _) = ticket(&client, p, None);
    let done = t.wait().await.unwrap();
    assert!(matches!(
        worker::decode_reply(&done.result.unwrap()).unwrap(),
        worker::Reply::Done(_)
    ));
    owner.shutdown().await;
}

#[tokio::test]
async fn production_heap_exit_survives_a_partial_large_job_write() {
    struct NoReads;
    impl process::Broker for NoReads {
        type Error = ();
        fn read(&mut self, _: &[u8]) -> Result<Vec<u8>, ()> {
            panic!("heap limit must fire before job decoding")
        }
    }
    let mut p = process();
    p.heap.bytes = 1;
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    for source in [
        ae_worker_heap::Source::Native,
        ae_worker_heap::Source::Service,
    ] {
        p.heap.source = source;
        for _ in 0..16 {
            let ticket = process::submit(
                &client,
                p.clone(),
                vec![0; 1 << 20],
                NoReads,
                Timeout::After(Duration::from_secs(5)),
            )
            .unwrap();
            let done = ticket.wait().await.unwrap();
            let exit = match done.result {
                Err(process::Error::Exit(s)) => s.code().and_then(ae_worker_heap::Exit::from_code),
                ref r => panic!(
                    "large startup cap: {r:?}; observation={:?}",
                    done.observation
                ),
            };
            assert_eq!(
                exit,
                Some(if source == ae_worker_heap::Source::Native {
                    ae_worker_heap::Exit::NativeHeap
                } else {
                    ae_worker_heap::Exit::ServiceHeap
                })
            );
        }
    }
    owner.shutdown().await;
}
