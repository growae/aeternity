use super::*;
use crate::dry_run::{self, Worker};
use std::{
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};
#[global_allocator]
static HEAP: ae_worker_heap::Guard = ae_worker_heap::Guard::new();
fn config() -> Config {
    let mut c = Config::new(std::env::current_exe().unwrap(), std::env::temp_dir());
    c.arguments = [
        "--exact",
        "dry_run::process::tests::child_entry",
        "--ignored",
        "--test-threads=1",
    ]
    .map(OsString::from)
    .to_vec();
    c.stderr = true;
    c.heap.bytes = 8 << 20;
    c.frame_bytes = 1 << 20;
    c.input_bytes = 1 << 20;
    c.output_bytes = 1 << 20;
    c.process_overhead = 1 << 20;
    c.drain = Duration::from_millis(500);
    c
}
fn worker(c: &Config, n: usize) -> (Client, Worker) {
    dry_run::create(dry_run::Config {
        threads: 1,
        requests: 1,
        bytes: c.reservation(n).unwrap(),
    })
    .unwrap()
}
async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("bounded process test")
}
struct Echo {
    reader: Arc<()>,
    calls: usize,
    fail: bool,
}
impl Broker for Echo {
    type Error = &'static str;
    fn read(&mut self, q: &[u8]) -> Result<Vec<u8>, Self::Error> {
        self.calls += 1;
        if self.fail {
            Err("captured storage failure")
        } else {
            Ok(q.to_vec())
        }
    }
}
fn echo() -> Echo {
    Echo {
        reader: Arc::new(()),
        calls: 0,
        fail: false,
    }
}
#[test]
#[ignore = "dedicated bounded worker entrypoint, called by process tests"]
fn child_entry() {
    let mut session = wire::Session::start(std::io::stdin(), std::io::stderr(), &HEAP).unwrap();
    let input = session.initial(wire::MAX_FRAME).unwrap();
    match input.as_slice() {
        b"echo" => {
            let data = session.read(b"captured-state").unwrap();
            session.finish(&data).unwrap()
        }
        b"fragment" => {
            // One byte per delayed write forces the supervisor to preserve a
            // partially read header/body across many monitor polling ticks.
            let mut output = std::io::stderr();
            let mut bytes = wire::Header {
                kind: wire::RESULT,
                id: 0,
                bytes: 10,
            }
            .encode(100)
            .unwrap()
            .to_vec();
            bytes.extend_from_slice(b"fragmented");
            for byte in bytes {
                output.write_all(&[byte]).unwrap();
                output.flush().unwrap();
                std::thread::sleep(Duration::from_millis(8));
            }
        }
        b"sleep" => std::thread::sleep(Duration::from_secs(5)),
        b"heap" | b"service_heap" => {
            std::hint::black_box(vec![
                std::hint::black_box(7_u8);
                std::hint::black_box(32 << 20)
            ]);
            panic!("heap guard bypassed")
        }
        b"exit" => std::process::exit(17),
        b"oversize" => {
            let mut h = wire::Header {
                kind: wire::RESULT,
                id: 0,
                bytes: 0,
            }
            .encode(1)
            .unwrap();
            h[9..].copy_from_slice(&u32::MAX.to_be_bytes());
            std::io::stderr().write_all(&h).unwrap();
            std::thread::sleep(Duration::from_secs(5));
        }
        b"partial" => {
            std::io::stderr().write_all(&[wire::RESULT, 0, 0]).unwrap();
        }
        b"trailing" => {
            session.finish(b"first").unwrap();
            std::io::stderr()
                .write_all(
                    &wire::Header {
                        kind: wire::RESULT,
                        id: 0,
                        bytes: 0,
                    }
                    .encode(1)
                    .unwrap(),
                )
                .unwrap();
        }
        b"many" => {
            for _ in 0..10 {
                session.read(b"q").unwrap();
            }
            session.finish(b"bad").unwrap()
        }
        b"wrong_id" => {
            std::io::stderr()
                .write_all(
                    &wire::Header {
                        kind: wire::READ,
                        id: 2,
                        bytes: 0,
                    }
                    .encode(1)
                    .unwrap(),
                )
                .unwrap();
            std::thread::sleep(Duration::from_secs(5));
        }
        b"linger" => {
            session.finish(b"finished").unwrap();
            std::thread::sleep(Duration::from_secs(5));
        }
        b"blocked" | b"panic" => {
            session.read(b"block").unwrap();
            session.finish(b"done").unwrap()
        }
        _ if input.first() == Some(&b'{') => {
            use ae_tx::CanonicalTransaction;
            let job: serde_json::Value = serde_json::from_slice(&input).unwrap();
            let root = ae_types::Hash256(
                hex::decode(job["root"].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            let tx =
                ae_tx::Spend::decode(&hex::decode(job["tx"].as_str().unwrap()).unwrap()).unwrap();
            let ctx = ae_execution::Context::transaction(
                job["height"].as_u64().unwrap(),
                ae_protocol::Protocol::try_from(job["protocol"].as_u64().unwrap() as u32).unwrap(),
            );
            let shared = Arc::new(Mutex::new(session));
            let reader = trie::Reader::new(shared.clone(), Default::default());
            let mut accounts = ae_state::accounts::Accounts::from_root(
                Some(root),
                ae_trie::Overlay::new(reader, Default::default()),
            )
            .unwrap();
            let names = ae_trie::Trie::new(ae_trie::MemoryStore::default());
            ae_execution::process_spend(&tx, &mut accounts, &names, &ctx).unwrap();
            let root = ae_state::StateRoots {
                accounts: accounts.root_hash(),
                ..Default::default()
            }
            .hash();
            shared.lock().unwrap().finish(&root.0).unwrap();
        }
        _ => panic!("unknown bounded child operation"),
    }
}
#[tokio::test]
async fn framed_reads_results_and_capacity_retain_the_captured_source() {
    for input in [b"echo".as_slice(), b"fragment"] {
        let c = config();
        let (client, owner) = worker(&c, input.len());
        let b = echo();
        let weak = Arc::downgrade(&b.reader);
        let response = within(
            submit(
                &client,
                c.clone(),
                input.to_vec(),
                b,
                Timeout::After(Duration::from_secs(5)),
            )
            .unwrap()
            .wait_retained(),
        )
        .await
        .unwrap();
        assert_eq!(
            response.value.result.as_ref().unwrap(),
            if input == b"echo" {
                b"captured-state".as_slice()
            } else {
                b"fragmented"
            }
        );
        assert!(
            response
                .value
                .observation
                .as_ref()
                .unwrap()
                .status
                .success()
        );
        assert_eq!(client.available(), (0, 0));
        assert!(weak.upgrade().is_some());
        drop(response);
        assert!(weak.upgrade().is_none());
        assert_eq!(client.available(), (1, c.reservation(input.len()).unwrap()));
        within(owner.shutdown()).await;
    }
}
#[tokio::test]
async fn real_spend_roots_match_native_through_read_broker_without_parent_writes() {
    let cases: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../ae-execution/tests/fixtures/erlang_spends.json"
    ))
    .unwrap();
    for protocol in 1..=8 {
        let case = cases
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["label"] == format!("{protocol}-1"))
            .unwrap();
        let mut accounts = ae_state::accounts::Accounts::default();
        for a in case["accounts"].as_array().unwrap() {
            let key = ae_types::AccountPublicKey(
                hex::decode(a["key"].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            let account = ae_state::accounts::Account::deserialize(
                key,
                &hex::decode(a["encoded"].as_str().unwrap()).unwrap(),
            )
            .unwrap();
            accounts.enter(&account).unwrap();
        }
        let root = accounts.root_hash().unwrap();
        assert_eq!(
            hex::encode(
                ae_state::StateRoots {
                    accounts: Some(root),
                    ..Default::default()
                }
                .hash()
                .0
            ),
            case["initial_root"]
        );
        let source = Arc::new(accounts.tree().store().clone());
        let original = source.nodes().clone();
        let input=serde_json::to_vec(&serde_json::json!({"root":hex::encode(root.0),"tx":case["transaction"],"height":case["height"],"protocol":protocol})).unwrap();
        assert!(input.len() < 4096);
        let c = config();
        let (client, owner) = worker(&c, input.len());
        let broker = trie::Store::new(source.clone(), (1 << 20) - 1).unwrap();
        let response = within(
            submit(
                &client,
                c,
                input,
                broker,
                Timeout::After(Duration::from_secs(5)),
            )
            .unwrap()
            .wait_retained(),
        )
        .await
        .unwrap();
        assert_eq!(
            hex::encode(response.value.result.as_ref().unwrap()),
            case["final_root"]
        );
        assert_eq!(source.nodes(), &original);
        assert!(response.value.observation.unwrap().status.success());
        drop(response.credits);
        within(owner.shutdown()).await;
    }
}
#[tokio::test]
async fn cap_exit_protocol_failure_and_broker_failure_are_distinct_and_recover() {
    let c = config();
    let (client, owner) = dry_run::create(dry_run::Config {
        threads: 1,
        requests: 1,
        bytes: c.reservation(c.input_bytes).unwrap(),
    })
    .unwrap();
    for input in [
        "heap",
        "service_heap",
        "exit",
        "oversize",
        "partial",
        "trailing",
        "many",
        "wrong_id",
        "linger",
        "echo",
    ] {
        let mut c = c.clone();
        c.read_requests = 2;
        c.drain = Duration::from_millis(100);
        c.heap.source = if input == "heap" {
            Source::Native
        } else {
            Source::Service
        };
        let mut broker = echo();
        broker.fail = input == "echo";
        let done = within(
            submit(
                &client,
                c,
                input.as_bytes().to_vec(),
                broker,
                Timeout::After(Duration::from_secs(3)),
            )
            .unwrap()
            .wait(),
        )
        .await
        .unwrap();
        match input {
            "heap" => assert!(
                matches!(done.result,Err(Error::Exit(s))if s.code().and_then(ae_worker_heap::Exit::from_code)==Some(ae_worker_heap::Exit::NativeHeap))
            ),
            "service_heap" => assert!(
                matches!(done.result,Err(Error::Exit(s))if s.code().and_then(ae_worker_heap::Exit::from_code)==Some(ae_worker_heap::Exit::ServiceHeap))
            ),
            "exit" => assert!(matches!(done.result,Err(Error::Exit(s))if s.code()==Some(17))),
            "oversize" | "many" => assert!(matches!(done.result, Err(Error::Capacity))),
            "trailing" | "wrong_id" => assert!(
                matches!(done.result, Err(Error::Protocol)),
                "{input}: {:?}; {:?}",
                done.result,
                done.observation
            ),
            "partial" => assert!(matches!(
                done.result,
                Err(Error::Io(io::ErrorKind::UnexpectedEof))
            )),
            "linger" => assert!(matches!(done.result, Err(Error::DrainTimeout))),
            "echo" => assert!(matches!(
                done.result,
                Err(Error::Broker("captured storage failure"))
            )),
            _ => unreachable!(),
        }
        assert!(done.observation.is_some());
        let recovered = within(
            submit(
                &client,
                config(),
                b"echo".to_vec(),
                echo(),
                Timeout::After(Duration::from_secs(3)),
            )
            .unwrap()
            .wait(),
        )
        .await
        .unwrap();
        assert_eq!(recovered.result.unwrap(), b"captured-state");
    }
    within(owner.shutdown()).await;
}
struct Blocking {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    drops: Arc<AtomicUsize>,
    panic: bool,
}
impl Drop for Blocking {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Broker for Blocking {
    type Error = ();
    fn read(&mut self, _: &[u8]) -> Result<Vec<u8>, ()> {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(!self.panic, "bounded broker panic");
        Ok(vec![])
    }
}
#[tokio::test]
async fn timeout_drop_and_shutdown_reap_child_before_blocked_broker_and_credits_release() {
    for mode in 0..3 {
        let mut c = config();
        let (reaped, dead) = mpsc::channel();
        c.reaped = Some(reaped);
        let (client, owner) = worker(&c, 7);
        let (entered, ready) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let timeout = if mode == 0 {
            Timeout::After(Duration::from_millis(100))
        } else {
            Timeout::Unbounded
        };
        let ticket = submit(
            &client,
            c,
            b"blocked".to_vec(),
            Blocking {
                entered,
                release: blocked,
                drops: drops.clone(),
                panic: false,
            },
            timeout,
        )
        .unwrap();
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        if mode == 0 {
            assert!(matches!(
                within(ticket.wait()).await,
                Err(dry_run::Error::Timeout)
            ))
        } else if mode == 1 {
            drop(ticket)
        } else {
            owner.stop();
            drop(ticket)
        }
        dead.recv_timeout(Duration::from_secs(5))
            .expect("actual child was reaped while broker still held snapshot");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(client.available(), (0, 0));
        release.send(()).unwrap();
        within(owner.shutdown()).await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn panic_spawn_failure_and_pre_admission_limits_do_not_orphan_processes() {
    let mut c = config();
    let (reaped, dead) = mpsc::channel();
    c.reaped = Some(reaped);
    let (client, owner) = worker(&c, 5);
    let (entered, ready) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let ticket = submit(
        &client,
        c.clone(),
        b"panic".to_vec(),
        Blocking {
            entered,
            release: blocked,
            drops: drops.clone(),
            panic: true,
        },
        Timeout::Unbounded,
    )
    .unwrap();
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        within(ticket.wait()).await,
        Err(dry_run::Error::Panicked)
    ));
    dead.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    c.executable = c.directory.join("missing-ae-worker-11288");
    let done = within(
        submit(&client, c, b"echo".to_vec(), echo(), Timeout::Unbounded)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert!(matches!(
        done.result,
        Err(Error::Io(io::ErrorKind::NotFound))
    ));
    assert!(done.observation.is_none());
    let mut invalid = config();
    invalid.frame_bytes = usize::MAX;
    assert!(matches!(
        submit(&client, invalid, vec![], echo(), Timeout::Unbounded),
        Err(dry_run::Error::Configuration)
    ));
    within(owner.shutdown()).await;
}

#[test]
#[ignore = "dedicated non-reading child, called by backpressure test"]
fn unresponsive_child_entry() {
    std::thread::sleep(Duration::from_secs(5));
}
#[tokio::test]
async fn cancellation_reaps_child_while_input_pipe_is_backpressured() {
    let mut c = config();
    c.arguments[1] = "dry_run::process::tests::unresponsive_child_entry".into();
    let (reaped, dead) = mpsc::channel();
    c.reaped = Some(reaped);
    let (client, owner) = worker(&c, c.input_bytes);
    let ticket = submit(
        &client,
        c.clone(),
        vec![0; c.input_bytes],
        echo(),
        Timeout::After(Duration::from_millis(100)),
    )
    .unwrap();
    assert!(matches!(
        within(ticket.wait()).await,
        Err(dry_run::Error::Timeout)
    ));
    dead.recv_timeout(Duration::from_secs(5))
        .expect("non-reading child reaped");
    within(owner.shutdown()).await;
    assert_eq!(
        client.available(),
        (1, c.reservation(c.input_bytes).unwrap())
    );
}
#[tokio::test]
async fn request_reply_and_result_limits_stop_transport_and_recover() {
    let c = config();
    let (client, owner) = worker(&c, 4);
    for (reads, output, calls) in [(12, 100, 0), (25, 100, 1), (26, 12, 1)] {
        let mut limited = c.clone();
        limited.read_bytes = reads;
        limited.output_bytes = output;
        let done = within(
            submit(
                &client,
                limited,
                b"echo".to_vec(),
                echo(),
                Timeout::Unbounded,
            )
            .unwrap()
            .wait(),
        )
        .await
        .unwrap();
        assert!(matches!(done.result, Err(Error::Capacity)));
        assert_eq!(done.broker.calls, calls);
        assert!(done.observation.is_some());
    }
    struct Oversize;
    impl Broker for Oversize {
        type Error = ();
        fn read(&mut self, _: &[u8]) -> Result<Vec<u8>, ()> {
            Ok(vec![0; (1 << 20) + 1])
        }
    }
    let done = within(
        submit(
            &client,
            c.clone(),
            b"echo".to_vec(),
            Oversize,
            Timeout::Unbounded,
        )
        .unwrap()
        .wait(),
    )
    .await
    .unwrap();
    assert!(matches!(done.result, Err(Error::Capacity)));
    assert!(done.observation.is_some());
    let done = within(
        submit(&client, c, b"echo".to_vec(), echo(), Timeout::Unbounded)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert_eq!(done.result.unwrap(), b"captured-state");
    within(owner.shutdown()).await;
}

fn incoming(kind: u8, id: u64, data: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(
        &wire::Header {
            kind,
            id,
            bytes: data.len(),
        }
        .encode(1000)
        .unwrap(),
    );
    out.extend_from_slice(data);
}
fn boot() -> Vec<u8> {
    wire::bootstrap(
        Limit {
            bytes: 1 << 20,
            source: Source::Service,
        },
        1000,
    )
    .unwrap()
    .to_vec()
}
#[test]
fn worker_session_rejects_invalid_bootstrap_sequences_and_reply_headers() {
    // Independent unused guards do not arm this parent process's allocator.
    for offset in [0, 12] {
        let mut bytes = boot();
        bytes[offset] = 255;
        assert!(matches!(
            wire::Session::start(
                std::io::Cursor::new(bytes),
                vec![],
                &ae_worker_heap::Guard::new()
            ),
            Err(wire::Error::Protocol)
        ));
    }
    let mut bytes = boot();
    incoming(wire::INIT, 0, b"job", &mut bytes);
    incoming(wire::VALUE, 1, b"answer", &mut bytes);
    let mut session = wire::Session::start(
        std::io::Cursor::new(bytes),
        vec![],
        &ae_worker_heap::Guard::new(),
    )
    .unwrap();
    assert!(matches!(
        session.read(b"too early"),
        Err(wire::Error::Protocol)
    ));
    assert_eq!(session.initial(3).unwrap(), b"job");
    assert!(matches!(session.initial(3), Err(wire::Error::Protocol)));
    assert_eq!(session.read(b"q").unwrap(), b"answer");
    session.finish(b"ok").unwrap();
    assert!(matches!(
        session.finish(b"again"),
        Err(wire::Error::Protocol)
    ));
    assert!(matches!(session.read(b"late"), Err(wire::Error::Protocol)));
    for (kind, id, size) in [
        (wire::VALUE, 2, 0),
        (wire::RESULT, 1, 0),
        (wire::VALUE, 1, 1001),
    ] {
        let mut bytes = boot();
        incoming(wire::INIT, 0, b"", &mut bytes);
        let mut header = wire::Header { kind, id, bytes: 0 }.encode(1000).unwrap();
        header[9..].copy_from_slice(&(size as u32).to_be_bytes());
        bytes.extend_from_slice(&header);
        let mut session = wire::Session::start(
            std::io::Cursor::new(bytes),
            vec![],
            &ae_worker_heap::Guard::new(),
        )
        .unwrap();
        session.initial(0).unwrap();
        let error = session.read(b"q").unwrap_err();
        if size > 1000 {
            assert!(matches!(error, wire::Error::Capacity));
        } else {
            assert!(matches!(error, wire::Error::Protocol));
        }
    }
}
#[test]
fn trie_broker_and_worker_verify_hashes_missing_nodes_and_decode_bounds() {
    use ae_trie::{Error as TrieError, NodeReader};
    struct Node(Option<Vec<u8>>);
    impl NodeReader for Node {
        fn node(&self, _: &ae_types::Hash256) -> Result<Option<Vec<u8>>, TrieError> {
            Ok(self.0.clone())
        }
    }
    let data = b"bounded node";
    let hash = ae_crypto::blake2b_256(data);
    let mut query = vec![0];
    query.extend_from_slice(&hash.0);
    let mut store = trie::Store::new(Arc::new(Node(Some(data.to_vec()))), data.len()).unwrap();
    assert!(matches!(store.read(&query[..32]), Err(trie::Error::Query)));
    assert!(matches!(store.read(&[1]), Err(trie::Error::Query)));
    assert_eq!(&store.read(&query).unwrap()[1..], data);
    let mut small = trie::Store::new(store.source.clone(), data.len() - 1).unwrap();
    assert!(matches!(small.read(&query), Err(trie::Error::Capacity)));
    let mut corrupt = trie::Store::new(Arc::new(Node(Some(vec![0]))), 100).unwrap();
    assert!(
        matches!(corrupt.read(&query), Err(trie::Error::Trie(TrieError::HashMismatch(h))) if h == hash)
    );
    let mut missing = trie::Store::new(Arc::new(Node(None)), 100).unwrap();
    assert_eq!(missing.read(&query).unwrap(), [0]);
    let found = store.read(&query).unwrap();
    for (reply, maximum, expected) in [
        (found.clone(), 100, 0),
        (vec![0], 100, 1),
        (vec![1, 0], 100, 2),
        (found, 1, 3),
        (vec![0, 1], 100, 4),
    ] {
        let mut bytes = boot();
        incoming(wire::INIT, 0, &[], &mut bytes);
        incoming(wire::VALUE, 1, &reply, &mut bytes);
        let mut session = wire::Session::start(
            std::io::Cursor::new(bytes),
            vec![],
            &ae_worker_heap::Guard::new(),
        )
        .unwrap();
        session.initial(0).unwrap();
        let reader = trie::Reader::new(
            Arc::new(Mutex::new(session)),
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: maximum,
                ..Default::default()
            },
        );
        match expected {
            0 => assert_eq!(reader.node(&hash).unwrap().unwrap(), data),
            1 => assert!(reader.node(&hash).unwrap().is_none()),
            2 => assert!(matches!(reader.node(&hash), Err(TrieError::HashMismatch(h)) if h==hash)),
            3 => assert!(matches!(reader.node(&hash), Err(TrieError::ResourceLimit))),
            _ => assert!(matches!(reader.node(&hash), Err(TrieError::Backend(_)))),
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn protocol_abort_racing_natural_exit_reaps_each_owned_leader() {
    let c = config();
    let (client, owner) = worker(&c, 8);
    for _ in 0..32 {
        let done = within(
            submit(
                &client,
                c.clone(),
                b"trailing".to_vec(),
                echo(),
                Timeout::After(Duration::from_secs(3)),
            )
            .unwrap()
            .wait(),
        )
        .await
        .unwrap();
        assert!(
            matches!(done.result, Err(Error::Protocol)),
            "{:?}",
            done.result
        );
        let pid = done.observation.unwrap().pid;
        assert_eq!(
            rustix::process::waitid(
                rustix::process::WaitId::Pid(rustix::process::Pid::from_raw(pid as i32).unwrap()),
                rustix::process::WaitIdOptions::EXITED | rustix::process::WaitIdOptions::NOHANG,
            )
            .unwrap_err(),
            rustix::io::Errno::CHILD
        );
        assert_eq!(client.available(), (1, c.reservation(8).unwrap()));
    }
    within(owner.shutdown()).await;
}
