use super::*;
use ae_chain::instrumentation::{Event, Gauge, Sink, Span, Stage, Value};
use std::sync::Mutex;

#[derive(Default)]
struct Observed(Mutex<Vec<Event>>);
impl Sink for Observed {
    fn event(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }
    fn start(&self, _: Stage) -> Option<Box<dyn Span>> {
        None
    }
}
impl Observed {
    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}
fn values(events: &[Event]) -> Vec<u64> {
    assert_eq!(events.len(), 7);
    events
        .iter()
        .zip([
            Gauge::PeersCount,
            Gauge::PeersInbound,
            Gauge::PeersOutbound,
            Gauge::PeersVerified,
            Gauge::PeersUnverified,
            Gauge::PeersErrored,
            Gauge::PeersBlocked,
        ])
        .map(|(e, expected)| match e {
            Event::Gauge(g, Value::Integer(n)) if *g == expected => *n,
            _ => panic!("{e:?}"),
        })
        .collect()
}
#[test]
fn peer_metrics_wait_for_durable_ack_and_recovery_publishes_once() {
    for fault in [Fault::Before, Fault::Absent, Fault::After] {
        let temp = Temp::new();
        let database = db(&temp);
        let sink = Arc::new(Observed::default());
        let _binding = database.instrumentation().bind(sink.clone()).unwrap();
        let mut core = faulty(database.clone());
        core.instrumentation = database.instrumentation().clone();
        assert!(sink.take().is_empty());
        core.store.fault = fault;
        assert!(core.apply(1, Command::Add(peer(1, false))).is_err());
        assert!(sink.take().is_empty());
        match fault {
            Fault::After => assert!(matches!(core.recover().unwrap(), Recovery::Committed(_))),
            Fault::Absent | Fault::Before => {
                assert_eq!(
                    core.recover().unwrap(),
                    if matches!(fault, Fault::Absent) {
                        Recovery::NotCommitted
                    } else {
                        Recovery::Unchanged
                    }
                );
                assert!(sink.take().is_empty());
                core.apply(2, Command::Add(peer(1, false))).unwrap();
            }
            Fault::None => unreachable!(),
        }
        assert_eq!(values(&sink.take()), [0, 0, 0, 0, 1, 0, 0]);
        drain(&mut core);
        assert_eq!(core.recover().unwrap(), Recovery::Unchanged);
        assert!(sink.take().is_empty());
        core.apply(3, Command::IsBlocked(key(99))).unwrap();
        assert_eq!(values(&sink.take()), [0, 0, 0, 0, 1, 0, 0]);
        // A rejected receipt cannot publish even if its backend wrote data.
        core.store.wrong_receipt = Some(core.version);
        assert!(matches!(
            core.apply(4, Command::Add(peer(2, false))),
            Err(Error::Inconsistent)
        ));
        assert!(sink.take().is_empty());
    }
}
#[test]
fn peer_metrics_real_startup_emits_each_block_and_batch_unblock_has_no_peer_events() {
    let temp = Temp::new();
    let database = db(&temp);
    let sink = Arc::new(Observed::default());
    let _binding = database.instrumentation().bind(sink.clone()).unwrap();
    let mut owner = Service::start(database, key(128), config()).unwrap();
    assert!(sink.take().is_empty());
    let blocked = [1, 2]
        .into_iter()
        .map(|id| {
            ae_p2p::address::encode(key(id), b"127.0.0.1", &3015.into(), Default::default())
                .unwrap()
        })
        .collect();
    owner
        .configure_startup(
            0,
            crate::registry::startup::Configuration {
                include_default_peers: false,
                blocked_peers: blocked,
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
    let emitted = sink.take();
    assert_eq!(emitted.len(), 14);
    assert_eq!(values(&emitted[..7]), [0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(values(&emitted[7..]), [0, 0, 0, 0, 0, 0, 2]);
    drain(&mut owner.core);
    owner.apply(1, Command::UnblockAll).unwrap();
    assert_eq!(values(&sink.take()), [0; 7]);
    assert!(drain(&mut owner.core).is_empty());
    assert_eq!(owner.policy().counts().blocked, 0);
    owner.apply(2, Command::UnblockAll).unwrap();
    assert_eq!(values(&sink.take()), [0; 7]);
}
#[test]
fn peer_metrics_worker_queries_are_silent_and_all_peers_keeps_response_budget() {
    let temp = Temp::new();
    let database = db(&temp);
    let sink = Arc::new(Observed::default());
    let _binding = database.instrumentation().bind(sink.clone()).unwrap();
    let worker = worker::Worker::start(
        Service::start(database, key(128), config()).unwrap(),
        worker::Settings {
            maximum_sample: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let client = worker.client();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(client.apply(1, Command::Add(peer(1, false))).unwrap())
        .unwrap();
    assert_eq!(values(&sink.take()), [0, 0, 0, 0, 1, 0, 0]);
    runtime.block_on(client.pool_counts().unwrap()).unwrap();
    runtime.block_on(client.inspect().unwrap()).unwrap();
    runtime.block_on(client.metrics().unwrap()).unwrap();
    assert!(sink.take().is_empty());
    assert!(matches!(
        client.apply(
            2,
            Command::AllPeers {
                target: discovery::Target::Both,
                maximum: 2
            }
        ),
        Err(worker::Error::RequestTooLarge)
    ));
    assert!(
        runtime
            .block_on(
                client
                    .apply(
                        2,
                        Command::AllPeers {
                            target: discovery::Target::Both,
                            maximum: 0
                        }
                    )
                    .unwrap()
            )
            .is_err()
    );
    assert!(sink.take().is_empty());
    assert_eq!(
        runtime
            .block_on(
                client
                    .apply(
                        3,
                        Command::AllPeers {
                            target: discovery::Target::Both,
                            maximum: 1
                        }
                    )
                    .unwrap()
            )
            .unwrap(),
        Outcome::Peers(vec![policy::Info::from(&peer(1, false))])
    );
    assert_eq!(values(&sink.take()), [0, 0, 0, 0, 1, 0, 0]);
    worker.shutdown().unwrap();
    assert!(sink.take().is_empty());
}
