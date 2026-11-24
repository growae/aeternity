use super::*;
use crate::{
    nonce::{Kind, Nonce},
    session::{Phase, Targets, Update},
    target,
};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{Shutdown, TcpStream},
    time::Instant,
};
const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
struct Sources {
    nonce: u64,
    fail: bool,
}
impl services::Sources for Sources {
    fn now(&mut self) -> Result<BigInt, services::Fault> {
        Ok(1000.into())
    }
    fn draw(&mut self, maximum: u64) -> Result<u64, services::Fault> {
        if self.fail {
            return Err(services::Fault::Entropy);
        }
        let n = self.nonce & maximum;
        self.nonce += 1;
        Ok(n)
    }
}
pub(super) fn config() -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        session: session::Config {
            port: 9999,
            extra_nonce_bytes: 4,
            message_timeout_ms: 2000,
            skip_blocks: 2,
            edge_bits: 29,
            max_jobs: 20,
            targets: Targets {
                initial: target::maximum(),
                maximum: target::maximum(),
                difference_threshold: 5.0,
                desired_solve_time: 30000.into(),
                maximum_solve_time: 60000.into(),
            },
        },
        services: services::Config {
            max_workers: 2,
            ..Default::default()
        },
        limits: Limits::default(),
    }
}
pub(super) fn owner(config: Config) -> Owner {
    Owner::with_sources(
        config,
        Sources {
            nonce: 541580363,
            fail: false,
        },
    )
    .unwrap()
}
fn request(method: &str, params: Value) -> Vec<u8> {
    let mut bytes =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .unwrap();
    bytes.push(b'\n');
    bytes
}
pub(super) fn configure() -> Vec<u8> {
    request("mining.configure", json!([]))
}
pub(super) fn subscribe() -> Vec<u8> {
    request("mining.subscribe", json!(["miner/1", null, null, 9999]))
}
pub(super) fn authorize(worker: &str) -> Vec<u8> {
    request(
        "mining.authorize",
        json!([format!("{ACCOUNT}.{worker}"), null]),
    )
}
pub(super) fn native(name: &str) -> Vec<Vec<u8>> {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_stratum_tcp.json")).unwrap();
    rows.iter().find(|r| r["name"] == name).unwrap()["packets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| hex::decode(p.as_str().unwrap()).unwrap())
        .collect()
}
struct Peer(BufReader<TcpStream>);
impl Peer {
    fn new(client: &Client) -> Self {
        let stream = TcpStream::connect(client.address()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        Self(BufReader::new(stream))
    }
    fn send(&mut self, bytes: &[u8]) {
        self.0.get_mut().write_all(bytes).unwrap();
    }
    fn packets(&mut self, count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|_| {
                let mut p = Vec::new();
                assert!(self.0.read_until(b'\n', &mut p).unwrap() > 0);
                assert_eq!(p.last(), Some(&b'\n'));
                p
            })
            .collect()
    }
    fn eof(&mut self) {
        assert_eq!(self.0.read(&mut [0; 1]).unwrap(), 0);
    }
    fn handshake(&mut self) -> Vec<Vec<u8>> {
        self.send(&[subscribe(), authorize("one")].concat());
        self.packets(3)
    }
}
fn until(mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    while !ready() {
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "condition timeout"
        );
        thread::sleep(Duration::from_millis(2));
    }
}
fn connection(client: &Client) -> Connection {
    until(|| client.connections().len() == 1);
    client.connections().pop().unwrap()
}
fn output(client: &Client) -> Lease<Output> {
    let mut out = None;
    until(|| {
        out = client.next_output();
        out.is_some()
    });
    out.unwrap()
}
fn clean(client: &Client) {
    until(|| {
        let i = client.services().info();
        i.connections == 0 && i.accounts == 0 && i.nonces == 0
    });
}
#[test]
fn real_tcp_fragmentation_pipeline_and_native_packet_order() {
    let owner = owner(config());
    let client = owner.client();
    let mut peer = Peer::new(&client);
    let cfg = configure();
    peer.send(&cfg[..cfg.len() - 2]);
    peer.0
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    assert!(matches!(
        peer.0.read(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    peer.0
        .get_ref()
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    peer.send(
        &[
            cfg[cfg.len() - 2..].to_vec(),
            subscribe(),
            authorize("one"),
            authorize("two"),
        ]
        .concat(),
    );
    assert_eq!(peer.packets(5), native("fragmented_pipeline"));
    let handle = connection(&client);
    let state = handle.status().unwrap().blocking_recv().unwrap().unwrap();
    assert_eq!(state.session.phase(), Phase::Authorized);
    assert_eq!(state.peer, handle.peer());
    assert_eq!(client.services().info().accounts, 1);
    drop(peer);
    clean(&client);
    let exit = output(&client);
    assert!(matches!(
        &*exit,
        Output::Closed(Exit {
            reason: ExitReason::PeerClosed,
            ..
        })
    ));
    assert!(handle.is_closed());
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    assert_eq!(report.services.nonces, 0);
}
#[test]
fn malformed_line_receives_original_error_then_connection_continues() {
    let owner = owner(config());
    let client = owner.client();
    let mut peer = Peer::new(&client);
    peer.send(&[b"[\n".to_vec(), configure()].concat());
    assert_eq!(peer.packets(2), native("malformed_then_configure"));
    assert_eq!(
        connection(&client)
            .status()
            .unwrap()
            .blocking_recv()
            .unwrap()
            .unwrap()
            .session
            .phase(),
        Phase::Configured
    );
    drop(peer);
    clean(&client);
    owner.shutdown();
}
#[test]
fn actual_connected_configured_and_subscribed_timers_expire() {
    for (name, packet) in [
        ("connected_timeout", None),
        ("configured_timeout", Some(configure())),
        ("subscribed_timeout", Some(subscribe())),
    ] {
        let mut cfg = config();
        cfg.session.message_timeout_ms = 80;
        let owner = owner(cfg);
        let client = owner.client();
        let mut peer = Peer::new(&client);
        let start = Instant::now();
        let count = usize::from(packet.is_some());
        if let Some(packet) = packet {
            peer.send(&packet);
        }
        assert_eq!(peer.packets(count), native(name));
        peer.eof();
        assert!(start.elapsed() >= Duration::from_millis(40));
        clean(&client);
        assert!(matches!(
            &*output(&client),
            Output::Closed(Exit {
                reason: ExitReason::Session(session::StopReason::Timeout),
                ..
            })
        ));
        owner.shutdown();
    }
}
#[test]
fn authorization_cancels_real_deadline_and_accepts_later_workers() {
    let mut cfg = config();
    cfg.session.message_timeout_ms = 80;
    let owner = owner(cfg);
    let client = owner.client();
    let mut peer = Peer::new(&client);
    let mut packets = peer.handshake();
    thread::sleep(Duration::from_millis(180));
    let status = connection(&client)
        .status()
        .unwrap()
        .blocking_recv()
        .unwrap()
        .unwrap();
    assert_eq!(status.session.phase(), Phase::Authorized);
    assert!(status.session.timer().is_none());
    peer.send(&authorize("two"));
    packets.extend(peer.packets(1));
    assert_eq!(packets, native("authorized_timer_cancel"));
    drop(peer);
    clean(&client);
    owner.shutdown();
}
#[test]
fn administrative_updates_and_retained_status_snapshots_are_bounded() {
    let mut cfg = config();
    cfg.limits.snapshot_records = 1;
    let owner = owner(cfg);
    let client = owner.client();
    let mut peer = Peer::new(&client);
    assert_eq!(peer.handshake(), native("administrative_updates"));
    let handle = connection(&client);
    for update in [
        Update::MaximumTarget(5.into()),
        Update::DifferenceThreshold(3.0),
        Update::DesiredSolveTime(100.into()),
        Update::MaximumSolveTime(200.into()),
    ] {
        handle
            .update(update)
            .unwrap()
            .blocking_recv()
            .unwrap()
            .unwrap();
    }
    assert!(matches!(
        handle.update(Update::DifferenceThreshold(f64::NAN)),
        Err(Error::Session(_))
    ));
    let view = handle.status().unwrap().blocking_recv().unwrap().unwrap();
    let held = view.clone();
    let t = view.session.targets();
    assert_eq!(t.maximum, Some(5.into()));
    assert_eq!(t.difference_threshold, Some(3.0));
    assert_eq!(t.desired_solve_time, Some(100.into()));
    assert_eq!(t.maximum_solve_time, Some(200.into()));
    drop(view);
    assert_eq!(client.retained_snapshots().0, 1);
    assert!(matches!(
        handle.status().unwrap().blocking_recv().unwrap(),
        Err(Error::Busy)
    ));
    drop(held);
    assert_eq!(client.retained_snapshots(), (0, 0));
    drop(handle.status().unwrap().blocking_recv().unwrap().unwrap());
    handle.close();
    peer.eof();
    clean(&client);
    assert!(matches!(
        &*output(&client),
        Output::Closed(Exit {
            reason: ExitReason::Administrative,
            ..
        })
    ));
    owner.shutdown();
}
#[test]
fn owner_drop_joins_sockets_releases_leases_and_preserves_terminal_report() {
    let owner = owner(config());
    let client = owner.client();
    let mut peer = Peer::new(&client);
    peer.handshake();
    let handle = connection(&client);
    drop(owner);
    peer.eof();
    clean(&client);
    assert!(client.is_closed());
    assert!(handle.is_closed());
    let report = client.take_dropped_report().unwrap();
    assert!(report.failure.is_none());
    assert!(report.services.closed);
    assert_eq!(report.unpublished_exits.len(), 1);
    assert_eq!(report.unpublished_exits[0].reason, ExitReason::Shutdown);
    assert!(client.take_dropped_report().is_none());
}
#[test]
fn frame_and_partial_eof_limits_close_only_the_offending_connection() {
    for partial in [false, true] {
        let mut cfg = config();
        cfg.limits.frame_bytes = 32;
        cfg.limits.read_bytes = 7;
        let owner = owner(cfg);
        let client = owner.client();
        let mut peer = Peer::new(&client);
        if partial {
            peer.send(b"{unfinished");
            peer.0.get_ref().shutdown(Shutdown::Write).unwrap();
        } else {
            peer.send(&[b'x'; 33]);
        }
        peer.eof();
        clean(&client);
        let out = output(&client);
        let Output::Closed(e) = &*out else {
            panic!("exit");
        };
        assert_eq!(
            e.reason,
            if partial {
                ExitReason::IncompleteFrame
            } else {
                ExitReason::FrameLimit
            }
        );
        assert!(!client.is_closed());
        owner.shutdown();
    }
}
#[test]
fn service_fault_stops_listener_and_closes_owned_connections() {
    let owner = Owner::with_sources(
        config(),
        Sources {
            nonce: 0,
            fail: true,
        },
    )
    .unwrap();
    let client = owner.client();
    let mut peer = Peer::new(&client);
    peer.send(&subscribe());
    peer.eof();
    until(|| client.is_closed());
    let report = owner.shutdown();
    assert_eq!(report.services.failure, Some(services::Fault::Entropy));
    assert_eq!(report.services.connections, 0);
    assert_eq!(report.services.nonces, 0);
    assert!(TcpStream::connect(client.address()).is_err());
}
#[test]
fn current_configuration_changes_subsequent_events_without_replacing_live_timer() {
    let owner = owner(config());
    let client = owner.client();
    let mut peer = Peer::new(&client);
    peer.send(&configure());
    peer.packets(1);
    let mut cfg = config().session;
    cfg.extra_nonce_bytes = 3;
    cfg.message_timeout_ms = 50;
    client.configure(cfg).unwrap();
    peer.send(&subscribe());
    let packet: Value = serde_json::from_slice(&peer.packets(1)[0]).unwrap();
    assert_eq!(packet["result"][1].as_str().unwrap().len(), 6);
    peer.eof();
    clean(&client);
    owner.shutdown();
}
#[test]
fn consumer_held_output_applies_backpressure_to_connection_retirement() {
    let mut cfg = config();
    cfg.limits.output_records = 1;
    cfg.limits.connections = 1;
    let owner = owner(cfg);
    let client = owner.client();
    let mut a = Peer::new(&client);
    a.send(&subscribe());
    assert_eq!(a.packets(1), native("peer_close_after_subscription"));
    drop(a);
    clean(&client);
    let held = output(&client);
    let clone = held.clone();
    drop(held);
    assert_eq!(client.retained_output().0, 1);
    let mut b = Peer::new(&client);
    b.send(&configure());
    b.packets(1);
    drop(b);
    clean(&client);
    let mut c = Peer::new(&client);
    c.send(&configure());
    c.0.get_ref()
        .set_read_timeout(Some(Duration::from_millis(40)))
        .unwrap();
    assert!(c.0.read(&mut [0; 1]).is_err());
    drop(clone);
    let second = output(&client);
    drop(second);
    c.0.get_ref()
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    assert_eq!(c.packets(1), native("configured_timeout"));
    drop(c);
    clean(&client);
    owner.shutdown();
}
fn corpus() -> Vec<Value> {
    let c: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_numeric_shares.json"
    ))
    .unwrap();
    c["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| {
            r["variant"] == "normal" && (r["mode"] == "integer" || r["mode"] == "first_float")
        })
        .cloned()
        .collect()
}
fn proof_request(row: &Value, id: &str) -> Vec<u8> {
    let (_, miner) = Nonce(row["nonce"].as_u64().unwrap())
        .split(Kind::Extra, 4)
        .unwrap();
    let proof: Vec<Value> = row["pow"]["list"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| {
            if let Some(i) = n.get("integer") {
                serde_json::from_str(i.as_str().unwrap()).unwrap()
            } else {
                json!(f64::from_bits(
                    u64::from_str_radix(n["float"].as_str().unwrap(), 16).unwrap()
                ))
            }
        })
        .collect();
    request(
        "mining.submit",
        json!([format!("{ACCOUNT}.one"), id, miner.to_hex(), proof]),
    )
}
fn proof_setup(row: &Value, records: usize) -> (Owner, Client, Peer, String, crate::job::Work) {
    let mut cfg = config();
    cfg.session.skip_blocks = 0;
    cfg.limits.output_records = records;
    cfg.limits.operation_timeout = Duration::from_secs(2);
    cfg.session.targets.initial = row["target"].as_str().unwrap().parse().unwrap();
    cfg.session.targets.maximum = cfg.session.targets.initial.clone();
    let work = crate::job::Work::new(
        row["hash"].as_str().unwrap().into(),
        row["version"].as_u64().unwrap() as u32,
        cfg.session.targets.initial.clone(),
    )
    .unwrap();
    let id = work.id().unwrap();
    let (extra, _) = Nonce(row["nonce"].as_u64().unwrap())
        .split(Kind::Extra, 4)
        .unwrap();
    let owner = Owner::with_sources(
        cfg,
        Sources {
            nonce: extra.value(),
            fail: false,
        },
    )
    .unwrap();
    let client = owner.client();
    let mut peer = Peer::new(&client);
    peer.handshake();
    client
        .services()
        .broadcast(session::Chain::Block(work.clone()))
        .unwrap();
    let notify: Value = serde_json::from_slice(&peer.packets(1)[0]).unwrap();
    assert_eq!(notify["method"], "mining.notify");
    assert_eq!(notify["params"][0], id);
    (owner, client, peer, id, work)
}
#[test]
fn twelve_saved_proof_submissions_use_real_tcp_and_owned_services() {
    let rows = corpus();
    assert_eq!(rows.len(), 12);
    for row in rows {
        let (owner, client, mut peer, id, _) = proof_setup(&row, 8);
        peer.send(&proof_request(&row, &id));
        if row["mode"] == "integer" {
            let response: Value = serde_json::from_slice(&peer.packets(1)[0]).unwrap();
            assert_eq!(response["result"], true);
            let pool = output(&client);
            assert!(
                matches!(&*pool,Output::Pool { effects, .. } if matches!(effects.as_slice(),[share::Effect::SubmitShare { .. },share::Effect::SubmitSolution { .. }]))
            );
            assert!(client.next_output().is_none());
            drop(peer);
            clean(&client);
        } else {
            peer.eof();
            clean(&client);
            assert!(matches!(
                &*output(&client),
                Output::Closed(Exit {
                    reason: ExitReason::Session(session::StopReason::Work(
                        crate::Error::ProofArithmetic
                    )),
                    ..
                })
            ));
        }
        assert_eq!(owner.shutdown().services.nonces, 0);
    }
}
#[test]
fn blocked_pool_handoff_retains_both_effects_during_shutdown() {
    let row = corpus()
        .into_iter()
        .find(|r| r["mode"] == "integer")
        .unwrap();
    let (owner, client, mut peer, id, work) = proof_setup(&row, 1);
    peer.send(&proof_request(&row, &id));
    peer.packets(1);
    let held = output(&client);
    // Explicit second work descriptor reuses this saved proof for a queue
    // pressure test; it is not a new mined or admitted canonical block.
    let mut work = work;
    work.version += 1;
    let second_id = work.id().unwrap();
    client
        .services()
        .broadcast(session::Chain::Block(work))
        .unwrap();
    peer.packets(1);
    peer.send(&proof_request(&row, &second_id));
    until(|| client.pending_pool_batches() == 1);
    assert_eq!(client.retained_output().0, 1);
    let report = owner.shutdown();
    peer.eof();
    assert_eq!(report.unpublished_exits.len(), 1);
    let exit = &report.unpublished_exits[0];
    assert_eq!(exit.reason, ExitReason::Shutdown);
    assert_eq!(exit.pending_pool.len(), 2);
    assert!(exit.unsent_bytes > 0);
    assert_eq!(client.pending_pool_batches(), 0);
    assert!(matches!(&*held, Output::Pool { .. }));
    drop(held);
    assert_eq!(client.retained_output(), (0, 0));
}
#[test]
fn self_notifications_preserve_fifo_and_overflow_releases_service_lease() {
    let owner = services::Owner::with_sources(
        services::Config {
            notifications_per_connection: 2,
            ..Default::default()
        },
        Sources {
            nonce: 1,
            fail: false,
        },
    )
    .unwrap();
    let client = owner.client();
    let c = client.connect().unwrap();
    c.execute(session::Command {
        id: 1,
        operation: session::Operation::AllocateNonce { bytes: 4 },
    })
    .unwrap();
    c.execute(session::Command {
        id: 2,
        operation: session::Operation::Register {
            user: share::User {
                account: ACCOUNT.into(),
                worker: "one".into(),
            },
        },
    })
    .unwrap();
    let work = crate::job::Work::new("00".repeat(32), 1, 1.into()).unwrap();
    client
        .broadcast(session::Chain::Block(work.clone()))
        .unwrap();
    c.enqueue(session::Chain::SetTarget).unwrap();
    assert_eq!(
        *c.next_notification().unwrap().unwrap(),
        session::Chain::Block(work)
    );
    assert_eq!(
        *c.next_notification().unwrap().unwrap(),
        session::Chain::SetTarget
    );
    c.enqueue(session::Chain::SetTarget).unwrap();
    c.enqueue(session::Chain::SetTarget).unwrap();
    assert_eq!(
        c.enqueue(session::Chain::SetTarget),
        Err(services::Error::Limit)
    );
    assert!(c.is_closed());
    assert_eq!(client.info().nonces, 0);
    assert_eq!(client.info().accounts, 0);
}

#[test]
fn releasing_consumer_credit_resumes_pool_handoff_before_acknowledgement() {
    let row = corpus()
        .into_iter()
        .find(|r| r["mode"] == "integer")
        .unwrap();
    let (owner, client, mut peer, id, mut work) = proof_setup(&row, 1);
    peer.send(&proof_request(&row, &id));
    peer.packets(1);
    let held = output(&client);
    // Replay the saved proof under an explicit second descriptor only to
    // exercise service pressure; no canonical block admission is claimed.
    work.version += 1;
    let next = work.id().unwrap();
    client
        .services()
        .broadcast(session::Chain::Block(work))
        .unwrap();
    peer.packets(1);
    peer.send(&proof_request(&row, &next));
    until(|| client.pending_pool_batches() == 1);
    drop(held);
    let reply: Value = serde_json::from_slice(&peer.packets(1)[0]).unwrap();
    assert_eq!(reply["result"], true);
    let out = output(&client);
    assert!(matches!(&*out, Output::Pool { effects, .. } if effects.len() == 2));
    assert_eq!(client.pending_pool_batches(), 0);
    assert!(client.next_output().is_none());
    drop(out);
    drop(peer);
    clean(&client);
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    assert!(
        report
            .unpublished_exits
            .iter()
            .all(|e| e.pending_pool.is_empty())
    );
}
