use super::*;
use ae_stratum::nonce::Kind;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
};
const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
struct Sources(u64);
impl services::Sources for Sources {
    fn now(&mut self) -> Result<num_bigint::BigInt, services::Fault> {
        Ok(1000.into())
    }
    fn draw(&mut self, max: u64) -> Result<u64, services::Fault> {
        Ok(self.0 & max)
    }
}
fn send(peer: &mut BufReader<TcpStream>, method: &str, params: Value) {
    let mut raw =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .unwrap();
    raw.push(b'\n');
    peer.get_mut().write_all(&raw).unwrap();
}
fn packet(peer: &mut BufReader<TcpStream>) -> Value {
    let mut bytes = vec![];
    assert!(peer.read_until(b'\n', &mut bytes).unwrap() > 0);
    serde_json::from_slice(&bytes).unwrap()
}
#[test]
fn automatic_work_crosses_real_tcp_admission_and_regenerates_on_the_committed_top() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let header = first();
    let clock = Wall::new(header.time);
    let (release, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (inputs, snapshots) = configuration(&db, &importer, clock.clone());
    let expected = candidate(&header);
    let target = expected.work().unwrap().block_target;
    let (extra, miner) = Nonce(header.nonce).split(Kind::Extra, 4).unwrap();
    let config = runtime::Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        session: session::Config {
            port: 9999,
            extra_nonce_bytes: 4,
            message_timeout_ms: 10000,
            skip_blocks: 0,
            edge_bits: 29,
            max_jobs: 20,
            targets: session::Targets {
                initial: target.clone(),
                maximum: target,
                difference_threshold: 5.0,
                desired_solve_time: 30000.into(),
                maximum_solve_time: 60000.into(),
            },
        },
        services: Default::default(),
        limits: Default::default(),
    };
    let owner = Service::with_sources_mining(
        db.clone(),
        config,
        Default::default(),
        Default::default(),
        Default::default(),
        inputs,
        Sources(extra.value()),
    )
    .unwrap();
    let network = owner.client();
    let accounting = owner.accounting();
    let client = accounting.mining().unwrap();
    let socket = TcpStream::connect(network.address()).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut peer = BufReader::new(socket);
    send(
        &mut peer,
        "mining.subscribe",
        json!(["miner/1", null, null, 9999]),
    );
    send(
        &mut peer,
        "mining.authorize",
        json!([format!("{ACCOUNT}.one"), null]),
    );
    for _ in 0..3 {
        packet(&mut peer);
    }
    release.send(()).unwrap();
    let notify = packet(&mut peer);
    assert_eq!(notify["method"], "mining.notify");
    let published = latest(&client, 1);
    assert_eq!(published.candidate, expected);
    assert_eq!(notify["params"][0], expected.work().unwrap().id().unwrap());
    assert_eq!(client.info().unwrap().last.as_ref(), Some(&published));
    assert_eq!(snapshots.live_snapshots(), 0);
    clock.set(header.time + 60_000);
    send(
        &mut peer,
        "mining.submit",
        json!([
            format!("{ACCOUNT}.one"),
            notify["params"][0],
            miner.to_hex(),
            header.seal.to_vec()
        ]),
    );
    let mut positive = false;
    let mut next = false;
    for _ in 0..3 {
        let p = packet(&mut peer);
        positive |= p["result"] == true;
        next |= p["method"] == "mining.notify";
        if positive && next {
            break;
        }
    }
    assert!(positive && next);
    let receipt = db
        .stratum(Default::default())
        .unwrap()
        .ingress(Default::default())
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert!(matches!(
        done(&client, receipt.id).state,
        journal::State::Done(journal::Outcome::Accepted(_))
    ));
    let second = latest(&client, 2);
    assert_eq!(
        second.candidate.parent(),
        Block::Key(header).hash().unwrap()
    );
    assert_eq!(second.candidate.header().common.height, 2);
    drop(peer);
    let report = owner.shutdown();
    assert!(report.transport.failure.is_none());
    assert!(report.accounting.failure.is_none());
    assert!(!report.accounting.production.unwrap().status.running);
    assert_eq!(snapshots.live_snapshots(), 0);
    stop(importer);
}
