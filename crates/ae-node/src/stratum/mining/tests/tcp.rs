use super::*;
use ae_stratum::{nonce::Kind, session};
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
    let mut bytes =
        serde_json::to_vec(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
            .unwrap();
    bytes.push(b'\n');
    peer.get_mut().write_all(&bytes).unwrap();
}
fn packet(peer: &mut BufReader<TcpStream>) -> Value {
    let mut bytes = vec![];
    assert!(peer.read_until(b'\n', &mut bytes).unwrap() > 0);
    serde_json::from_slice(&bytes).unwrap()
}
#[test]
fn saved_mainnet_proof_crosses_tcp_durable_pool_and_normal_chain_admission() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let header = first();
    let candidate = candidate(&header);
    let work = candidate.work().unwrap();
    let job = work.id().unwrap();
    let target = work.block_target.clone();
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
        inputs(&importer),
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
    assert!(publish(&client, candidate.clone()).published);
    network
        .services()
        .broadcast(session::Chain::Block(work))
        .unwrap();
    let notification = packet(&mut peer);
    assert_eq!(notification["method"], "mining.notify");
    assert_eq!(notification["params"][0], job);
    send(
        &mut peer,
        "mining.submit",
        json!([
            format!("{ACCOUNT}.one"),
            job,
            miner.to_hex(),
            header.seal.to_vec()
        ]),
    );
    assert_eq!(packet(&mut peer)["result"], true);
    let receipt = db
        .stratum(Default::default())
        .unwrap()
        .ingress(Default::default())
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(receipt.input.account, ACCOUNT);
    assert_eq!(receipt.input.hash, candidate.hash());
    let record = done(&client, receipt.id);
    assert_eq!(record.accounted_key, Some(receipt.share_key));
    assert!(matches!(
        record.state,
        journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Committed(_)))
    ));
    assert_eq!(
        db.view().unwrap().status().tip,
        Some(record.solution().unwrap().unwrap().hash())
    );
    drop(peer);
    let report = owner.shutdown();
    assert!(report.transport.failure.is_none());
    assert!(report.accounting.failure.is_none());
    assert!(report.accounting.admission.is_none());
    assert_eq!(accounting.retained(), (0, 0));
    stop(importer);
}
