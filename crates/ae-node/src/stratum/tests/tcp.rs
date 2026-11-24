use super::*;
use ae_stratum::{
    job,
    nonce::{Kind, Nonce},
    session,
};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};
const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
struct Sources(u64);
impl services::Sources for Sources {
    fn now(&mut self) -> Result<BigInt, services::Fault> {
        Ok(1000.into())
    }
    fn draw(&mut self, max: u64) -> Result<u64, services::Fault> {
        Ok(self.0 & max)
    }
}
fn request(method: &str, params: Value) -> Vec<u8> {
    let mut raw =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .unwrap();
    raw.push(b'\n');
    raw
}
fn packet(peer: &mut BufReader<TcpStream>) -> Value {
    let mut raw = vec![];
    assert!(peer.read_until(b'\n', &mut raw).unwrap() > 0);
    serde_json::from_slice(&raw).unwrap()
}
#[test]
fn twelve_native_proof_cases_cross_tcp_and_durable_accounting_before_positive_response() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_numeric_shares.json"
    ))
    .unwrap();
    let rows: Vec<_> = fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| {
            r["variant"] == "normal" && (r["mode"] == "integer" || r["mode"] == "first_float")
        })
        .collect();
    assert_eq!(rows.len(), 12);
    for row in rows {
        let t = Temp::new();
        let db = open(&t);
        let target: BigInt = row["target"].as_str().unwrap().parse().unwrap();
        let work = job::Work::new(
            row["hash"].as_str().unwrap().into(),
            row["version"].as_u64().unwrap() as u32,
            target.clone(),
        )
        .unwrap();
        let job = work.id().unwrap();
        let (extra, miner) = Nonce(row["nonce"].as_u64().unwrap())
            .split(Kind::Extra, 4)
            .unwrap();
        let cfg = runtime::Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            session: session::Config {
                port: 9999,
                extra_nonce_bytes: 4,
                message_timeout_ms: 5000,
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
        let owner = Service::with_sources(
            db.clone(),
            cfg,
            Default::default(),
            Default::default(),
            Default::default(),
            Sources(extra.value()),
        )
        .unwrap();
        let client = owner.client();
        let accounting = owner.accounting();
        let socket = TcpStream::connect(client.address()).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut peer = BufReader::new(socket);
        peer.get_mut()
            .write_all(
                &[
                    request("mining.subscribe", json!(["miner/1", null, null, 9999])),
                    request("mining.authorize", json!([format!("{ACCOUNT}.one"), null])),
                ]
                .concat(),
            )
            .unwrap();
        for _ in 0..3 {
            packet(&mut peer);
        }
        client
            .services()
            .broadcast(session::Chain::Block(work))
            .unwrap();
        let notify = packet(&mut peer);
        assert_eq!(notify["method"], "mining.notify");
        assert_eq!(notify["params"][0], job);
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
        let submit = request(
            "mining.submit",
            json!([format!("{ACCOUNT}.one"), job, miner.to_hex(), proof]),
        );
        peer.get_mut().write_all(&submit).unwrap();
        let receipt = if row["mode"] == "integer" {
            assert_eq!(packet(&mut peer)["result"], true);
            let stored = db.stratum(Default::default()).unwrap();
            let ingress = stored.ingress(Default::default()).unwrap();
            let receipt = ingress.last().unwrap().clone();
            assert_eq!(
                receipt.id,
                Id {
                    epoch: accounting.epoch(),
                    sequence: 1
                }
            );
            assert_eq!(receipt.input.account, ACCOUNT);
            let solution = receipt.input.solution.as_ref().unwrap();
            assert_eq!(solution.nonce.0, row["nonce"].as_u64().unwrap());
            for (n, edge) in solution
                .proof
                .iter()
                .zip(row["pow"]["list"].as_array().unwrap())
            {
                assert_eq!(
                    *n,
                    edge["integer"].as_str().unwrap().parse::<u64>().unwrap()
                );
            }
            assert_eq!(stored.info().counts, [1, 1, 0, 0, 0, 0]);
            assert_eq!(
                ingress.pending(None, 10).unwrap().0.as_slice(),
                std::slice::from_ref(&receipt)
            );
            peer.get_mut().write_all(&submit).unwrap();
            let duplicate = packet(&mut peer);
            assert!(!duplicate["error"].is_null());
            assert_eq!(
                db.stratum(Default::default())
                    .unwrap()
                    .ingress(Default::default())
                    .unwrap()
                    .last(),
                Some(&receipt)
            );
            assert!(client.next_output().is_none());
            Some(receipt)
        } else {
            assert_eq!(peer.read(&mut [0u8; 1]).unwrap(), 0);
            assert_eq!(
                db.stratum(Default::default()).unwrap().info().counts,
                [0; 6]
            );
            assert!(
                db.stratum(Default::default())
                    .unwrap()
                    .ingress(Default::default())
                    .unwrap()
                    .last()
                    .is_none()
            );
            None
        };
        drop(peer);
        let report = owner.shutdown();
        assert!(report.accounting.failure.is_none());
        assert!(report.accounting.pending.is_empty());
        assert_eq!(report.accounting.last, receipt);
        assert!(accounting.is_closed());
        assert_eq!(accounting.retained(), (0, 0));
        drop(report);
        drop(db);
        let db = open(&t);
        let ingress = db
            .stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap();
        assert_eq!(ingress.last(), receipt.as_ref());
        assert_eq!(
            ingress.pending(None, 10).unwrap().0,
            receipt.into_iter().collect::<Vec<_>>()
        );
    }
}
