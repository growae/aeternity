use super::*;
use ae_codec::{
    api,
    headers::{CommonHeader, Header, KeyHeader},
};
use ae_consensus::cuckoo::{self, solver};
use ae_stratum::nonce::{Kind, Part};
use ae_types::{Hash256, PeerPublicKey};
#[path = "stratum/payout.rs"]
mod payout;

fn bytes(value: &Value) -> Vec<u8> {
    api::decode(value.as_str().unwrap()).unwrap().1
}
fn header(value: &Value) -> KeyHeader {
    for field in [
        "info",
        "prev_hash",
        "prev_key_hash",
        "state_hash",
        "miner",
        "beneficiary",
    ] {
        assert!(value[field].is_string(), "missing {field}: {value}");
    }
    let info = bytes(&value["info"]);
    KeyHeader {
        common: CommonHeader {
            version: value["version"].as_u64().unwrap() as u32,
            height: value["height"].as_u64().unwrap(),
            previous_hash: bytes(&value["prev_hash"]).try_into().unwrap(),
            previous_key_hash: bytes(&value["prev_key_hash"]).try_into().unwrap(),
            state_hash: bytes(&value["state_hash"]).try_into().unwrap(),
        },
        miner: bytes(&value["miner"]).try_into().unwrap(),
        beneficiary: bytes(&value["beneficiary"]).try_into().unwrap(),
        target: value["target"].as_u64().unwrap() as u32,
        nonce: value["nonce"].as_u64().unwrap_or(0),
        seal: value["pow"].as_array().map_or([0; 42], |p| {
            p.iter()
                .map(|n| n.as_u64().unwrap() as u32)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        }),
        time: value["time"].as_u64().unwrap(),
        info: (!info.is_empty()).then(|| info.try_into().unwrap()),
        hole: false,
        end_of_epoch: false,
    }
}
fn solve(hash: Hash256, target: u32, extra: Part) -> (Part, [u64; 42]) {
    let end = Instant::now() + Duration::from_secs(20);
    for n in 0..4096 {
        let miner = Part::new(Kind::Miner, n, extra.complement()).unwrap();
        let nonce = extra.merge(miner).unwrap().0;
        let result = solver::solve(
            solver::Input {
                hash,
                nonce,
                target: ae_consensus::target::Target(target),
                edge_bits: 15,
            },
            solver::Limits {
                memory_bytes: 16 * 1024 * 1024,
                work: 200_000_000,
                ..Default::default()
            },
            |_| Instant::now() >= end,
        )
        .unwrap();
        if let Some(proof) = result.solution {
            return (miner, proof);
        }
    }
    panic!("bounded local Cuckoo search exhausted")
}
fn packet(peer: &mut BufReader<TcpStream>) -> Value {
    let mut text = String::new();
    assert!(peer.read_line(&mut text).unwrap() > 0);
    serde_json::from_str(&text).unwrap()
}
fn reply(
    peer: &mut BufReader<TcpStream>,
    id: u64,
    jobs: &mut std::collections::VecDeque<Value>,
) -> Value {
    loop {
        let value = packet(peer);
        if value["id"] == id {
            return value;
        }
        if value["method"] == "mining.notify" {
            jobs.push_back(value);
        }
    }
}
fn send(peer: &mut BufReader<TcpStream>, id: u64, method: &str, params: Value) {
    writeln!(
        peer.get_mut(),
        "{}",
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
    )
    .unwrap();
}
fn height(address: SocketAddr) -> u64 {
    let value = read(address, "/v3/key-blocks/current/height");
    value["height"].as_u64().unwrap()
}
fn read(address: SocketAddr, path: &str) -> Value {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let (code, value) = request(
            address,
            &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
        );
        if code == 503 && value["reason"] == "Temporary overload" {
            assert!(Instant::now() < end, "read remained saturated");
            thread::sleep(Duration::from_millis(5));
            continue;
        }
        assert_eq!(code, 200, "{value}");
        return value;
    }
}
fn at_height(address: SocketAddr, expected: u64) {
    let end = Instant::now() + Duration::from_secs(10);
    while height(address) < expected {
        assert!(Instant::now() < end, "block admission did not finish");
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(height(address), expected);
}
struct Peer {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<thread::JoinHandle<()>>,
}
impl Peer {
    // A real encrypted peer supplies the first top change, as the native
    // Stratum integration suite does. It never writes the node's database.
    fn gossip(ready: &Value, remote: PeerPublicKey, key: KeyHeader) -> Self {
        use ae_p2p::{
            message::{Kind, Message, Value as Field},
            noise, ping, runtime as net,
        };
        let address: SocketAddr = ready["sync"].as_str().unwrap().parse().unwrap();
        let genesis = Hash256(bytes(&ready["genesis"]).try_into().unwrap());
        let network = ready["network_id"].as_str().unwrap().as_bytes().to_vec();
        let (stop, wait) = tokio::sync::oneshot::channel();
        let (sent, completed) = mpsc::channel();
        let task = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
                    let identity = noise::Identity::from_secret([81; 32]);
                    let network = noise::Network::new(1, genesis, &network).unwrap();
                    let (client, _events, driver) = net::establish(
                        socket,
                        &identity,
                        &network,
                        noise::Role::Initiator(remote),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                    let task = tokio::spawn(driver.run());
                    let ping = ping::encode(
                        &ping::Local {
                            genesis,
                            best: genesis,
                            height: 0,
                            difficulty: 1u64.into(),
                            temporary: false,
                        },
                        vec![],
                        &[],
                        ping::Encoding {
                            version: 2,
                            minimum_version: 1,
                            port: 3015,
                            share: 0u64.into(),
                            limits: Default::default(),
                        },
                    )
                    .unwrap();
                    assert!(matches!(
                        client.request(&ping, None).unwrap().await.unwrap(),
                        net::Reply::Response(_)
                    ));
                    let block = Message::new(
                        Kind::KeyBlock,
                        1,
                        [(
                            "key_block".into(),
                            Field::Binary(Header::Key(key).encode().unwrap()),
                        )]
                        .into(),
                        Default::default(),
                    )
                    .unwrap();
                    assert!(matches!(
                        client.send(&block).unwrap().await.unwrap(),
                        net::Reply::Sent
                    ));
                    sent.send(()).unwrap();
                    let _ = wait.await;
                    client.shutdown();
                    task.await.unwrap();
                });
        });
        let owner = Self {
            stop: Some(stop),
            task: Some(task),
        };
        completed.recv_timeout(Duration::from_secs(20)).unwrap();
        owner
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.take().unwrap().join().unwrap();
    }
}

#[test]
fn foreground_small_graph_peer_and_stratum_solutions_use_real_admission_and_survive_reopen() {
    let lab = Lab::new("pow_cuckoo", true);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut config = lab.config();
    config["mining"]["cuckoo"]["edge_bits"] = json!(15);
    config["stratum"] = json!({"enabled":true,"connection":{"port":port,"max_connections":4,"num_acceptors":2},"session":{"edge_bits":15,"skip_num_blocks":0,"max_solve_time":120},"reward":{"keys":{"dir":"pool-keys"},"beneficiaries":[]}});
    lab.write("node.json", &config);
    // Load/create through the ordinary key owner, then release its file lease.
    // The foreground process reloads this peer pair and creates fresh miner keys.
    let keys = ae_sync::keys::Worker::spawn(ae_sync::keys::Config::new(
        lab.0.join("keys"),
        b"local-test".to_vec(),
    ))
    .unwrap();
    let remote = keys.info().keys.peer;
    keys.shutdown().unwrap();
    let mut process = Process::spawn(&lab, lab.command(), "stratum-solved");
    let ready = process.event("ready");
    let address = addresses(&ready)[0];
    let transport: SocketAddr = ready["stratum"].as_str().unwrap().parse().unwrap();
    let socket = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, transport.port())).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut stream = BufReader::new(socket);
    let mut jobs = std::collections::VecDeque::new();
    send(
        &mut stream,
        1,
        "mining.subscribe",
        json!(["local-test/1", null, null, port]),
    );
    let subscribed = packet(&mut stream);
    assert_eq!(subscribed["id"], 1);
    assert!(subscribed["error"].is_null(), "{subscribed}");
    let extra = Part::from_hex(Kind::Extra, subscribed["result"][1].as_str().unwrap(), 4).unwrap();
    let account = api::encode(api::ApiType::AccountPubkey, &[77; 32]);
    send(
        &mut stream,
        2,
        "mining.authorize",
        json!([format!("{account}.one"), null]),
    );
    let authorized = reply(&mut stream, 2, &mut jobs);
    assert_eq!(authorized["result"], true, "{authorized}");
    let pending = read(address, "/v3/key-blocks/pending");
    let mut first = header(&pending);
    let (miner, proof) = solve(
        ae_crypto::blake2b_256(&cuckoo::sealing_bytes(&first).unwrap()),
        first.target,
        Part::new(Kind::Extra, 0, 4).unwrap(),
    );
    first.nonce = miner.value();
    first.seal = proof.map(|e| u32::try_from(e).unwrap());
    assert!(
        cuckoo::verify_key_seal_with_edge_bits(&first, cuckoo::EdgeBits::new(15).unwrap()).unwrap()
    );
    assert!(!cuckoo::verify_key_seal(&first).unwrap());
    let peer = Peer::gossip(&ready, remote, first);
    at_height(address, 1);
    let mut submitted = false;
    for attempt in 0..4 {
        let work = jobs.pop_front().unwrap_or_else(|| {
            loop {
                let p = packet(&mut stream);
                if p["method"] == "mining.notify" {
                    break p;
                }
            }
        });
        // A successor notification also proves the preceding solution has
        // completed admission. Do not accidentally mine a third key block.
        if height(address) == 2 {
            submitted = true;
            break;
        }
        let hash = Hash256(
            hex::decode(work["params"][2].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let (miner, proof) = solve(hash, 0x2100ffff, extra);
        let id = 3 + attempt;
        send(
            &mut stream,
            id,
            "mining.submit",
            json!([
                format!("{account}.one"),
                work["params"][0],
                miner.to_hex(),
                proof.to_vec()
            ]),
        );
        let submitted_reply = reply(&mut stream, id, &mut jobs);
        assert_eq!(submitted_reply["result"], true, "{submitted_reply}");
        if height(address) == 2 {
            submitted = true;
            break;
        }
    }
    assert!(submitted);
    at_height(address, 2);
    let accepted = read(address, "/v3/key-blocks/current");
    let sealed = header(&accepted);
    assert!(
        cuckoo::verify_key_seal_with_edge_bits(&sealed, cuckoo::EdgeBits::new(15).unwrap())
            .unwrap()
    );
    assert!(!cuckoo::verify_key_seal(&sealed).unwrap());
    drop(peer);
    drop(stream);
    process.signal(rustix::process::Signal::TERM);
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":false})
    );
    process.wait(true);
    let mut process = Process::spawn(&lab, lab.command(), "stratum-solved-reopen");
    let ready = process.event("ready");
    at_height(addresses(&ready)[0], 2);
    let reopened = read(addresses(&ready)[0], "/v3/key-blocks/current");
    assert_eq!(reopened, accepted);
    process.signal(rustix::process::Signal::TERM);
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":false})
    );
    process.wait(true);
    let command = lab.command();
    let ae_node::application::command::Command::Run(args) =
        ae_node::application::command::Command::parse(command.get_args().map(ToOwned::to_owned))
            .unwrap()
    else {
        panic!()
    };
    let prepared = args
        .prepare(
            &lab.0,
            &[("HOME".into(), lab.0.as_os_str().to_owned())].into(),
            std::path::Path::new(env!("CARGO_BIN_EXE_ae-node")),
        )
        .unwrap();
    let state = prepared
        .configured()
        .chain()
        .database()
        .stratum(Default::default())
        .unwrap();
    let mining = state.mining(Default::default()).unwrap();
    assert!(mining.info().counts[2] > 0);
    let expected = Header::Key(sealed).hash().unwrap();
    let records = mining
        .page(ae_storage::stratum::mining::Phase::Done, None, 8)
        .unwrap()
        .0;
    let record = records
        .iter()
        .find(|r| r.solution().unwrap().is_some_and(|s| s.hash() == expected))
        .unwrap();
    assert_eq!(record.attempts, 1);
    assert_eq!(record.receipt.input.account, account);
    assert!(record.accounted_key.is_some());
    assert!(matches!(
        record.state,
        ae_storage::stratum::mining::State::Done(ae_storage::stratum::mining::Outcome::Accepted(_))
    ));
    assert_eq!(
        prepared
            .configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status()
            .tip,
        Some(expected)
    );
}
