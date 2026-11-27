use super::*;
use ae_node::{
    application,
    configuration::{self, stratum::Rewards},
};
use ae_tx::{CanonicalTransaction, ContractTransaction, SignedTransaction};
use ae_types::{ContractPublicKey, Id, IdTag};
use std::sync::Arc;

struct App {
    ready: Value,
    handle: ae_node::runtime::Handle,
    database: Arc<ae_storage::Database>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<thread::JoinHandle<()>>,
}
impl App {
    fn start(lab: &Lab, rewards: Option<Rewards>) -> Self {
        let directory = lab.0.clone();
        let (send, started) = mpsc::channel();
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let task = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let configuration = configuration::Configuration::from_value(
                    serde_json::from_slice(&std::fs::read(directory.join("node.json")).unwrap()).unwrap()
                ).unwrap();
                let context = configuration::resolution::Context::new(configuration::resolution::Directories {
                    home: directory.to_str().unwrap().into(),
                    data: directory.join("resources").to_str().unwrap().into(),
                    log: directory.join("log").to_str().unwrap().into(),
                }).unwrap();
                let mut options = application::Options::new(
                    directory.clone(), directory.join("chain"),
                    ae_node::http::DryRunConfig::new(PathBuf::from(env!("CARGO_BIN_EXE_ae-dry-run-worker")), directory.join("dry-run")),
                );
                options.stratum_rewards = rewards;
                let prepared = application::Prepared::load(
                    &configuration, Arc::new(context),
                    &configuration::execution::Specification::from_value(&profile::default_value()).unwrap(), options,
                ).unwrap();
                let identity = prepared.configured().chain().identity().clone();
                let database = prepared.configured().chain().database().clone();
                let mut running = prepared.launch().unwrap();
                let listeners = running.start_http().await.unwrap();
                let mut handle = running.handle();
                let ready = json!({
                    "network_id":String::from_utf8_lossy(&identity.network),
                    "genesis":api::encode(api::ApiType::KeyBlockHash, identity.genesis.as_ref()),
                    "sync":handle.connections().unwrap().current().unwrap().network.status().listening.unwrap().to_string(),
                    "stratum":handle.pow().and_then(|p| p.stratum()).map(|s| s.transport.address().to_string()),
                    "http":listeners.iter().map(|h|h.status().address.to_string()).collect::<Vec<_>>()
                });
                send.send((ready, handle.clone(), database)).unwrap();
                tokio::pin!(stopping);
                loop {
                    while handle.try_output().is_some() {}
                    assert!(handle.status().running && handle.status().failure.is_none(), "{:?}", handle.status());
                    tokio::select! {
                        _ = &mut stopping => break,
                        _ = handle.changed() => (),
                        _ = tokio::time::sleep(Duration::from_millis(5)) => (),
                    }
                }
                let exit = running.shutdown().await.unwrap();
                assert!(exit.status().joined && exit.status().failure.is_none(), "{:?}", exit.status());
            });
        });
        let (ready, handle, database) = started.recv_timeout(Duration::from_secs(20)).unwrap();
        Self {
            ready,
            handle,
            database,
            stop: Some(stop),
            task: Some(task),
        }
    }
    fn address(&self) -> SocketAddr {
        addresses(&self.ready)[0]
    }
    fn mining(&self) -> ae_node::stratum::mining::Client {
        self.handle
            .pow()
            .unwrap()
            .stratum()
            .unwrap()
            .accounting
            .mining()
            .unwrap()
    }
}
impl Drop for App {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.take().unwrap().join().unwrap();
    }
}
fn until<T>(mut f: impl FnMut() -> Option<T>) -> T {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < end, "bounded application payout wait");
        thread::sleep(Duration::from_millis(10));
    }
}
fn peer_key(lab: &Lab) -> PeerPublicKey {
    let keys = ae_sync::keys::Worker::spawn(ae_sync::keys::Config::new(
        lab.0.join("keys"),
        b"local-test".to_vec(),
    ))
    .unwrap();
    let key = keys.info().keys.peer;
    keys.shutdown().unwrap();
    key
}
fn advance(app: &App, remote: PeerPublicKey) -> Peer {
    let mut key = header(&read(app.address(), "/v3/key-blocks/pending"));
    let next = key.common.height;
    let (miner, proof) = solve(
        ae_crypto::blake2b_256(&cuckoo::sealing_bytes(&key).unwrap()),
        key.target,
        Part::new(Kind::Extra, 0, 4).unwrap(),
    );
    key.nonce = miner.value();
    key.seal = proof.map(|e| u32::try_from(e).unwrap());
    let peer = Peer::gossip(&app.ready, remote, key);
    at_height(app.address(), next);
    peer
}
fn branch(app: &App, remote: PeerPublicKey, parent: Hash256) -> Hash256 {
    let builder = app
        .handle
        .importer()
        .unwrap()
        .stratum_builder(
            ae_storage::snapshots::Owner::current(&app.database).unwrap(),
            Default::default(),
        )
        .unwrap();
    let prepared = builder
        .start(
            ae_sync::chain::pow::preparation::Request {
                parent,
                miner: ae_types::AccountPublicKey([91; 32]),
                beneficiary: ae_types::AccountPublicKey([92; 32]),
                release_info: 0,
            },
            Arc::new(ae_sync::chain::pow::generator::SystemClock),
        )
        .unwrap()
        .wait()
        .unwrap()
        .result
        .unwrap();
    let mut key = prepared.header;
    drop(prepared.state);
    let (nonce, proof) = solve(
        ae_crypto::blake2b_256(&cuckoo::sealing_bytes(&key).unwrap()),
        key.target,
        Part::new(Kind::Extra, 0, 4).unwrap(),
    );
    key.nonce = nonce.value();
    key.seal = proof.map(|e| u32::try_from(e).unwrap());
    let hash = Header::Key(key.clone()).hash().unwrap();
    let peer = Peer::gossip(&app.ready, remote, key);
    until(|| {
        app.database
            .view()
            .unwrap()
            .block(hash)
            .unwrap()
            .map(|_| ())
    });
    drop(peer);
    hash
}
fn deploy(
    secret: &ae_crypto::Ed25519SecretKey,
) -> (SignedTransaction<ContractTransaction>, ContractPublicKey) {
    let packed: Value = serde_json::from_str(include_str!(
        "../../fixtures/erlang_stratum_payout_execution.json"
    ))
    .unwrap();
    fn expand(v: &Value, pool: &[Value]) -> Value {
        if let Some(n) = v.get("$ref") {
            return expand(&pool[n.as_u64().unwrap() as usize], pool);
        }
        match v {
            Value::Array(a) => Value::Array(a.iter().map(|x| expand(x, pool)).collect()),
            Value::Object(o) => Value::Object(
                o.iter()
                    .map(|(k, v)| (k.clone(), expand(v, pool)))
                    .collect(),
            ),
            _ => v.clone(),
        }
    }
    let data = expand(&packed["root"], packed["values"].as_array().unwrap());
    let original = ContractTransaction::decode(
        &hex::decode(data["create"]["transaction"][1].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, secret.public_key().0),
        original.operation().clone(),
    )
    .unwrap();
    tx.set_nonce(1.into());
    tx.set_fee(1_400_000_000_000_000u64.into());
    tx.set_gas(100_000.into());
    tx.set_gas_price(1_000_000_000u64.into());
    let contract = ContractPublicKey(
        ae_state::contracts::contract_id(
            secret.public_key(),
            ae_state::contracts::Nonce::Number(tx.nonce()),
        )
        .0,
    );
    let mut data = b"ae_application_tests".to_vec();
    data.extend(tx.encode());
    (
        SignedTransaction::new(tx, vec![secret.sign(&data)]),
        contract,
    )
}
fn post(app: &App, tx: &SignedTransaction<ContractTransaction>) {
    let body = json!({"tx":api::encode(api::ApiType::Transaction,&tx.encode())}).to_string();
    let (code, response) = request(
        app.address(),
        &format!(
            "POST /v3/transactions HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    );
    assert_eq!(code, 200, "{response}");
    assert_eq!(
        response["tx_hash"],
        api::encode(api::ApiType::TxHash, tx.hash().as_ref())
    );
}
fn included(app: &App, hash: Hash256) -> Value {
    let path = format!(
        "/v3/transactions/{}",
        api::encode(api::ApiType::TxHash, hash.as_ref())
    );
    until(|| {
        let v = read(app.address(), &path);
        (v["block_height"].as_i64().unwrap() > 0).then_some(v)
    })
}

#[test]
fn application_mined_share_payout_survives_pending_restart_peer_reorg_and_confirmation() {
    let lab = Lab::new("pow_cuckoo", true);
    let secret = ae_crypto::Ed25519SecretKey::from_seed(&[7; 32]);
    let caller = api::encode(api::ApiType::AccountPubkey, &secret.public_key().0);
    lab.write("alloc.json", &json!({caller.clone():serde_json::from_str::<Value>("1000000000000000000000000000").unwrap()}));
    std::fs::create_dir_all(lab.0.join("pool-keys")).unwrap();
    std::fs::write(lab.0.join("pool-keys/sign_key.pub"), secret.public_key().0).unwrap();
    std::fs::write(
        lab.0.join("pool-keys/sign_key"),
        [vec![7; 32], secret.public_key().0.to_vec()].concat(),
    )
    .unwrap();
    let mut cfg = lab.config();
    cfg["chain"]["hard_forks"] = json!({"1":0,"3":1});
    cfg["mining"]["cuckoo"]["edge_bits"] = json!(15);
    cfg["mining"]["autostart"] = json!(true);
    cfg["mining"]["micro_block_cycle"] = json!(100);
    lab.write("node.json", &cfg);
    let remote = peer_key(&lab);
    let (create, contract) = deploy(&secret);
    let app = App::start(&lab, None);
    let peer = advance(&app, remote);
    post(&app, &create);
    assert_eq!(included(&app, create.hash())["block_height"], 1);
    let deployed = read(
        app.address(),
        &format!(
            "/v3/contracts/{}",
            api::encode(api::ApiType::ContractPubkey, &contract.0)
        ),
    );
    assert_eq!(deployed["owner_id"], caller);
    drop(peer);
    drop(app);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    cfg["stratum"] = json!({"enabled":true,"connection":{"port":port,"max_connections":4,"num_acceptors":2},"session":{"edge_bits":15,"skip_num_blocks":0,"max_solve_time":120},"reward":{"keys":{"dir":lab.0.join("pool-keys")},"beneficiaries":[]}});
    lab.write("node.json", &cfg);
    let rewards = Rewards {
        contract,
        reward_delay: 0,
        confirmation_delay: 2,
    };
    let app = App::start(&lab, Some(rewards));
    let socket = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut stream = BufReader::new(socket);
    let mut jobs = std::collections::VecDeque::new();
    send(
        &mut stream,
        1,
        "mining.subscribe",
        json!(["payout-test/1", null, null, port]),
    );
    let subscribed = reply(&mut stream, 1, &mut jobs);
    assert!(subscribed["error"].is_null(), "{subscribed}");
    let extra = Part::from_hex(Kind::Extra, subscribed["result"][1].as_str().unwrap(), 4).unwrap();
    let miner_account = api::encode(api::ApiType::AccountPubkey, &[77; 32]);
    send(
        &mut stream,
        2,
        "mining.authorize",
        json!([format!("{miner_account}.one"), null]),
    );
    assert_eq!(reply(&mut stream, 2, &mut jobs)["result"], true);
    let peer = advance(&app, remote);
    let mut submitted = false;
    for attempt in 0..4 {
        let work = jobs.pop_front().unwrap_or_else(|| {
            loop {
                let v = packet(&mut stream);
                if v["method"] == "mining.notify" {
                    break v;
                }
            }
        });
        if height(app.address()) == 3 {
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
        send(
            &mut stream,
            3 + attempt,
            "mining.submit",
            json!([
                format!("{miner_account}.one"),
                work["params"][0],
                miner.to_hex(),
                proof.to_vec()
            ]),
        );
        let result = reply(&mut stream, 3 + attempt, &mut jobs);
        assert_eq!(result["result"], true, "{result}");
        if height(app.address()) == 3 {
            submitted = true;
            break;
        }
    }
    assert!(submitted);
    at_height(app.address(), 3);
    drop(stream);
    drop(peer);
    let record = until(|| {
        let info = app.mining().payout().unwrap().unwrap();
        let r = info.pending?;
        matches!(
            r.signed.as_ref().map(|s| &s.dispatch),
            Some(ae_storage::stratum::payouts::Dispatch::Done {
                outcome: ae_storage::stratum::payouts::Outcome::Accepted { .. },
                ..
            })
        )
        .then_some(r)
    });
    assert_eq!(record.payment.id.0, 3);
    let hash = record.hash().unwrap().unwrap();
    let tx = included(&app, hash);
    assert_eq!(tx["block_height"], 3);
    let absolute = record.request.as_ref().unwrap().prepared.absolute();
    assert_eq!(absolute.len(), 1);
    let expected = absolute.get(miner_account.as_bytes()).unwrap().to_string();
    let paid = read(app.address(), &format!("/v3/accounts/{miner_account}"));
    assert_eq!(paid["balance"].to_string(), expected);
    let info = read(
        app.address(),
        &format!(
            "/v3/transactions/{}/info",
            api::encode(api::ApiType::TxHash, hash.as_ref())
        ),
    );
    assert_eq!(info["call_info"]["return_type"], "ok", "{info}");
    assert_eq!(app.mining().payout().unwrap().unwrap().last, None);
    let original_epoch = app.mining().payout().unwrap().unwrap().epoch;
    drop(app);
    let app = App::start(&lab, Some(rewards));
    let resumed = app.mining().payout().unwrap().unwrap();
    assert!(resumed.epoch > original_epoch);
    assert_eq!(resumed.pending.as_ref().unwrap().signed, record.signed);
    assert_eq!(
        resumed.pending.as_ref().unwrap().hash().unwrap(),
        Some(hash)
    );
    assert!(resumed.last.is_none());
    let key3 = app
        .database
        .view()
        .unwrap()
        .canonical_key(3)
        .unwrap()
        .unwrap();
    let fork4 = branch(&app, remote, key3);
    let fork5 = branch(&app, remote, fork4);
    at_height(app.address(), 5);
    assert_eq!(
        app.database.view().unwrap().canonical_key(5).unwrap(),
        Some(fork5)
    );
    let before = app.mining().payout_status().unwrap().confirmation_checks;
    until(|| (app.mining().payout_status().unwrap().confirmation_checks > before).then_some(()));
    let pending = app.mining().payout().unwrap().unwrap();
    assert!(pending.last.is_none());
    assert_eq!(pending.pending.as_ref().unwrap().signed, record.signed);
    let (code, absent) = request(
        app.address(),
        &format!(
            "GET /v3/accounts/{miner_account} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        ),
    );
    assert_eq!(code, 404, "{absent}");
    // Restore the branch containing the original payment via ordinary gossip.
    // All candidate state comes from real captured historical views.
    let included_micro = Hash256(bytes(&tx["block_hash"]).try_into().unwrap());
    let restored4 = branch(&app, remote, included_micro);
    let restored5 = branch(&app, remote, restored4);
    let restored6 = branch(&app, remote, restored5);
    at_height(app.address(), 6);
    assert_eq!(
        app.database.view().unwrap().canonical_key(6).unwrap(),
        Some(restored6)
    );
    let completed = until(|| {
        let i = app.mining().payout().unwrap().unwrap();
        i.last.filter(|c| c.hash == hash)
    });
    assert_eq!(completed.height, 3);
    assert_eq!(height(app.address()), 6);
    assert_eq!(app.mining().payout().unwrap().unwrap().context.height, 6);
    assert!(completed.pruned);
    assert!(app.mining().payout().unwrap().unwrap().pending.is_none());
    drop(app);
    let app = App::start(&lab, Some(rewards));
    assert_eq!(
        app.mining().payout().unwrap().unwrap().last,
        Some(completed)
    );
    assert!(app.mining().payout().unwrap().unwrap().pending.is_none());
    assert_eq!(
        read(app.address(), &format!("/v3/accounts/{miner_account}"))["balance"].to_string(),
        expected
    );
    assert_eq!(included(&app, hash), tx);
    drop(app);
}
