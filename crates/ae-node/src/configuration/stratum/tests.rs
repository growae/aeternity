use super::*;
use crate::configuration::{
    Configuration,
    resolution::{Context, Directories},
};
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
struct Lab(PathBuf);
impl Lab {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-stratum-setup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn keys(&self, kind: &str) -> AccountPublicKey {
        let mut seed = [0; 32];
        seed[31] = 17;
        let key = Ed25519SecretKey::from_seed(&seed);
        let public = key.public_key();
        let mut secret = seed.to_vec();
        secret.extend_from_slice(&public.0);
        if kind == "wrong_suffix" {
            secret[32..].fill(0);
        }
        if kind == "short_secret" {
            secret.truncate(32);
        }
        std::fs::write(
            self.0.join("sign_key.pub"),
            if kind == "wrong_public" {
                [0; 32]
            } else {
                public.0
            },
        )
        .unwrap();
        std::fs::write(self.0.join("sign_key"), secret).unwrap();
        public
    }
    fn resolve(&self, value: Value) -> Result<Option<Plan>, Error> {
        let configuration = Configuration { value };
        let context = Context::new(Directories {
            home: "/release".into(),
            data: "/data".into(),
            log: "/log".into(),
        })
        .unwrap();
        Plan::resolve(
            &configuration.resolve(&context).unwrap(),
            b"ae_mainnet",
            &self.0,
        )
    }
    fn user(&self) -> Value {
        json!({"stratum":{"enabled":true,"reward":{"keys":{"dir":self.0},"beneficiaries":["ak_one:1"]}}})
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn original_stratum_setup_defaults_numbers_nonce_capacity_and_key_pairs_match() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_stratum_setup.json"
    ))
    .unwrap();
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 68);
    let lab = Lab::new();
    let mut success = 0;
    for (input, expected) in fixture["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .zip(fixture["rows"].as_array().unwrap())
    {
        lab.keys(input["keys"].as_str().unwrap());
        let mut value = if input["user"].is_null() {
            json!({})
        } else {
            json!({"stratum":input["user"]})
        };
        if value["stratum"]["reward"]["keys"]["dir"] == "$KEYS" {
            value["stratum"]["reward"]["keys"]["dir"] = json!(lab.0);
        }
        let configuration = Configuration { value };
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "/data".into(),
            log: "/log".into(),
        })
        .unwrap();
        context
            .set("aestratum".into(), "obsolete".into(), json!(true))
            .unwrap();
        context
            .set("aestratum".into(), "port".into(), json!(1234))
            .unwrap();
        let result = Plan::resolve(
            &configuration.resolve(&context).unwrap(),
            input["network"].as_str().unwrap().as_bytes(),
            &lab.0,
        )
        .and_then(|p| p.map(Plan::load).transpose());
        assert_eq!(
            result.is_ok(),
            expected["output"]["kind"] == "ok",
            "{}: {:?}",
            input["name"],
            result.as_ref().err()
        );
        if let Ok(prepared) = result {
            success += 1;
            let actual = prepared.map_or(json!({"enabled":false}), |p| p.public_configuration());
            assert_eq!(actual, expected["output"]["value"], "{}", input["name"]);
        }
        assert_eq!(expected["obsolete_cleared"], true);
    }
    assert_eq!(success, 50);
}

#[test]
fn loaded_stratum_keys_sign_and_build_native_reward_and_runtime_settings() {
    let lab = Lab::new();
    let public = lab.keys("valid");
    let mut value = lab.user();
    value["stratum"]["connection"] = json!({"host":"ignored.invalid","transport":"ssl","port":0,"num_acceptors":2,"max_connections":3});
    value["stratum"]["session"] =
        json!({"desired_solve_time":30,"max_solve_time":60,"msg_timeout":15,"max_workers":2});
    value["stratum"]["reward"]["beneficiaries"] = json!(["ak_one:1", "ak_one:2.5", "ak_two:4"]);
    let plan = lab.resolve(value).unwrap().unwrap();
    let prepared = plan.load().unwrap();
    let transport = prepared.transport(Default::default()).unwrap();
    assert!(transport.bind.ip().is_unspecified());
    assert_eq!(transport.bind.port(), 0);
    assert_eq!(transport.limits.acceptors, 2);
    assert_eq!(transport.limits.connections, 3);
    assert_eq!(transport.services.max_workers, 2);
    assert_eq!(transport.session.message_timeout_ms, 15);
    assert_eq!(transport.session.targets.desired_solve_time, 30000.into());
    assert_eq!(transport.session.targets.maximum_solve_time, 60000.into());
    let rounds = prepared.rounds().unwrap();
    assert_eq!(rounds.last_n, 2);
    assert_eq!(rounds.delay, 180);
    assert_eq!(rounds.percent, Number::Float(7.5f64.to_bits()));
    assert_eq!(prepared.payouts().caller, public);
    assert_eq!(prepared.payouts().delay, 180);
    let signer = prepared.signer(Default::default()).unwrap();
    let client = signer.client();
    let ticket = client.sign_binary(b"stratum setup", public).unwrap();
    let signature = ticket
        .wait_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert!(ae_crypto::verify_ed25519(
        &public,
        b"stratum setup",
        &signature
    ));
    drop(ticket);
    let info = signer.retire();
    assert!(info.joined && !info.accepting);
}

#[test]
fn stratum_key_loading_is_bounded_and_failed_preparation_does_not_create_files() {
    let lab = Lab::new();
    let plan = lab.resolve(lab.user()).unwrap().unwrap();
    assert!(matches!(
        plan.load(),
        Err(Error::Keys(std::io::ErrorKind::NotFound))
    ));
    assert_eq!(std::fs::read_dir(&lab.0).unwrap().count(), 0);
    lab.keys("valid");
    std::fs::write(lab.0.join("sign_key"), [0; 65]).unwrap();
    assert!(matches!(
        lab.resolve(lab.user()).unwrap().unwrap().load(),
        Err(Error::KeyPair)
    ));
    lab.keys("valid");
    let mut value = lab.user();
    value["stratum"]["reward"]["keys"]["dir"] = json!(".");
    let plan = lab.resolve(value).unwrap().unwrap();
    assert_eq!(plan.key_directory(), lab.0.join("."));
    assert!(plan.load().is_ok());
    let mut value = lab.user();
    value["stratum"]["connection"] = json!({"max_connections":0});
    let prepared = lab.resolve(value).unwrap().unwrap().load().unwrap();
    assert!(prepared.transport(Default::default()).is_err());
}

#[test]
fn stratum_key_wait_retries_missing_files_and_is_cancelled_by_dropping_the_future() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            let lab = Lab::new();
            let plan = lab.resolve(lab.user()).unwrap().unwrap();
            let mut waiting = Box::pin(plan.load_when_available());
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read_dir(&lab.0).unwrap().count(), 0);
            let public = lab.keys("valid");
            let prepared = tokio::time::timeout(Duration::from_secs(2), waiting)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(prepared.caller(), public);

            let cancelled = Lab::new();
            let mut waiting = Box::pin(
                cancelled
                    .resolve(cancelled.user())
                    .unwrap()
                    .unwrap()
                    .load_when_available(),
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                    .await
                    .is_err()
            );
            drop(waiting);
            assert_eq!(std::fs::read_dir(&cancelled.0).unwrap().count(), 0);
            cancelled.keys("wrong_public");
            let invalid = cancelled
                .resolve(cancelled.user())
                .unwrap()
                .unwrap()
                .load_when_available();
            assert!(matches!(
                tokio::time::timeout(Duration::from_millis(50), invalid)
                    .await
                    .unwrap(),
                Err(Error::KeyPair)
            ));
        });
}

#[test]
fn configured_stratum_acceptors_serve_bounded_tcp_sessions_and_join() {
    let lab = Lab::new();
    lab.keys("valid");
    let mut value = lab.user();
    value["stratum"]["connection"] = json!({"port":9999,"num_acceptors":10,"max_connections":2});
    let prepared = lab.resolve(value).unwrap().unwrap().load().unwrap();
    let mut config = prepared.transport(Default::default()).unwrap();
    config.bind.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    // Isolate the test socket; keep the configured port in the protocol request.
    config.bind.set_port(0);
    let owner = runtime::Owner::bind(config).unwrap();
    let client = owner.client();
    let address = client.address();
    let subscribe = format!(
        "{}\n",
        json!({"jsonrpc":"2.0","id":1,"method":"mining.subscribe","params":["test/1",null,null,9999]})
    );
    let mut peers = vec![];
    for _ in 0..2 {
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut peer = BufReader::new(socket);
        peer.get_mut().write_all(subscribe.as_bytes()).unwrap();
        let mut line = String::new();
        peer.read_line(&mut line).unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert!(response["error"].is_null(), "{response}");
        peers.push(peer);
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while client.connections().len() != 2 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut overflow = TcpStream::connect(address).unwrap();
    overflow
        .set_read_timeout(Some(Duration::from_millis(40)))
        .unwrap();
    overflow.write_all(subscribe.as_bytes()).unwrap();
    assert!(overflow.read(&mut [0]).is_err());
    assert_eq!(client.connections().len(), 2);
    let report = owner.shutdown();
    assert!(report.failure.is_none(), "{report:?}");
    assert!(client.is_closed());
    for mut peer in peers {
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    }
    drop(overflow);
    drop(std::net::TcpListener::bind(address).unwrap());
}
