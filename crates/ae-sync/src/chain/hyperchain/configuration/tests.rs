use super::*;
use serde_json::json;
fn native() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/hc_configuration.json"
    )))
    .unwrap()
}
fn parse(v: &Value) -> Result<Configuration, Error> {
    Configuration::parse(&serde_json::to_vec(v).unwrap())
}
fn uint(n: &Unsigned) -> u64 {
    num_bigint::BigUint::from_bytes_be(n.as_bytes())
        .try_into()
        .unwrap()
}
#[test]
fn original_startup_defaults_endpoints_keys_signatures_and_failure_cases() {
    let fixture = native();
    assert_eq!(fixture["full_node_started"], false);
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 30);
    for row in fixture["rows"].as_array().unwrap() {
        let result = parse(&row["config"]);
        assert_eq!(
            result.is_ok(),
            row["ok"].as_bool().unwrap(),
            "{}",
            row["name"]
        );
        let Ok(c) = result else { continue };
        let deps = row["dependencies"].as_array().unwrap();
        assert_eq!(
            c.public_keys()
                .map(|k| hex::encode(k.0))
                .collect::<Vec<_>>(),
            deps[0]["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| k.as_str().unwrap())
                .collect::<Vec<_>>()
        );
        for key in deps[0]["signatures"].as_array().unwrap() {
            let public = AccountPublicKey(
                hex::decode(key["key"].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            assert_eq!(
                hex::encode(c.keys().sign(public, b"hc configuration").unwrap().0),
                key["signature"].as_str().unwrap()
            );
            assert_eq!(
                hex::encode(c.child_keys[&public].sign(b"hc configuration").0),
                key["signature"].as_str().unwrap()
            );
        }
        assert_eq!(uint(&c.child_block_time), deps[1]["time"].as_u64().unwrap());
        let parent = &deps[2];
        assert_eq!(c.network, parent["network"].as_str().unwrap().as_bytes());
        assert_eq!(
            c.fetch_interval.as_millis(),
            parent["fetch"].as_u64().unwrap() as u128
        );
        let accounts: Vec<_> = c
            .accounts
            .iter()
            .map(|(a, b)| json!([hex::encode(a.0), hex::encode(b.0)]))
            .collect();
        assert_eq!(json!(accounts), parent["accounts"]);
        let hosts = parent["hosts"].as_array().unwrap();
        assert_eq!(hosts.len(), c.nodes.len());
        for (node, h) in c.nodes.iter().zip(hosts) {
            assert_eq!(node.chain(), c.chain);
            assert_eq!(node.origin().scheme(), h["scheme"].as_str().unwrap());
            assert_eq!(
                node.origin().host_str().unwrap(),
                h["host"].as_str().unwrap()
            );
            assert_eq!(
                node.origin().port(),
                h.get("port").map(|v| v.as_u64().unwrap() as u16)
            );
            match &node.credentials {
                Some((u, p)) => {
                    assert_eq!(u, h["user"].as_str().unwrap().as_bytes());
                    assert_eq!(p, h["password"].as_str().unwrap().as_bytes());
                }
                None => {
                    assert!(h.get("user").is_none());
                    assert!(h.get("password").is_none());
                }
            }
        }
        let (callback, signer) = match c.chain {
            parent_pins::Chain::Aeternity => ("aehttpc_aeternity", "aec_preset_keys"),
            parent_pins::Chain::Bitcoin => ("aehttpc_btc", "undefined"),
            parent_pins::Chain::Dogecoin => ("aehttpc_doge", "undefined"),
        };
        assert_eq!(parent["callback"], callback);
        assert_eq!(parent["signer"], signer);
        for (field, n) in [
            ("start", &c.cache.child_start_height),
            ("retry", &c.cache.retry_interval),
            ("cache", &c.cache.max_size),
            ("finality", &c.cache.finality),
        ] {
            assert_eq!(uint(n), deps[3][field].as_u64().unwrap());
        }
        assert_eq!(
            hex::encode(c.election),
            deps[4]["contract"].as_str().unwrap()
        );
        assert_eq!(c.default_pinning, deps[4]["enabled"].as_bool().unwrap());
    }
}
#[test]
fn invalid_or_overwritten_secrets_never_start_and_errors_do_not_contain_input() {
    for url in ["http://", "https://user:password@/", "//", "http://:3013"] {
        assert!(parent_pins::Node::from_config_url(parent_pins::Chain::Aeternity, url).is_err());
    }
    let upper = parent_pins::Node::from_config_url(
        parent_pins::Chain::Aeternity,
        "HTTPS://localhost:443/ignored",
    )
    .unwrap();
    assert_eq!(upper.origin().as_str(), "https://localhost/");

    let fixture = native();
    let original = fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "ae")
        .unwrap()["config"]
        .clone();
    for (field, value) in [
        ("priv", json!("SENSITIVE secret")),
        ("pub", json!("SENSITIVE public")),
    ] {
        let mut v = original.clone();
        let valid = v["stakers"][0].clone();
        v["stakers"][0]["hyper_chain_account"][field] = value;
        v["stakers"].as_array_mut().unwrap().push(valid);
        let err = parse(&v).err().unwrap();
        assert!(!format!("{err:?}").contains("SENSITIVE"));
    }
    assert!(matches!(
        Configuration::parse(&vec![b' '; (16 << 20) + 1]),
        Err(Error::Limit)
    ));
    assert!(matches!(Configuration::parse(b"{"), Err(Error::Json)));
    for (path, value) in [
        ("child_block_time", json!(0)),
        ("child_block_time", json!(-1)),
        ("child_block_production_time", json!(0)),
        ("default_pinning_behavior", json!(1)),
        ("parent_chain", json!([])),
    ] {
        let mut v = original.clone();
        v[path] = value;
        assert!(parse(&v).is_err(), "{path}");
    }
    let mut v = original.clone();
    v["parent_chain"]["polling"] = json!({"nodes":vec!["http://localhost";33]});
    assert!(matches!(parse(&v), Err(Error::Limit)));
}
#[test]
fn empty_configured_parent_is_available_for_metadata_but_never_absent_pin_evidence() {
    for chain in [
        parent_pins::Chain::Aeternity,
        parent_pins::Chain::Bitcoin,
        parent_pins::Chain::Dogecoin,
    ] {
        let service =
            parent_pins::Service::spawn_configured(chain, vec![], Default::default(), None)
                .unwrap();
        let client = service.client();
        assert_eq!(client.chain(), Ok(chain));
        assert_eq!(client.signing_network(), Ok(None));
        assert!(matches!(
            client.request(b"tx"),
            Err(parent_pins::Error::NoParents)
        ));
        assert!(matches!(
            client.post(parent_pins::posting::Post {
                pin: ae_hyperchain::pin::Parent {
                    epoch: 1.into(),
                    height: 3.into(),
                    block_hash: vec![1; 32]
                },
                account: AccountPublicKey([1; 32]),
                amount: 1.into(),
                fee: 1.into()
            }),
            Err(parent_pins::Error::NoParents)
        ));
        service.shutdown().unwrap();
        assert_eq!(client.chain(), Err(parent_pins::Error::Closed));
    }
    assert_eq!(
        parent_blocks::Backend::configured(vec![], Default::default())
            .unwrap()
            .hosts(),
        0
    );
    assert!(parent_blocks::Backend::new(vec![], Default::default()).is_err());
    assert!(parent_pins::Service::spawn(vec![], Default::default()).is_err());
}
#[test]
fn missing_configured_wallet_credentials_fail_before_network_or_absence_inference() {
    let node = parent_pins::Node::from_config_url(
        parent_pins::Chain::Dogecoin,
        "http://no-dns-must-run.invalid:22555/ignored",
    )
    .unwrap();
    assert!(node.credentials.is_none());
    let service = parent_pins::Service::spawn_configured(
        parent_pins::Chain::Dogecoin,
        vec![node.clone()],
        Default::default(),
        None,
    )
    .unwrap();
    let mut request = service.client().request(b"pin-hash").unwrap();
    let end = std::time::Instant::now() + Duration::from_secs(2);
    let result = loop {
        if let Some(result) = request.try_result() {
            break result;
        }
        assert!(std::time::Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(result, Err(parent_pins::Error::Configuration));
    service.shutdown().unwrap();
    assert!(parent_blocks::Backend::configured(vec![node], Default::default()).is_ok());
}
