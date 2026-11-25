use ae_codec::api::{self, ApiType};
use ae_execution::transactions::{self, owned::Owned};
use ae_node::{
    bootstrap::{self, genesis::Prepared},
    configuration::{
        Configuration,
        chain::Settings,
        presets::{self, Limits, Source},
        resolution::{Context, Directories},
    },
};
use ae_protocol::ConsensusType;
use ae_state::{Trees, accounts::Account};
use ae_types::{AccountPublicKey, Hash256};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
#[path = "../../ae-sync/tests/common/ledger_fixtures.rs"]
mod execution_fixture;
#[path = "common/execution_profile.rs"]
mod startup_profile;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "ae-genesis-assembly-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let p = self.0.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, bytes).unwrap();
        p
    }
    fn context(&self) -> Context {
        Context::new(Directories {
            home: "/release".into(),
            data: self.0.to_str().unwrap().into(),
            log: "/logs".into(),
        })
        .unwrap()
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn empty() -> Trees {
    Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn ak(n: u8) -> String {
    api::encode(ApiType::AccountPubkey, &[n; 32])
}
fn fingerprint() -> Hash256 {
    ae_crypto::blake2b_256(b"explicit isolated genesis assembly profile")
}
fn settings(cfg: &Configuration, ctx: &Context, network: &[u8]) -> Settings {
    Settings::resolve(&cfg.resolve(ctx).unwrap(), Some(network)).unwrap()
}
#[test]
fn public_presets_reconstruct_canonical_genesis_ignore_overrides_and_reopen() {
    for (network, file, data) in [
        (
            b"ae_mainnet".as_slice(),
            "accounts.json",
            include_str!("../../ae-state/data/mainnet_accounts.json"),
        ),
        (
            b"ae_uat".as_slice(),
            "accounts_uat.json",
            include_str!("../../ae-state/data/uat_accounts.json"),
        ),
    ] {
        let tmp = Temp::new();
        tmp.write(&format!("aecore/.genesis/{file}"), data);
        let ctx = tmp.context();
        let cfg =
            Configuration::from_value(json!({"chain":{"genesis_accounts":{ak(91):123}}})).unwrap();
        let s = settings(&cfg, &ctx, network);
        let execution = execution_fixture::execution(s.profile());
        let load = || {
            Prepared::load(
                &cfg.resolve(&ctx).unwrap(),
                Some(network),
                &empty(),
                &execution,
                fingerprint(),
                &6_000_000.into(),
                Limits::default(),
            )
            .unwrap()
        };
        let prepared = load();
        assert_eq!(prepared.inputs().len(), 1);
        assert_eq!(
            prepared.inputs()[0].hash,
            ae_crypto::blake2b_256(data.as_bytes())
        );
        assert!(
            prepared
                .trees()
                .accounts
                .lookup(AccountPublicKey([91; 32]))
                .unwrap()
                .is_none()
        );
        let expected = ae_state::genesis::header(if network == b"ae_mainnet" {
            ae_state::genesis::Network::Mainnet
        } else {
            ae_state::genesis::Network::Uat
        })
        .unwrap()
        .hash()
        .unwrap();
        let path = tmp.0.join("db");
        let opened = prepared.open(&path, Default::default()).unwrap();
        assert_eq!(opened.identity().genesis, expected);
        let status = opened.database().view().unwrap().status();
        drop(opened);
        let opened = load().open(&path, Default::default()).unwrap();
        assert_eq!(opened.database().view().unwrap().status(), status);
        drop(opened);
        let changed=Configuration::from_value(json!({"system":{"custom_prefunded_accs_file":tmp.write("changed.json",format!("{{\"{}\":1}}",ak(7))).to_str().unwrap()}})).unwrap();
        assert!(matches!(
            Prepared::load(
                &changed.resolve(&ctx).unwrap(),
                Some(network),
                &empty(),
                &execution,
                fingerprint(),
                &6_000_000.into(),
                Limits::default()
            ),
            Err(bootstrap::genesis::Error::Configuration(_))
        ));
    }
    println!(
        "Genesis assembly: canonical mainnet/UAT allocations, configured-account exclusion and durable reopen"
    );
}
#[test]
fn custom_accounts_merge_before_balance_validation_and_preserve_initial_trees() {
    let tmp = Temp::new();
    let address = ak(3);
    tmp.write(
        "aecore/.genesis/accounts_test.json",
        format!("{{\"{address}\":7,\"{address}\":-1,\"{}\":9}}", ak(5)),
    );
    let ctx = tmp.context();
    let cfg = Configuration::from_value(json!({"chain":{"genesis_accounts":{address.clone():13}}}))
        .unwrap();
    let s = settings(&cfg, &ctx, b"ae_dev");
    let execution = execution_fixture::execution(s.profile());
    let mut initial = empty();
    initial
        .accounts
        .enter(&Account::new(AccountPublicKey([8; 32]), 99.into()))
        .unwrap();
    let before = initial.roots();
    let prepared = Prepared::load(
        &cfg.resolve(&ctx).unwrap(),
        Some(b"ae_dev"),
        &initial,
        &execution,
        fingerprint(),
        &6_000_000.into(),
        Limits::default(),
    )
    .unwrap();
    for (n, balance) in [(3, 13), (5, 9), (8, 99)] {
        assert_eq!(
            prepared
                .trees()
                .accounts
                .lookup(AccountPublicKey([n; 32]))
                .unwrap()
                .unwrap()
                .balance(),
            &balance.into()
        );
    }
    assert_eq!(initial.roots(), before);
    let no_override = Configuration::empty();
    assert!(matches!(
        Prepared::load(
            &no_override.resolve(&ctx).unwrap(),
            Some(b"ae_dev"),
            &initial,
            &execution,
            fingerprint(),
            &6_000_000.into(),
            Limits::default()
        ),
        Err(bootstrap::genesis::Error::Presets(presets::Error::Balance))
    ));
    let path = tmp.0.join("db");
    let opened = prepared.open(&path, Default::default()).unwrap();
    assert_eq!(
        opened.identity().algorithm,
        ae_protocol::ConsensusAlgorithm::OnDemand
    );
    drop(opened);
    let changed =
        Configuration::from_value(json!({"chain":{"genesis_accounts":{address:14}}})).unwrap();
    let prepared = Prepared::load(
        &changed.resolve(&ctx).unwrap(),
        Some(b"ae_dev"),
        &initial,
        &execution,
        fingerprint(),
        &6_000_000.into(),
        Limits::default(),
    )
    .unwrap();
    assert!(prepared.open(&path, Default::default()).is_err());
}
#[test]
fn execution_binding_and_hyperchain_input_limits_fail_before_publication() {
    let tmp = Temp::new();
    let ctx = tmp.context();
    let cfg = Configuration::empty();
    let s = settings(&cfg, &ctx, b"ae_dev");
    let wrong_family = execution_fixture::execution_with_consensus(s.profile(), ConsensusType::Pos);
    let other = settings(&cfg, &ctx, b"other_network");
    let wrong_network = execution_fixture::execution(other.profile());
    for execution in [&wrong_family, &wrong_network] {
        assert!(matches!(
            Prepared::load(
                &cfg.resolve(&ctx).unwrap(),
                Some(b"ae_dev"),
                &empty(),
                execution,
                fingerprint(),
                &6_000_000.into(),
                Limits::default()
            ),
            Err(bootstrap::genesis::Error::Binding)
        ));
    }
    assert!(!tmp.0.exists());
    let mut value = json!({"contract_owner":ak(1),"election_contract":api::encode(ApiType::ContractPubkey,&[2;32]),"staking_contract":api::encode(ApiType::ContractPubkey,&[3;32]),"rewards_contract":api::encode(ApiType::ContractPubkey,&[4;32]),"child_epoch_length":3,"initial_validators":[{"owner":ak(5)}]});
    let parsed = presets::hyperchain::Settings::from_value(&value, Limits::default()).unwrap();
    let validator = &parsed.validators[0];
    assert_eq!(validator.owner, validator.sign_key);
    assert_eq!(validator.owner, validator.caller);
    assert_eq!(validator.stake, 0.into());
    assert!(!validator.restake);
    assert_eq!(parsed.pin_reward, 0.into());
    assert!(matches!(
        presets::hyperchain::Settings::from_value(
            &value,
            Limits {
                validators: 0,
                ..Default::default()
            }
        ),
        Err(presets::Error::Limit)
    ));
    value["initial_validators"][0]["restake"] = json!(0);
    assert!(presets::hyperchain::Settings::from_value(&value, Limits::default()).is_err());
    for input in [
        b"{}".as_slice(),
        b"{\"contracts\":[],\"calls\":[{}]}",
        b"{\"contracts\":[{}],\"calls\":[]}",
    ] {
        assert!(parsed.plan(input, Limits::default()).is_err());
    }
}
#[test]
fn file_selection_distinguishes_custom_empty_missing_invalid_and_bounded_assets() {
    use presets::{Asset, Error};
    let tmp = Temp::new();
    let data = tmp.0.join("aecore");
    let good = tmp.write("accounts.json", format!("{{\"{}\":1}}", ak(1)));
    let forks = json!({"6":{"height":0}});
    let custom = json!(good);
    assert_eq!(
        presets::source(
            &data,
            b"private",
            false,
            6,
            Asset::Accounts,
            Some(&forks),
            Some(&custom)
        )
        .unwrap(),
        Source::Empty
    );
    assert!(matches!(
        presets::source(
            &data,
            b"private",
            true,
            6,
            Asset::Contracts,
            Some(&forks),
            Some(&custom)
        ),
        Err(Error::MissingConfiguration)
    ));
    let forks = json!({"6":{"height":0,"accounts_file":"relative.json"}});
    let relative = tmp.write("aecore/relative.json", format!("{{\"{}\":2}}", ak(2)));
    assert_eq!(
        presets::source(
            &data,
            b"private",
            false,
            6,
            Asset::Accounts,
            Some(&forks),
            Some(&custom)
        )
        .unwrap(),
        Source::File {
            path: relative,
            custom_prefunded: false
        }
    );
    let source = presets::source(
        &data,
        b"private",
        false,
        6,
        Asset::Accounts,
        Some(&json!({"6":0})),
        Some(&custom),
    )
    .unwrap();
    assert_eq!(source.read(1000).unwrap(), std::fs::read(&good).unwrap());
    assert!(matches!(source.read(1), Err(Error::Limit)));
    let missing = Source::File {
        path: tmp.0.join("absent"),
        custom_prefunded: false,
    };
    assert!(matches!(
        missing.read(100),
        Err(Error::Io {
            kind: std::io::ErrorKind::NotFound,
            ..
        })
    ));
    let missing = Source::File {
        path: tmp.0.join("absent"),
        custom_prefunded: true,
    };
    assert!(matches!(
        missing.read(100),
        Err(Error::CustomFileNotFound(_))
    ));
    assert!(
        Source::File {
            path: tmp.0.clone(),
            custom_prefunded: false
        }
        .read(100)
        .is_err()
    );
    for bytes in [
        b"null".as_slice(),
        b"true",
        b"[1]",
        b"{",
        b"{} 0",
        b"{\"bad\":1}",
        b"{\"bad\":1,\"bad\":2}",
    ] {
        assert!(
            presets::parse_accounts(bytes, Limits::default()).is_err(),
            "{bytes:?}"
        );
    }
    assert!(
        presets::parse_accounts(b"[]", Limits::default())
            .unwrap()
            .is_empty()
    );
    let twice = format!("{{\"{}\":1,\"{}\":2}}", ak(1), ak(1));
    assert_eq!(
        presets::parse_accounts(twice.as_bytes(), Limits::default())
            .unwrap()
            .len(),
        2
    );
    assert!(matches!(
        presets::parse_accounts(
            twice.as_bytes(),
            Limits {
                accounts: 1,
                ..Default::default()
            }
        ),
        Err(Error::Limit)
    ));
}
fn unpack(s: &str) -> Value {
    fn expand(v: &Value, p: &[Value]) -> Value {
        match v {
            Value::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                expand(&p[v["$ref"].as_u64().unwrap() as usize], p)
            }
            Value::Object(m) => {
                Value::Object(m.iter().map(|(k, v)| (k.clone(), expand(v, p))).collect())
            }
            Value::Array(a) => Value::Array(a.iter().map(|v| expand(v, p)).collect()),
            v => v.clone(),
        }
    }
    let d: Value = serde_json::from_str(s).unwrap();
    expand(&d["root"], d["values"].as_array().unwrap())
}
fn native_bytes(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn public(v: &Value) -> String {
    api::encode(ApiType::AccountPubkey, &native_bytes(v))
}
fn num(v: &Value) -> Value {
    if let Some(s) = v.as_str() {
        serde_json::from_str(s).unwrap()
    } else {
        v.clone()
    }
}
fn contract_numbers(mut v: Value) -> Value {
    for key in [
        "amount",
        "vm_version",
        "abi_version",
        "nonce",
        "fee",
        "gas",
        "gas_price",
    ] {
        if let Some(value) = v.get_mut(key) {
            *value = num(value);
        }
    }
    v
}
#[test]
fn configured_hyperchain_genesis_matches_original_contract_roots_and_reopens() {
    let data = unpack(include_str!(
        "../../ae-hyperchain/tests/fixtures/erlang_hc_genesis.json"
    ));
    for row in data["rows"].as_array().unwrap() {
        let tmp = Temp::new();
        let ctx = tmp.context();
        let protocol = row["protocol"].as_u64().unwrap();
        let specs: Vec<_> = row["specs"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(contract_numbers)
            .collect();
        let calls: Vec<_> = row["calls"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(contract_numbers)
            .collect();
        let contract_path = tmp.write(
            "contracts.json",
            serde_json::to_vec(&json!({"contracts":specs,"calls":calls})).unwrap(),
        );
        let mut owner = [0u8; 32];
        owner[31] = 1;
        let validators:Vec<_>=row["validators"].as_array().unwrap().iter().map(|v|json!({"owner":public(&v["owner"]),"caller":public(&v["caller"]),"sign_key":public(&v["sign_key"]),"stake":num(&v["stake"]),"restake":v["restake"]})).collect();
        let cfg=Configuration::from_value(json!({"chain":{"hard_forks":{protocol.to_string():{"height":0,"contracts_file":contract_path}},"consensus":{"0":{"type":"hyperchain","config":{"contract_owner":api::encode(ApiType::AccountPubkey,&owner),"election_contract":specs[1]["pubkey"],"rewards_contract":specs[1]["pubkey"],"staking_contract":specs[0]["pubkey"],"initial_validators":validators,"child_epoch_length":3,"pinning_reward_value":7}}}}})).unwrap();
        let s = settings(&cfg, &ctx, b"ae_hc_reference");
        let base = execution_fixture::execution_with_consensus(s.profile(), ConsensusType::Pos);
        let order = execution_fixture::otp_order::profile(&data["profile"]);
        let execution = base.with_config(|c| {
            let f = c.fate.unwrap();
            let fate = ae_execution::contracts::fate::Config {
                order: &order,
                history: f.history.clone(),
                auth_tx: f.auth_tx.clone(),
                diagnostic_profile: ae_vm_fate::diagnostic::Profile::Release,
                ..*f
            };
            Owned::capture(&transactions::Config {
                fate: Some(&fate),
                ..*c
            })
            .unwrap()
        });
        let mut initial = empty();
        for a in row["initial"]["accounts"].as_array().unwrap() {
            let key = AccountPublicKey(native_bytes(&a["key"]).try_into().unwrap());
            let account = Account::deserialize(key, &native_bytes(&a["encoded"])).unwrap();
            initial.accounts.enter(&account).unwrap();
        }
        let before = initial.roots();
        let original_contracts = std::fs::read(&contract_path).unwrap();
        let mut bad_contracts: Value = serde_json::from_slice(&original_contracts).unwrap();
        bad_contracts["contracts"][0]["pubkey"] =
            json!(api::encode(ApiType::ContractPubkey, &[99; 32]));
        std::fs::write(&contract_path, serde_json::to_vec(&bad_contracts).unwrap()).unwrap();
        assert!(matches!(
            Prepared::load(
                &cfg.resolve(&ctx).unwrap(),
                Some(b"ae_hc_reference"),
                &initial,
                &execution,
                fingerprint(),
                &6_000_000.into(),
                Limits::default()
            ),
            Err(bootstrap::genesis::Error::Hyperchain(_))
        ));
        assert_eq!(initial.roots(), before);
        assert!(!tmp.0.join("db").exists());
        std::fs::write(&contract_path, original_contracts).unwrap();
        let load = || {
            Prepared::load(
                &cfg.resolve(&ctx).unwrap(),
                Some(b"ae_hc_reference"),
                &initial,
                &execution,
                fingerprint(),
                &6_000_000.into(),
                Limits::default(),
            )
            .unwrap()
        };
        let prepared = load();
        let expected = &row["stages"].as_array().unwrap().last().unwrap()["trees"];
        assert_eq!(
            hex::encode(prepared.trees().roots().hash().0),
            expected["state_root"],
            "protocol {protocol}"
        );
        assert!(prepared.trees().roots().calls.is_none());
        assert_eq!(prepared.inputs().len(), 2);
        let path = tmp.0.join("db");
        let opened = prepared.open(&path, Default::default()).unwrap();
        let identity = opened.identity().clone();
        assert_eq!(identity.consensus, ConsensusType::Pos);
        let status = opened.database().view().unwrap().status();
        drop(opened);
        let reopened = load().open(&path, Default::default()).unwrap();
        assert_eq!(reopened.database().view().unwrap().status(), status);
        assert_eq!(reopened.identity(), &identity);
        drop(reopened);
        let specification = startup_profile::specification(&data["profile"]);
        let configured = ae_node::bootstrap::configured::Prepared::load(
            &cfg.resolve(&ctx).unwrap(),
            Some(b"ae_hc_reference"),
            &tmp.0,
            &initial,
            &specification,
            Default::default(),
            &6_000_000.into(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            hex::encode(configured.genesis().trees().roots().hash().0),
            expected["state_root"]
        );
        let profile_hash = configured.execution().fingerprint();
        let configured = configured
            .open(tmp.0.join("configured-db"), Default::default())
            .unwrap();
        assert_eq!(
            configured.chain().identity().execution_profile,
            profile_hash
        );
        drop(configured);
        assert_eq!(
            hex::encode(initial.roots().hash().0),
            row["initial"]["state_root"]
        );
    }
    println!(
        "Genesis assembly: configured original Hyperchain contracts, validators, epochs, roots and durable reopen at protocols 6–8"
    );
}
