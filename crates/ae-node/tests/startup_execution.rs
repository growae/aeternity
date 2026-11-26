use ae_codec::api::{self, ApiType};
use ae_node::{
    bootstrap::configured,
    configuration::{
        Configuration, auxiliary,
        chain::Settings,
        execution::{self, Bound, Resources, Specification},
        resolution::{Context, Directories},
    },
};
use ae_protocol::ConsensusType;
use ae_state::Trees;
use ae_vm_fate::{encoding, maps::order::Atom, value::Value as Fate};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
#[path = "../../ae-sync/tests/common/ledger_fixtures.rs"]
mod execution_fixture;
#[path = "common/execution_profile.rs"]
mod profile;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-startup-execution-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(p.join("aecore")).unwrap();
        Self(p)
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
            log: "/log".into(),
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
fn overrides() -> Vec<u8> {
    let key = Fate::Map([(Fate::Integer(1.into()), Fate::Integer(2.into()))].into());
    let value = Fate::Integer(9.into());
    let k = api::encode(
        ApiType::ContractBytearray,
        &encoding::encode(&key, Default::default()).unwrap(),
    );
    let v = api::encode(
        ApiType::ContractBytearray,
        &encoding::encode(&value, Default::default()).unwrap(),
    );
    serde_json::to_vec(&json!({k:v})).unwrap()
}

#[test]
fn auction_override_resolution_and_execution_identity_preserve_native_precedence() {
    let data: Value = serde_json::from_str(include_str!(
        "../../ae-execution/tests/fixtures/erlang_auction_override.json"
    ))
    .unwrap();
    let tmp = Temp::new();
    let mut seen = std::collections::BTreeSet::new();
    for row in data["transactions"].as_array().unwrap() {
        let setting = &row["configuration"];
        if !seen.insert(setting.to_string()) {
            continue;
        }
        let cfg = Configuration::from_value(setting["user"].clone()).unwrap();
        let mut ctx = tmp.context();
        if !setting["application"].is_null() {
            ctx.set(
                "aecore".into(),
                "name_claim_bid_timeout".into(),
                setting["application"].clone(),
            )
            .unwrap();
        }
        let resolved = cfg.resolve(&ctx).unwrap();
        let value = execution::name_claim_bid_timeout(&resolved).unwrap();
        let expected = row["override"]
            .as_str()
            .map(|s| ae_types::Unsigned::from_decimal(s).unwrap());
        assert_eq!(value, expected);
        for network in [
            ae_protocol::NetworkProfile::mainnet(),
            ae_protocol::NetworkProfile::uat(),
        ] {
            let aux = auxiliary::Plan::resolve(&resolved, network.network_id(), &tmp.0)
                .unwrap()
                .load(Default::default())
                .unwrap();
            let spec = Specification::from_value(&profile::default_value()).unwrap();
            let base = Bound::build(
                &spec,
                &aux,
                &network,
                ConsensusType::Pow,
                &6_000_000.into(),
                Default::default(),
            )
            .unwrap();
            let bound = Bound::build_with_name_auction_timeout(
                &spec,
                &aux,
                &network,
                ConsensusType::Pow,
                &6_000_000.into(),
                Default::default(),
                value.clone(),
            )
            .unwrap();
            assert_eq!(bound.name_claim_bid_timeout(), expected.as_ref());
            bound
                .execution()
                .with_config(|c| assert_eq!(c.name_claim_bid_timeout, expected.as_ref()));
            assert_eq!(
                base.fingerprint() == bound.fingerprint(),
                expected.is_none()
            );
            if expected.is_none() {
                assert_eq!(base.canonical(), bound.canonical());
            }
        }
    }
    assert_eq!(seen.len(), 7);
    for value in [
        json!(-1),
        json!(1.5),
        json!(false),
        json!("17"),
        Value::Null,
    ] {
        let mut ctx = tmp.context();
        ctx.set("aecore".into(), "name_claim_bid_timeout".into(), value)
            .unwrap();
        assert!(
            execution::name_claim_bid_timeout(&Configuration::empty().resolve(&ctx).unwrap())
                .is_err()
        );
    }
}
#[test]
fn explicit_profiles_reject_unsupported_choices_and_bind_each_semantic_input() {
    let tmp = Temp::new();
    let ctx = tmp.context();
    let cfg = Configuration::empty();
    let resolved = cfg.resolve(&ctx).unwrap();
    let settings = Settings::resolve(&resolved, Some(b"ae_dev")).unwrap();
    let aux = auxiliary::Plan::resolve(&resolved, b"ae_dev", &tmp.0)
        .unwrap()
        .load(Default::default())
        .unwrap();
    let raw = profile::default_value();
    let spec = Specification::from_value(&raw).unwrap();
    assert_eq!(spec.missing_atoms(), vec![Atom::Void, Atom::MaxTtl]);
    let bind = |v: &Value| {
        Bound::build(
            &Specification::from_value(v).unwrap(),
            &aux,
            settings.profile(),
            ConsensusType::Pow,
            &6_000_000.into(),
            Default::default(),
        )
        .unwrap()
    };
    let base = bind(&raw);
    assert_eq!(base.missing_atoms(), &[Atom::Void, Atom::MaxTtl]);
    let mut changed = vec![];
    for (field, value) in [
        ("fate_word_size", json!(32)),
        ("arithmetic_word_size", json!(32)),
        ("diagnostic", json!("test")),
        ("aevm_store", json!("otp26-word64-jenkins")),
        ("channel_state_atoms", json!([])),
    ] {
        let mut v = raw.clone();
        v[field] = value;
        changed.push(v);
    }
    let mut v = raw.clone();
    v["fate_mixing"] = json!(if raw["fate_mixing"] == "crc32c" {
        "jenkins"
    } else {
        "crc32c"
    });
    changed.push(v);
    let mut v = raw.clone();
    v["fate_atoms"]["integer"] = json!(10_000_000);
    changed.push(v);
    for v in changed {
        assert_ne!(base.fingerprint(), bind(&v).fingerprint());
    }
    let mut a = raw.clone();
    a["channel_state_atoms"] = json!(["x", "y", "x"]);
    let mut b = a.clone();
    b["channel_state_atoms"] = json!(["y", "x"]);
    assert_eq!(bind(&a).canonical(), bind(&b).canonical());
    for (field, value) in [
        ("version", json!(2)),
        ("unicode", json!("host")),
        ("bls", json!("auto")),
        ("aevm_float", json!("native")),
        ("fate_mixing", json!("host")),
        ("diagnostic", json!("default")),
    ] {
        let mut v = raw.clone();
        v[field] = value;
        assert!(Specification::from_value(&v).is_err());
    }
    let mut v = raw.clone();
    v["extra"] = json!(true);
    assert!(Specification::from_value(&v).is_err());
    let mut v = raw.clone();
    v.as_object_mut().unwrap().remove("unicode");
    assert!(Specification::from_value(&v).is_err());
    let mut v = raw.clone();
    v["fate_atoms"]["integer"] = v["fate_atoms"]["boolean"].clone();
    assert!(matches!(
        Specification::from_value(&v),
        Err(execution::Error::Order(_))
    ));
    let limits = Resources {
        fate_steps: 77,
        ..Default::default()
    };
    let bounded = Bound::build(
        &spec,
        &aux,
        settings.profile(),
        ConsensusType::Pow,
        &6_000_000.into(),
        limits,
    )
    .unwrap();
    assert_eq!(base.fingerprint(), bounded.fingerprint());
    bounded
        .execution()
        .with_config(|c| assert_eq!(c.maximum_fate_steps, 77));
    let changed_gas = Bound::build(
        &spec,
        &aux,
        settings.profile(),
        ConsensusType::Pow,
        &1.into(),
        limits,
    )
    .unwrap();
    assert_ne!(base.fingerprint(), changed_gas.fingerprint());
    assert_eq!(changed_gas.effective_gas_limit(), &1.into());
    let public = Settings::resolve(&resolved, Some(b"ae_mainnet")).unwrap();
    assert!(matches!(
        Bound::build(
            &spec,
            &aux,
            public.profile(),
            ConsensusType::Pow,
            &1.into(),
            limits
        ),
        Err(execution::Error::Network)
    ));
    let public_aux = auxiliary::Plan::resolve(&resolved, b"ae_mainnet", &tmp.0)
        .unwrap()
        .load(Default::default())
        .unwrap();
    let public_a = Bound::build(
        &spec,
        &public_aux,
        public.profile(),
        ConsensusType::Pow,
        &1.into(),
        limits,
    )
    .unwrap();
    let public_b = Bound::build(
        &spec,
        &public_aux,
        public.profile(),
        ConsensusType::Pow,
        &6_000_000.into(),
        limits,
    )
    .unwrap();
    assert_eq!(public_a.fingerprint(), public_b.fingerprint());
    assert_eq!(public_a.effective_gas_limit(), &6_000_000.into());
}
#[test]
fn owned_execution_receives_historical_overrides_and_explicit_resource_limits() {
    let tmp = Temp::new();
    let bytes = overrides();
    tmp.write("aecore/.pre_iris_map_ordering_test.json", &bytes);
    let ctx = tmp.context();
    let cfg = Configuration::empty();
    let resolved = cfg.resolve(&ctx).unwrap();
    let settings = Settings::resolve(&resolved, Some(b"ae_dev")).unwrap();
    let plan = auxiliary::Plan::resolve(&resolved, b"ae_dev", &tmp.0).unwrap();
    let aux = plan.load(Default::default()).unwrap();
    let mut raw = profile::default_value();
    raw["channel_state_atoms"] = json!(["state", "ok"]);
    let spec = Specification::from_value(&raw).unwrap();
    let mut resources = Resources::default();
    resources.signatures.max_layers = 7;
    resources.channels.state.max_entries = 31;
    resources.oracles.fate.max_nodes = 41;
    resources.fate.max_nodes = 3333;
    resources.fate_steps = 1234;
    resources.aevm.max_work = 2222;
    let bound = Bound::build(
        &spec,
        &aux,
        settings.profile(),
        ConsensusType::Pow,
        &6_000_000.into(),
        resources,
    )
    .unwrap();
    bound.execution().with_config(|c| {
        assert_eq!(c.signature_limits.max_layers, 7);
        assert_eq!(c.channel_limits.state.max_entries, 31);
        assert_eq!(c.oracle_limits.fate.max_nodes, 41);
        assert_eq!(c.fate_limits.max_nodes, 3333);
        assert_eq!(c.maximum_fate_steps, 1234);
        assert_eq!(c.aevm_limits.max_work, 2222);
        assert!(c.channel_state_atoms.unwrap().contains(b"state".as_slice()));
        let f = c.fate.unwrap();
        let map = [(Fate::Integer(1.into()), Fate::Integer(2.into()))].into();
        assert_eq!(
            f.overrides
                .list(map, 4, f.order, c.fate_limits)
                .unwrap()
                .value
                .as_ref(),
            &Fate::Integer(9.into())
        );
        let map = [(Fate::Integer(1.into()), Fate::Integer(2.into()))].into();
        assert_eq!(
            f.overrides
                .list(map, 5, f.order, c.fate_limits)
                .unwrap()
                .value
                .as_ref(),
            &Fate::List(vec![Fate::Tuple(vec![
                Fate::Integer(1.into()),
                Fate::Integer(2.into())
            ])])
        );
        assert_eq!(f.environment.network_id, b"ae_dev");
        assert_eq!(
            f.diagnostic_profile,
            ae_vm_fate::diagnostic::Profile::Release
        );
        assert_eq!(
            c.aevm.unwrap().float_profile,
            ae_vm_aevm::gas::FloatProfile::Otp26Word64
        );
    });
    let mut altered = bytes.clone();
    altered.push(b'\n');
    std::fs::write(&plan.ordering, &altered).unwrap();
    let changed = plan.load(Default::default()).unwrap();
    let other = Bound::build(
        &spec,
        &changed,
        settings.profile(),
        ConsensusType::Pow,
        &6_000_000.into(),
        resources,
    )
    .unwrap();
    assert_ne!(bound.fingerprint(), other.fingerprint());
    std::fs::write(&plan.ordering, bytes).unwrap();
    let cfg2 = Configuration::from_value(
        json!({"sync":{"whitelist_file":tmp.write("white.json",b"{}").to_str().unwrap()}}),
    )
    .unwrap();
    let aux2 = auxiliary::Plan::resolve(&cfg2.resolve(&ctx).unwrap(), b"ae_dev", &tmp.0)
        .unwrap()
        .load(Default::default())
        .unwrap();
    assert_eq!(
        bound.fingerprint(),
        Bound::build(
            &spec,
            &aux2,
            settings.profile(),
            ConsensusType::Pow,
            &6_000_000.into(),
            resources
        )
        .unwrap()
        .fingerprint()
    );
    println!(
        "Execution startup: explicit historical VM/resource choices, pre-Iris override application, protocol boundary and content-bound identity"
    );
}
#[tokio::test]
async fn configured_public_genesis_whitelist_admission_and_profile_bound_reopen() {
    let tmp = Temp::new();
    tmp.write(
        "aecore/.genesis/accounts.json",
        include_str!("../../ae-state/data/mainnet_accounts.json"),
    );
    let white = tmp.write(
        "aecore/.block_whitelist.json",
        serde_json::to_vec(&json!({"1":api::encode(ApiType::KeyBlockHash,&[9;32])})).unwrap(),
    );
    let ctx = tmp.context();
    let cfg = Configuration::empty();
    let resolved = cfg.resolve(&ctx).unwrap();
    let raw = profile::default_value();
    let spec = Specification::from_value(&raw).unwrap();
    let prepare = |spec: &Specification| {
        configured::Prepared::load(
            &resolved,
            Some(b"ae_mainnet"),
            &tmp.0,
            &empty(),
            spec,
            Default::default(),
            &6_000_000.into(),
            Default::default(),
            Default::default(),
        )
        .unwrap()
    };
    let path = tmp.0.join("db");
    let opened = prepare(&spec).open(&path, Default::default()).unwrap();
    let identity = opened.chain().identity().clone();
    let fingerprint = opened.execution().fingerprint();
    assert_eq!(identity.execution_profile, fingerprint);
    let block = execution_fixture::blocks(opened.chain().settings().profile()).remove(0);
    let time = match &block {
        ae_chain::block::Block::Key(k) => k.time + 60_000,
        _ => panic!(),
    };
    let make_importer = |opened: &configured::Open| {
        let mut rules = execution_fixture::rules(opened.chain().identity().genesis);
        rules.checkpoints = opened.checkpoints();
        ae_sync::chain::Importer::new(
            Arc::clone(opened.chain().database()),
            opened.execution().execution().clone(),
            rules,
            Default::default(),
        )
        .unwrap()
    };
    let importer = make_importer(&opened);
    let before = opened.chain().database().view().unwrap().status();
    let error = importer
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert!(matches!(
        error.error,
        ae_sync::chain::Error::BlockedByWhitelist
    ));
    assert_eq!(opened.chain().database().view().unwrap().status(), before);
    importer.stop();
    importer.join().await;
    drop(importer);
    drop(opened);
    std::fs::write(
        &white,
        serde_json::to_vec(
            &json!({"1":api::encode(ApiType::KeyBlockHash,&block.hash().unwrap().0)}),
        )
        .unwrap(),
    )
    .unwrap();
    let opened = prepare(&spec).open(&path, Default::default()).unwrap();
    assert_eq!(opened.execution().fingerprint(), fingerprint);
    let importer = make_importer(&opened);
    let report = importer
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(report.accepted.len(), 1);
    assert_eq!(
        opened.chain().database().view().unwrap().status().tip,
        Some(block.hash().unwrap())
    );
    importer.stop();
    importer.join().await;
    drop(importer);
    drop(opened);
    let mut changed = raw.clone();
    changed["diagnostic"] = json!("test");
    assert!(
        prepare(&Specification::from_value(&changed).unwrap())
            .open(&path, Default::default())
            .is_err()
    );
    let opened = prepare(&spec).open(&path, Default::default()).unwrap();
    assert_eq!(opened.chain().identity(), &identity);
    assert_eq!(
        opened.chain().database().view().unwrap().status().tip,
        Some(block.hash().unwrap())
    );
    drop(opened);
    tmp.write("aecore/.pre_iris_map_ordering.json", b"{}");
    assert!(prepare(&spec).open(&path, Default::default()).is_err());
    println!(
        "Configured startup: canonical mainnet genesis, actual first key block whitelist rejection/admission, retained chain and changed-profile reopen rejection"
    );
}
