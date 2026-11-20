use super::*;
use ae_execution::{
    Origin,
    contracts::{self, aevm, fate},
};
use ae_state::{calls::Calls, names::Names, oracles::Oracles};
use ae_vm_fate::{
    machine,
    maps::{listing::Overrides, order::WordSize},
};
use std::sync::Arc;
static DATA: LazyLock<J> = LazyLock::new(|| {
    unpack(include_str!(
        "../fixtures/erlang_paying_for_execution_vectors.json"
    ))
});
fn trees(v: &J) -> Trees {
    let mut t = initial(v);
    let c = tree(field(v, "contracts"));
    let calls = tree(field(v, "calls"));
    let n = tree(field(v, "names"));
    let nc = tree(field(v, "name_cache"));
    let o = tree(field(v, "oracles"));
    let oc = tree(field(v, "oracle_cache"));
    t.contracts = Contracts::from_root(c.root_hash(), c.into_store()).unwrap();
    t.calls = Calls::from_root(calls.root_hash(), calls.into_store()).unwrap();
    t.channels = tree(field(v, "channels")).into();
    t.names = Names::from_roots(
        n.root_hash(),
        nc.root_hash(),
        n.into_store(),
        nc.into_store(),
    )
    .unwrap();
    t.oracles = Oracles::from_roots(
        o.root_hash(),
        oc.root_hash(),
        o.into_store(),
        oc.into_store(),
    )
    .unwrap();
    assert_eq!(bin(t.roots().hash().0), *field(v, "root"));
    t
}
fn check<A: NodeReader, C: NodeReader>(t: &Trees<A, C>, r: &Receipt, v: &J) {
    check_snapshot(t, r, v);
    for (name, tree) in [
        ("contracts", t.contracts.tree()),
        ("calls", t.calls.tree()),
        ("channels", t.channels.tree()),
        ("names", t.names.tree()),
        ("name_cache", t.names.cache()),
        ("oracles", t.oracles.tree()),
        ("oracle_cache", t.oracles.cache()),
    ] {
        assert_eq!(entries(tree), *field(v, name), "{name}");
    }
}
fn vm() -> machine::Context {
    machine::Context {
        current_contract: [0; 32],
        vm_version: ae_vm_fate::types::Version::Three,
        in_auth: false,
        on_chain: true,
        reserved_gas: 0.into(),
        arithmetic_word_size: WordSize::Bits64,
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
    }
}
fn small(n: u8) -> [u8; 32] {
    let mut b = [0; 32];
    b[31] = n;
    b
}
fn environment() -> machine::program::remote::context::Environment {
    machine::program::remote::context::Environment {
        auth_tx_hash: None,
        origin: [0; 32],
        gas_price: 1_000_000.into(),
        fee: 1_000_000_000_000u64.into(),
        beneficiary: small(5),
        timestamp: 12345.into(),
        difficulty: 7.into(),
        block_gas_limit: 6_000_000.into(),
        network_id: b"ae_mainnet".to_vec(),
        consensus: machine::program::remote::context::Consensus::Pow,
        key_hash: [0; 32],
    }
}
fn aevm() -> aevm::Config {
    aevm::Config {
        environment: aevm::Environment {
            genesis_height: 0,
            key_hash: [0; 32],
            history: Arc::default(),
            beneficiary: AccountPublicKey(small(5)),
            difficulty: 7.into(),
            timestamp_ms: 12345.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
    }
}
fn contract_reason(e: &contracts::Error) -> &str {
    match e {
        contracts::Error::Rejected(s) => s,
        contracts::Error::Primitive(e) => {
            reason::<MemoryStore, MemoryStore>(&Error::Primitive(e.clone()))
        }
        e => panic!("{e:?}"),
    }
}
fn failure<A, C>(e: &Error<A, C>) -> J {
    let reason = match e {
        Error::Contract(e) => contract_reason(e),
        Error::Aevm(e) => match e.as_ref() {
            ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Host(
                aevm::HostError::Transaction(e),
            )) => contract_reason(e),
            _ => panic!("unexpected AEVM error"),
        },
        Error::ContractCallContextCase(Origin::Contract) => {
            return json!({"kind":"native","class":"error","reason":tuple(vec![atom("case_clause"),atom("aetx_contract")])});
        }
        e => reason(e),
    };
    json!({"kind":"ok","value":tuple(vec![atom("error"),atom(reason)])})
}
fn tx_context(row: &J) -> Context {
    let mut c = context(row);
    c.origin = match row["context"].as_str().unwrap() {
        "aetx_ga" => Origin::Generalized,
        "aetx_contract" => Origin::Contract,
        _ => Origin::Transaction,
    };
    c
}
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
    limits: SignatureLimits,
    fate_steps: usize,
    aevm_limits: ae_vm_aevm::execution::Limits,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&DATA["profile"]);
    let overrides = Overrides::default();
    let f = fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &env,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let a = aevm();
    let config = Config {
        network: &network,
        fate: Some(&f),
        aevm: Some(&a),
        channel_limits: Default::default(),
        consensus: ae_protocol::ConsensusType::Pow,
        channel_state_atoms: None,
        signature_limits: limits,
        oracle_limits: Default::default(),
        fate_limits: Default::default(),
        maximum_fate_steps: fate_steps,
        aevm_limits,
    };
    let s = SignedTransaction::decode(&bytes(&row["signed"])).unwrap();
    transactions::execute_signed(&s, t, &tx_context(row), &config)
}
fn run<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
) -> Result<Receipt, Error<A, C>> {
    execute(
        row,
        t,
        SignatureLimits::default(),
        1_000_000,
        Default::default(),
    )
}
#[test]
fn sponsored_all_leaf_callbacks_both_vms_and_failure_settlement_match_original() {
    let rows = DATA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1600);
    let mut counts = [0; 3];
    for row in rows {
        let mut t = trees(&row["before"]);
        let before = (
            t.roots(),
            t.names.cache_root_hash(),
            t.oracles.cache_root_hash(),
        );
        match run(row, &mut t) {
            Ok(r) => {
                counts[0] += 1;
                assert_eq!(row["result"]["kind"], "ok", "{row}");
                let v = &row["result"]["value"];
                assert_eq!(v[1][0], atom("ok"), "{row}");
                check(&t, &r, &v[1][1]);
                check_used_gas(&r.gas, &v[1][2]);
                assert_eq!(
                    transactions::accounting::used_gas(
                        signed(row).transaction().view(),
                        &50.into(),
                        tx_context(row).protocol,
                        &t
                    )
                    .unwrap(),
                    magnitude(&r.gas)
                );
            }
            Err(e) => {
                if matches!(e, Error::ContractCallContextCase(_)) {
                    counts[2] += 1
                } else {
                    counts[1] += 1
                }
                assert_eq!(
                    failure(&e),
                    row["result"],
                    "{} {} {} {}: {e:?}",
                    row["protocol"],
                    row["operation"],
                    row["mode"],
                    row["context"]
                );
                assert_eq!(
                    (
                        t.roots(),
                        t.names.cache_root_hash(),
                        t.oracles.cache_root_hash()
                    ),
                    before
                );
            }
        }
    }
    assert_eq!(counts, [376, 1160, 64]);
}

#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;

#[path = "batch.rs"]
mod batch;

#[test]
fn strict_and_authenticated_signer_derivation_matches_original() {
    let network = NetworkProfile::mainnet();
    let rows = DATA["signatures"].as_array().unwrap();
    assert_eq!(rows.len(), 2560);
    let mut counts = [0; 2];
    for row in rows {
        let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        assert_eq!(signed.encode(), bytes(&row["signed"]));
        let mut c = Context::transaction(
            50,
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        if row["payer"] == true {
            c.payer = Some(ae_crypto::Ed25519SecretKey::from_seed(&small(2)).public_key());
        }
        for k in row["authenticated"].as_array().unwrap() {
            c.ga_authenticated.insert(
                ae_crypto::Ed25519SecretKey::from_seed(&small(k.as_u64().unwrap() as u8))
                    .public_key(),
            );
        }
        for (i, label, r) in [
            (
                0,
                "strict",
                transactions::verify(
                    signed.transaction().view(),
                    signed.signatures(),
                    &network,
                    c.protocol,
                    false,
                    SignatureLimits::default(),
                ),
            ),
            (
                1,
                "with_context",
                transactions::verify_with_context(
                    signed.transaction().view(),
                    signed.signatures(),
                    &network,
                    &c,
                    SignatureLimits::default(),
                ),
            ),
        ] {
            let actual = match r {
                Ok(()) => {
                    counts[i] += 1;
                    atom("ok")
                }
                Err(SignatureError::CheckFailed) => {
                    tuple(vec![atom("error"), atom("signature_check_failed")])
                }
                Err(SignatureError::VerificationFailed) => {
                    tuple(vec![atom("error"), atom("signature_verification_failed")])
                }
                e => panic!("{e:?}"),
            };
            assert_eq!(
                json!({"kind":"ok","value":actual}),
                row[label],
                "{label}: {row}"
            );
        }
    }
    assert!(counts.iter().all(|n| *n > 100));
}
#[path = "failures.rs"]
mod failures;

#[path = "ga_attach.rs"]
mod ga_attach;

#[path = "ga_meta.rs"]
mod ga_meta;
