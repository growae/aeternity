use super::*;
use ae_execution::contracts::{self, aevm, fate};
use ae_vm_fate::{
    machine,
    maps::{listing::Overrides, order::WordSize},
};
use std::sync::Arc;
#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
pub(super) mod otp_order;
pub(super) static WRAPPED: LazyLock<J> = LazyLock::new(|| {
    unpack(include_str!(
        "../fixtures/erlang_channel_wrapped_execution.json"
    ))
});
pub(super) fn vm() -> machine::Context {
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
pub(super) fn environment() -> machine::program::remote::context::Environment {
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
pub(super) fn aevm() -> aevm::Config {
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

pub(super) fn run<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    s: &SignedTransaction<Transaction>,
    t: &mut Trees<A, C>,
    c: &Context,
) -> Result<Receipt, Error<A, C>> {
    execute(s, t, c, SignatureLimits::default())
}
pub(super) fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    s: &SignedTransaction<Transaction>,
    t: &mut Trees<A, C>,
    c: &Context,
    signature_limits: SignatureLimits,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&WRAPPED["profile"]);
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
        signature_limits,
        oracle_limits: Default::default(),
        fate_limits: Default::default(),
        maximum_fate_steps: 1_000_000,
        aevm_limits: Default::default(),
    };
    transactions::execute_signed(s, t, c, &config)
}
fn contract_error(e: &contracts::Error) -> J {
    let s = match e {
        contracts::Error::Rejected(s) => *s,
        contracts::Error::Primitive(e) => primitive(e),
        e => panic!("unprojected contract error {e:?}"),
    };
    json!({"kind":"ok","value":tuple(vec![atom("error"),atom(s)])})
}
fn failure(e: &Error) -> J {
    match e {
        Error::Contract(e) => contract_error(e),
        Error::Aevm(e) => match e.as_ref() {
            ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Host(
                aevm::HostError::Transaction(e),
            )) => contract_error(e),
            e => panic!("unprojected AEVM error {e:?}"),
        },
        Error::Signature(e) => verification(Err(e.clone())),
        e => captured_error(e),
    }
}
#[test]
fn signed_channel_wrappers_execute_both_original_authorization_vms() {
    assert_eq!(WRAPPED["runs"].as_array().unwrap().len(), 672);
    for (i, r) in WRAPPED["runs"].as_array().unwrap().iter().enumerate() {
        let s = SignedTransaction::<Transaction>::decode(&bytes(&r["signed"])).unwrap();
        let c = Context::transaction(
            50,
            Protocol::try_from(r["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        let mut t = initial(r);
        let before = t.roots();
        let result = run(&s, &mut t, &c);
        let expected = &r["result"];
        match result {
            Ok(receipt) => {
                if expected["kind"] != "ok" || expected["value"][1][0] != atom("ok") {
                    panic!(
                        "row {i} unexpected success: {} {} {}",
                        r["protocol"], r["operation"], r["mode"]
                    );
                }
                let state = &expected["value"][1][1];
                let actual = snapshot(&t, &receipt, Some(&s));
                for k in [
                    "root",
                    "accounts",
                    "channels",
                    "contracts",
                    "events",
                    "context",
                    "payer",
                ] {
                    if let Some(d) = difference(
                        &normalize(field(&actual, k)),
                        &normalize(field(state, k)),
                        k,
                    ) {
                        panic!(
                            "row {i} {} {} ABI {} protocol {}: {d}",
                            r["operation"], r["mode"], r["abi"], r["protocol"]
                        );
                    }
                }
                assert_eq!(
                    entries(t.calls.tree()),
                    *field(state, "calls"),
                    "calls row {i}"
                );
                assert_eq!(number(&receipt.gas), expected["value"][1][2], "gas row {i}");
                let ids: std::collections::BTreeSet<_> = field(state, "ga_auth_ids")[1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|b| AccountPublicKey(bytes(b).try_into().unwrap()))
                    .collect();
                assert_eq!(receipt.context.ga_authenticated, ids, "auth IDs row {i}");
                for pair in field(state, "ga_nonces")[1].as_array().unwrap() {
                    let key = AccountPublicKey(bytes(&pair[1][0]).try_into().unwrap());
                    let expected = &pair[1][1];
                    let actual = receipt
                        .context
                        .ga_nonces
                        .get(&key)
                        .map(|n| tuple(vec![atom("value"), bin(n)]))
                        .unwrap_or(atom("none"));
                    assert_eq!(actual, *expected, "nonce row {i}");
                }
            }
            Err(e) => {
                assert_eq!(t.roots(), before, "rollback row {i}");
                if let Some(d) = difference(&normalize(&failure(&e)), &normalize(expected), "") {
                    panic!(
                        "row {i} {} {} ABI {} protocol {}: {d}",
                        r["operation"], r["mode"], r["abi"], r["protocol"]
                    );
                }
            }
        }
    }
}
