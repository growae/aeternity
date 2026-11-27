use ae_execution::{
    Context, contracts, dry_run as dry,
    transactions::{self, batch},
};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeReader, Overlay, OverlayLimits};
use ae_tx::{SignedTransaction, Transaction, TransactionKind};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::{machine, maps::listing::Overrides};
use num_bigint::{BigInt, Sign};
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};

#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;

static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    fn unpack(v: &J, pool: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, pool)))
                    .collect(),
            ),
            J::Array(a) => J::Array(a.iter().map(|v| unpack(v, pool)).collect()),
            v => v.clone(),
        }
    }
    let v: J =
        serde_json::from_str(include_str!("fixtures/erlang_dry_run_sequences.json")).unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn small(n: u8) -> AccountPublicKey {
    let mut key = [0; 32];
    key[31] = n;
    AccountPublicKey(key)
}
fn amount(v: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, v.as_bytes())
}
fn kind(k: TransactionKind) -> &'static str {
    match k {
        TransactionKind::Spend => "spend_tx",
        TransactionKind::ContractCall => "contract_call_tx",
        k => panic!("unexpected {k:?}"),
    }
}
fn event(v: &batch::Event) -> J {
    use ae_execution::{Event as P, contracts::Event as C, transactions::Event as E};
    let mut out = match &v.value {
        E::Primitive(P::Spend {
            from,
            to,
            amount: a,
        }) => {
            json!({"kind":"spend","from":hex::encode_upper(from.0),"to":hex::encode_upper(to.0),"amount":amount(a).to_string(),"info":"Spend.amount"})
        }
        E::Primitive(P::FeeDebit { account, amount: a }) => {
            json!({"kind":"delta","key":hex::encode_upper(account.0),"amount":(-amount(a)).to_string(),"info":"Chain.fee"})
        }
        E::Contract(C::Delta {
            account,
            amount: a,
            debit,
            info,
        }) => {
            json!({"kind":"delta","key":hex::encode_upper(account.0),"amount":(if *debit {-amount(a)}else{amount(a)}).to_string(),"info":info})
        }
        e => panic!("unexpected event {e:?}"),
    };
    out["type"] = json!(kind(v.transaction_type));
    out["tx_hash"] = if out["kind"] == "spend" {
        json!(hex::encode_upper(v.transaction_hash.0))
    } else {
        json!(hex::encode(v.transaction_hash.0))
    };
    out
}
fn profile(row: &J) -> dry::Profile {
    match row["profile"].as_str().unwrap() {
        "public" => dry::Profile::Public,
        "internal" => dry::Profile::Internal,
        "replay" => dry::Profile::Replay,
        "includability" => dry::Profile::Includability,
        "missing" => dry::Profile::Unspecified,
        "unknown" => dry::Profile::Other,
        _ => panic!(),
    }
}
fn configured<R>(profile: &J, f: impl FnOnce(&transactions::Config<'_>) -> R) -> R {
    let network = NetworkProfile::mainnet();
    let vm = machine::Context {
        current_contract: [0; 32],
        vm_version: ae_vm_fate::types::Version::Three,
        in_auth: false,
        on_chain: true,
        reserved_gas: 0.into(),
        arithmetic_word_size: ae_vm_fate::maps::order::WordSize::Bits64,
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
    };
    let environment = machine::program::remote::context::Environment {
        auth_tx_hash: None,
        origin: [0; 32],
        gas_price: 0.into(),
        fee: 0.into(),
        beneficiary: [0; 32],
        timestamp: 0.into(),
        difficulty: 0.into(),
        block_gas_limit: 6_000_000.into(),
        network_id: b"ae_mainnet".to_vec(),
        consensus: machine::program::remote::context::Consensus::Pow,
        key_hash: [0; 32],
    };
    let order = otp_order::profile(profile);
    let overrides = Overrides::default();
    let fate = contracts::fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &environment,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    f(&transactions::Config {
        fate: Some(&fate),
        ..transactions::Config::primitives(&network)
    })
}
fn trees(row: &J) -> Trees<Overlay<MemoryStore>, Overlay<MemoryStore>> {
    Trees::deserialize_from_binary(
        &bytes(&row["initial"]),
        Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        Default::default(),
    )
    .unwrap()
}
fn funding() -> Vec<dry::Funding> {
    vec![
        dry::Funding {
            account: small(9),
            amount: 19.into(),
        },
        dry::Funding {
            account: small(9),
            amount: 23.into(),
        },
    ]
}
fn signed(row: &J) -> Vec<SignedTransaction<Transaction>> {
    row["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| SignedTransaction::decode(&bytes(&v["signed"])).unwrap())
        .collect()
}
fn prepared<'a>(row: &J, signed: &'a [SignedTransaction<Transaction>]) -> Vec<dry::Prepared<'a>> {
    signed
        .iter()
        .zip(row["transactions"].as_array().unwrap())
        .map(|(tx, v)| dry::Prepared {
            stateless: v["stateless"].as_bool().unwrap(),
            ..dry::Prepared::transaction(tx)
        })
        .collect()
}
#[test]
fn original_sequences_calls_events_and_profile_metering() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 560);
    for runtime_profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
        for (i, row) in rows.iter().enumerate() {
            let initial = trees(row);
            let before = initial.serialize_to_binary(Default::default()).unwrap();
            let signed = signed(row);
            let prepared = prepared(row, &signed);
            let base = Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap();
            let options = dry::Options {
                profile: profile(row),
                store_read_gas_metering: row["metering_enabled"].as_bool().unwrap(),
                tx_events: row["events_enabled"].as_bool().unwrap(),
                ..Default::default()
            };
            let (protocol, floor) = options
                .profile
                .metering(base, options.store_read_gas_metering);
            assert_eq!(json!(protocol as u32), row["effective_protocol"], "row {i}");
            assert_eq!(
                json!(floor.map(|p| p as u32)),
                row["metering_floor"],
                "row {i}"
            );
            let out = configured(runtime_profile, |cfg| {
                dry::run(
                    initial.clone(),
                    &Context::transaction(50, base),
                    cfg,
                    &funding(),
                    &prepared,
                    &options,
                )
            })
            .unwrap();
            let results:Vec<_>=out.results.iter().map(|r| {
            match &r.result {
                Ok(s)=>{let mut j=json!({"type":kind(r.transaction_type),"status":"ok","events":s.events.iter().map(event).collect::<Vec<_>>()});if let Some(c)=&s.call {j["call"]=json!(hex::encode_upper(c.serialize().unwrap()));j["call_id"]=json!(hex::encode_upper(c.id));}j},
                Err(batch::Failure::Transaction(e))=>{let reason=transactions::reasons::transaction(e).unwrap();assert!(!reason.tuple);json!({"type":kind(r.transaction_type),"status":"error","reason":reason.tag})},
                Err(e)=>panic!("unexpected {e:?}"),
            }
        }).collect();
            assert_eq!(json!(results), row["results"], "row {i}: {}", row["name"]);
            assert_eq!(
                json!(out.events.iter().map(event).collect::<Vec<_>>()),
                row["final_events"],
                "row {i}"
            );
            assert_eq!(
                initial.serialize_to_binary(Default::default()).unwrap(),
                before,
                "input changed at {i}"
            );
            if signed.is_empty() {
                assert_eq!(
                    out.trees.serialize_to_binary(Default::default()).unwrap(),
                    bytes(&row["funded"])
                );
            }
        }
    }
}

// The read-only base deliberately shares a counter. An execution API accepting
// its writable counterpart could leak writes even if all roots were rolled back.
#[derive(Debug, Clone)]
struct ReadOnly {
    nodes: Arc<MemoryStore>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl NodeReader for ReadOnly {
    fn node(&self, hash: &ae_types::Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.nodes.node(hash)
    }
}
#[test]
fn resource_failures_abort_and_never_write_the_backing_state() {
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "spend_success" && r["protocol"] == 6)
        .unwrap();
    let raw = Trees::deserialize_from_binary(
        &bytes(&row["initial"]),
        MemoryStore::default(),
        MemoryStore::default(),
        Default::default(),
    )
    .unwrap();
    let base = ReadOnly {
        nodes: Arc::new(raw.accounts.tree().store().clone()),
        reads: Arc::default(),
    };
    let before = base.nodes.nodes().clone();
    let signed = signed(row);
    let prepared = prepared(row, &signed);
    let trees = Trees {
        accounts: ae_state::accounts::Accounts::from_root(
            raw.accounts.root_hash(),
            Overlay::new(base.clone(), OverlayLimits { nodes: 0, bytes: 0 }),
        )
        .unwrap(),
        calls: ae_state::calls::Calls::new(Overlay::new(
            MemoryStore::default(),
            OverlayLimits::default(),
        )),
        channels: ae_state::channels::Channels::new(Overlay::new(
            MemoryStore::default(),
            OverlayLimits::default(),
        )),
        contracts: ae_state::contracts::Contracts::new(Overlay::new(
            MemoryStore::default(),
            OverlayLimits::default(),
        )),
        names: ae_state::names::Names::new(
            Overlay::new(MemoryStore::default(), OverlayLimits::default()),
            Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        ),
        oracles: ae_state::oracles::Oracles::new(
            Overlay::new(MemoryStore::default(), OverlayLimits::default()),
            Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        ),
    };
    let reads_before = base.reads.load(std::sync::atomic::Ordering::Relaxed);
    let rejected = configured(&FIXTURE["profile"], |cfg| {
        dry::run(
            trees.clone(),
            &Context::transaction(50, Protocol::Ceres),
            cfg,
            &[],
            &prepared,
            &dry::Options {
                max_transactions: 0,
                ..Default::default()
            },
        )
    });
    assert!(matches!(rejected, Err(dry::Error::ResourceLimit)));
    assert_eq!(
        base.reads.load(std::sync::atomic::Ordering::Relaxed),
        reads_before
    );
    let result = configured(&FIXTURE["profile"], |cfg| {
        dry::run(
            trees,
            &Context::transaction(50, Protocol::Ceres),
            cfg,
            &[],
            &prepared,
            &dry::Options::default(),
        )
    });
    assert!(matches!(
        result,
        Err(dry::Error::Funding(ae_state::accounts::AccountError::Tree(
            ae_trie::Error::ResourceLimit
        )))
    ));
    assert!(base.reads.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert_eq!(base.nodes.nodes(), &before);
}

#[test]
fn execution_resource_failure_after_success_is_not_a_native_rejection() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    let row = rows
        .iter()
        .find(|r| r["name"] == "call_success" && r["protocol"] == 6)
        .unwrap();
    let spend = rows
        .iter()
        .find(|r| r["name"] == "spend_success" && r["protocol"] == 6)
        .unwrap();
    let calls = signed(row);
    let spends = signed(spend);
    let prepared = [
        dry::Prepared::transaction(&spends[0]),
        dry::Prepared::transaction(&calls[1]),
    ];
    let initial = trees(row);
    let before = initial.serialize_to_binary(Default::default()).unwrap();
    let result = configured(&FIXTURE["profile"], |cfg| {
        let cfg = transactions::Config {
            maximum_fate_steps: 0,
            ..*cfg
        };
        dry::run(
            initial.clone(),
            &Context::transaction(50, Protocol::Ceres),
            &cfg,
            &funding(),
            &prepared,
            &dry::Options::default(),
        )
    });
    assert!(
        matches!(result, Err(dry::Error::Operational { index: 1, .. })),
        "{result:?}"
    );
    assert_eq!(
        initial.serialize_to_binary(Default::default()).unwrap(),
        before
    );
}
