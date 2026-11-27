#![allow(dead_code)]
use ae_chain::{
    block::{Block, MicroBlock},
    key::Rules,
};
use ae_codec::{
    api::{self, ApiType},
    headers::{Header, HeaderMode},
};
use ae_execution::{
    contracts::{aevm, fate},
    transactions::{self, owned::Owned},
};
use ae_protocol::NetworkProfile;
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::{
    machine::{self, program::remote::context},
    maps::{listing::Overrides, order::WordSize},
};
use std::sync::Arc;
#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
pub(super) mod otp_order;

// Explicit isolated-reference profiles, not a deployed-mainnet attestation.
pub fn execution(network: &NetworkProfile) -> Owned {
    execution_with_consensus(network, ae_protocol::ConsensusType::Pow)
}
pub fn execution_with_consensus(
    network: &NetworkProfile,
    consensus: ae_protocol::ConsensusType,
) -> Owned {
    let context = machine::Context {
        current_contract: [0; 32],
        vm_version: ae_vm_fate::types::Version::Three,
        in_auth: false,
        on_chain: true,
        reserved_gas: 0.into(),
        arithmetic_word_size: WordSize::Bits64,
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
    };
    let profile = serde_json::from_str(include_str!(
        "../../../ae-vm-fate/tests/fixtures/erlang_fate_order_0.json"
    ))
    .unwrap();
    let order = otp_order::profile(&profile);
    let overrides = Overrides::default();
    let environment = context::Environment {
        auth_tx_hash: None,
        origin: [0; 32],
        gas_price: 0.into(),
        fee: 0.into(),
        beneficiary: [0; 32],
        timestamp: 0.into(),
        difficulty: 0.into(),
        block_gas_limit: 6_000_000.into(),
        network_id: network.network_id().to_vec(),
        consensus: match consensus {
            ae_protocol::ConsensusType::Pow => context::Consensus::Pow,
            ae_protocol::ConsensusType::Pos => context::Consensus::Pos,
        },
        key_hash: [0; 32],
    };
    let fate = fate::Config {
        control: None,
        context: &context,
        order: &order,
        overrides: &overrides,
        environment: &environment,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let aevm = aevm::Config {
        control: None,
        environment: aevm::Environment {
            genesis_height: 0,
            key_hash: [0; 32],
            history: Arc::default(),
            beneficiary: AccountPublicKey([0; 32]),
            difficulty: 0.into(),
            timestamp_ms: 0.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: network.network_id().into(),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
    };
    Owned::capture(&transactions::Config {
        consensus,
        fate: Some(&fate),
        aevm: Some(&aevm),
        ..transactions::Config::primitives(network)
    })
    .unwrap()
}
pub fn rules(genesis: Hash256) -> Rules {
    Rules::new(
        ae_chain::admission::PowRules {
            now_ms: 0,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        genesis,
    )
}
pub fn blocks(network: &NetworkProfile) -> Vec<Block> {
    let d: serde_json::Value = serde_json::from_str(include_str!(
        "../../../ae-storage/tests/fixtures/public_bootstrap.json"
    ))
    .unwrap();
    let mut blocks = vec![];
    for row in d["generations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["network"].as_str().unwrap().as_bytes() == network.network_id())
    {
        let Header::Key(key) = Header::decode(
            &hex::decode(row["hex"].as_str().unwrap()).unwrap(),
            HeaderMode::ProofOfWork,
        )
        .unwrap() else {
            panic!()
        };
        blocks.push(Block::Key(key));
        for hash in row["micro_blocks"].as_array().unwrap() {
            let m = d["micros"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["hash"] == *hash)
                .unwrap();
            let Header::Micro(header) = Header::decode(
                &hex::decode(m["hex"].as_str().unwrap()).unwrap(),
                HeaderMode::ProofOfWork,
            )
            .unwrap() else {
                panic!()
            };
            let transactions = m["transactions"]["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    ae_tx::SignedTransaction::decode(
                        &api::decode_as(ApiType::Transaction, t["encoded_tx"].as_str().unwrap())
                            .unwrap(),
                    )
                    .unwrap()
                })
                .collect();
            blocks.push(Block::Micro(MicroBlock {
                header,
                transactions,
                proof: None,
            }));
        }
    }
    blocks
}
