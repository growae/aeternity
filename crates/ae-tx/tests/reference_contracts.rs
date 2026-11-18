// Original aect_* / aetx / governance fa2a6d7 and aeserialization a1204df.
use ae_codec::sophia::{FunctionInfo, SophiaCode};
use ae_protocol::{
    Protocol,
    contracts::{Version, serialization_available},
};
use ae_tx::{CanonicalTransaction, ContractOperation, ContractTransaction};
use ae_types::{Id, Unsigned};
use serde_json::{Value as J, json};
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn int(j: &J) -> Unsigned {
    Unsigned::from_decimal(j.as_str().unwrap()).unwrap()
}
fn id(j: &J) -> Id {
    Id::from_bytes(&bytes(j)).unwrap()
}
fn functions(j: &J) -> Vec<FunctionInfo> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|f| {
            let n = f.as_array().unwrap().len();
            FunctionInfo {
                hash: bytes(&f[0]),
                name: bytes(&f[1]),
                payable: if n == 5 {
                    Some(f[2].as_bool().unwrap())
                } else {
                    None
                },
                arguments: bytes(&f[n - 2]),
                returns: bytes(&f[n - 1]),
            }
        })
        .collect()
}
fn info_json(f: &FunctionInfo) -> J {
    let mut a = vec![json!(hex::encode(&f.hash)), json!(hex::encode(&f.name))];
    if let Some(p) = f.payable {
        a.push(json!(p));
    }
    a.extend([
        json!(hex::encode(&f.arguments)),
        json!(hex::encode(&f.returns)),
    ]);
    json!(a)
}
fn metadata(code: &SophiaCode) -> J {
    json!({"version":code.version,"source_hash":hex::encode(&code.source_hash),"type_info":code.type_info.iter().map(info_json).collect::<Vec<_>>(),"byte_code":hex::encode(&code.byte_code),"compiler":code.compiler_version.as_ref().map(hex::encode),"payable":code.payable,"canonical":hex::encode(code.encode().unwrap())})
}

#[test]
fn sophia_wrapper_encoding_and_historical_decode_admission_match_erlang() {
    let f: J = serde_json::from_str(include_str!("fixtures/erlang_sophia_code.json")).unwrap();
    for (i, row) in f["encode"].as_array().unwrap().iter().enumerate() {
        let a = &row["input"];
        let version = row["version"].as_u64().unwrap() as u8;
        let mut c = SophiaCode::from_source(
            version,
            &a["source"]
                .as_str()
                .map(|s| hex::decode(s).unwrap())
                .unwrap_or_default(),
            bytes(&a["byte_code"]),
            functions(&a["type_info"]),
        );
        if a["source_hash"].is_string() {
            c.source_hash = bytes(&a["source_hash"]);
        }
        c.payable = a["payable"].as_bool().unwrap();
        c.compiler_version = if a["compiler"]["binary"].is_string() {
            Some(bytes(&a["compiler"]["binary"]))
        } else {
            a["compiler"]["integer"]
                .as_str()
                .map(|s| s.as_bytes().to_vec())
        };
        assert_eq!(
            c.encode().ok().map(hex::encode),
            row["encoded"].as_str().map(str::to_string),
            "encode {i}"
        );
    }
    for (i, row) in f["decode"].as_array().unwrap().iter().enumerate() {
        let result = SophiaCode::decode(&bytes(&row["bytes"]));
        let actual = result.as_ref().map(metadata).unwrap_or(J::Null);
        assert_eq!(actual, row["result"], "decode {i}: {result:?}");
    }
}

#[test]
fn sophia_serialization_rules_match_all_modeled_protocols() {
    let f: J = serde_json::from_str(include_str!("fixtures/erlang_sophia_code.json")).unwrap();
    for row in f["rules"].as_array().unwrap() {
        let result = serialization_available(
            &int(&row["abi"]),
            &int(&row["version"]),
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        assert_eq!(result.ok(), row["result"].as_bool(), "{row}");
    }
}
fn construct(kind: &str, a: &J) -> Result<ContractTransaction, ae_tx::Error> {
    let operation = if kind == "create" {
        ContractOperation::Create {
            version: Version {
                vm: int(&a["vm_version"]).to_u64().unwrap() as u16,
                abi: int(&a["abi_version"]).to_u64().unwrap() as u16,
            },
            code: bytes(&a["code"]),
            deposit: int(&a["deposit"]),
            call_data: bytes(&a["call_data"]),
        }
    } else {
        ContractOperation::Call {
            contract: id(&a["contract_id"]),
            abi_version: int(&a["abi_version"]),
            call_data: bytes(&a["call_data"]),
        }
    };
    let mut tx = ContractTransaction::new(id(&a["actor"]), operation)?;
    tx.set_nonce(int(&a["nonce"]));
    tx.set_fee(int(&a["fee"]));
    tx.set_ttl(int(&a["ttl"]));
    tx.set_amount(int(&a["amount"]));
    tx.set_gas(int(&a["gas"]));
    tx.set_gas_price(int(&a["gas_price"]));
    if kind == "call" {
        tx.set_call_context(
            a["origin"]
                .as_str()
                .map(|s| hex::decode(s).unwrap().try_into().unwrap())
                .unwrap_or(tx.actor().value),
            a["call_stack"]
                .as_array()
                .unwrap()
                .iter()
                .map(int)
                .collect(),
        )?;
    }
    Ok(tx)
}
fn check_transaction(tx: &ContractTransaction, row: &J) {
    assert_eq!(tx.encode(), bytes(&row["encoded"]));
    assert_eq!(tx.actor(), id(&row["actor"]));
    assert_eq!(tx.signer().0, tx.actor().value);
    assert_eq!(tx.nonce(), &int(&row["nonce"]));
    assert_eq!(tx.fee(), &int(&row["fee"]));
    assert_eq!(tx.ttl(), &int(&row["ttl"]));
    assert_eq!(tx.amount(), &int(&row["amount"]));
    assert_eq!(tx.gas(), &int(&row["gas"]));
    assert_eq!(tx.gas_price(), &int(&row["gas_price"]));
    assert_eq!(tx.operation().call_data(), bytes(&row["call_data"]));
    assert_eq!(tx.operation().abi_version(), int(&row["abi"]));
    match tx.operation() {
        ContractOperation::Create {
            version,
            code,
            deposit,
            ..
        } => {
            assert_eq!(u64::from(version.vm), row["vm"].as_u64().unwrap());
            assert_eq!(*code, bytes(&row["code"]));
            assert_eq!(*deposit, int(&row["deposit"]));
        }
        ContractOperation::Call { contract, .. } => {
            assert_eq!(*contract, id(&row["target"]));
            assert_eq!(tx.call_origin().as_slice(), bytes(&row["call_origin"]));
            assert_eq!(
                tx.call_stack(),
                row["call_stack"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(int)
                    .collect::<Vec<_>>()
            );
        }
    }
}
#[test]
fn contract_constructors_preserve_internal_call_context_and_wire_boundaries() {
    let f: J = serde_json::from_str(include_str!("fixtures/erlang_contract_wire.json")).unwrap();
    for (i, row) in f["constructors"].as_array().unwrap().iter().enumerate() {
        let result = construct(row["kind"].as_str().unwrap(), &row["args"]);
        if row["result"].is_null() {
            assert!(result.is_err(), "constructor {i}");
        } else {
            let tx = result.unwrap();
            check_transaction(&tx, &row["result"]);
            assert_eq!(tx.wire_size(), tx.encode().len());
            if tx.actor().tag == ae_types::IdTag::Contract {
                assert!(ContractTransaction::decode(&tx.encode()).is_err());
            } else {
                let decoded = ContractTransaction::decode(&tx.encode()).unwrap();
                assert!(decoded.call_stack().is_empty());
                assert_eq!(decoded.call_origin(), tx.actor().value);
            }
        }
    }
}
#[test]
fn contract_wire_normalization_and_gas_pricing_match_erlang() {
    let f: J = serde_json::from_str(include_str!("fixtures/erlang_contract_wire.json")).unwrap();
    let mut noncanonical = 0;
    for (i, row) in f["transactions"].as_array().unwrap().iter().enumerate() {
        let raw = bytes(&row["bytes"]);
        let decoded = ContractTransaction::decode(&raw);
        if row["result"].is_null() {
            assert!(decoded.is_err(), "decode {i}");
            continue;
        }
        let tx = decoded.unwrap();
        check_transaction(&tx, &row["result"]);
        assert_eq!(tx.wire_size(), raw.len());
        if tx.encode() != raw {
            noncanonical += 1;
        }
        for p in row["prices"].as_array().unwrap() {
            let protocol = Protocol::try_from(p["protocol"].as_u64().unwrap() as u32).unwrap();
            assert_eq!(
                tx.valid_at_protocol(protocol),
                p["valid"],
                "admission {i} {protocol:?}"
            );
            assert_eq!(tx.gas_limit(), int(&p["gas"]), "gas {i}");
            assert_eq!(tx.inner_gas_limit(), int(&p["inner_gas"]));
            assert_eq!(tx.fee_gas(), int(&p["fee_gas"]));
            assert_eq!(tx.minimum_fee(protocol, false), int(&p["minimum_fee"]));
            assert_eq!(tx.minimum_fee(protocol, true), int(&p["ga_minimum_fee"]));
            assert_eq!(
                tx.minimum_gas_price(false).unwrap(),
                int(&p["minimum_gas_price"])
            );
            assert_eq!(
                tx.minimum_gas_price(true).unwrap(),
                int(&p["inner_minimum_gas_price"])
            );
        }
        let mut changed = tx.clone();
        changed.set_nonce(tx.nonce().clone());
        assert_eq!(changed.encode(), tx.encode());
        assert_eq!(changed.wire_size(), tx.encode().len());
    }
    assert!(noncanonical > 0);
    for row in f["used_gas"].as_array().unwrap() {
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        assert_eq!(tx.used_gas(&int(&row["vm_gas"])), int(&row["result"]));
    }
}

#[test]
fn signatures_use_normalized_contract_bytes_while_fee_metering_keeps_incoming_size() {
    use ae_codec::{
        object::Value,
        rlp::{self, Rlp},
    };
    use ae_crypto::{Ed25519SecretKey, blake2b_256};
    use ae_protocol::NetworkProfile;
    use ae_tx::{SignedTransaction, WireTransaction};
    use ae_types::IdTag;
    let secret = Ed25519SecretKey::from_seed(&[31; 32]);
    let network = NetworkProfile::mainnet();
    let code = SophiaCode::from_source(3, b"contract Example =", vec![0], vec![])
        .encode()
        .unwrap();
    let tx = ContractTransaction::new(
        Id::new(IdTag::Account, secret.public_key().0),
        ContractOperation::Create {
            version: Version { vm: 7, abi: 3 },
            code,
            deposit: 0.into(),
            call_data: vec![],
        },
    )
    .unwrap();
    let canonical = tx.encode();
    let wire = WireTransaction::decode(&canonical).unwrap();
    let mut fields = wire.fields().to_vec();
    let mut packed = [0; 33];
    packed[0] = 1;
    packed[30] = 7;
    packed[32] = 3;
    fields
        .iter_mut()
        .find(|(n, _)| n == "ct_version")
        .unwrap()
        .1 = Value::Integer(Unsigned::from_field_bytes(&packed).unwrap());
    let incoming = WireTransaction::new(wire.kind(), wire.version(), fields)
        .unwrap()
        .encode();
    assert_ne!(incoming.len(), canonical.len());
    for v in 1..=8 {
        let protocol = Protocol::try_from(v).unwrap();
        for inner in [false, true] {
            for hashed in [false, true] {
                let mut payload = network.signing_prefix(inner);
                payload.extend(if hashed {
                    blake2b_256(&canonical).0.to_vec()
                } else {
                    canonical.clone()
                });
                let signed = SignedTransaction::new(tx.clone(), vec![secret.sign(&payload)]);
                let encoded = signed.encode();
                let Rlp::List(mut parts) = rlp::decode(&encoded).unwrap() else {
                    panic!()
                };
                parts[3] = Rlp::Bytes(&incoming);
                let decoded = SignedTransaction::<ContractTransaction>::decode(&rlp::encode(
                    &Rlp::List(parts),
                ))
                .unwrap();
                assert_eq!(decoded.transaction().wire_size(), incoming.len());
                assert_eq!(decoded.encode(), encoded);
                assert_eq!(
                    decoded
                        .verify_signers(&[secret.public_key()], &network, protocol, inner)
                        .is_ok(),
                    !hashed || protocol >= Protocol::Lima
                );
                assert!(
                    decoded
                        .verify_signers(
                            &[secret.public_key()],
                            &NetworkProfile::uat(),
                            protocol,
                            inner
                        )
                        .is_err()
                );
                assert!(
                    decoded
                        .verify_signers(&[secret.public_key()], &network, protocol, !inner)
                        .is_err()
                );
            }
        }
    }
}
