use ae_protocol::{NetworkProfile, Protocol};
use ae_tx::{
    CanonicalTransaction, Error, GaAttachTransaction, GaMeta, PayingFor, Spend, Transaction,
    TransactionKind, TransactionLeaf, TransactionRef, Wrapper,
};
use ae_types::{AccountPublicKey, Id, IdTag, Signature, Unsigned};
use serde_json::{Value as J, json};
use std::sync::LazyLock;

fn fixture(text: &str) -> J {
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
            _ => v.clone(),
        }
    }
    let v: J = serde_json::from_str(text).unwrap();
    if v["format"] == "interned-json-v1" {
        unpack(&v["root"], v["values"].as_array().unwrap())
    } else {
        v
    }
}
static PREP: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_ga_wrapper_preparation.json")));
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn number(v: &J) -> Unsigned {
    assert_eq!(v[0], "integer");
    Unsigned::from_decimal(v[1].as_str().unwrap()).unwrap()
}
fn int(v: impl ToString) -> J {
    json!(["integer", v.to_string()])
}
fn uint(n: &Unsigned) -> J {
    int(num_bigint::BigUint::from_bytes_be(n.as_bytes()))
}
fn key(n: u8) -> AccountPublicKey {
    let mut b = [0; 32];
    b[31] = n;
    AccountPublicKey(b)
}
fn kind(k: TransactionKind) -> &'static str {
    match k {
        TransactionKind::GaAttach => "ga_attach_tx",
        TransactionKind::GaMeta => "ga_meta_tx",
        TransactionKind::PayingFor => "paying_for_tx",
        TransactionKind::Spend => "spend_tx",
        TransactionKind::NamePreclaim => "name_preclaim_tx",
        TransactionKind::NameClaim => "name_claim_tx",
        TransactionKind::NameUpdate => "name_update_tx",
        TransactionKind::NameTransfer => "name_transfer_tx",
        TransactionKind::NameRevoke => "name_revoke_tx",
        TransactionKind::OracleRegister => "oracle_register_tx",
        TransactionKind::OracleExtend => "oracle_extend_tx",
        TransactionKind::OracleQuery => "oracle_query_tx",
        TransactionKind::OracleResponse => "oracle_response_tx",
        TransactionKind::ContractCreate => "contract_create_tx",
        TransactionKind::ContractCall => "contract_call_tx",
        _ => panic!("{k:?}"),
    }
}
fn check(tx: TransactionRef<'_>, row: &J) {
    assert_eq!(kind(tx.kind()), row["kind"].as_str().unwrap());
    assert_eq!(tx.encode(), bytes(&row["bytes"]));
    assert_eq!(tx.wire_size(), row["size"].as_u64().unwrap() as usize);
    assert_eq!(uint(&tx.nonce()), row["nonce"]);
    assert_eq!(uint(tx.fee()), row["fee"]);
    assert_eq!(
        tx.ttl().map(uint).unwrap_or(json!(["atom", "max_ttl"])),
        row["ttl"]
    );
    assert_eq!(tx.origin().unwrap().0.to_vec(), bytes(&row["origin"]));
    assert_eq!(
        tx.gas_price()
            .map(uint)
            .unwrap_or(json!(["atom", "undefined"])),
        row["gas_price"]
    );
    for p in row["prices"].as_array().unwrap() {
        let protocol = Protocol::try_from(p["protocol"].as_u64().unwrap() as u32).unwrap();
        assert_eq!(
            tx.valid_at_protocol(protocol).unwrap(),
            p["valid"].as_bool().unwrap()
        );
        assert_eq!(int(tx.gas_limit(&50.into(), protocol)), p["gas"]["value"]);
        let costs = tx.pricing(&50.into(), protocol);
        if !p["minimum_block_gas"].is_null() {
            assert_eq!(
                int(&costs.minimum_block_gas),
                p["minimum_block_gas"]["value"]
            );
        }
        for (name, value) in [
            ("gas", costs.gas_limit),
            ("inner_gas", costs.inner_gas_limit),
            ("fee_gas", costs.fee_gas),
            ("minimum_fee", costs.minimum_fee),
            ("minimum_gas_price", costs.minimum_gas_price),
        ] {
            assert_eq!(p[name]["kind"], "ok", "{p}");
            assert_eq!(int(value), p[name]["value"], "{name}: {p}; {row}");
        }
    }
    if let Some(layer) = tx.layer()
        && !row["inner_signatures"].is_null()
    {
        let expected: Vec<Vec<u8>> = row["inner_signatures"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(bytes)
            .collect();
        assert_eq!(
            layer
                .inner_signatures()
                .iter()
                .map(|s| s.0.to_vec())
                .collect::<Vec<_>>(),
            expected
        );
    }
    if let Some(layer) = tx.layer()
        && let Wrapper::GaMeta(m) = layer.wrapper()
    {
        assert_eq!(m.auth_id().0.to_vec(), bytes(&row["auth_id"]));
        let inner = tx.inner().unwrap().encode();
        assert_eq!(inner, bytes(&row["inner_bytes"]));
        for a in row["auth_serializations"].as_array().unwrap() {
            let mut input = bytes(&a["network"]);
            input.extend_from_slice(&inner);
            assert_eq!(m.serialize_auth_data(&input), bytes(&a["bytes"]));
        }
        if let Some(hashes) = row["auth_hashes"].as_array() {
            for h in hashes {
                let network = if bytes(&h["network"]) == b"ae_mainnet" {
                    NetworkProfile::mainnet()
                } else {
                    NetworkProfile::uat()
                };
                let protocol = Protocol::try_from(h["protocol"].as_u64().unwrap() as u32).unwrap();
                assert_eq!(
                    tx.auth_hash(protocol, &network).unwrap().0.to_vec(),
                    bytes(&h["hash"])
                );
            }
        }
    }
}

static EXPANDED: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_ga_wrappers.json")));
fn check_row(row: &J) -> bool {
    let input = bytes(&row["input"]);
    let decoded = Transaction::decode(&input);
    if row["result"]["kind"] == "native" {
        let error = decoded.unwrap_err();
        assert!(
            !matches!(
                error,
                Error::UnsupportedTransaction(_) | Error::ResourceLimit
            ),
            "{row}"
        );
        if row["result"]["reason"]
            .to_string()
            .contains("not_allowed_to_pay_for_recursively")
        {
            assert_eq!(error, Error::RecursivePayer);
        }
        false
    } else {
        let tx = decoded.unwrap_or_else(|e| panic!("{e:?}: {row}"));
        check(tx.view(), &row["result"]["value"]);
        true
    }
}
#[test]
fn all_supported_leaf_families_and_nested_auth_hashes_match_original() {
    let rows = EXPANDED["leaves"].as_array().unwrap();
    assert_eq!(rows.len(), 972);
    assert_eq!(rows.iter().filter(|r| check_row(r)).count(), 864);
    assert!(rows.iter().any(|r| {
        r["result"]["value"]["prices"].as_array().is_some_and(|ps| {
            ps.iter().any(|p| {
                p["minimum_gas_price"]["value"][1]
                    .as_str()
                    .is_some_and(|v| v.starts_with('-'))
            })
        })
    }));
}
#[test]
fn nested_normalization_retains_original_sizes_signatures_and_signed_fee_arithmetic() {
    let rows = EXPANDED["normalization"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    for row in rows {
        assert!(check_row(row));
    }
    let constructed = EXPANDED["constructed_normalization"].as_array().unwrap();
    assert_eq!(constructed.len(), 24);
    let mut negative = 0;
    for row in constructed {
        let inner = Transaction::decode(&bytes(&row["input"])).unwrap();
        let mode = row["mode"].as_str().unwrap();
        let wrapper = if mode == "payer" {
            Wrapper::PayingFor(PayingFor {
                payer: key(3),
                nonce: 1.into(),
                fee: 1_000_000_000_000u64.into(),
            })
        } else {
            let mut auth_data = key(1).0.to_vec();
            auth_data.extend([0, 255]);
            Wrapper::GaMeta(GaMeta {
                account: key(1),
                auth_data,
                abi_version: 3.into(),
                fee: 1_000_000_000_000u64.into(),
                gas: 1001.into(),
                gas_price: 1_000_000.into(),
                ttl: (mode == "meta_v1").then(|| 77.into()),
            })
        };
        let tx = inner.wrap(wrapper, vec![]);
        check(tx.view(), &row["result"]);
        if tx.view().pricing(&50.into(), Protocol::Ceres).fee_gas < 0.into() {
            negative += 1;
        }
    }
    assert!(negative > 0);
}
#[test]
fn malformed_wrapper_admission_and_attach_version_gates_match_original() {
    let rows = EXPANDED["mutations"].as_array().unwrap();
    assert_eq!(rows.len(), 61);
    assert_eq!(rows.iter().filter(|r| check_row(r)).count(), 27);
    let rows = EXPANDED["attach_gates"].as_array().unwrap();
    assert_eq!(rows.len(), 440);
    for row in rows {
        let version = ae_protocol::contracts::Version {
            vm: row["vm"].as_u64().unwrap() as u16,
            abi: row["abi"].as_u64().unwrap() as u16,
        };
        let t = GaAttachTransaction::new(key(1), version, vec![], vec![], vec![]);
        let p = Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap();
        assert_eq!(t.version_available(p), row["available"].as_bool().unwrap());
    }
}
#[test]
fn deeply_nested_transactions_use_bounded_iterative_decode_and_small_native_stack() {
    let rows = EXPANDED["deep"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    for row in rows {
        assert!(check_row(row));
    }
    let row = rows.last().unwrap();
    let input = bytes(&row["input"]);
    let before = input.clone();
    let expected = row["result"]["value"].clone();
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            let tx = Transaction::decode(&input).unwrap();
            assert_eq!(tx.layers().len(), 1024);
            assert_eq!(tx.encode(), input);
            assert_eq!(
                tx.view()
                    .pricing(&50.into(), Protocol::Ceres)
                    .gas_limit
                    .to_string(),
                expected["prices"][5]["gas"]["value"][1].as_str().unwrap()
            );
            let limited = ae_tx::TransactionLimits {
                max_layers: 1023,
                ..Default::default()
            };
            assert_eq!(
                Transaction::decode_with_limits(&input, limited),
                Err(Error::ResourceLimit)
            );
            let limited = ae_tx::TransactionLimits {
                max_total_bytes: input.len() - 1,
                ..Default::default()
            };
            assert_eq!(
                Transaction::decode_with_limits(&input, limited),
                Err(Error::ResourceLimit)
            );
            let mut limited = ae_tx::TransactionLimits::default();
            limited.rlp.max_items = 2;
            assert_eq!(
                Transaction::decode_with_limits(&input, limited),
                Err(Error::Object(ae_codec::object::ObjectError::Rlp(
                    ae_codec::rlp::DecodeError::ResourceLimit
                )))
            );
            assert_eq!(Transaction::decode(&input).unwrap(), tx);
            assert_eq!(input, before);
            drop(tx);
        })
        .unwrap()
        .join()
        .unwrap();
}
#[test]
fn ga_attach_wire_protocol_and_prices_match_original() {
    let rows = PREP["attach"].as_array().unwrap();
    assert_eq!(rows.len(), 24);
    for row in rows {
        let row = &row["result"];
        let tx = GaAttachTransaction::decode(&bytes(&row["bytes"])).unwrap();
        let mut created = GaAttachTransaction::new(
            tx.owner(),
            tx.version(),
            tx.code().to_vec(),
            tx.auth_fun().to_vec(),
            tx.call_data().to_vec(),
        );
        created.set_nonce(tx.nonce().clone());
        created.set_fee(tx.fee().clone());
        created.set_ttl(tx.ttl().clone());
        created.set_gas(tx.gas().clone());
        created.set_gas_price(tx.gas_price().clone());
        assert_eq!(created, tx);
        check(Transaction::new(TransactionLeaf::GaAttach(tx)).view(), row);
    }
}
fn construct(row: &J) -> Transaction {
    let n = number(&row["nonce"]);
    let fee = n.add(&1_000_000_000_000u64.into());
    let mut spend = Spend::new(key(1), Id::new(IdTag::Account, key(2).0)).unwrap();
    spend.set_nonce(1.into());
    spend.set_amount(7.into());
    spend.set_fee(1_000_000_000_000u64.into());
    spend.set_ttl(row["inner_ttl"].as_u64().unwrap().into());
    spend.set_payload(vec![0, 255]);
    let mut tx = Transaction::new(TransactionLeaf::Spend(spend));
    let signatures: Vec<Signature> = row["input_signatures"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| Signature::try_from(bytes(b).as_slice()).unwrap())
        .collect();
    for mode in row["scenario"][1].as_array().unwrap() {
        let mode = mode[1].as_str().unwrap();
        let wrapper = if mode == "payer" {
            Wrapper::PayingFor(PayingFor {
                payer: key(3),
                nonce: n.clone(),
                fee: fee.clone(),
            })
        } else {
            let mut auth_data = vec![0; 32 - n.as_bytes().len()];
            auth_data.extend_from_slice(n.as_bytes());
            auth_data.extend([0, 255]);
            Wrapper::GaMeta(GaMeta {
                account: key(1),
                auth_data,
                abi_version: row["abi"].as_u64().unwrap().into(),
                fee: fee.clone(),
                gas: n.add(&1000.into()),
                gas_price: 1_000_000.into(),
                ttl: (mode == "meta_v1").then(|| 77.into()),
            })
        };
        tx = tx.wrap(wrapper, signatures.clone());
    }
    tx
}
#[test]
fn nested_meta_and_payer_wire_fees_ttl_auth_and_rejections_match_original() {
    let rows = PREP["wrappers"].as_array().unwrap();
    assert_eq!(rows.len(), 216);
    let mut rejected = 0;
    for row in rows {
        let tx = construct(row);
        let result = &row["result"];
        check(tx.view(), result);
        let decoded = Transaction::decode(&bytes(&result["bytes"]));
        if result["round_trip"]["kind"] == "native" {
            assert_eq!(tx.validate_payer_nesting(), Err(Error::RecursivePayer));
            assert_eq!(decoded, Err(Error::RecursivePayer));
            rejected += 1;
        } else {
            let decoded = decoded.unwrap();
            check(decoded.view(), result);
            assert_eq!(decoded, tx);
        }
        if tx.view().kind() == TransactionKind::GaMeta {
            assert_ne!(
                tx.view()
                    .auth_hash(Protocol::Lima, &NetworkProfile::mainnet())
                    .unwrap(),
                tx.view()
                    .auth_hash(Protocol::Ceres, &NetworkProfile::mainnet())
                    .unwrap()
            );
        }
    }
    assert_eq!(rejected, 24);
}
