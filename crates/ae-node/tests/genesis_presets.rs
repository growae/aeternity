// Original Aeternity preset functions, fa2a6d7. Copyright (c) Aeternity.
// ISC: repository LICENSE. Captures use explicit environment facades.
use ae_codec::api::{self, ApiType};
use ae_node::configuration::presets::{self, Account, Asset, Error, Limits};
use ae_types::AccountPublicKey;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

fn ak(n: u8) -> String {
    api::encode(ApiType::AccountPubkey, &[n; 32])
}
fn chars(v: &Value) -> String {
    String::from_utf8(
        v.as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap() as u8)
            .collect(),
    )
    .unwrap()
}
fn binary(v: &Value) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn asset(s: &str) -> Asset {
    match s {
        "accounts" => Asset::Accounts,
        "extra_accounts" => Asset::ExtraAccounts,
        "contracts" => Asset::Contracts,
        _ => panic!("unknown asset"),
    }
}
fn portable_accounts(rows: &[Account]) -> Value {
    json!(
        rows.iter()
            .map(|a| json!([
                "tuple",
                [["binary", hex::encode_upper(a.public_key.0)], a.amount]
            ]))
            .collect::<Vec<_>>()
    )
}
fn input(i: usize) -> Value {
    match i {
        1 => json!({}),
        2 => json!({ak(1):1}),
        3 => json!({ak(1):-1}),
        4 => {
            json!({ak(1):serde_json::from_str::<Value>("1234567890123456789012345678901234567890").unwrap()})
        }
        5 => json!([[ak(2), 2], [ak(1), 1], [ak(2), 3]]),
        6 => json!({"bad":1}),
        7 => json!([[api::encode(ApiType::ContractPubkey, &[1; 32]), 2]]),
        8 => json!([]),
        9 => json!(17),
        _ => unreachable!(),
    }
}
fn compare(result: Result<Vec<Account>, Error>, native: &Value) {
    if native["ok"] == true {
        assert_eq!(portable_accounts(&result.unwrap()), native["value"]);
    } else {
        match native["reason"].as_str() {
            Some("function_clause") => assert_eq!(result, Err(Error::Shape)),
            _ => assert_eq!(result, Err(Error::Address)),
        }
    }
}
fn merge_input(i: usize) -> Vec<Account> {
    let pairs = match i {
        1 => vec![],
        2 => vec![(1, 10)],
        3 => vec![(2, 20), (1, 10)],
        4 => vec![(1, 1), (1, 2), (2, 3)],
        _ => unreachable!(),
    };
    pairs
        .into_iter()
        .map(|(n, amount)| Account {
            public_key: AccountPublicKey([n; 32]),
            amount: json!(amount),
        })
        .collect()
}
#[test]
fn original_paths_decoding_configuration_merges_and_actual_allocations() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_genesis_presets.json")).unwrap();
    assert_eq!(rows.len(), 256);
    let limits = Limits::default();
    let mut checked = 0;
    let mut erlang_only_config = 0;
    for row in rows {
        let name = &row["name"][1];
        let result = &row["result"];
        match name[0].as_str().unwrap() {
            "path" => {
                let network = binary(&name[1]);
                let protocol = name[3].as_u64().unwrap() as u32;
                assert_eq!(
                    presets::directory(protocol).unwrap(),
                    chars(&result["value"][1][0])
                );
                assert_eq!(
                    presets::filename(&network, name[2] == "pos", asset(&chars(&name[4]))).unwrap(),
                    chars(&result["value"][1][1])
                );
            }
            "decode" => {
                let i = name[1].as_u64().unwrap() as usize;
                let value = input(i);
                // Native Erlang proplists have tuples, not JSON nested arrays.
                // Exercise the ordered-pair API for these two term inputs.
                let decoded = if matches!(i, 5 | 7) {
                    presets::decode_pairs(
                        value
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|p| (p[0].as_str().unwrap(), &p[1])),
                        limits,
                    )
                } else {
                    presets::decode_accounts(&value, limits)
                };
                compare(decoded, result);
            }
            "configured" => {
                let network = binary(&name[1]);
                let i = name[2].as_u64().unwrap() as usize;
                if matches!(i, 5 | 7) && !matches!(network.as_slice(), b"ae_mainnet" | b"ae_uat") {
                    erlang_only_config += 1;
                    continue;
                }
                compare(
                    presets::configured_accounts(&network, Some(&input(i)), limits),
                    result,
                );
            }
            "merge" => compare(
                presets::merge_accounts(
                    merge_input(name[1].as_u64().unwrap() as usize),
                    merge_input(name[2].as_u64().unwrap() as usize),
                    limits,
                ),
                result,
            ),
            "real_preset" => {
                let bytes = match binary(&name[1]).as_slice() {
                    b"ae_mainnet" => include_str!("../../ae-state/data/mainnet_accounts.json"),
                    b"ae_uat" => include_str!("../../ae-state/data/uat_accounts.json"),
                    b"ae_dev" => include_str!("fixtures/genesis_test_accounts.json"),
                    _ => unreachable!(),
                };
                compare(presets::parse_accounts(bytes.as_bytes(), limits), result);
            }
            _ => unreachable!(),
        }
        checked += 1;
    }
    assert_eq!((checked, erlang_only_config), (252, 4));
    println!(
        "Genesis presets: 252 original path/decode/configuration/merge/allocation cases; four Erlang-only tuple-config cases explicitly outside the JSON interface"
    );
}
struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn expand(v: &Value, data: &Path) -> Value {
    match v {
        Value::String(s) => json!(s.replace("$DATA", data.to_str().unwrap())),
        Value::Array(a) => json!(a.iter().map(|v| expand(v, data)).collect::<Vec<_>>()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, data)))
                .collect(),
        ),
        v => v.clone(),
    }
}
fn normalized(path: &Path, data: &Path) -> String {
    path.to_str()
        .unwrap()
        .replace(data.to_str().unwrap(), "$DATA")
}
#[test]
fn original_file_resolution_reads_duplicates_and_failures() {
    let tmp = Temp(std::env::temp_dir().join(format!("ae-genesis-files-{}", std::process::id())));
    std::fs::create_dir_all(tmp.0.join("directory")).unwrap();
    let inputs: Value =
        serde_json::from_str(include_str!("fixtures/erlang_genesis_file_inputs.json")).unwrap();
    for (name, bytes) in inputs["files"].as_object().unwrap() {
        let path = tmp.0.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes.as_str().unwrap()).unwrap();
    }
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_genesis_files.json")).unwrap();
    assert_eq!(rows.len(), 227);
    for row in rows {
        let input = expand(&row["input"], &tmp.0);
        let native = &row["result"];
        let fork = (!input["fork"].is_null()).then_some(&input["fork"]);
        let custom = (!input["custom"].is_null()).then_some(&input["custom"]);
        let result=presets::source(&tmp.0,b"ae_dev",false,input["protocol"].as_u64().unwrap() as u32,asset(input["asset"].as_str().unwrap()),fork,custom).and_then(|s|s.read_at(&tmp.0,Limits::default().bytes)).and_then(|bytes| {
            if input["mode"]=="decode" {
                presets::parse_accounts(&bytes,Limits::default()).map(|a| json!({"ok":true,"accounts":a.into_iter().map(|a|json!([hex::encode_upper(a.public_key.0),a.amount])).collect::<Vec<_>>()}))
            } else { Ok(json!({"ok":true,"bytes":hex::encode_upper(bytes)})) }
        });
        match result {
            Ok(value) => assert_eq!(&value, native, "{}", input["name"]),
            Err(error) => {
                assert_eq!(native["ok"], false, "{}: {error:?}", input["name"]);
                let kind = match error {
                    Error::Io { path, kind } => {
                        assert_eq!(normalized(&path, &tmp.0), native["path"]);
                        let reason = match kind {
                            std::io::ErrorKind::NotFound => "enoent",
                            std::io::ErrorKind::IsADirectory => "eisdir",
                            _ => panic!("unexpected IO: {kind:?}"),
                        };
                        assert_eq!(reason, native["reason"]);
                        "missing"
                    }
                    Error::CustomFileNotFound(path) => {
                        assert_eq!(normalized(&path, &tmp.0), native["path"]);
                        "custom_missing"
                    }
                    Error::Configuration(_) => "configuration",
                    Error::Shape => "shape",
                    Error::Json => "json",
                    Error::Address => "address",
                    Error::MissingConfiguration => "missing_configuration",
                    other => panic!("unexpected failure: {other:?}"),
                };
                assert_eq!(kind, native["kind"], "{}", input["name"]);
            }
        }
    }
    println!(
        "Genesis files: 227 original file selection/read/decode cases including relative custom paths and exact missing-file paths"
    );
}
