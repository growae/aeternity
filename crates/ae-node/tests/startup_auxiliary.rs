// Original Aeternity fa2a6d7 startup functions and actual setup 2.1.1.
// Copyright (c) Aeternity. ISC: repository LICENSE.
use ae_node::configuration::{
    Configuration,
    auxiliary::{self, Asset, Error, Limits, Plan},
    resolution::{Context, Definition, Directories, Variable},
};
use ae_vm_fate::{
    encoding,
    maps::listing::Overrides,
    value::{ObjectKind, ObjectPayload, Value as Fate},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-startup-aux-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(p.join("aecore")).unwrap();
        Self(p)
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
fn binary(value: &Value) -> Vec<u8> {
    assert_eq!(value[0], "binary");
    hex::decode(value[1].as_str().unwrap()).unwrap()
}
fn text(value: &Value) -> String {
    if value[0] == "binary" {
        String::from_utf8(binary(value)).unwrap()
    } else {
        String::from_utf8(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_u64().unwrap() as u8)
                .collect(),
        )
        .unwrap()
    }
}
fn mapped_path(value: &Value, data: &Path) -> String {
    let path = text(value);
    let path = if let Some((_, tail)) = path.split_once("/startup-aux2/data") {
        format!("$DATA{tail}")
    } else {
        path
    };
    path.replace("$DATA", data.to_str().unwrap())
}
fn fate(value: &Value) -> Fate {
    if let Some(b) = value.as_bool() {
        return Fate::Boolean(b);
    }
    if value.is_number() {
        return Fate::Integer(value.to_string().parse().unwrap());
    }
    let a = value.as_array().unwrap();
    match a.first().and_then(Value::as_str) {
        Some("binary") => Fate::String(binary(value)),
        Some("map") => Fate::Map(
            a[1].as_array()
                .unwrap()
                .iter()
                .map(|r| (fate(&r[0]), fate(&r[1])))
                .collect(),
        ),
        Some("tuple") => {
            let terms = a[1].as_array().unwrap();
            match terms[0].as_str().unwrap() {
                "tuple" => {
                    assert_eq!(terms[1][0], "tuple");
                    Fate::Tuple(terms[1][1].as_array().unwrap().iter().map(fate).collect())
                }
                "address" => {
                    Fate::Object(ObjectKind::Address, ObjectPayload::Bytes(binary(&terms[1])))
                }
                other => panic!("unexpected FATE native tuple {other}"),
            }
        }
        _ => Fate::List(a.iter().map(fate).collect()),
    }
}
fn compare_failure(error: Error, native: &Value) {
    assert_eq!(native["ok"], false, "{error:?}");
    let reason = &native["reason"];
    match error {
        Error::Read(ae_node::configuration::presets::Error::Io { kind, .. }) => {
            assert_eq!(kind, std::io::ErrorKind::IsADirectory);
            assert_eq!(reason[1][1][1][1], "eisdir");
        }
        Error::Json | Error::Height => assert_eq!(reason, "badarg"),
        Error::Object => assert_eq!(reason[1][0], "badmap"),
        Error::Encoding(ae_codec::api::ApiError::MissingPrefix) => {
            assert_eq!(reason, "missing_prefix")
        }
        Error::Prefix { .. } | Error::Fate(ae_vm_fate::Error::TrailingBytes) => {
            assert_eq!(reason[1][0], "badmatch")
        }
        other => panic!("unexpected error: {other:?}, native {native}"),
    }
}
#[test]
fn original_auxiliary_files_match_including_hamt_collisions_and_real_overrides() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_startup_auxiliary.json")).unwrap();
    assert_eq!(rows.len(), 116);
    for row in rows {
        let tmp = Temp::new();
        let data = tmp.0.join("aecore");
        let ctx = tmp.context();
        let name = &row["name"][1];
        let network = binary(&name[1]);
        let native = &row["result"];
        if name[0] == "paths" {
            let user = if name[2] == "undefined" {
                json!({})
            } else {
                json!({"sync":{"whitelist_file":mapped_path(&name[2],&data)}})
            };
            let config = Configuration::from_value(user).unwrap();
            let resolved = config.resolve(&ctx).unwrap();
            let plan = Plan::resolve(&resolved, &network, &data).unwrap();
            assert_eq!(
                plan.whitelist.to_str().unwrap(),
                mapped_path(&native["value"][1][0], &data)
            );
            assert_eq!(
                plan.ordering.file_name().unwrap().to_str().unwrap(),
                text(&native["value"][1][1])
            );
            continue;
        }
        let config = Configuration::empty();
        let resolved = config.resolve(&ctx).unwrap();
        let plan = Plan::resolve(&resolved, &network, &data).unwrap();
        let asset = if name[0] == "whitelist" {
            Asset::Whitelist
        } else {
            Asset::PreIrisOrdering
        };
        let path = data.join(auxiliary::filename(&network, asset));
        let input = &name[3];
        if input == "directory" {
            std::fs::create_dir(&path).unwrap();
        } else if input != "missing" {
            std::fs::write(&path, binary(input)).unwrap();
        }
        match plan.load(Limits::default()) {
            Err(error) => compare_failure(error, native),
            Ok(loaded) => {
                assert_eq!(native["ok"], true, "{}", name[0]);
                assert_eq!(native["value"][0], "map");
                let pairs = native["value"][1].as_array().unwrap();
                if name[0] == "whitelist" {
                    let expected: BTreeMap<_, _> = pairs
                        .iter()
                        .map(|p| {
                            (
                                p[0].to_string().parse::<num_bigint::BigInt>().unwrap(),
                                ae_types::Hash256(binary(&p[1]).try_into().unwrap()),
                            )
                        })
                        .collect();
                    assert_eq!(loaded.whitelist().entries(), &expected, "{name}");
                    for (height, hash) in loaded.whitelist().checkpoints() {
                        assert_eq!(expected.get(&height.into()), Some(&hash));
                    }
                } else {
                    let expected: BTreeMap<_, _> =
                        pairs.iter().map(|p| (fate(&p[0]), fate(&p[1]))).collect();
                    assert_eq!(
                        loaded
                            .overrides()
                            .entries()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<BTreeMap<_, _>>(),
                        expected,
                        "{} / {}",
                        name[0],
                        name[2]
                    );
                }
                let receipt = &loaded.inputs()[if name[0] == "whitelist" { 0 } else { 1 }];
                if input == "missing" {
                    assert_eq!(receipt.hash, None);
                    assert_eq!(receipt.bytes, 0);
                } else {
                    let bytes = binary(input);
                    assert_eq!(receipt.hash, Some(ae_crypto::blake2b_256(&bytes)));
                    assert_eq!(receipt.bytes, bytes.len());
                }
            }
        }
    }
    println!(
        "Auxiliary startup: 116 original path/file cases, 30 decoded-key collision cases across the native 32-entry boundary, and both original public pre-Iris tables"
    );
}
#[test]
fn original_setup_expansion_and_filename_composition() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_startup_auxiliary_paths.json")).unwrap();
    let original_cwd = rows[1]["result"]["path"].as_str().unwrap();
    let cwd = Path::new("/capture-cwd");
    let mut checked = 0;
    let mut schema_rejected = 0;
    for row in &rows {
        let mut ctx = Context::new(Directories {
            home: "/release".into(),
            data: "/data".into(),
            log: "/log".into(),
        })
        .unwrap();
        ctx.intern("unset".into()).unwrap();
        ctx.set("aeutils".into(), "other".into(), json!("/utils/white.json"))
            .unwrap();
        ctx.set("aecore".into(), "other".into(), json!("/core/white.json"))
            .unwrap();
        ctx.set_global_variables(vec![
            Definition::new("ROOT".into(), Variable::Literal(json!("$env(other)"))).unwrap(),
        ])
        .unwrap();
        let config = match Configuration::from_value(row["user"].clone()) {
            Ok(config) => config,
            Err(ae_node::configuration::Error::Schema { path }) => {
                assert_eq!(path, "/sync/whitelist_file");
                assert!(matches!(
                    row["user"]["sync"]["whitelist_file"],
                    Value::Bool(_) | Value::Number(_)
                ));
                schema_rejected += 1;
                continue;
            }
            Err(error) => panic!("unexpected configuration failure: {error:?}"),
        };
        let resolved = config.resolve(&ctx).unwrap();
        let actual = Plan::resolve(&resolved, row["network"].as_str().unwrap().as_bytes(), cwd);
        let native = &row["result"];
        assert_eq!(
            actual.is_ok(),
            native["ok"].as_bool().unwrap(),
            "{}: {actual:?}",
            row["name"]
        );
        if let Ok(plan) = actual {
            assert_eq!(
                plan.whitelist.to_str().unwrap(),
                native["path"]
                    .as_str()
                    .unwrap()
                    .replace(original_cwd, cwd.to_str().unwrap()),
                "{}",
                row["name"]
            );
        }
        checked += 1;
    }
    assert_eq!(rows.len(), 54);
    assert_eq!((checked, schema_rejected), (48, 6));
    println!(
        "Auxiliary paths: 48 original setup/user-cache compositions; six raw non-string inputs rejected by the existing configuration schema"
    );
}
#[test]
fn missing_special_files_and_resource_exhaustion_do_not_publish_partial_inputs() {
    let tmp = Temp::new();
    let ctx = tmp.context();
    let config = Configuration::empty();
    let resolved = config.resolve(&ctx).unwrap();
    assert!(matches!(
        Plan::resolve(&resolved, b"ae_dev", Path::new("relative")),
        Err(Error::WorkingDirectory)
    ));
    let plan = Plan::resolve(&resolved, b"ae_dev", &tmp.0).unwrap();
    assert!(
        plan.load(Limits::default())
            .unwrap()
            .inputs()
            .iter()
            .all(|i| i.hash.is_none())
    );
    std::fs::write(&plan.ordering, b"{}").unwrap();
    assert!(matches!(
        plan.load(Limits {
            bytes: 1,
            ..Default::default()
        }),
        Err(Error::Read(ae_node::configuration::presets::Error::Limit))
    ));
    let hash = ae_codec::api::encode(ae_codec::api::ApiType::KeyBlockHash, &[7; 32]);
    let bytes = serde_json::to_vec(&json!({"1":hash})).unwrap();
    assert!(matches!(
        auxiliary::parse_whitelist(
            &bytes,
            Limits {
                entries: 0,
                ..Default::default()
            }
        ),
        Err(Error::Limit)
    ));
    let one = encoding::encode(&Fate::Integer(1.into()), Default::default()).unwrap();
    let two = encoding::encode(&Fate::Integer(2.into()), Default::default()).unwrap();
    let mut table = Overrides::default();
    let mut remaining = ae_vm_fate::Limits::default();
    table.insert_encoded(&one, &two, &mut remaining).unwrap();
    let before = (
        remaining.max_bytes,
        remaining.max_nodes,
        table
            .entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>(),
    );
    let mut bad = two.clone();
    bad.push(0);
    assert_eq!(
        table.insert_encoded(&two, &bad, &mut remaining),
        Err(ae_vm_fate::Error::TrailingBytes)
    );
    assert_eq!(
        before,
        (
            remaining.max_bytes,
            remaining.max_nodes,
            table
                .entries()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        )
    );
    let mut small = ae_vm_fate::Limits {
        max_nodes: 1,
        ..Default::default()
    };
    assert_eq!(
        table.insert_encoded(&two, &one, &mut small),
        Err(ae_vm_fate::Error::ResourceLimit)
    );
    assert_eq!(small.max_nodes, 1);
    assert_eq!(table.entries().len(), 1);
}
