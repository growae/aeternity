use super::*;
use ae_sync::chain::pow::{configuration as mining, producer};
use ae_types::AccountPublicKey;
use num_bigint::BigInt;

fn number(v: &BigInt) -> Value {
    serde_json::from_str(&v.to_string()).unwrap()
}
fn definition(m: &mining::Miner, bits: &BigInt) -> Value {
    json!({"executable":m.executable,"extra_args":m.extra_args,"executable_group":m.executable_group,"hex_encoded_header":m.hex_encoded_header,"repeats":number(&m.repeats),"edge_bits":number(bits),"addressed_instances":m.addressed_instances.as_ref().map(|ids|ids.iter().map(number).collect::<Vec<_>>())})
}
#[test]
fn application_miner_tuples_match_native_selection_guards_order_and_instances() {
    let rows: Value =
        serde_json::from_str(include_str!("../fixtures/erlang_application_miners.json")).unwrap();
    let mut accepted = 0;
    assert_eq!(rows.as_array().unwrap().len(), 26);
    for row in rows.as_array().unwrap() {
        let cfg = Configuration::from_value(json!({"mining":{"cuckoo":row["user"]}})).unwrap();
        let mut ctx = context();
        if !row["sys"].is_null() {
            ctx.set("aecore".into(), "aec_mining".into(), row["sys"].clone())
                .unwrap();
        }
        let resolved = cfg.resolve(&ctx).unwrap();
        let result = resolved.mining(&Default::default());
        if row["check"]["status"] != "ok"
            || row["result"]["status"] != "ok"
            || row["graph"]["status"] != "ok"
        {
            assert!(result.is_err(), "{row}");
            continue;
        }
        let parsed = result.unwrap_or_else(|e| panic!("{e:?}: {row}"));
        assert_eq!(number(&parsed.edge_bits), row["graph"]["value"]);
        let configs: Vec<_> = parsed
            .miners
            .iter()
            .map(|m| definition(m, &parsed.edge_bits))
            .collect();
        assert_eq!(json!(configs), row["result"]["configs"], "{row}");
        let instances: Vec<_> = parsed.instances().unwrap().iter().map(|i| json!({"id":i.native_id,"device":i.device.as_ref().map(number),"state":"available","config":definition(&parsed.miners[i.configuration_index],&parsed.edge_bits)})).collect();
        assert_eq!(json!(instances), row["result"]["instances"], "{row}");
        accepted += 1;
    }
    assert_eq!(accepted, 12);
}

#[test]
fn captured_application_miners_expand_and_prepare_exact_external_worker_configuration() {
    use std::sync::Arc;
    let cfg = Configuration::empty();
    let mut ctx = context();
    ctx.set("aecore".into(), "exec".into(), json!("selected"))
        .unwrap();
    ctx.set(
        "aecore".into(),
        "aec_mining".into(),
        json!([
            15,
            [
                [
                    "$env(exec)",
                    "--first  $HOME\tlast",
                    true,
                    3,
                    [2, 0, 2],
                    "aecuckooprebuilt"
                ],
                ["ordinary", "", false, 2, null, "aecuckoo"]
            ]
        ]),
    )
    .unwrap();
    let snapshot = cfg.capture(Arc::new(ctx.clone())).unwrap();
    ctx.set("aecore".into(), "aec_mining".into(), json!([29, []]))
        .unwrap();
    let fallback = mining::System {
        edge_bits: 17.into(),
        miners: vec![mining::Miner::new("fallback".into())],
    };
    let parsed = snapshot.resolved().mining(&fallback).unwrap();
    assert_eq!(parsed.edge_bits, 15.into());
    let prepared = parsed
        .prepare(
            &mining::Directories {
                cuckoo: Some("/miners".into()),
                prebuilt: Some("/prebuilt".into()),
            },
            Default::default(),
            producer::mining::Config::new(AccountPublicKey([1; 32])),
        )
        .unwrap();
    assert_eq!(prepared.config.edge_bits.get(), 15);
    assert_eq!(
        prepared
            .instances
            .iter()
            .map(|i| i.native_id)
            .collect::<Vec<_>>(),
        vec![3, 2, 1, 0]
    );
    assert_eq!(
        prepared
            .instances
            .iter()
            .map(|i| i.device.clone())
            .collect::<Vec<_>>(),
        vec![Some(2.into()), Some(0.into()), Some(2.into()), None]
    );
    for worker in &prepared.config.instances[..3] {
        let external = worker.external.as_ref().unwrap();
        assert_eq!(worker.repeats, 3);
        assert_eq!(external.executable, PathBuf::from("/prebuilt/selected"));
        assert_eq!(external.directory, PathBuf::from("/prebuilt"));
        assert_eq!(
            external.extra_args,
            vec![
                std::ffi::OsString::from("--first"),
                std::ffi::OsString::from("/release\tlast")
            ]
        );
        assert!(external.hex_header);
    }
    let ordinary = &prepared.config.instances[3];
    assert_eq!(ordinary.repeats, 2);
    assert_eq!(
        ordinary.external.as_ref().unwrap().executable,
        PathBuf::from("/miners/ordinary")
    );
    assert!(!ordinary.external.as_ref().unwrap().hex_header);
    assert!(
        cfg.resolve(&ctx)
            .unwrap()
            .mining(&fallback)
            .unwrap()
            .miners
            .is_empty()
    );
    ctx.remove("aecore", "aec_mining");
    assert_eq!(
        cfg.resolve(&ctx).unwrap().mining(&fallback).unwrap().miners,
        fallback.miners
    );
}
