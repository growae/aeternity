use super::*;
use ae_types::AccountPublicKey;
use serde_json::json;

fn number(value: &BigInt) -> Value {
    serde_json::from_str(&value.to_string()).unwrap()
}
fn integer(value: &Value) -> BigInt {
    value.to_string().parse().unwrap()
}
fn system(value: &Value) -> System {
    if value.is_null() {
        return System::default();
    }
    System {
        edge_bits: integer(&value[0]),
        miners: Vec::new(),
    }
    .with_miners(&value[1])
    .unwrap()
}
fn definition(miner: &Miner, edge_bits: &BigInt) -> Value {
    json!({"executable":miner.executable,"executable_group":miner.executable_group,"extra_args":miner.extra_args,"hex_encoded_header":miner.hex_encoded_header,"repeats":number(&miner.repeats),"edge_bits":number(edge_bits),"addressed_instances":miner.addressed_instances.as_ref().map(|ids| ids.iter().map(number).collect::<Vec<_>>())})
}
fn parse(value: Value) -> Result<Configuration, Error> {
    Configuration::parse(&serde_json::to_vec(&value).unwrap(), &System::default())
}
fn directories() -> Directories {
    Directories {
        cuckoo: Some(std::env::temp_dir()),
        prebuilt: Some(std::env::temp_dir().join("prebuilt miners")),
    }
}
fn prepare(config: &Configuration) -> Result<Prepared, Error> {
    config.prepare(
        &directories(),
        Default::default(),
        mining::Config::new(AccountPublicKey([7; 32])),
    )
}

#[test]
fn system_tuple_translation_retains_existing_resource_limits_before_workers() {
    let row = json!(["miner", "", false, 1, null, "aecuckoo"]);
    let parse = |value: Value| System::default().with_miners(&value);
    assert_eq!(
        parse(json!(vec![row.clone(); MAX_DEFINITIONS + 1])),
        Err(Error::Limit("miners"))
    );
    let mut device_limit = row.clone();
    device_limit[4] = json!(vec![0; MAX_INSTANCES + 1]);
    assert_eq!(parse(json!([device_limit])), Err(Error::Limit("instances")));
    let mut text_limit = row.clone();
    text_limit[1] = json!("x".repeat(MAX_TEXT + 1));
    assert_eq!(parse(json!([text_limit])), Err(Error::Limit("miner text")));
    let mut huge = row;
    huge[3] = serde_json::from_str("18446744073709551616").unwrap();
    let system = parse(json!([huge])).unwrap();
    let config = Configuration::from_value(&json!({}), &system).unwrap();
    assert_eq!(config.miners[0].repeats, BigInt::from(u64::MAX) + 1);
    assert!(matches!(prepare(&config), Err(Error::Attempts)));
}

#[test]
fn native_configuration_and_composed_instance_capture_matches_without_integer_narrowing() {
    let rows: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_pow_mining_configuration.json"
    )))
    .unwrap();
    let mut matched = 0;
    let mut conflicts = 0;
    let mut invalid_devices = 0;
    for row in rows.as_array().unwrap() {
        let result = Configuration::from_value(&row["user"], &system(&row["sys"]));
        if row["check"]["status"] != "ok" {
            assert_eq!(result, Err(Error::Conflict));
            conflicts += 1;
        } else if row["name"] == "field-addressed_instances-[<<\"gpu\">>]" {
            // The native raw constructor accepts any list; its execution path
            // later requires integer_to_list. User schema also rejects this.
            assert_eq!(result, Err(Error::Field("addressed_instances")));
            invalid_devices += 1;
        } else if row["result"]["status"] != "ok" {
            assert!(result.is_err(), "{row}");
            matched += 1;
        } else {
            let config = result.unwrap_or_else(|e| panic!("{e:?}: {row}"));
            let observed: Vec<_> = config
                .miners
                .iter()
                .map(|m| definition(m, &config.edge_bits))
                .collect();
            assert_eq!(json!(observed), row["result"]["configs"], "{row}");
            let instances: Vec<_> = config.instances().unwrap().iter().map(|i| json!({"id":i.native_id,"device":i.device.as_ref().map(number),"state":"available","config":definition(&config.miners[i.configuration_index], &config.edge_bits)})).collect();
            assert_eq!(json!(instances), row["result"]["instances"], "{row}");
            matched += 1;
        }
    }
    assert_eq!((matched, conflicts, invalid_devices), (82, 3, 1));
}

#[test]
fn original_complete_node_schema_acceptance_matches_without_inserting_defaults() {
    let rows: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_pow_mining_schema.json"
    )))
    .unwrap();
    let mut accepted = 0;
    for row in rows.as_array().unwrap() {
        let user = &row["user"];
        let expected = row["result"]["status"] == "ok";
        assert_eq!(parse::schema(user).is_ok(), expected, "{row}");
        let parsed = Configuration::parse(&serde_json::to_vec(user).unwrap(), &System::default());
        if expected {
            accepted += 1;
            assert_eq!(row["result"]["unchanged"], true);
            assert_eq!(&row["result"]["value"]["mining"]["cuckoo"], user);
            // Native constructors impose further guards after schema checks:
            // integer-valued negative repeats/edge bits are schema-valid.
            assert_eq!(parsed, Configuration::from_value(user, &System::default()));
        } else {
            assert!(parsed.is_err(), "{row}");
        }
    }
    assert_eq!((rows.as_array().unwrap().len(), accepted), (98, 58));
}

#[test]
fn user_schema_distinguishes_omitted_empty_deprecated_and_ignored_settings() {
    let default = parse(json!({})).unwrap();
    assert_eq!(default.miners[0].executable, "mean29-generic");
    assert_eq!(default.instances().unwrap().len(), 1);
    assert!(
        parse(json!({"miners":[]}))
            .unwrap()
            .instances()
            .unwrap()
            .is_empty()
    );
    for (count, devices) in [(0, vec![]), (3, vec![2, 1, 0])] {
        let cfg = parse(json!({"miner":{"executable":"legacy", "extra_args":"", "edge_bits":-1, "instances":count, "nice":-20}})).unwrap();
        assert_eq!(cfg.edge_bits, 29.into());
        assert_eq!(
            cfg.instances()
                .unwrap()
                .into_iter()
                .map(|i| i.device.unwrap())
                .collect::<Vec<_>>(),
            devices.into_iter().map(BigInt::from).collect::<Vec<_>>()
        );
    }
    let modern = parse(json!({"miners":[{"executable":"a","nice":20}]})).unwrap();
    assert_eq!(modern.miners[0], Miner::new("a".into()));
    for value in [
        json!({"unknown":0}),
        json!({"miners":[{"executable":"a","edge_bits":29}]}),
        json!({"miners":[{"executable":"a","addressed_instances":[]}]}),
        json!({"miners":[{"executable":"a","nice":false}]}),
        json!({"miner":{"executable":"a","edge_bits":29}}),
        json!({"miner":{"executable":"a","extra_args":""}}),
        json!({"miners":[{"executable":"a","executable_group":"unknown"}]}),
        json!({"miners":[{"executable":"a","repeats":1.0}]}),
        json!(null),
    ] {
        assert!(parse(value.clone()).is_err(), "{value}");
    }
    assert_eq!(
        parse(json!({"miners":[],"miner":{"executable":"b","extra_args":"","edge_bits":29}})),
        Err(Error::Conflict)
    );
}

#[test]
fn precedence_and_deprecated_override_use_only_selected_native_fields() {
    let mut sys = System::default();
    sys.miners[0].repeats = 0.into();
    assert!(Configuration::from_value(&json!({}), &sys).is_err());
    assert!(Configuration::from_value(&json!({"miners":[]}), &sys).is_ok());
    let cfg = Configuration::from_value(&json!({"miner":{"executable":"a","instances":2,"addressed_instances":"ignored","edge_bits":15}}),&System::default()).unwrap();
    assert_eq!(cfg.edge_bits, 29.into());
    assert_eq!(
        cfg.miners[0].addressed_instances,
        Some(vec![0.into(), 1.into()])
    );
    sys.edge_bits = 17.into();
    let cfg = Configuration::from_value(&json!({"miners":[{"executable":"user"}]}), &sys).unwrap();
    assert_eq!(cfg.edge_bits, 17.into());
    assert_eq!(prepare(&cfg).unwrap().config.edge_bits.get(), 17);
}

#[test]
fn preparation_preserves_order_paths_arguments_devices_and_runtime_controls() {
    let huge: Value = serde_json::from_str("18446744073709551616").unwrap();
    let absolute = std::env::temp_dir().join("absolute miner");
    let cfg = parse(json!({"miners":[{"executable":"relative miner","repeats":2,"extra_args":"-x  a\tb","hex_encoded_header":true,"addressed_instances":[-1,huge]}, {"executable":absolute,"executable_group":"aecuckooprebuilt","repeats":3}]})).unwrap();
    let mut original = mining::Config::new(AccountPublicKey([7; 32]));
    original.retry = std::time::Duration::from_millis(8);
    original.attempt_timeout = None;
    original.release_info = 17;
    let prepared = cfg
        .prepare(&directories(), Default::default(), original.clone())
        .unwrap();
    assert_eq!(
        prepared
            .instances
            .iter()
            .map(|i| i.native_id)
            .collect::<Vec<_>>(),
        [2, 1, 0]
    );
    assert_eq!(prepared.config.beneficiary, original.beneficiary);
    assert_eq!(prepared.config.retry, original.retry);
    assert_eq!(prepared.config.attempt_timeout, None);
    assert_eq!(prepared.config.release_info, 17);
    assert_eq!(
        prepared
            .config
            .instances
            .iter()
            .map(|i| i.repeats)
            .collect::<Vec<_>>(),
        [2, 2, 3]
    );
    for (slot, expected) in prepared.config.instances.iter().zip(&prepared.instances) {
        let external = slot.external.as_ref().unwrap();
        assert_eq!(external.device, expected.device);
        if expected.native_id == 0 {
            assert_eq!(external.executable, absolute);
            assert_eq!(external.directory, directories().prebuilt.unwrap());
        } else {
            assert_eq!(
                external.executable,
                std::env::temp_dir().join("relative miner")
            );
            assert_eq!(
                external.extra_args,
                ["-x", "a\tb"].map(std::ffi::OsString::from)
            );
            assert!(external.hex_header);
        }
    }
}

#[test]
fn resource_and_consensus_bounds_fail_before_any_runtime_is_created() {
    let cfg = parse(json!({"miners":[{"executable":"a","repeats":65}]})).unwrap();
    assert_eq!(prepare(&cfg).err(), Some(Error::Attempts));
    let cfg = parse(
        serde_json::from_str(r#"{"miners":[{"executable":"a","repeats":18446744073709551616}]}"#)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cfg.miners[0].repeats.to_string(), "18446744073709551616");
    assert_eq!(prepare(&cfg).err(), Some(Error::Attempts));
    let cfg = parse(json!({"edge_bits":15})).unwrap();
    assert_eq!(prepare(&cfg).unwrap().config.edge_bits.get(), 15);
    let cfg = parse(json!({"edge_bits":64})).unwrap();
    assert_eq!(prepare(&cfg).err(), Some(Error::Consensus));
    let cfg = parse(json!({})).unwrap();
    assert_eq!(
        cfg.prepare(
            &Directories::default(),
            Default::default(),
            mining::Config::new(AccountPublicKey([7; 32]))
        )
        .err(),
        Some(Error::Directory)
    );
    let mut dirs = directories();
    dirs.cuckoo = Some("relative".into());
    assert_eq!(
        cfg.prepare(
            &dirs,
            Default::default(),
            mining::Config::new(AccountPublicKey([7; 32]))
        )
        .err(),
        Some(Error::Directory)
    );
    assert_eq!(
        cfg.prepare(
            &directories(),
            miner::Config {
                requests: 0,
                ..Default::default()
            },
            mining::Config::new(AccountPublicKey([7; 32]))
        )
        .err(),
        Some(Error::Worker(miner::Error::Configuration))
    );
    for executable in ["", "bad\0path"] {
        assert_eq!(
            prepare(&parse(json!({"miners":[{"executable":executable}]})).unwrap()).err(),
            Some(Error::Executable)
        );
    }
    assert!(
        Configuration::from_value(
            &json!({"miner":{"executable":"a","instances":65}}),
            &System::default()
        )
        .is_err()
    );
    assert_eq!(
        parse(json!({"miners":vec![json!({"executable":"a"});65]})),
        Err(Error::Limit("instances"))
    );
    assert_eq!(
        parse(json!({"miners":vec![json!({"executable":"a","addressed_instances":[]});1025]})),
        Err(Error::Limit("miners"))
    );
    assert_eq!(
        parse(json!({"miners":[{"executable":"x".repeat(MAX_TEXT+1)}]})),
        Err(Error::Limit("miner text"))
    );
    assert_eq!(
        Configuration::parse(&vec![b' '; (16 << 20) + 1], &System::default()),
        Err(Error::Limit("json"))
    );
    assert_eq!(
        Configuration::parse(b"{", &System::default()),
        Err(Error::Json)
    );
    let mut large = cfg;
    large.edge_bits = BigInt::from(1) << 16385;
    assert_eq!(large.instances(), Err(Error::Limit("integer")));
    let empty = parse(json!({"miners":[]}))
        .unwrap()
        .prepare(
            &Directories::default(),
            Default::default(),
            mining::Config::new(AccountPublicKey([7; 32])),
        )
        .unwrap();
    assert!(empty.config.instances.is_empty() && empty.instances.is_empty());
}
