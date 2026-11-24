use ae_node::configuration::{
    Configuration, Discovery, Error, Format, MAX_BYTES, Source, schema_default,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, ffi::OsString, fs, path::PathBuf, process::Command};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_node_configuration.json")).unwrap()
}
#[test]
fn original_file_validation_and_exact_values() {
    let f = fixture();
    let mut cases = 0;
    for case in f["files"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let format = if case["format"] == "json" {
            Format::Json
        } else {
            Format::Yaml
        };
        let result = Configuration::parse(case["text"].as_str().unwrap().as_bytes(), format);
        assert_eq!(
            result.is_ok(),
            case["ok"].as_bool().unwrap(),
            "{name}: {result:?}"
        );
        if let Ok(config) = result {
            assert!(
                same_native(config.user_map(), &case["value"]),
                "{name}: {:?} != {:?}",
                config.user_map(),
                case["value"]
            );
        }
        cases += 1;
    }
    println!("Node configuration: {cases} native file comparisons, including YAML 1.1");
}
#[test]
fn original_environment_order_coercion_merge_and_defaults() {
    let f = fixture();
    for case in f["env"].as_array().unwrap() {
        let env = case["env"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().into()))
            .collect();
        let base = Configuration::from_value(case["base"].clone()).unwrap();
        let result = base.with_environment(&env);
        assert_eq!(
            result.is_ok(),
            case["ok"].as_bool().unwrap(),
            "{:?}: {result:?}",
            case["env"]
        );
        if let Ok(config) = result {
            assert_eq!(config.user_map(), &case["value"]);
        }
        assert_eq!(base.user_map(), &case["base"]);
    }
    for case in f["defaults"].as_array().unwrap() {
        let path: Vec<_> = case["path"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(schema_default(&path), Some(&case["value"]));
    }
    assert_eq!(
        Configuration::empty().get_or_schema_default(&["sync", "port"]),
        Some(&json!(3015))
    );
    assert_eq!(Configuration::empty().user_map(), &json!({}));
    println!("Node configuration: sixteen native environment cases and 333 schema-default lookups");
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn file(&self, name: &str, text: &str) -> PathBuf {
        let p = self.0.join(name);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, text).unwrap();
        p
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn discovery_precedence_read_limits_and_immutable_environment() {
    let d = Directory::new();
    let release = d.0.join("release");
    fs::create_dir(&release).unwrap();
    let mut select = Discovery {
        command_line: None,
        application_config: None,
        home: d.0.clone(),
        release_home: release,
    };
    let mut env = BTreeMap::new();
    assert!(select.select(&env).is_none());
    let legacy = d.file(".epoch/epoch/epoch.yaml", "{}");
    assert_eq!(
        select.select(&env),
        Some(Source {
            path: legacy,
            deprecated: true
        })
    );
    let rel = d.file("release/aeternity.yaml", "mempool: {tx_ttl: 1}");
    assert_eq!(select.select(&env).unwrap().path, rel);
    let home = d.file(
        ".aeternity/aeternity/aeternity.yaml",
        "mempool: {tx_ttl: 2}",
    );
    assert_eq!(select.select(&env).unwrap().path, home);
    let json = d.file(
        ".aeternity/aeternity/aeternity.json",
        "{\"mempool\":{\"tx_ttl\":3}}",
    );
    assert_eq!(select.select(&env).unwrap().path, json);
    env.insert(
        OsString::from("EPOCH_CONFIG"),
        OsString::from("absent.yaml"),
    );
    assert_eq!(select.select(&env).unwrap().path, json);
    select.application_config = Some(home.clone());
    assert_eq!(select.select(&env).unwrap().path, home);
    env.insert("AETERNITY_CONFIG".into(), rel.as_os_str().into());
    assert_eq!(select.select(&env).unwrap().path, rel);
    select.command_line = Some(json.clone());
    assert_eq!(select.select(&env).unwrap().path, json);
    env.insert("AE__MEMPOOL__TX_TTL".into(), "+00042".into());
    let (loaded, _) = select.load(&env).unwrap();
    assert_eq!(loaded.get(&["mempool", "tx_ttl"]), Some(&json!(42)));
    let saved = env.clone();
    assert_eq!(select.load(&env).unwrap().0, loaded);
    assert_eq!(env, saved);
    select.command_line = Some(d.0.join("absent.json"));
    assert!(matches!(select.load(&env), Err(Error::Io(_))));
    let huge = d.file("huge.json", "");
    fs::File::options()
        .write(true)
        .open(&huge)
        .unwrap()
        .set_len(MAX_BYTES as u64 + 1)
        .unwrap();
    assert_eq!(
        Source {
            path: huge,
            deprecated: false
        }
        .read(),
        Err(Error::Limit("input bytes"))
    );
    let dir = d.0.join("directory.yaml");
    fs::create_dir(&dir).unwrap();
    assert!(matches!(
        Source {
            path: dir,
            deprecated: false
        }
        .read(),
        Err(Error::Io(_))
    ));
    assert_eq!(
        Source {
            path: d.file("a.yml", "{}"),
            deprecated: false
        }
        .read(),
        Err(Error::Format)
    );
}
#[test]
fn yaml_encodings_alias_expansion_depth_and_numeric_limits() {
    let text = "keys: {dir: '路径/🔑'}";
    let expected = Configuration::parse(text.as_bytes(), Format::Yaml).unwrap();
    for little in [false, true] {
        let mut utf16 = if little {
            vec![255, 254]
        } else {
            vec![254, 255]
        };
        for word in text.encode_utf16() {
            utf16.extend(if little {
                word.to_le_bytes()
            } else {
                word.to_be_bytes()
            });
        }
        assert_eq!(
            Configuration::parse(&utf16, Format::Yaml).unwrap(),
            expected
        );
        let mut utf32 = if little {
            vec![255, 254, 0, 0]
        } else {
            vec![0, 0, 254, 255]
        };
        for c in text.chars() {
            utf32.extend(if little {
                (c as u32).to_le_bytes()
            } else {
                (c as u32).to_be_bytes()
            });
        }
        assert_eq!(
            Configuration::parse(&utf32, Format::Yaml).unwrap(),
            expected
        );
    }
    assert_eq!(
        Configuration::parse(&[255, 254, 0], Format::Yaml),
        Err(Error::Encoding)
    );
    let huge = format!("mempool: {{tx_ttl: {}}}", "1".repeat(4097));
    assert!(matches!(
        Configuration::parse(huge.as_bytes(), Format::Yaml),
        Err(Error::Limit(_))
    ));
    let deep = format!("{}0{}", "[".repeat(70), "]".repeat(70));
    assert!(matches!(
        Configuration::parse(deep.as_bytes(), Format::Yaml),
        Err(Error::Limit(_))
    ));
    let bomb = format!(
        "system: {{plugins: [{{name: a, config: {{x: [{}]}}}}]}}",
        std::iter::repeat_n("x", 100_001)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(matches!(
        Configuration::parse(bomb.as_bytes(), Format::Yaml),
        Err(Error::Limit(_))
    ));
    assert_eq!(
        Configuration::parse(&vec![b' '; MAX_BYTES + 1], Format::Yaml),
        Err(Error::Limit("input bytes"))
    );
}
#[test]
fn checker_process_exit_status_and_secret_redaction() {
    let d = Directory::new();
    let valid = d.file("ok.yaml", "{}");
    let success = Command::new(env!("CARGO_BIN_EXE_ae-check-config"))
        .arg(valid)
        .output()
        .unwrap();
    assert!(success.status.success());
    assert_eq!(success.stdout, b"Configuration is valid.\n");
    let invalid = d.file(
        "bad.json",
        "{\"keys\":{\"peer_password\":\"DO_NOT_LOG_ME\"},\"sync\":{\"port\":\"DO_NOT_LOG_ME\"}}",
    );
    let failed = Command::new(env!("CARGO_BIN_EXE_ae-check-config"))
        .arg(invalid)
        .output()
        .unwrap();
    assert_eq!(failed.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&failed.stderr).contains("DO_NOT_LOG_ME"));
    let usage = Command::new(env!("CARGO_BIN_EXE_ae-check-config"))
        .output()
        .unwrap();
    assert_eq!(usage.status.code(), Some(2));
    let c = Configuration::parse(
        br#"{"keys":{"peer_password":"DO_NOT_LOG_ME"}}"#,
        Format::Json,
    )
    .unwrap();
    assert!(!format!("{c:?}").contains("DO_NOT_LOG_ME"));
}

#[test]
fn original_yaml_aliases_and_version_specific_scalars() {
    let cases: Value = serde_json::from_str(include_str!(
        "fixtures/erlang_node_configuration_aliases.json"
    ))
    .unwrap();
    for case in cases["files"].as_array().unwrap() {
        let result = Configuration::parse(case["text"].as_str().unwrap().as_bytes(), Format::Yaml);
        assert_eq!(
            result.is_ok(),
            case["ok"].as_bool().unwrap(),
            "{}: {result:?}",
            case["name"]
        );
        if let Ok(config) = result {
            assert!(
                same_native(config.user_map(), &case["value"]),
                "{}: {:?} != {:?}",
                case["name"],
                config.user_map(),
                case["value"]
            );
        }
    }
    println!("Node configuration: 59 native YAML anchor and version cases");
}

// JSX and serde_json use different decimal spellings for the same IEEE value.
// Integers remain exact and cannot compare equal to a floating-point scalar.
fn same_native(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_str().contains(['.', 'e', 'E']);
            let bf = b.as_str().contains(['.', 'e', 'E']);
            af == bf
                && if af {
                    a.as_f64().map(f64::to_bits) == b.as_f64().map(f64::to_bits)
                } else {
                    a == b
                }
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|w| same_native(v, w)))
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(v, w)| same_native(v, w))
        }
        _ => a == b,
    }
}

#[test]
#[cfg(unix)]
fn special_files_are_rejected_without_waiting_for_a_writer() {
    let dir = Directory::new();
    let fifo = dir.0.join("pipe.yaml");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_ae-check-config"))
        .arg(fifo)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(1));
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(3) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("checker blocked opening a named pipe");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
