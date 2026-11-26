use super::*;
use crate::configuration::{
    Configuration,
    resolution::{Context, Directories},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Lab(PathBuf);
impl Lab {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ae-currency-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn write(&self, name: &str, bytes: &[u8]) {
        std::fs::write(self.0.join(name), bytes).unwrap();
    }
    fn context(&self) -> Context {
        Context::new(Directories {
            home: self.0.display().to_string(),
            data: self.0.display().to_string(),
            log: self.0.display().to_string(),
        })
        .unwrap()
    }
    fn plan(&self, user: Value, network: &[u8]) -> Plan {
        let config = Configuration::from_value(user).unwrap();
        let context = self.context();
        Plan::resolve(&config.resolve(&context).unwrap(), network, &self.0).unwrap()
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn json(metadata: Metadata) -> Value {
    let mut fields = metadata.fields;
    fields.insert(
        "network_name".into(),
        String::from_utf8(metadata.network_name).unwrap().into(),
    );
    Value::Object(fields)
}

#[test]
fn original_currency_dispatcher_matches_defaults_precedence_logo_files_and_live_changes() {
    let corpus: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/erlang_currency.json")).unwrap();
    let rows = corpus["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 83);
    let lab = Lab::new();
    let bytes = [0, 1, 2, 127, 128, 255, 10, 13, 34, 92];
    std::fs::create_dir(lab.0.join("nested.dir")).unwrap();
    std::fs::create_dir(lab.0.join("directory.png")).unwrap();
    for name in [
        "logo.png",
        "logo.SVG",
        "logo",
        ".hidden",
        "logo.",
        "a.b.c",
        "..logo",
        "space logo.ä",
        "nested.dir/logo",
        "back\\slash.png",
    ] {
        lab.write(name, &bytes);
    }
    lab.write("empty.png", b"");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("logo.png", lab.0.join("link.SVG")).unwrap();
        std::os::unix::fs::symlink("missing.png", lab.0.join("dangling.png")).unwrap();
    }
    let mut compared = 0;
    for row in rows {
        if row["code"] != 200 {
            continue;
        }
        let filename = row["config"]["chain"]["display"]["logo_file"].as_str();
        #[cfg(not(unix))]
        if matches!(filename, Some("link.SVG" | "dangling.png")) {
            continue;
        }
        if filename == Some("changing.png") {
            if let Some(data) = row["body"]["logo"]["data"].as_str() {
                lab.write("changing.png", &STANDARD.decode(data).unwrap());
            } else {
                std::fs::remove_file(lab.0.join("changing.png")).unwrap();
            }
        }
        let p = lab.plan(
            row["config"].clone(),
            row["network"].as_str().unwrap().as_bytes(),
        );
        let result = json(p.read(Limits::default()).unwrap());
        assert_eq!(result, row["body"], "case {compared}: {row}");
        assert_eq!(row["queue"], "http_update");
        assert_eq!(row["network_reads"], 1);
        compared += 1;
    }
    #[cfg(unix)]
    assert_eq!(compared, 81);
}

#[test]
fn currency_uses_resolved_user_values_explicit_cwd_and_raw_network_identity() {
    let lab = Lab::new();
    let mut ctx = lab.context();
    ctx.set(
        "aecore".into(),
        "currency".into(),
        json!({"name":"wrong","fiat_conversion_url":"wrong"}),
    )
    .unwrap();
    ctx.set(
        "aecore".into(),
        "display".into(),
        json!({"network_name":"wrong","logo_file":"wrong.png"}),
    )
    .unwrap();
    let config = Configuration::from_value(json!({"chain":{"currency":{"name":"$DATA_DIR"},"display":{"logo_file":"$DATA_DIR/logo.svg"}}})).unwrap();
    let resolved = config.resolve(&ctx).unwrap();
    lab.write("logo.svg", b"old");
    let plan = Plan::resolve(&resolved, &[0xff, 0xed, 0xa0, 0x41], &lab.0).unwrap();
    let first = plan.read(Limits::default()).unwrap();
    assert_eq!(first.network_name, [0xff, 0xed, 0xa0, 0x41]);
    assert_eq!(first.fields["name"], lab.0.to_str().unwrap());
    assert_eq!(first.fields["symbol"], "AE");
    assert!(!first.fields.contains_key("fiat_conversion_url"));
    assert_eq!(first.fields["logo"], json!({"type":"svg","data":"b2xk"}));
    lab.write("logo.svg", b"new");
    assert_eq!(
        plan.read(Limits::default()).unwrap().fields["logo"]["data"],
        "bmV3"
    );
    std::fs::remove_file(lab.0.join("logo.svg")).unwrap();
    assert!(
        !plan
            .read(Limits::default())
            .unwrap()
            .fields
            .contains_key("logo")
    );
    assert!(matches!(
        Plan::resolve(&resolved, b"custom", Path::new(".")),
        Err(Error::WorkingDirectory)
    ));
    let independent = lab.plan(json!({}), b"ae_uat");
    assert_eq!(
        independent.read(Limits::default()).unwrap().network_name,
        b"Testnet"
    );
}

#[test]
fn currency_bounds_files_aggregate_output_and_special_files_before_encoding() {
    let lab = Lab::new();
    let plan = lab.plan(
        json!({"chain":{"display":{"logo_file":"large.svg"}}}),
        b"custom",
    );
    lab.write("large.svg", &[7; 4096]);
    assert!(matches!(
        plan.read(Limits {
            logo_bytes: 4095,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        plan.read(Limits {
            output_bytes: 5000,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    ));
    let metadata = plan
        .read(Limits {
            logo_bytes: 4096,
            output_bytes: 6000,
        })
        .unwrap();
    assert!(serde_json::to_vec(&json(metadata)).unwrap().len() <= 6000);
    assert!(matches!(
        plan.read(Limits {
            output_bytes: usize::MAX,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    ));
    let control = "\u{1}\"\\".repeat(1000);
    let p = lab.plan(
        json!({"chain":{"currency":{"name":control},"display":{"network_name":control}}}),
        b"custom",
    );
    assert!(matches!(
        p.read(Limits {
            output_bytes: 4000,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    ));
    let data = p
        .read(Limits {
            output_bytes: 40000,
            ..Default::default()
        })
        .unwrap();
    assert!(serde_json::to_vec(&json(data)).unwrap().len() <= 40000);
    lab.write("empty.svg", b"");
    let p = lab.plan(
        json!({"chain":{"display":{"logo_file":"empty.svg"}}}),
        b"custom",
    );
    assert_eq!(
        p.read(Limits {
            logo_bytes: 0,
            ..Default::default()
        })
        .unwrap()
        .fields["logo"]["data"],
        ""
    );
    #[cfg(unix)]
    {
        assert!(
            std::process::Command::new("mkfifo")
                .arg(lab.0.join("pipe.svg"))
                .status()
                .unwrap()
                .success()
        );
        let p = lab.plan(
            json!({"chain":{"display":{"logo_file":"pipe.svg"}}}),
            b"custom",
        );
        assert!(matches!(p.read(Limits::default()), Err(Error::FileType)));
    }
}
