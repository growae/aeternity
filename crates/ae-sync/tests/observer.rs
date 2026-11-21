use ae_sync::observe::{
    Error, RotatingFile,
    metrics::{self, Kind, Number},
};
use ae_types::Unsigned;
use serde_json::Value;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

#[test]
fn original_erlang_numeric_statsd_lines() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_observer_metrics.json")).unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 559);
    for row in fixture["cases"].as_array().unwrap() {
        let n = &row["number"];
        let value = if let Some(hexadecimal) = n["unsigned_hex"].as_str() {
            let padded = if hexadecimal.len() % 2 == 1 {
                format!("0{hexadecimal}")
            } else {
                hexadecimal.into()
            };
            Number::Unsigned(Unsigned::from_field_bytes(&hex::decode(padded).unwrap()).unwrap())
        } else if let Some(integer) = n["integer"].as_str() {
            Number::Integer(integer.parse().unwrap())
        } else {
            Number::Float(f64::from_bits(
                u64::from_str_radix(n["float_bits"].as_str().unwrap(), 16).unwrap(),
            ))
        };
        let kind = match row["type"].as_str().unwrap() {
            "gauge" => Kind::Gauge,
            "counter" => Kind::Counter,
            "timer" => Kind::Timer,
            "histogram" => Kind::Histogram,
            "meter" => Kind::Meter,
            "set" => Kind::Set,
            _ => panic!(),
        };
        assert_eq!(
            metrics::line(row["name"].as_str().unwrap(), &value, kind).unwrap(),
            row["line"].as_str().unwrap().as_bytes(),
            "{row}"
        );
    }
}
#[test]
fn statsd_rejects_injection_and_nonfinite_values() {
    for name in ["", "peer\nforged:1|c", "peer:1", "peer|g", "peer#tag"] {
        assert_eq!(
            metrics::line(name, &Number::Integer(0), Kind::Gauge),
            Err(Error::Encoding)
        );
    }
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(Number::Float(n).text(), Err(Error::Encoding));
    }
}
#[test]
fn snapshots_preserve_integer_precision_and_export_cumulative_totals_as_gauges() {
    let big = "18446744073709551616000001";
    let snapshot = metrics::Snapshot {
        chain_revision: 7,
        pool_chain_revision: 6,
        registry_revision: 9,
        metrics: vec![
            metrics::Metric {
                name: "ae.epoch.aecore.chain.total_difficulty",
                value: Number::Unsigned(Unsigned::from_decimal(big).unwrap()),
            },
            metrics::Metric {
                name: "ae.rust.peer.served",
                value: Number::Integer(42),
            },
            metrics::Metric {
                name: "ae.epoch.aecore.sync.progress.value",
                value: Number::Float(1.0625),
            },
        ],
    };
    let json = snapshot.json().unwrap();
    assert_eq!(
        json["metrics"]["ae.epoch.aecore.chain.total_difficulty"].to_string(),
        big
    );
    assert_eq!(
        json["metrics"]["ae.epoch.aecore.sync.progress.value"].as_f64(),
        Some(1.0625)
    );
    assert_eq!(
        (
            json["chain_revision"].as_u64(),
            json["pool_chain_revision"].as_u64(),
            json["registry_revision"].as_u64()
        ),
        (Some(7), Some(6), Some(9))
    );
    let lines = snapshot.datagrams("node-1").unwrap();
    assert_eq!(
        lines[0],
        format!("node-1.ae.epoch.aecore.chain.total_difficulty:{big}|g").as_bytes()
    );
    assert_eq!(lines[1], b"node-1.ae.rust.peer.served:42|g");
    assert_eq!(
        lines[2],
        b"node-1.ae.epoch.aecore.sync.progress.value:1.063|g"
    );
}
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "ae-observer-log-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn rotating_log_bounds_new_files_preserves_records_and_reopens() {
    let directory = Directory::new();
    let path = directory.0.join("node.log");
    let mut log = RotatingFile::open(&path, 8, 2).unwrap();
    for line in [
        b"one\n", b"two\n", b"tri\n", b"for\n", b"fiv\n", b"six\n", b"svn\n",
    ] {
        log.write_all(line).unwrap();
    }
    log.flush().unwrap();
    drop(log);
    assert_eq!(fs::read(&path).unwrap(), b"svn\n");
    assert_eq!(
        fs::read(path.with_extension("log.1")).unwrap(),
        b"fiv\nsix\n"
    );
    assert_eq!(
        fs::read(path.with_extension("log.2")).unwrap(),
        b"tri\nfor\n"
    );
    assert!(!path.with_extension("log.3").exists());
    let mut log = RotatingFile::open(&path, 8, 2).unwrap();
    assert_eq!(
        log.write_all(b"oversized").unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    log.write_all(b"end\n").unwrap();
    log.flush().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"svn\nend\n");
}
#[test]
fn rotation_preserves_existing_oversized_file_and_reports_failure() {
    let directory = Directory::new();
    let path = directory.0.join("node.log");
    fs::write(&path, b"existing-large-operator-file").unwrap();
    let mut log = RotatingFile::open(&path, 8, 1).unwrap();
    log.write_all(b"new\n").unwrap();
    assert_eq!(
        fs::read(path.with_extension("log.1")).unwrap(),
        b"existing-large-operator-file"
    );
    fs::remove_file(path.with_extension("log.1")).unwrap();
    fs::create_dir(path.with_extension("log.1")).unwrap();
    assert!(log.write_all(b"rotate\n").is_err());
    assert_eq!(
        log.write_all(b"x").unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(fs::read(path).unwrap(), b"new\n");
}
