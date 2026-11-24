use super::*;
use crate::chain::pow::miner::{self, Worker};
use ae_consensus::{
    cuckoo::solver::Nonces,
    target::{HIGHEST_TARGET, Target},
};
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use std::{fs, io::Write, sync::Arc, time::Instant};

fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < end, "external miner wait");
        std::thread::sleep(Duration::from_millis(1));
    }
}
pub(crate) fn configuration(mode: &str) -> Config {
    let mut config = Config::new(std::env::current_exe().unwrap(), std::env::temp_dir());
    config.prefix_args = [
        "--exact",
        "chain::pow::miner::external::tests::child_entry",
        "--nocapture",
        "--ignored",
        "--",
    ]
    .map(OsString::from)
    .to_vec();
    config.extra_args = vec!["--fixture-mode".into(), mode.into()];
    config
}
fn work() -> Work {
    Work {
        hash: blake2b_256(b"wsffgujnjkqhduihsahswgdf"),
        target: HIGHEST_TARGET,
        edge_bits: 15,
        nonces: Nonces::reserve(38, 1).unwrap(),
    }
}
fn proof() -> String {
    let rows: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-consensus/tests/fixtures/erlang_pow_solver_solutions.json"
    )))
    .unwrap();
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["nonce"] == 38 && r["accepted"] == true)
        .unwrap();
    row["proof"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| format!("{:x}", v.as_u64().unwrap()))
        .collect::<Vec<_>>()
        .join(" ")
}
fn run(config: Config, work: Work) -> Arc<miner::Completion> {
    let worker = Worker::spawn_external(miner::Config::default(), config).unwrap();
    let ticket = worker.client().submit(work).unwrap();
    until(|| ticket.try_result().is_some());
    let result = ticket.blocking_wait();
    assert_eq!(result.id, ticket.id());
    assert_eq!(result.work, work);
    assert!(Arc::ptr_eq(&result, &ticket.blocking_wait()));
    let report = result.external.unwrap();
    assert_eq!(report.phase, Phase::Reaped, "{result:?}");
    assert!(report.pid.is_some() && report.exit.is_some());
    assert_eq!(result.attempts, None);
    assert_eq!(result.work_units, None);
    assert_eq!(worker.shutdown().completed, 1);
    result
}

#[test]
fn native_argument_and_parser_capture_preserves_supported_values_and_repairs_stream_boundaries() {
    let rows: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_pow_external_miner.json"
    )))
    .unwrap();
    let mut config = Config::new("/miner".into(), "/miners".into());
    let mut work = work();
    work.hash = Hash256([0; 32]);
    for row in rows["hash_encoding"].as_array().unwrap() {
        let nonce = row["nonce"].as_u64().unwrap();
        let packed = format!(
            "{}{}",
            STANDARD.encode(work.hash.0),
            STANDARD.encode(nonce.to_le_bytes())
        );
        assert_eq!(packed, row["raw"]);
    }
    config.hex_header = true;
    config.device = Some(7);
    config.extra_args = Config::native_extra_args("a  b\tc");
    let args = config.arguments(work);
    assert_eq!(args[1], OsString::from(rows["hex_hash"].as_str().unwrap()));
    assert_eq!(&args[6..], ["a", "b\tc", "-d", "7"].map(OsString::from));
    let mut native_matches = 0;
    let mut rejected_unrepresentable = 0;
    for row in rows["parsing"].as_array().unwrap() {
        work.target = Target(row["target"].as_u64().unwrap() as u32);
        work.edge_bits = 29;
        let mut got = Ok(None);
        for line in row["lines_base64"].as_array().unwrap() {
            got = protocol::parse(&STANDARD.decode(line.as_str().unwrap()).unwrap(), work);
            if !matches!(got, Ok(None)) {
                break;
            }
        }
        if row["result"]["status"] == "ok" {
            if let Some(nonce) = row["result"]["nonce"].as_u64()
                && row["result"]["proof"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|v| v.as_u64().is_some())
            {
                let Some(Outcome::Found {
                    nonce: observed,
                    proof,
                }) = got.unwrap()
                else {
                    panic!("{row}")
                };
                assert_eq!(observed, nonce);
                assert_eq!(serde_json::json!(proof.as_slice()), row["result"]["proof"]);
                native_matches += 1;
            } else {
                assert!(got.is_err());
                rejected_unrepresentable += 1;
            }
        } else {
            assert!(!matches!(got, Ok(Some(_))), "{row}");
            native_matches += 1;
        }
    }
    assert_eq!(native_matches + rejected_unrepresentable, 35);
    assert_eq!(rejected_unrepresentable, 3);
    for row in rows["fragmentation"].as_array().unwrap() {
        let mut framing = protocol::Framing::new(8192);
        let mut observed = vec![];
        let mut all: Vec<u8> = vec![];
        for chunk in row["chunks_base64"].as_array().unwrap() {
            let bytes = STANDARD.decode(chunk.as_str().unwrap()).unwrap();
            all.extend(&bytes);
            framing
                .feed::<()>(&bytes, |line| {
                    if !line.is_empty() {
                        observed.push(line.to_vec());
                    }
                    Ok(None)
                })
                .unwrap();
        }
        let mut expected: Vec<_> = all.split(|&b| b == b'\n').map(|s| s.to_vec()).collect();
        let pending = expected.pop().unwrap();
        expected.retain(|line| !line.is_empty());
        assert_eq!(observed, expected);
        assert_eq!(framing.buffer, pending);
    }
}

#[test]
fn actual_process_fragments_combined_output_and_returns_a_verified_replayed_small_graph_proof() {
    for mode in ["proof", "fragmented", "proof_exit"] {
        let result = run(configuration(mode), work());
        let Ok(Outcome::Found { nonce, proof }) = &result.result else {
            panic!("{result:?}")
        };
        assert_eq!(*nonce, 38);
        assert!(cuckoo::verify_proof_from_hash(
            work().hash,
            *nonce,
            proof.as_ref(),
            15
        ));
        assert!(result.external.unwrap().lines >= 1);
    }
}

#[test]
fn malformed_invalid_unreserved_unterminated_and_nonzero_results_remain_operational_failures() {
    for (mode, expected) in [
        ("invalid", miner::Error::InvalidProof),
        ("malformed", miner::Error::External(Error::BadSize)),
    ] {
        assert_eq!(
            run(configuration(mode), work()).result,
            Err(expected),
            "{mode}"
        );
    }
    for (mode, code) in [("unterminated", 0), ("nonzero", 7)] {
        let result = run(configuration(mode), work());
        assert_eq!(result.result, Ok(Outcome::NoSolution));
        assert_eq!(
            result.external.unwrap().exit,
            Some(Exit {
                code: Some(code),
                signal: None
            })
        );
    }
    let mut other = work();
    other.nonces = Nonces::reserve(39, 1).unwrap();
    assert_eq!(
        run(configuration("proof"), other).result,
        Err(miner::Error::External(Error::NonceOutsideReservation))
    );
    let mut line = configuration("flood");
    line.line_bytes = 500;
    assert_eq!(
        run(line, work()).result,
        Err(miner::Error::External(Error::LineLimit))
    );
    let mut total = configuration("diagnostics");
    total.output_bytes = 1024;
    assert_eq!(
        run(total, work()).result,
        Err(miner::Error::External(Error::OutputLimit))
    );
}

#[test]
fn cancellation_and_shutdown_join_the_process_and_keep_exact_receipts_and_capacity() {
    let worker = Worker::spawn_external(
        miner::Config {
            requests: 2,
            ..Default::default()
        },
        configuration("sleep"),
    )
    .unwrap();
    let client = worker.client();
    let active = client.submit(work()).unwrap();
    until(|| client.status().external.is_some_and(|r| r.lines > 2));
    let queued = client.submit(work()).unwrap();
    assert!(active.cancel_if_pending());
    assert!(!active.cancel_if_pending());
    worker.request_shutdown();
    let status = worker.shutdown();
    assert_eq!(status.completed, 2);
    let first = active.blocking_wait();
    assert_eq!(first.result, Err(miner::Error::Cancelled));
    assert_eq!(first.external.unwrap().phase, Phase::Reaped);
    assert_eq!(first.attempts, None);
    let second = queued.blocking_wait();
    assert_eq!(second.result, Err(miner::Error::Cancelled));
    assert_eq!(second.attempts, Some(0));
    assert!(second.external.is_none_or(|r| r.pid.is_none()));
    assert!(Arc::ptr_eq(&first, &active.blocking_wait()));
    assert_eq!(client.retained(), 2);
    drop(active);
    drop(queued);
    drop(first);
    drop(second);
    assert_eq!(client.retained(), 0);
}

#[test]
fn spawn_failures_and_panic_cleanup_do_not_fabricate_process_accounting() {
    let mut config = configuration("sleep");
    config.executable =
        std::env::temp_dir().join(format!("missing-ae-miner-{}", rand::random::<u128>()));
    let worker = Worker::spawn_external(Default::default(), config).unwrap();
    let result = worker.client().submit(work()).unwrap().blocking_wait();
    assert!(matches!(
        result.result,
        Err(miner::Error::External(Error::Io {
            operation: Operation::Spawn,
            ..
        }))
    ));
    assert_eq!(result.attempts, Some(0));
    assert_eq!(result.external.unwrap().pid, None);
    worker.shutdown();
    let worker = Worker::spawn_external(Default::default(), configuration("sleep")).unwrap();
    let pid = Arc::new(std::sync::Mutex::new(None));
    let captured = pid.clone();
    worker.on_external_progress(move |report| {
        if report.lines > 2 {
            *captured.lock().unwrap() = report.pid;
            panic!("controlled external owner panic");
        }
    });
    let ticket = worker.client().submit(work()).unwrap();
    until(|| worker.is_finished());
    assert_eq!(worker.shutdown().failure, Some(miner::Error::Panicked));
    let result = ticket.blocking_wait();
    assert_eq!(result.result, Err(miner::Error::Panicked));
    assert_eq!(result.attempts, None);
    assert!(result.external.is_none());
    #[cfg(unix)]
    assert_eq!(
        rustix::process::waitid(
            rustix::process::WaitId::Pid(
                rustix::process::Pid::from_raw(pid.lock().unwrap().unwrap() as i32).unwrap()
            ),
            rustix::process::WaitIdOptions::EXITED | rustix::process::WaitIdOptions::NOHANG
        )
        .unwrap_err(),
        rustix::io::Errno::CHILD
    );
}

#[cfg(unix)]
#[test]
fn inherited_output_writers_are_killed_on_cancel_and_on_leader_exit() {
    use rustix::fs::{FlockOperation, flock};
    for mode in ["descendant_hold", "descendant_exit"] {
        let path = std::env::temp_dir().join(format!("ae-miner-lock-{}", rand::random::<u128>()));
        let mut config = configuration(mode);
        config
            .extra_args
            .extend(["--fixture-lock".into(), path.as_os_str().into()]);
        let worker = Worker::spawn_external(Default::default(), config).unwrap();
        let client = worker.client();
        let ticket = client.submit(work()).unwrap();
        if mode == "descendant_hold" {
            until(|| path.with_extension("ready").exists());
            ticket.request_cancel();
        }
        until(|| ticket.try_result().is_some());
        let result = ticket.blocking_wait();
        if mode == "descendant_exit" {
            assert_eq!(result.result, Ok(Outcome::NoSolution));
            assert_eq!(
                result.external.unwrap().exit,
                Some(Exit {
                    code: Some(0),
                    signal: None
                })
            );
        } else {
            assert_eq!(result.result, Err(miner::Error::Cancelled));
        }
        worker.shutdown();
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        until(|| flock(&lock, FlockOperation::NonBlockingLockExclusive).is_ok());
        fs::remove_file(path.with_extension("ready")).unwrap();
        fs::remove_file(path).unwrap();
    }
}

// A Rust child-process fixture, invoked explicitly through this test executable.
// Public mining commands never receive the test-only libtest argument prefix.
#[test]
#[ignore]
fn child_entry() {
    let args: Vec<_> = std::env::args().collect();
    let value = |key: &str| {
        args.iter()
            .position(|s| s == key)
            .map(|i| args[i + 1].clone())
    };
    let Some(mode) = value("--fixture-mode") else {
        return;
    };
    println!();
    let line = value("--fixture-proof").unwrap_or_else(|| format!("Solution(26) {}\n", proof()));
    if let Some(path) = value("--fixture-argv") {
        fs::write(path, serde_json::to_vec(&args[1..]).unwrap()).unwrap();
    }
    match mode.as_str() {
        "proof" => print!("{line}"),
        "proof_exit" => {
            print!("{line}");
            io::stdout().flush().unwrap();
            std::process::exit(7);
        }
        "fragmented" => {
            for (index, chunk) in line.as_bytes().chunks(7).enumerate() {
                if index % 2 == 0 {
                    io::stdout().write_all(chunk).unwrap();
                    io::stdout().flush().unwrap();
                } else {
                    io::stderr().write_all(chunk).unwrap();
                    io::stderr().flush().unwrap();
                }
            }
        }
        "invalid" => println!("Solution(26) {}", ["0"; 42].join(" ")),
        "malformed" => println!("Solution(26) 0"),
        "unterminated" => {
            print!("{}", line.trim_end());
            io::stdout().flush().unwrap();
            std::process::exit(0);
        }
        "nonzero" => std::process::exit(7),
        "flood" => print!("{}", "x".repeat(20_000)),
        "diagnostics" => {
            for _ in 0..1000 {
                println!("diagnostic");
            }
        }
        "sleep" => {
            println!("ready");
            io::stdout().flush().unwrap();
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        #[cfg(unix)]
        "descendant_hold" | "descendant_exit" => {
            let mut child_args = args[1..].to_vec();
            let pos = child_args
                .iter()
                .position(|a| a == "--fixture-mode")
                .unwrap();
            child_args[pos + 1] = "descendant_sleep".into();
            #[expect(
                clippy::zombie_processes,
                reason = "fixture deliberately exits before its descendant; the miner must kill the inherited process group"
            )]
            let _child = std::process::Command::new(&args[0])
                .args(child_args)
                .spawn()
                .unwrap();
            let path = PathBuf::from(value("--fixture-lock").unwrap());
            until(|| path.with_extension("ready").exists());
            println!("descendant ready");
            io::stdout().flush().unwrap();
            if mode == "descendant_exit" {
                std::process::exit(0);
            }
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        #[cfg(unix)]
        "descendant_sleep" => {
            let path = PathBuf::from(value("--fixture-lock").unwrap());
            let file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&path)
                .unwrap();
            rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive).unwrap();
            fs::write(path.with_extension("ready"), b"ready").unwrap();
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        _ => panic!("unknown fixture mode"),
    }
}

#[test]
fn process_receives_literal_native_arguments_and_the_selected_working_directory() {
    let directory = std::env::temp_dir().join(format!("ae miner args {}", rand::random::<u128>()));
    fs::create_dir(&directory).unwrap();
    let mut config = configuration("proof");
    config.directory = directory.clone();
    config.hex_header = true;
    config.device = Some(7);
    config.extra_args.extend([
        "--fixture-argv".into(),
        "arguments.json".into(),
        "a literal argument with spaces; $HOME `untouched`".into(),
    ]);
    let expected: Vec<_> = config
        .arguments(work())
        .into_iter()
        .map(|a| a.into_string().unwrap())
        .collect();
    assert!(matches!(
        run(config, work()).result,
        Ok(Outcome::Found { .. })
    ));
    let observed: Vec<String> =
        serde_json::from_slice(&fs::read(directory.join("arguments.json")).unwrap()).unwrap();
    assert_eq!(observed, expected);
    fs::remove_file(directory.join("arguments.json")).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[cfg(unix)]
#[test]
fn already_exited_unreaped_leader_keeps_exit_status_and_is_always_reaped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let config = configuration("nonzero");
    let (_reader, stdout, stderr) = platform::pipe(miner::Id {
        worker: 1,
        sequence: 1,
    })
    .unwrap();
    let mut command = std::process::Command::new(&config.executable);
    command
        .args(config.arguments(work()))
        .stdout(stdout)
        .stderr(stderr);
    let mut child = platform::Child::spawn(command).unwrap();
    let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
    until(|| child.exited().unwrap());
    assert_eq!(child.finish().unwrap().code(), Some(7));
    assert!(child.termination().is_some());
    assert_eq!(
        rustix::process::waitid(
            rustix::process::WaitId::Pid(pid),
            rustix::process::WaitIdOptions::EXITED | rustix::process::WaitIdOptions::NOHANG
        )
        .unwrap_err(),
        rustix::io::Errno::CHILD
    );
}
