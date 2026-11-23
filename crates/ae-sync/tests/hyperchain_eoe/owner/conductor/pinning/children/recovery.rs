use super::*;
use std::{
    io::BufRead,
    process::{Command, Stdio},
};
#[tokio::test]
async fn crash_worker() {
    let Some(path) = std::env::var_os("AE_CHILD_ADMISSION_CRASH_PATH") else {
        return;
    };
    let phase = std::env::var("AE_CHILD_ADMISSION_CRASH_PHASE").unwrap();
    let mut record = originals()
        .await
        .into_iter()
        .find(|r| r.intent.kind == journal::Kind::Note)
        .unwrap();
    record.receipt = None;
    let node = Node::at_path(path.into(), 0);
    let wallet = Wallet::new();
    let c = configured(&node, &wallet, |cfg| {
        cfg.parent.pins.as_mut().unwrap().agent = None
    });
    for block in node.blocks().into_iter().take(5) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    let mut writer = journal::Writer::start(node.db.clone(), Default::default()).unwrap();
    writer.reserve(record.clone()).unwrap();
    println!("AE_CHILD_ADMISSION_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "admit\n");
    let result = c
        .pool()
        .push(
            Arc::new(record.signed().unwrap()),
            ae_mempool::pool::Event::Received,
            false,
        )
        .unwrap()
        .wait()
        .unwrap();
    if phase == "receipt" {
        writer
            .record_receipt(&record, journal::Receipt::Submitted(result))
            .unwrap();
    }
    println!("AE_CHILD_ADMISSION_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}
#[test]
fn killed_child_owners_reconcile_actual_pool_admission_without_repeating_it() {
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn marker(r: &mut impl BufRead, expected: &str) {
        loop {
            let mut line = String::new();
            assert_ne!(
                r.read_line(&mut line).unwrap(),
                0,
                "exited before {expected}"
            );
            if line.contains(expected) {
                break;
            }
        }
    }
    for phase in ["reserved", "admitted", "receipt"] {
        let path = Directory(
            std::env::temp_dir().join(format!("ae-child-admission-{}-{phase}", std::process::id())),
        );
        assert!(!path.0.exists());
        let mut child = Worker(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "owner::conductor::pinning::durable_children::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_CHILD_ADMISSION_CRASH_PATH", &path.0)
                .env("AE_CHILD_ADMISSION_CRASH_PHASE", phase)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut out = std::io::BufReader::new(child.0.stdout.take().unwrap());
        marker(&mut out, "AE_CHILD_ADMISSION_READY");
        if phase != "reserved" {
            child
                .0
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"admit\n")
                .unwrap();
            child.0.stdin.as_mut().unwrap().flush().unwrap();
            marker(&mut out, "AE_CHILD_ADMISSION_DURABLE");
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let node = Node::at_path(path.0.clone(), 0);
        let kept = records(&node);
        assert_eq!(kept.len(), 1);
        let record = &kept[0];
        let tx = record.signed().unwrap();
        assert_eq!(record.receipt.is_some(), phase == "receipt");
        let pool_revision = node.db.pool_view().unwrap().revision();
        let chain = node.db.view().unwrap().status();
        let mut writer = journal::Writer::start(node.db.clone(), Default::default()).unwrap();
        let recovered = writer.observe(record).unwrap();
        if phase == "reserved" {
            assert!(recovered.is_none());
            assert_eq!(
                node.db.pool_view().unwrap().location(tx.hash()).unwrap(),
                ae_mempool::pool::Location::Unknown
            );
        } else {
            let recovered = recovered.unwrap();
            assert_eq!(recovered.transaction, record.transaction);
            if phase == "admitted" {
                assert!(matches!(
                    recovered.receipt,
                    Some(journal::Receipt::Observed {
                        location: ae_mempool::pool::Location::Pool,
                        ..
                    })
                ));
            } else {
                assert_eq!(recovered.receipt, record.receipt);
            }
            assert_eq!(writer.observe(&recovered).unwrap(), Some(recovered));
            assert_eq!(
                node.db
                    .pool_view()
                    .unwrap()
                    .transaction(tx.hash())
                    .unwrap()
                    .unwrap()
                    .encode(),
                record.transaction
            );
        }
        assert_eq!(node.db.pool_view().unwrap().revision(), pool_revision);
        assert_eq!(node.db.view().unwrap().status(), chain);
        eprintln!("Child admission crash recovery {phase}: exact original transaction, no replay");
    }
}
