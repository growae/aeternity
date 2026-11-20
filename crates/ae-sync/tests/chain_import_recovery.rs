mod common;
use ae_chain::{
    block::Block,
    repository::{Identity, ledger},
};
use ae_codec::headers::Header;
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_storage::Database;
use ae_sync::chain::{Direction, Importer, Outcome};
use common::{
    import::packet,
    ledger_fixtures::{blocks, execution, rules},
};
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const NOW: u64 = u64::MAX - 600_000;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temporary(std::path::PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::mainnet(),
        genesis::header(Network::Mainnet).unwrap().hash().unwrap(),
        ae_crypto::blake2b_256(b"generation-import-crash"),
    )
}
fn importer(db: Arc<Database>) -> Importer {
    Importer::new(
        db,
        execution(&NetworkProfile::mainnet()),
        rules(identity().genesis),
        Default::default(),
    )
    .unwrap()
}
fn generation() -> (Block, Vec<Block>) {
    let all = blocks(&NetworkProfile::mainnet());
    let next = all
        .iter()
        .position(|b| matches!(b,Block::Key(k) if k.common.height==2))
        .unwrap();
    (all[next].clone(), all[1..next].to_vec())
}
#[tokio::test]
async fn import_crash_worker() {
    let Some(path) = std::env::var_os("AE_GENERATION_IMPORT_CRASH_PATH") else {
        return;
    };
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let importer = importer(db);
    let (key, micros) = generation();
    println!("AE_IMPORT_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "go");
    importer
        .import(
            packet(&key, &micros, Direction::Backward),
            key.hash().unwrap(),
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    println!("AE_IMPORT_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_generation_imports_reopen_as_complete_block_prefixes_and_resume() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn wait_for(reader: &mut impl BufRead, marker: &str) {
        loop {
            let mut line = String::new();
            assert!(
                reader.read_line(&mut line).unwrap() > 0,
                "worker exited before {marker}"
            );
            if line.trim() == marker {
                break;
            }
        }
    }
    let network = NetworkProfile::mainnet();
    let all = blocks(&network);
    let (key, micros) = generation();
    let expected: Vec<_> = micros.iter().chain(std::iter::once(&key)).collect();
    assert_eq!(expected.len(), 2);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for phase in ["before", "race0", "race1", "race5", "race10", "after"] {
        let temp = Temporary(std::env::temp_dir().join(format!(
            "ae-import-crash-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        let db = Arc::new(Database::open(&temp.0, identity()).unwrap());
        // Bootstrap is distinct from network import and is the actual public
        // genesis; only the first validated key is preinstalled.
        let Header::Key(genesis) = genesis::header(Network::Mainnet).unwrap() else {
            panic!()
        };
        let trees = Trees {
            accounts: genesis::state(Network::Mainnet).unwrap().0,
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        db.commit(
            ledger::bootstrap(
                &db.view().unwrap(),
                &network,
                &genesis,
                &trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
        rt.block_on(async {
            let service = importer(db.clone());
            service
                .import(
                    packet(&all[0], &[], Direction::Backward),
                    all[0].hash().unwrap(),
                    Direction::Backward,
                    NOW,
                )
                .unwrap()
                .wait()
                .await
                .unwrap();
            service.stop();
            service.join().await;
        });
        drop(db);
        let mut child = Child(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "import_crash_worker", "--nocapture"])
                .env("AE_GENERATION_IMPORT_CRASH_PATH", &temp.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
        wait_for(&mut output, "AE_IMPORT_READY");
        if phase != "before" {
            child.0.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
            if phase == "after" {
                wait_for(&mut output, "AE_IMPORT_DURABLE");
            } else {
                let millis = phase.trim_start_matches("race").parse().unwrap();
                std::thread::sleep(std::time::Duration::from_millis(millis));
            }
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let db = Arc::new(Database::open(&temp.0, identity()).unwrap());
        let prefix = {
            let view = db.view().unwrap();
            let mut prefix = 0;
            for block in &expected {
                if view.block(block.hash().unwrap()).unwrap().is_some() {
                    prefix += 1
                } else {
                    break;
                }
            }
            for (i, block) in expected.iter().enumerate() {
                let hash = block.hash().unwrap();
                assert_eq!(view.block(hash).unwrap().is_some(), i < prefix);
                if i < prefix {
                    assert_eq!(
                        view.require_record(hash).unwrap().roots.state.hash().0,
                        block.header().common().state_hash
                    );
                }
            }
            assert_eq!(view.status().revision, 2 + prefix as u64);
            assert_eq!(
                view.status().tip,
                Some(if prefix == 0 {
                    all[0].hash().unwrap()
                } else {
                    expected[prefix - 1].hash().unwrap()
                })
            );
            prefix
        };
        if phase == "before" {
            assert_eq!(prefix, 0)
        }
        if phase == "after" {
            assert_eq!(prefix, 2)
        }
        rt.block_on(async {
            let service = importer(db.clone());
            let report = service
                .import(
                    packet(&key, &micros, Direction::Backward),
                    key.hash().unwrap(),
                    Direction::Backward,
                    NOW,
                )
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(report.accepted.len(), 2);
            for (i, accepted) in report.accepted.iter().enumerate() {
                assert_eq!(matches!(accepted.outcome, Outcome::Present), i < prefix);
            }
            service.stop();
            service.join().await;
        });
        assert_eq!(db.view().unwrap().status().tip, Some(key.hash().unwrap()));
        drop(db);
        let reopened = Database::open(&temp.0, identity()).unwrap();
        assert_eq!(reopened.view().unwrap().status().revision, 4);
        println!("Generation import crash recovery {phase}: committed prefix {prefix}");
    }
}
