use super::*;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn settings() -> ae_storage::pool::worker::Settings {
    ae_storage::pool::worker::Settings {
        origins_interval: None,
        ..Default::default()
    }
}
fn wait_for_file(path: &Path) {
    for _ in 0..60_000 {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("bounded child-process barrier expired");
}
async fn child(root: PathBuf) {
    let mode = std::env::var("AE_SHARED_SYNC_KILL_MODE").unwrap();
    let target: u64 = std::env::var("AE_SHARED_SYNC_KILL_COMMIT")
        .unwrap()
        .parse()
        .unwrap();
    let source = seeded(Node::open(root.join("source"), settings())).await;
    let node = Node::open(root.join("local"), settings());
    let mut owned = importer(&node);
    let calls = AtomicU64::new(0);
    let files = root.clone();
    Arc::get_mut(&mut owned.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        let number = calls.fetch_add(1, Ordering::SeqCst) + 1;
        // Admit all three real peer sessions before any local publication.
        if number == 1 {
            wait_for_file(&files.join("started"));
        }
        if number == target && mode != "after" {
            fs::write(files.join("entered"), number.to_string()).unwrap();
            wait_for_file(&files.join("release"));
        }
        let committed = db.commit(update)?;
        if number == target && mode == "after" {
            fs::write(files.join("entered"), number.to_string()).unwrap();
            wait_for_file(&files.join("release"));
        }
        Ok(committed)
    }));
    let mut running = Running::new(&node, &source, owned).await;
    running.shared().await;
    fs::write(root.join("started"), b"three admitted sessions").unwrap();
    within(async {
        while !root.join("entered").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    fs::write(root.join("ready"), b"durable boundary reached").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_shared_owners_reopen_complete_prefixes_and_resume() {
    if let Some(root) = std::env::var_os("AE_SHARED_SYNC_KILL_ROOT") {
        child(root.into()).await;
        return;
    }
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    for (name, mode, target, delay, expected) in [
        ("before-micro", "before", 2, None, Some(1)),
        ("after-key1", "after", 1, None, Some(1)),
        ("after-micro", "after", 2, None, Some(2)),
        ("after-key2", "after", 3, None, Some(3)),
        ("race0", "race", 2, Some(0), None),
        ("race5", "race", 2, Some(5), None),
    ] {
        let root = std::env::temp_dir().join(format!(
            "ae-shared-owner-kill-{}-{name}",
            std::process::id()
        ));
        assert!(!root.exists());
        fs::create_dir(&root).unwrap();
        let _directory = Directory(root.clone());
        let log = fs::File::create(root.join("child.log")).unwrap();
        let mut process=Process(Command::new(std::env::current_exe().unwrap())
            .args(["--exact","chain::sync::service::tests::recovery::killed_shared_owners_reopen_complete_prefixes_and_resume","--nocapture"])
            .env("AE_SHARED_SYNC_KILL_ROOT",&root)
            .env("AE_SHARED_SYNC_KILL_MODE",mode)
            .env("AE_SHARED_SYNC_KILL_COMMIT",target.to_string())
            .stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log))
            .spawn().unwrap());
        within(async {
            while !root.join("ready").exists() {
                assert!(
                    process.0.try_wait().unwrap().is_none(),
                    "child stopped: {}",
                    fs::read_to_string(root.join("child.log")).unwrap()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if let Some(delay) = delay {
            fs::write(root.join("release"), b"continue").unwrap();
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        process.0.kill().unwrap();
        assert!(!process.0.wait().unwrap().success());
        let node = Node::open(root.join("local"), settings());
        let source = Node::open(root.join("source"), settings());
        let present: Vec<_> = blocks
            .iter()
            .take(3)
            .map(|b| {
                node.db
                    .view()
                    .unwrap()
                    .block(b.hash().unwrap())
                    .unwrap()
                    .is_some()
            })
            .collect();
        let prefix = present.iter().take_while(|found| **found).count();
        assert!(
            present[prefix..].iter().all(|found| !*found),
            "torn prefix in {name}: {present:?}"
        );
        if let Some(expected) = expected {
            assert_eq!(prefix, expected, "{name}");
        } else {
            assert!((1..=3).contains(&prefix));
        }
        assert_eq!(node.db.view().unwrap().status().revision, 1 + prefix as u64);
        assert_eq!(
            node.db.view().unwrap().status().tip,
            Some(blocks[prefix - 1].hash().unwrap())
        );
        for block in blocks.iter().take(prefix) {
            assert_eq!(
                node.db
                    .view()
                    .unwrap()
                    .require_record(block.hash().unwrap())
                    .unwrap()
                    .roots
                    .state
                    .hash()
                    .0,
                block.header().common().state_hash
            );
        }
        let mut resumed = Running::new(&node, &source, importer(&node)).await;
        resumed.completed().await;
        let status = resumed.stop().await;
        assert_eq!(status.counts.failed, 0, "{name}: {status:?}");
        assert_eq!(status.counts.finished, 3);
        assert_eq!(
            node.db.view().unwrap().status().tip,
            Some(blocks[2].hash().unwrap())
        );
        assert_eq!(
            node.db.view().unwrap().status().revision,
            source.db.view().unwrap().status().revision
        );
        println!("Shared owner crash recovery {name}: prefix {prefix}, reopened and resumed");
    }
}
