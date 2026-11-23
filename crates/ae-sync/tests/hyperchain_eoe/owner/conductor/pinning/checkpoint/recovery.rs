use super::*;
use std::{
    io::BufRead,
    process::{Command, Stdio},
};

#[tokio::test]
async fn crash_worker() {
    let Some(path) = std::env::var_os("AE_AGENT_CRASH_PATH") else {
        return;
    };
    let phase = std::env::var("AE_AGENT_CRASH_PHASE").unwrap();
    let endpoint = parent_pins::Node::new(
        parent_pins::Chain::Aeternity,
        &std::env::var("AE_AGENT_CRASH_ENDPOINT").unwrap(),
        None,
    )
    .unwrap();
    let node = Node::at_path(path.into(), 0);
    let mut cfg = configuration_at(&node, &endpoint);
    cfg.parent
        .pins
        .as_mut()
        .unwrap()
        .agent
        .as_mut()
        .unwrap()
        .outputs = 1;
    let mut c = new(&node, cfg).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    prefix(&node, &mut c, &mut vec![], 5).await;
    within(
        c.importer()
            .import_block(node.blocks()[5].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    if phase == "parent" {
        wait(|| {
            node.db
                .parent_posts()
                .unwrap()
                .records(Default::default())
                .unwrap()
                .len()
                == 1
        });
    } else {
        wait(|| {
            saved(&node)
                .state
                .active
                .as_ref()
                .is_some_and(|a| a.child_note)
        });
        if phase == "proof" {
            settled(
                &mut c,
                &mut vec![],
                node.db.view().unwrap().status().revision,
            );
            remaining(&node, &mut c, 6, &mut vec![]).await;
        }
    }
    println!("AE_AGENT_CRASH_READY");
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}

#[tokio::test]
async fn killed_agents_resume_parent_note_and_proof_boundaries_without_duplicate_effects() {
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
    for phase in ["parent", "note", "proof"] {
        let wallet = Wallet::new();
        if phase == "parent" {
            wallet.mode.store(3, Ordering::Release);
        }
        let path = Directory(
            std::env::temp_dir().join(format!("ae-agent-crash-{}-{phase}", std::process::id())),
        );
        assert!(!path.0.exists());
        let mut child = Worker(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "owner::conductor::pinning::checkpoint::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_AGENT_CRASH_PATH", &path.0)
                .env("AE_AGENT_CRASH_PHASE", phase)
                .env("AE_AGENT_CRASH_ENDPOINT", wallet.node.origin().as_str())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut out = std::io::BufReader::new(child.0.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                out.read_line(&mut line).unwrap(),
                0,
                "worker exited before durable checkpoint"
            );
            if line.contains("AE_AGENT_CRASH_READY") {
                break;
            }
        }
        wait(|| wallet.posted.lock().unwrap().len() == 1);
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        wallet.mode.store(0, Ordering::Release);
        let node = Node::at_path(path.0.clone(), 0);
        let before = saved(&node);
        let original = stored_children(&node);
        if phase == "parent" {
            let op = &before.pending.as_ref().unwrap().operation;
            assert!(op.post.is_some() && op.parent.is_none());
        } else {
            assert_eq!(original.len(), if phase == "proof" { 2 } else { 1 });
        }
        let mut c = configured(&node, &wallet, |_| {});
        let mut reports = vec![];
        settled(
            &mut c,
            &mut reports,
            node.db.view().unwrap().status().revision,
        );
        if phase != "proof" {
            remaining(&node, &mut c, 6, &mut reports).await;
        }
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        assert_eq!(
            wallet.requests.load(Ordering::Acquire),
            if phase == "parent" { 2 } else { 3 }
        );
        let after = stored_children(&node);
        assert_eq!(after.len(), if phase == "parent" { 0 } else { 2 });
        for record in &original {
            assert!(after.contains(record));
        }
        assert_eq!(children(&reports).len(), usize::from(phase == "note"));
        assert!(!saved(&node).state.enabled);
        assert!(saved(&node).pending.is_none());
        assert_eq!(saved(&node).consumed, node.db.view().unwrap().status());
        drop(shutdown(c));
        eprintln!(
            "Pinning agent crash recovery {phase}: exact original effects, durable state and cursor"
        );
    }
}
