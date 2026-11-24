use super::*;
use miner::external::{self, Phase};
use std::sync::mpsc;

fn replay_configuration(header: &KeyHeader) -> external::Config {
    let mut config = external::tests::configuration("proof");
    config.extra_args.extend([
        "--fixture-proof".into(),
        format!(
            "Solution({:x}) {}\n",
            header.nonce,
            header
                .seal
                .iter()
                .map(|n| format!("{n:x}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
        .into(),
    ]);
    config
}
fn poll(mining: &mut Mining<Otp26>, header: &KeyHeader, now: Instant) {
    mining
        .poll(
            Hash256(header.common.previous_hash),
            AccountPublicKey(header.miner),
            now,
        )
        .unwrap();
}

// Replay unchanged public proofs through actual child output, canonical
// verification and normal admission. No fresh canonical proof is mined here.
#[tokio::test]
async fn external_public_proofs_pass_canonical_verification_and_durable_admission() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let importer = importer(&node);
    let mut count = 0;
    for block in fixtures::blocks(&NetworkProfile::mainnet()) {
        let Block::Key(header) = &block else {
            importer
                .import_block(block.encode().unwrap(), WALL)
                .unwrap()
                .wait()
                .await
                .unwrap();
            continue;
        };
        let mut config = configuration(header);
        config.instances[0].external = Some(replay_configuration(header));
        let mut mining = ready(&importer, snapshots.clone(), header, config);
        mining.act(&importer, context(header, true, false)).unwrap();
        let ticket = mining.cpus[0].job.as_ref().unwrap().ticket.clone();
        until(|| {
            poll(&mut mining, header, Instant::now());
            mining.output.is_some()
        });
        let Some(Completion::Searched {
            result,
            selected: true,
            timed_out: false,
            ..
        }) = mining.take_output()
        else {
            panic!("expected a verified external proof")
        };
        assert!(Arc::ptr_eq(&result, &ticket.blocking_wait()));
        assert!(
            matches!(&result.result, Ok(miner::Outcome::Found { nonce, proof }) if *nonce == header.nonce && **proof == header.seal.map(u64::from))
        );
        assert_eq!(result.external.unwrap().phase, Phase::Reaped);
        assert_eq!(result.attempts, None);
        mining
            .act(&importer, context(header, true, count % 2 == 0))
            .unwrap();
        assert!(mining.admission.is_some());
        mining.invalidate().unwrap();
        let outputs = retire(mining);
        let admitted: Vec<_> = outputs
            .iter()
            .filter_map(|o| match o {
                Completion::Admitted { hash, result, .. } => Some((hash, result)),
                _ => None,
            })
            .collect();
        assert_eq!(admitted.len(), 1);
        assert_eq!(*admitted[0].0, block.hash().unwrap());
        assert_eq!(
            admitted[0].1.as_ref().unwrap().accepted[0].hash,
            block.hash().unwrap()
        );
        assert_eq!(
            node.db
                .view()
                .unwrap()
                .require_block(block.hash().unwrap())
                .unwrap()
                .encode()
                .unwrap(),
            block.encode().unwrap()
        );
        assert_eq!(snapshots.live_snapshots(), 0);
        count += 1;
    }
    assert_eq!(count, 6);
}

struct Pause {
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}
impl Pause {
    fn new(mining: &Mining<Otp26>, index: usize, proof: bool) -> Self {
        let (entered, wait) = mpsc::channel();
        let (release, resumed) = mpsc::channel();
        let mut used = false;
        mining.cpus[index]
            .worker
            .as_ref()
            .unwrap()
            .on_external_progress(move |report| {
                if !used
                    && if proof {
                        report.phase == Phase::Verifying
                    } else {
                        report.lines > 2
                    }
                {
                    used = true;
                    entered.send(()).unwrap();
                    resumed.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        Self {
            entered: wait,
            release,
        }
    }
    fn wait(&self) {
        self.entered.recv_timeout(Duration::from_secs(10)).unwrap();
    }
}
impl Drop for Pause {
    fn drop(&mut self) {
        let _ = self.release.send(());
    }
}

#[test]
fn external_deadline_under_output_pressure_keeps_the_child_until_joined_retirement() {
    let node = Node::new();
    let importer = importer(&node);
    let Block::Key(header) = fixtures::blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.attempt_timeout = Some(Duration::from_millis(5));
    let mut external = config.instances[0].clone();
    external.external = Some(external::tests::configuration("sleep"));
    config.instances.push(external);
    let mut mining = ready(&importer, snapshots, &header, config);
    let paused = Pause::new(&mining, 1, false);
    let ctx = context(&header, true, false);
    let now = ctx.now;
    mining.act(&importer, ctx).unwrap();
    paused.wait();
    let first = mining.cpus[0].job.as_ref().unwrap().ticket.blocking_wait();
    let ticket = mining.cpus[1].job.as_ref().unwrap().ticket.clone();
    poll(&mut mining, &header, now);
    assert!(
        matches!(&mining.output, Some(Completion::Searched { result, .. }) if Arc::ptr_eq(result, &first))
    );
    poll(&mut mining, &header, now + Duration::from_millis(5));
    assert_eq!(mining.status().timeouts, 1);
    assert_eq!(mining.status().timeouts_pending, 1);
    assert!(ticket.try_result().is_none());
    let mut retired = mining.retire();
    let mut outputs = vec![];
    assert!(!retired.poll(&mut outputs));
    drop(paused);
    until(|| retired.poll(&mut outputs));
    let original = ticket.blocking_wait();
    assert_eq!(original.result, Err(miner::Error::Cancelled));
    assert_eq!(original.external.unwrap().phase, Phase::Reaped);
    assert!(outputs.iter().any(|o| matches!(o, Completion::Searched { result, timed_out: true, selected: false, .. } if Arc::ptr_eq(result, &original))));
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(o, Completion::Joined(_)))
            .count(),
        2
    );
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    let count = outputs.len();
    assert!(retired.poll(&mut outputs));
    assert_eq!(outputs.len(), count);
}

#[test]
fn external_verified_proof_finishing_after_deadline_is_retained_without_selection() {
    let node = Node::new();
    let importer = importer(&node);
    let Block::Key(header) = fixtures::blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.attempt_timeout = Some(Duration::from_millis(5));
    config.instances[0].external = Some(replay_configuration(&header));
    let mut mining = ready(&importer, snapshots, &header, config);
    // Stop after receiving a complete proof line, before verification and
    // receipt publication. The deadline must still fence its later result.
    let paused = Pause::new(&mining, 0, true);
    let ctx = context(&header, true, false);
    let now = ctx.now;
    mining.act(&importer, ctx).unwrap();
    paused.wait();
    let ticket = mining.cpus[0].job.as_ref().unwrap().ticket.clone();
    poll(&mut mining, &header, now + Duration::from_millis(5));
    assert_eq!(mining.status().timeouts, 1);
    drop(paused);
    until(|| ticket.try_result().is_some());
    let result = ticket.blocking_wait();
    assert!(matches!(result.result, Ok(miner::Outcome::Found { .. })));
    assert_eq!(result.external.unwrap().phase, Phase::Reaped);
    poll(&mut mining, &header, now + Duration::from_millis(6));
    assert!(
        matches!(mining.take_output(), Some(Completion::Searched { result: observed, timed_out: true, selected: false, .. }) if Arc::ptr_eq(&observed, &result))
    );
    assert!(mining.solution.is_none() && mining.admission.is_none());
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    retire(mining);
}
