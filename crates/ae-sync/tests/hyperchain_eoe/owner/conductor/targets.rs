use super::*;
use ae_hyperchain::parent_connector::fetch::{Backend, BackendError, Context, Query, Remote};

struct ParentChain(ParentBackend);
impl Backend for ParentChain {
    fn fetch(
        &self,
        host: usize,
        query: &Query,
        seed: &[u8; 16],
        context: &Context,
    ) -> Result<Remote, BackendError> {
        // Native early target heights are signed. This isolated parent has no
        // negative blocks; absence must not masquerade as a backend outage.
        if matches!(query, Query::Height(h) if h < &BigInt::from(0)) {
            return Ok(Remote::NotFound);
        }
        self.0.fetch(host, query, seed, context)
    }
}

fn automatic(node: &Node, producing: bool) -> Conductor<Otp26> {
    Conductor::start(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        config(node, producing),
        Arc::new(ParentChain(ParentBackend {
            missing: AtomicBool::new(false),
        })),
    )
    .unwrap()
}
fn cached(c: &Conductor<Otp26>, height: u64) -> bool {
    matches!(
        c.parent().snapshot().cache.blocks().get(&height.into()),
        Some(parent_cache::Entry::Block(_))
    )
}

#[test]
fn ordinary_startup_fetches_contract_targets_and_produces_across_epoch_changes() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let mut c = automatic(&node, true);
        wait(|| cached(&c, 16));
        let mut wall = 1000;
        let mut clock = Instant::now();
        let end = clock + Duration::from_secs(30);
        let mut keys = vec![];
        let mut eoe = 0;
        while keys.len() < 7 {
            assert!(Instant::now() < end, "stalled {:?}", c.status());
            tick(&mut c, &mut wall, &mut clock);
            if let Some(output) = c.take_output() {
                let Output::Attempt(done) = output else {
                    panic!("{output:?}")
                };
                if let production::Outcome::Produced { key, is_eoe, .. } = done.outcome {
                    keys.push(key.hash);
                    eoe += usize::from(is_eoe);
                }
            }
        }
        assert!(eoe >= 2);
        // Committed top delivery must drive new queries and real connector
        // requests; there are no injected target heights or parent posts.
        while !cached(&c, 26) {
            assert!(
                Instant::now() < end,
                "target delivery stalled {:?}",
                c.status()
            );
            tick(&mut c, &mut wall, &mut clock);
            c.take_output();
        }
        assert!(cached(&c, 21));
        for hash in keys {
            let Block::Key(key) = node.db.view().unwrap().require_block(hash).unwrap() else {
                panic!()
            };
            ae_hyperchain::signing::verify_key(&key).unwrap();
        }
        let report = shutdown(c);
        assert!(report.owner.errors.is_empty());
    }
}

#[tokio::test]
async fn passive_imports_advance_live_target_fetches_and_restart_at_committed_tip() {
    let node = Node::new(6);
    let mut c = automatic(&node, false);
    wait(|| cached(&c, 16));
    // Received execution consumes captured entropy. Populate it through the
    // real public lookup path before importing the frozen native chain, whose
    // signatures and state roots were generated with those parent blocks.
    for h in [1, 6] {
        c.parent()
            .get_block(h.into(), 5000.into())
            .unwrap()
            .unwrap();
    }
    for b in node.blocks() {
        c.importer()
            .import_block(b.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let end = Instant::now() + Duration::from_secs(10);
    while !cached(&c, 26) {
        assert!(Instant::now() < end);
        c.step(1_000_000, Instant::now()).unwrap();
        assert!(c.take_output().is_none());
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(c.status().attempts, 0);
    shutdown(c);
    let c = automatic(&node, false);
    wait(|| cached(&c, 26) && cached(&c, 6));
    assert_eq!(c.parent().snapshot().cache.child_top().height, 7.into());
    shutdown(c);
}
