use super::*;
use crate::test_support::hyperchain::Node as Hyperchain;
use ae_hyperchain::eoe::pool;

struct Fixture {
    node: Hyperchain,
    hub: pool::Hub,
    relay: independent::Relay,
    publications: Publications,
    cache: GossipCache,
    config: Config,
}
impl Fixture {
    fn new(index: usize) -> Self {
        let node = Hyperchain::new(index);
        let hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
        let config = Config {
            reply_bytes: Unsigned::from(7).as_bytes().len() + 128,
            ..Default::default()
        };
        let publications = Publications::new(&node.db, config).unwrap();
        let relay = independent::Relay::new(hub.clone(), publications.clone()).unwrap();
        Self {
            node,
            hub,
            relay,
            publications,
            cache: Default::default(),
            config,
        }
    }
    fn spawn(&self) -> Worker {
        Worker::spawn_independent(
            self.node.db.clone(),
            self.cache.clone(),
            self.config,
            &self.relay,
            Default::default(),
        )
        .unwrap()
    }
    fn signed(&self, kind: u64) -> Vec<u8> {
        let key = SigningKey::from_bytes(&[91; 32]);
        let tx = Transaction::new(TransactionLeaf::HcVote(HcVote::new(
            ae_types::AccountPublicKey(key.verifying_key().to_bytes()),
            7.into(),
            kind.into(),
            Default::default(),
        )));
        let mut message = self.node.config.profile.network.network_id().to_vec();
        message.extend(tx.encode());
        Signed::new(tx, vec![ae_types::Signature(key.sign(&message).to_bytes())]).encode()
    }
}

struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Release {
    fn release(mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
async fn gate(client: &Client, fail: bool) -> (Ticket<()>, Release) {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let ticket = client
        .inner
        .submit(0, move |_| {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(!fail, "injected independent vote worker failure");
            Ok(())
        })
        .unwrap();
    let release = Release(Some(release));
    within(waiting).await.unwrap();
    (ticket, release)
}
async fn finished(worker: &Worker) {
    within(async {
        while !worker.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn joined(retired: &Retired) {
    assert!(retired.info.joined);
    assert!(!retired.info.accepting);
    assert!(!retired.info.running);
    assert_eq!(retired.info.pending, 0);
    assert_eq!(retired.info.control_pending, 0);
    assert_eq!(retired.info.bytes, 0);
}

#[tokio::test]
async fn graceful_retirement_waits_for_thread_exit_and_keeps_accepted_results() {
    for index in [1, 3, 5] {
        let mut f = Fixture::new(index);
        let worker = f.spawn();
        let old = worker.client();
        let pool = f.hub.client().unwrap();
        let bytes = f.signed(1);
        accepted(
            push(&old, bytes.clone(), policy::Event::Created).await,
            true,
            policy::Event::Created,
        );
        let before = old.info();
        let (blocked, release) = gate(&old, false).await;
        let advance = old.advance_epoch(9.into()).unwrap();
        let unread = old.stats().unwrap();
        worker.request_shutdown();
        assert!(!old.info().accepting);
        assert!(old.info().running);
        assert!(!worker.is_finished());
        assert!(!old.info().joined);
        assert_eq!(old.info().pending, 3);
        assert!(!pool.is_closed());
        assert!(matches!(old.stats(), Err(Error::Worker(Fault::Stopped))));
        release.release();
        finished(&worker).await;
        assert!(!old.info().joined);
        let retired = worker.retire();
        joined(&retired);
        assert_eq!(retired.result, Ok(()));
        assert!(retired.info.failure.is_none());
        assert_eq!(retired.info.submitted, before.submitted + 3);
        assert_eq!(retired.info.completed, before.completed + 3);
        within(blocked).await.unwrap();
        within(advance).await.unwrap();
        let held = within(unread).await.unwrap();
        assert_eq!(held.epoch, 9.into());
        assert_eq!(held.entries, 1); // First epoch observation preserves the pool.
        assert!(pool.is_closed());
        assert!(f.hub.client().is_none());
        assert!(!f.hub.is_closed());
        let mut events = f.publications.listen().unwrap();
        assert_eq!(f.relay.step().unwrap(), independent::Progress::Published);
        assert_eq!(
            within(events.recv()).await.unwrap().transaction.encode(),
            bytes
        );
        let replacement = f.spawn();
        assert!(!pool.same_pool(&f.hub.client().unwrap()));
        assert_eq!(
            f.hub.client().unwrap().inspect(|p| p.epoch().clone()),
            Ok(0.into())
        );
        assert!(matches!(
            within(replacement.client().stats().unwrap()).await,
            Err(Error::Worker(Fault::Busy))
        ));
        drop(held);
        assert_eq!(
            within(replacement.client().stats().unwrap())
                .await
                .unwrap()
                .entries,
            0
        );
        assert!(replacement.retire().result.is_ok());
    }
}

#[tokio::test]
async fn panicked_retirement_keeps_unknown_tickets_and_reply_credits_across_pool_replacement() {
    for index in [1, 3, 5] {
        let mut f = Fixture::new(index);
        let mut inbox = f.hub.subscribe(Default::default()).unwrap();
        let worker = f.spawn();
        let old = worker.client();
        let old_pool = f.hub.client().unwrap();
        f.hub.new_epoch(7.into()).unwrap();
        let first = f.signed(1);
        let queued = f.signed(2);
        push(&old, first.clone(), policy::Event::Received).await;
        let unread = old.stats().unwrap();
        within(old.inner.submit(0, |_| Ok(())).unwrap())
            .await
            .unwrap();
        let before = old.info();
        let (blocked, release) = gate(&old, true).await;
        let unknown_vote = old.push(queued.clone(), policy::Event::Received).unwrap();
        let unknown_stats = old.stats().unwrap();
        assert_eq!(old.info().pending, 3);
        assert!(!worker.is_finished());
        release.release();
        finished(&worker).await;
        let retired = worker.retire();
        joined(&retired);
        assert_eq!(retired.result, Err(Error::Worker(Fault::Panicked)));
        assert_eq!(retired.info.failure, Some(Error::Worker(Fault::Panicked)));
        assert_eq!(retired.info.submitted, before.submitted + 3);
        assert_eq!(retired.info.completed, before.completed);
        assert_eq!(
            within(blocked).await,
            Err(Error::Worker(Fault::Disconnected))
        );
        assert_eq!(
            within(unknown_vote).await,
            Err(Error::Worker(Fault::Disconnected))
        );
        assert!(matches!(
            within(unknown_stats).await,
            Err(Error::Worker(Fault::Disconnected))
        ));
        assert!(old_pool.is_closed());
        assert!(f.hub.client().is_none());
        // The subscriber observes epochs during the gap; a new pool misses
        // them. Accepted old commands and missed epochs are never replayed.
        f.hub.new_epoch(9.into()).unwrap();
        let replacement = f.spawn();
        let next = replacement.client();
        let new_pool = f.hub.client().unwrap();
        assert!(!old_pool.same_pool(&new_pool));
        assert_eq!(
            new_pool.inspect(|p| (p.epoch().clone(), p.len())),
            Ok((0.into(), 0))
        );
        assert!(matches!(
            within(next.stats().unwrap()).await,
            Err(Error::Worker(Fault::Busy))
        ));
        let held = within(unread).await.unwrap();
        assert_eq!(held.epoch, 7.into());
        assert_eq!(held.entries, 1);
        assert!(matches!(
            within(next.stats().unwrap()).await,
            Err(Error::Worker(Fault::Busy))
        ));
        assert_eq!(
            push(&next, first.clone(), policy::Event::Created).await,
            policy::PushOutcome::Cached
        );
        assert_eq!(new_pool.inspect(|p| p.len()), Ok(0));
        let mut events = f.publications.listen().unwrap();
        assert_eq!(f.relay.step().unwrap(), independent::Progress::Published);
        assert_eq!(
            within(events.recv()).await.unwrap().transaction.encode(),
            first
        );
        assert_eq!(f.relay.step().unwrap(), independent::Progress::Idle);
        let mut epochs = Vec::new();
        let mut received = Vec::new();
        while let Some(input) = inbox.acknowledge() {
            match input.event() {
                pool::Event::NewEpoch(epoch) => epochs.push(epoch.clone()),
                pool::Event::Vote(vote) => received.push(vote.signed().encode()),
            }
        }
        assert_eq!(epochs, vec![7.into(), 9.into()]);
        assert_eq!(received, vec![first]);
        // Only this explicit new invocation admits the previously unknown vote.
        accepted(
            push(&next, queued, policy::Event::Received).await,
            true,
            policy::Event::Received,
        );
        drop(held);
        let stats = within(next.stats().unwrap()).await.unwrap();
        assert_eq!(stats.epoch, 0.into());
        assert_eq!(stats.entries, 1);
        assert!(matches!(old.stats(), Err(Error::Worker(Fault::Stopped))));
        assert!(replacement.retire().result.is_ok());
    }
}

#[tokio::test]
async fn terminal_hub_loss_is_reported_and_cannot_start_a_new_pool() {
    for index in [1, 3, 5] {
        let f = Fixture::new(index);
        let worker = f.spawn();
        f.hub.close();
        finished(&worker).await;
        let retired = worker.retire();
        joined(&retired);
        assert_eq!(retired.result, Err(Error::AuthorityClosed));
        assert_eq!(retired.info.failure, Some(Error::AuthorityClosed));
        assert!(matches!(
            Worker::spawn_independent(
                f.node.db.clone(),
                f.cache,
                f.config,
                &f.relay,
                Default::default()
            ),
            Err(Error::Worker(Fault::Configuration))
        ));
    }
}
