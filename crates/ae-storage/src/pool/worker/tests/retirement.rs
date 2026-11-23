use super::*;

#[test]
fn graceful_retirement_waits_for_the_real_thread_and_preserves_unread_mutations() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let old = worker.client();
    let cursor = old.event_cursor();
    let (release, first) = gate(&old);
    let pushed = old.push(tx(), Event::Created, false).unwrap();
    worker.request_shutdown();
    assert!(!worker.is_finished() && !old.info().joined && !old.info().accepting);
    assert!(matches!(
        Worker::spawn(db.clone(), NetworkProfile::mainnet(), config(), settings()),
        Err(Error::Service(Failure::AlreadyRunning))
    ));
    release.send(()).unwrap();
    first.wait().unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while !worker.is_finished() {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    assert!(!old.info().joined);
    let retired = worker.retire();
    assert!(retired.result.is_ok() && retired.info.joined && old.info().joined);
    let next = spawn(db.clone());
    let events = old.events_after(&cursor, 10).unwrap();
    assert!(events.closed && events.events.len() == 1);
    assert_eq!(
        next.client().events_after(&cursor, 10).err(),
        Some(events::ReadError::WrongStream)
    );
    assert!(old.same_worker(&old.clone()) && !old.same_worker(&next.client()));
    assert_eq!(
        next.client().peek(10, None, None).unwrap().wait().unwrap(),
        vec![tx()]
    );
    assert!(matches!(
        pushed.wait().unwrap().outcome,
        PushOutcome::Accepted { .. }
    ));
    assert_eq!(old.refresh().err(), Some(Error::Stopped));
    next.shutdown().unwrap();
}

#[test]
fn retirement_preserves_post_commit_unknown_and_queued_stopped_without_replaying_either() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let old = worker.client();
    let (release, first) = gate(&old);
    let committed: Ticket<()> = old
        .submit(BASE_WEIGHT, |service| {
            service.push(tx(), Event::Created, false)?;
            panic!("injected pool post-commit panic");
        })
        .unwrap();
    let never_run = old.delete(tx().hash()).unwrap();
    release.send(()).unwrap();
    first.wait().unwrap();
    let retired = worker.retire();
    assert_eq!(retired.result, Err(Error::Panicked));
    assert!(retired.info.joined && !retired.info.running);
    assert_eq!(retired.info.last_error, Some(Error::Panicked));
    assert_eq!(committed.wait(), Err(Error::Disconnected));
    assert_eq!(never_run.wait(), Err(Error::Stopped));
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Pool
    );
    let next = spawn(db);
    assert_eq!(next.client().info().submitted, 0);
    assert_eq!(
        next.client().peek(10, None, None).unwrap().wait().unwrap(),
        vec![tx()]
    );
    assert_eq!(old.delete(tx().hash()).err(), Some(Error::Stopped));
    next.shutdown().unwrap();
}
