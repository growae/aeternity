use super::*;
#[tokio::test]
async fn posting_and_retry_height_updates_apply_only_to_exact_worker_generations() {
    let ((a, _, _da), (b, _, _db)) = crate::test_support::pair(7, 8).await;
    let ((replacement, _, _dr), (_, _, _ds)) = crate::test_support::pair(7, 8).await;
    let limits = coordination::Limits::default();
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    let mut book = Book::<Otp26>::new();
    let header = blocks[2].header();
    let chain = Chain::from_header(1, a.peer(), &header, limits).unwrap();
    assert!(matches!(
        book.join(&chain, &a, limits).unwrap(),
        Join::Joined(1)
    ));
    let chain = Chain::from_header(2, b.peer(), &header, limits).unwrap();
    assert!(matches!(
        book.join(&chain, &b, limits).unwrap(),
        Join::Joined(1)
    ));
    let point = Point {
        height: 1,
        hash: blocks[0].hash().unwrap(),
    };
    let item = Item {
        point,
        got: Some(Ready {
            peer: a.peer(),
            ticket: 1,
        }),
        attempts: 0,
    };
    let lease = Lease {
        task: 1,
        sequence: 4,
        peer: a.peer(),
        generation: a.generation().clone(),
    };
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.agreed = Some(point);
        state.adding = vec![item];
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&a.peer()).unwrap().lease = Some((4, Kind::Post { posted: 1 }));
    }
    book.post_complete(&lease).unwrap();
    assert_eq!(book.sync_height(&a), Some(1));
    assert_eq!(book.sync_height(&b), Some(1));
    assert_eq!(book.sync_height(&replacement), None);
    let mut item = item;
    item.point.height = 2;
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.adding = vec![item];
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&a.peer()).unwrap().lease = Some((4, Kind::Post { posted: 0 }));
    }
    let error = Error::Import(chain::Failure {
        progress: Some(Default::default()),
        error: chain::Error::Validation {
            operational: false,
            reason: "invalid body".into(),
        },
    });
    assert!(
        book.post_failed(&lease, item, error, vec![])
            .unwrap()
            .is_empty()
    );
    assert_eq!(book.sync_height(&a), Some(2));
    assert_eq!(book.sync_height(&b), Some(2));
    book.detach(1, &replacement, limits);
    assert_eq!(book.sync_height(&a), Some(2));
    book.detach(1, &a, limits);
    assert_eq!(book.sync_height(&a), None);
    assert_eq!(book.sync_height(&b), Some(2));
    let lease = Lease {
        task: 1,
        sequence: 5,
        peer: b.peer(),
        generation: b.generation().clone(),
    };
    book.group_mut(1)
        .unwrap()
        .members
        .get_mut(&b.peer())
        .unwrap()
        .lease = Some((5, Kind::Forward));
    book.forwarded(&lease, point).unwrap();
    assert_eq!(book.sync_height(&b), None);
}
