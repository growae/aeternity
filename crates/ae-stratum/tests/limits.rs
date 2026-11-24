use ae_stratum::{
    Error,
    job::{Job, Jobs, Limits, Timing, Work},
    nonce::{Kind, Nonce, Part},
    queue::Queue,
    share::{Context, Share, User},
    target,
};
use num_bigint::BigInt;

fn share() -> Share {
    Share {
        user: User {
            account: "a".into(),
            worker: "w".into(),
        },
        miner_nonce: Part::new(Kind::Miner, 1, 4).unwrap(),
        proof: [1; 42],
        validity: None,
        created: 1001.into(),
    }
}
fn job(id: &str, limits: Limits) -> Job {
    Job::new(
        id.into(),
        Work::new("00".repeat(32), 1, 100.into()).unwrap(),
        200.into(),
        Timing {
            desired: 30000.into(),
            maximum: 60000.into(),
            created: 1000.into(),
        },
        limits,
    )
    .unwrap()
}

#[test]
fn nonce_domains_updates_and_byte_orders_are_explicit() {
    for bytes in 1..=7 {
        let max = Part::maximum(bytes).unwrap();
        let mut part = Part::new(Kind::Extra, max, bytes).unwrap();
        let old = part;
        assert_eq!(part.update(max + 1), Err(Error::Nonce));
        assert_eq!(part, old);
        part.update(0).unwrap();
        assert_eq!(part.value(), 0);
        let (e, m) = Nonce(u64::MAX).split(Kind::Extra, bytes).unwrap();
        assert_eq!(e.merge(m).unwrap(), Nonce(u64::MAX));
        assert_eq!(m.merge(e).unwrap(), Nonce(u64::MAX));
        assert_eq!(
            Part::from_hex(Kind::Extra, &e.to_hex().to_uppercase(), bytes).unwrap(),
            e
        );
        assert_eq!(e.merge(e), Err(Error::Nonce));
    }
    assert_eq!(Nonce(0x0807060504030201).to_hex(), "0102030405060708");
    for text in [
        "",
        "1",
        " 000000000000001",
        "000000000000000g",
        "+000000000000001",
        "00000000000000000",
    ] {
        assert!(Nonce::from_hex(text).is_err());
    }
    for bytes in [0, 8, u8::MAX] {
        assert!(Part::maximum(bytes).is_err());
        assert!(Nonce(1).split(Kind::Miner, bytes).is_err());
    }
    assert!(
        Part::new(Kind::Miner, 1, 3)
            .unwrap()
            .merge(Part::new(Kind::Extra, 1, 4).unwrap())
            .is_err()
    );
}

#[test]
fn target_errors_and_resource_bounds_never_truncate_values() {
    assert_eq!(target::maximum(), BigInt::from(0xffffu32) << 240);
    assert_eq!(
        target::recalculate(&[], &0.into(), &(-1).into()).unwrap(),
        (-1).into()
    );
    assert_eq!(
        target::recalculate(&[(0.into(), 1.into())], &1.into(), &10.into()),
        Err(Error::Arithmetic)
    );
    assert_eq!(
        target::recalculate(
            &[(100000000000000u64.into(), 1.into())],
            &1.into(),
            &1.into()
        ),
        Err(Error::Arithmetic)
    );
    assert_eq!(
        target::difference(&0.into(), &0.into()).unwrap(),
        target::Difference::Unchanged
    );
    assert_eq!(
        target::difference(&1.into(), &0.into()),
        Err(Error::Arithmetic)
    );
    assert_eq!(
        target::difference(&(BigInt::from(1) << 1024), &1.into()),
        Err(Error::Float)
    );
    assert_eq!(
        target::to_hex(&(BigInt::from(1) << 16385)),
        Err(Error::Limit("integer"))
    );
    assert_eq!(
        target::from_hex(&"f".repeat(4097)),
        Err(Error::Limit("target text"))
    );
    assert_eq!(
        target::recalculate(&vec![(1.into(), 1.into()); 1025], &1.into(), &10.into()),
        Err(Error::Limit("samples"))
    );
    let w = Work::new("01".repeat(32), 256, 10.into()).unwrap();
    assert_eq!(w.id(), Err(Error::Version));
    assert!(Work::new("not a hash".into(), 1, 10.into()).is_err());
}

#[test]
fn job_and_queue_limits_leave_original_state_available_for_retry() {
    let mut j = job(
        "x",
        Limits {
            shares: 1,
            errors: 1,
        },
    );
    j.add_share(share()).unwrap();
    j.add_error(share()).unwrap();
    let old = j.clone();
    assert_eq!(j.add_share(share()), Err(Error::Limit("shares")));
    assert_eq!(j, old);
    assert_eq!(j.add_error(share()), Err(Error::Limit("error shares")));
    assert_eq!(j, old);
    let mut jobs = Jobs::new(3, 2).unwrap();
    jobs.add(j.clone()).unwrap();
    let old = jobs.clone();
    assert_eq!(jobs.add(j), Err(Error::Limit("retained shares")));
    assert_eq!(jobs, old);
    let mut invalid = share();
    invalid.user.worker = "x".repeat(1025);
    let mut empty = job("x", Limits::default());
    let before = empty.clone();
    assert_eq!(empty.add_share(invalid), Err(Error::Limit("user")));
    assert_eq!(empty, before);
    assert!(Jobs::new(0, 1).is_err());
    assert!(Jobs::new(1025, 1).is_err());
    assert!(Jobs::new(1, 65537).is_err());
    assert!(
        Job::new(
            "x".into(),
            Work::new("00".repeat(32), 1, 1.into()).unwrap(),
            1.into(),
            Timing {
                desired: 1.into(),
                maximum: 1.into(),
                created: 1.into()
            },
            Limits {
                shares: 65537,
                errors: 1
            }
        )
        .is_err()
    );
}

#[test]
fn duplicate_key_submission_is_atomic_under_global_retention_limits() {
    let mut jobs = Jobs::new(3, 1).unwrap();
    jobs.add(job("x", Limits::default())).unwrap();
    jobs.add(job("x", Limits::default())).unwrap();
    let before = jobs.clone();
    let ctx = Context {
        extra_nonce: Part::new(Kind::Extra, 0, 4).unwrap(),
        authorized: false,
        maximum_solve_time: &60000.into(),
        edge_bits: 29,
    };
    assert_eq!(
        jobs.submit("x", share(), &ctx),
        Err(Error::Limit("retained shares"))
    );
    assert_eq!(jobs, before);
    let absent = jobs.submit("missing", share(), &ctx).unwrap();
    assert!(!absent.job_found);
    assert_eq!(jobs, before);
    let mut one = Jobs::new(1, 1).unwrap();
    one.add(job("x", Limits::default())).unwrap();
    one.submit("x", share(), &ctx).unwrap();
    let evicted = one.add(job("y", Limits::default())).unwrap().unwrap();
    assert_eq!(evicted.errors().len(), 1);
    assert_eq!(one.front().unwrap().id(), "y");
    one.submit("y", share(), &ctx).unwrap();
    assert_eq!(one.front().unwrap().errors().len(), 1);
}

#[test]
fn first_accepted_insertion_sets_solve_time_and_errors_do_not_mark_submission() {
    let mut j = job("x", Limits::default());
    j.add_error(share()).unwrap();
    assert!(!j.is_submitted());
    assert_eq!(j.solve_time(), 60000.into());
    let mut first = share();
    first.created = 1100.into();
    j.add_share(first).unwrap();
    let mut later = share();
    later.created = 900.into();
    j.add_share(later).unwrap();
    assert_eq!(j.solve_time(), 100.into());
    assert_eq!(j.shares()[0].created, 900.into());
    assert_eq!(j.shares()[1].created, 1100.into());
    assert!(j.contains(share().miner_nonce, &share().proof));
    let mut q = Queue::new(2).unwrap();
    q.push(0, 1);
    q.push(0, 2);
    assert_eq!(q.find(&0), Some(&1));
    q.replace(&0, 3);
    assert_eq!(q.pop(), Some((0, 3)));
    assert_eq!(q.pop(), Some((0, 3)));
    assert!(q.is_empty());
}
