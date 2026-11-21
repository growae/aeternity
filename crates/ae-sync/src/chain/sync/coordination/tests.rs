use super::*;
use serde_json::Value as Json;

fn fixture() -> Json {
    serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_chain_coordination.json"
    ))
    .unwrap()
}
fn bytes(j: &Json) -> [u8; 32] {
    hex::decode(j.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}
fn hash(j: &Json) -> Hash256 {
    Hash256(bytes(j))
}
fn peer(j: &Json) -> PeerPublicKey {
    PeerPublicKey(bytes(j))
}
fn point(j: &Json) -> Point {
    Point {
        height: j["height"].as_u64().unwrap(),
        hash: hash(&j["hash"]),
    }
}
fn array(j: &Json) -> &[Json] {
    j.as_array().unwrap()
}
fn chain(j: &Json) -> Chain {
    Chain::new(
        j["id"].as_u64().unwrap(),
        array(&j["peers"]).iter().map(peer).collect(),
        array(&j["blocks"]).iter().map(point).collect(),
        Limits::default(),
    )
    .unwrap()
}
fn ready(j: &Json) -> Ready {
    Ready {
        peer: peer(&j["peer"]),
        ticket: j["ticket"].as_u64().unwrap(),
    }
}
fn items(j: &Json) -> Vec<Item> {
    array(j)
        .iter()
        .map(|j| Item {
            point: point(&j["point"]),
            attempts: j["attempts"].as_u64().unwrap(),
            got: (!j["got"].is_null()).then(|| ready(&j["got"])),
        })
        .collect()
}
fn state(j: &Json) -> State {
    State {
        chain: chain(&j["chain"]),
        suspect: j["suspect"].as_bool().unwrap(),
        agreed: (!j["agreed"].is_null()).then(|| point(&j["agreed"])),
        pool: items(&j["pool"]),
        adding: items(&j["adding"]),
        pending: array(&j["pending"]).iter().map(items).collect(),
        workers: array(&j["workers"]).iter().map(peer).collect(),
        next_pool: match j["next_pool"]["kind"].as_str().unwrap() {
            "none" => NextPool::None,
            "fetching" => NextPool::Fetching,
            "ready" => NextPool::Ready(items(&j["next_pool"]["items"])),
            other => panic!("unexpected {other}"),
        },
    }
}
fn work(j: &Json) -> Work {
    match j["kind"].as_str().unwrap() {
        "agree" => Work::Agree,
        "break" => Work::Break,
        "generation" => Work::Generation(point(&j["point"])),
        "post" => Work::Post(items(&j["items"])),
        "fill" => Work::Fill {
            from: hash(&j["from"]),
            target: hash(&j["target"]),
        },
        other => panic!("unexpected {other}"),
    }
}
fn input(j: &Json) -> ResultInput {
    match j["kind"].as_str().unwrap() {
        "agreed" => ResultInput::Agreed(point(&j["point"])),
        "hashes" => ResultInput::Hashes {
            first_height: j["first_height"].as_u64().unwrap(),
            items: items(&j["items"]),
        },
        "generation" => ResultInput::Generation {
            point: point(&j["point"]),
            ready: ready(&j["ready"]),
        },
        "posted" => ResultInput::Posted,
        "posted_at" => ResultInput::PostedAt {
            height: j["height"].as_u64().unwrap(),
            stats: j["stats"].as_u64().unwrap(),
        },
        "post_error" => ResultInput::PostError {
            peer: peer(&j["peer"]),
            height: j["height"].as_u64().unwrap(),
            rejected: j["rejected"].as_bool().unwrap(),
        },
        other => panic!("unexpected {other}"),
    }
}
fn effects(j: &Json) -> Vec<Effect> {
    array(j)
        .iter()
        .map(|j| match j["kind"].as_str().unwrap() {
            "height" => Effect::SyncHeight {
                peer: peer(&j["peer"]),
                height: j["height"].as_u64().unwrap(),
            },
            "disconnect" => Effect::Disconnect(peer(&j["peer"])),
            "suspect" => Effect::Suspect(peer(&j["peer"])),
            "done" => Effect::Done(peer(&j["peer"])),
            "stats" => Effect::Statistics {
                task: j["task"].as_u64().unwrap(),
                stats: j["stats"].as_u64().unwrap(),
            },
            other => panic!("unexpected {other}"),
        })
        .collect()
}
#[test]
fn original_chain_hints_merge_matching_and_lookup_priority_match() {
    let corpus = fixture();
    let limits = Limits::default();
    assert_eq!(array(&corpus["initial"]).len(), 8);
    for row in array(&corpus["initial"]) {
        let expected = chain(&row["result"]);
        let header = ae_codec::headers::Header::decode(
            &hex::decode(row["header"].as_str().unwrap()).unwrap(),
            ae_codec::headers::HeaderMode::ProofOfWork,
        )
        .unwrap();
        assert_eq!(
            Chain::from_header(expected.id(), expected.peers()[0], &header, limits).unwrap(),
            expected
        );
    }
    assert_eq!(array(&corpus["matches"]).len(), 484);
    for (index, row) in array(&corpus["matches"]).iter().enumerate() {
        let result = &row["result"];
        let expected = match result["kind"].as_str().unwrap() {
            "equal" => Comparison::Equal,
            "different" => Comparison::Different,
            "need" => Comparison::Need {
                height: result["height"].as_u64().unwrap(),
                side: if result["side"] == "fst" {
                    Side::First
                } else {
                    Side::Second
                },
            },
            other => panic!("unexpected {other}"),
        };
        assert_eq!(
            chain(&row["left"]).compare(&chain(&row["right"])),
            expected,
            "compare row {index}"
        );
    }
    assert_eq!(array(&corpus["merges"]).len(), 484);
    for (index, row) in array(&corpus["merges"]).iter().enumerate() {
        assert_eq!(
            chain(&row["left"])
                .merge(&chain(&row["right"]), limits)
                .unwrap(),
            chain(&row["result"]),
            "merge row {index}"
        );
    }
    assert_eq!(array(&corpus["known"]).len(), 242);
    for row in array(&corpus["known"]) {
        assert_eq!(
            chain(&row["chain"]).next_known_hash(row["height"].as_str().unwrap().parse().unwrap()),
            hash(&row["result"])
        );
    }
    assert_eq!(array(&corpus["tasks"]).len(), 110);
    for (index, row) in array(&corpus["tasks"]).iter().enumerate() {
        let candidate = chain(&row["candidate"]);
        let tasks: Vec<_> = array(&row["tasks"]).iter().map(chain).collect();
        let result = &row["result"];
        let expected = match result["kind"].as_str().unwrap() {
            "new" => TaskMatch::New,
            "existing" => TaskMatch::Existing(
                tasks
                    .iter()
                    .position(|c| c.id() == result["id"].as_u64().unwrap())
                    .unwrap(),
            ),
            "need" => {
                let id = result["id"].as_u64().unwrap();
                let c = if id == candidate.id() {
                    &candidate
                } else {
                    tasks.iter().find(|c| c.id() == id).unwrap()
                };
                assert_eq!(
                    c.peers(),
                    array(&result["peers"]).iter().map(peer).collect::<Vec<_>>()
                );
                TaskMatch::Need {
                    chain: id,
                    height: result["height"].as_u64().unwrap(),
                }
            }
            other => panic!("unexpected {other}"),
        };
        assert_eq!(
            match_tasks(&candidate, &tasks),
            expected,
            "task row {index}"
        );
    }
}
#[test]
fn original_scheduling_and_result_transitions_preserve_queues_and_effect_order() {
    let corpus = fixture();
    assert_eq!(array(&corpus["next"]).len(), 215);
    let mut repaired = 0;
    for (index, row) in array(&corpus["next"]).iter().enumerate() {
        let before = state(&row["state"]);
        let mut scheduler = Scheduler::restore(before.clone(), Limits::default()).unwrap();
        if row["result"]["kind"] == "error" {
            assert!(before.suspect);
            assert_eq!(
                row["result"]["reason"],
                "836802770b636173655f636c6175736577046e6f6e65"
            );
            assert_eq!(scheduler.next_work().unwrap(), Work::Break);
            assert_eq!(scheduler.state(), &before);
            repaired += 1;
        } else {
            assert_eq!(
                scheduler.next_work().unwrap(),
                work(&row["result"]["work"]),
                "next row {index}"
            );
            assert_eq!(
                scheduler.state(),
                &state(&row["result"]["state"]),
                "next state row {index}"
            );
        }
    }
    assert_eq!(repaired, 1);
    assert_eq!(array(&corpus["results"]).len(), 356);
    for (index, row) in array(&corpus["results"]).iter().enumerate() {
        assert_eq!(row["result"]["kind"], "ok");
        let mut scheduler = Scheduler::restore(state(&row["state"]), Limits::default()).unwrap();
        assert_eq!(
            scheduler.apply(input(&row["input"])).unwrap(),
            effects(&row["effects"]),
            "effects row {index}"
        );
        assert_eq!(
            scheduler.state(),
            &state(&row["result"]["state"]),
            "result state row {index}"
        );
    }
}
#[test]
fn original_out_of_order_download_traces_post_only_ready_prefixes() {
    let corpus = fixture();
    assert_eq!(array(&corpus["traces"]).len(), 4);
    for (index, trace) in array(&corpus["traces"]).iter().enumerate() {
        let mut scheduler =
            Scheduler::restore(state(&trace["initial"]), Limits::default()).unwrap();
        for (step, row) in array(&trace["steps"]).iter().enumerate() {
            if row["action"] == "next" {
                assert_eq!(
                    scheduler.next_work().unwrap(),
                    work(&row["work"]),
                    "trace {index} step {step}"
                );
            } else {
                scheduler.apply(input(&row["input"])).unwrap();
            }
            assert_eq!(
                scheduler.state(),
                &state(&row["state"]),
                "trace {index} step {step}"
            );
        }
    }
}
fn p(height: u64) -> Point {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&height.to_be_bytes());
    Point {
        height,
        hash: Hash256(bytes),
    }
}
fn c() -> Chain {
    Chain::new(
        1,
        vec![PeerPublicKey([1; 32]), PeerPublicKey([2; 32])],
        vec![p(10), p(0)],
        Limits::default(),
    )
    .unwrap()
}
#[test]
fn local_queue_limits_and_invalid_transitions_are_atomic() {
    let limits = Limits {
        chunk: 2,
        items: 3,
        ..Limits::default()
    };
    let mut scheduler = Scheduler::new(c(), limits).unwrap();
    assert_eq!(
        scheduler.next_for(PeerPublicKey([9; 32])).unwrap(),
        Work::Abort
    );
    assert_eq!(
        scheduler.next_for(PeerPublicKey([1; 32])).unwrap(),
        Work::Agree
    );
    scheduler.apply(ResultInput::Agreed(p(0))).unwrap();
    let before = scheduler.state().clone();
    assert_eq!(
        scheduler.apply(ResultInput::Hashes {
            first_height: 1,
            items: (1..=4).map(|h| Item::missing(p(h))).collect()
        }),
        Err(Error::Capacity)
    );
    assert_eq!(scheduler.state(), &before);
    assert_eq!(
        scheduler.apply(ResultInput::Hashes {
            first_height: 1,
            items: vec![]
        }),
        Err(Error::State)
    );
    assert_eq!(scheduler.state(), &before);
    assert_eq!(
        scheduler.apply(ResultInput::PostError {
            peer: PeerPublicKey([1; 32]),
            height: 1,
            rejected: true
        }),
        Err(Error::State)
    );
    assert_eq!(scheduler.state(), &before);
    scheduler
        .apply(ResultInput::Hashes {
            first_height: 1,
            items: (1..=3).map(|h| Item::missing(p(h))).collect(),
        })
        .unwrap();
    for h in [3, 2, 1] {
        scheduler
            .apply(ResultInput::Generation {
                point: p(h),
                ready: Ready {
                    peer: PeerPublicKey([1; 32]),
                    ticket: h,
                },
            })
            .unwrap();
    }
    let Work::Post(items) = scheduler.next_work().unwrap() else {
        panic!("post")
    };
    assert_eq!(
        items.iter().map(|i| i.point.height).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(scheduler.state().pending[0][0].point, p(3));
    scheduler.apply(ResultInput::Posted).unwrap();
    assert!(matches!(scheduler.next_work().unwrap(),Work::Post(v)if v.len()==1));
    let before = scheduler.state().clone();
    assert_eq!(
        scheduler.workers(vec![PeerPublicKey([1; 32]); 129]),
        Err(Error::Capacity)
    );
    assert_eq!(scheduler.state(), &before);
    let mut overflow = before;
    overflow.adding.clear();
    overflow.pool = vec![Item {
        point: p(9),
        got: None,
        attempts: u64::MAX,
    }];
    overflow.next_pool = NextPool::Fetching;
    let mut scheduler = Scheduler::restore(overflow.clone(), limits).unwrap();
    assert_eq!(scheduler.next_work(), Err(Error::Overflow));
    assert_eq!(scheduler.state(), &overflow);
    assert_eq!(
        c().merge(&c().with_id(2), Limits::default()),
        Err(Error::Identity)
    );
    assert_eq!(
        c().merge(
            &c(),
            Limits {
                hints: 1,
                ..Limits::default()
            }
        ),
        Err(Error::Capacity)
    );
    assert_eq!(
        Chain::new(1, vec![], vec![], Limits::default()),
        Err(Error::Empty)
    );
    assert_eq!(
        Chain::new(1, vec![], vec![p(1), p(2)], Limits::default()),
        Err(Error::Order)
    );
}
