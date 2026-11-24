use ae_stratum::{
    Error,
    job::{Job, Jobs, Limits, Timing, Work},
    nonce::{Kind, Nonce, Part},
    queue::Queue,
    share::{Context, Effect, Rejection, Response, Share, User, Validity},
    target,
};
use num_bigint::{BigInt, Sign};
use serde_json::{Value, json};

fn integer(v: &Value) -> BigInt {
    v.to_string().parse().unwrap()
}
fn number(v: &BigInt) -> Value {
    serde_json::from_str(&v.to_string()).unwrap()
}
fn part(p: Part) -> Value {
    json!({"type":if p.kind()==Kind::Extra {"extra"}else{"miner"},"value":p.value(),"bytes":p.bytes(),"hex":p.to_hex()})
}
fn validity(v: Validity) -> &'static str {
    match v {
        Validity::ValidBlock => "valid_block",
        Validity::ValidShare => "valid_share",
        Validity::UserNotFound => "user_not_found",
        Validity::InvalidMinerNonce => "invalid_miner_nonce",
        Validity::DuplicateShare => "duplicate_share",
        Validity::InvalidSolution => "invalid_solution",
        Validity::HighTargetShare => "high_target_share",
        Validity::MaxSolveTimeExceeded => "max_solve_time_exceeded",
        Validity::JobNotFound => "job_not_found",
    }
}
fn response(r: Response) -> Value {
    match r {
        Response::Result(value) => json!({"type":"rsp","method":"submit","id":7,"result":value}),
        Response::Error(reason, data) => json!({"type":"rsp","id":7,"reason":match reason {
            Rejection::UnauthorizedWorker=>"unauthorized_worker", Rejection::DuplicateShare=>"duplicate_share", Rejection::LowDifficultyShare=>"low_difficulty_share", Rejection::JobNotFound=>"job_not_found", Rejection::UnknownError=>"unknown_error"
        },"data":data}),
    }
}
fn effect(e: &Effect) -> Value {
    match e {
        Effect::SubmitShare {
            account,
            target,
            hash,
        } => {
            json!({"kind":"share","account":account,"target":number(target),"hash":hex::encode(hash.0)})
        }
        Effect::SubmitSolution { hash, nonce, proof } => {
            json!({"kind":"solution","hash":hex::encode(hash.0),"nonce":nonce.0,"pow":proof.to_vec()})
        }
    }
}
fn share_json(s: &Share) -> Value {
    json!({"user":[s.user.account,s.user.worker],"miner_nonce":s.miner_nonce.to_hex(),"pow":s.proof.to_vec(),"created":number(&s.created),"validity":s.validity.map(validity)})
}
fn job_json(j: &Job) -> Value {
    json!({"status":{"id":j.id(),"block_hash":j.work().hash_text(),"block_version":j.work().version,"block_target":target::to_hex(&j.work().block_target).unwrap(),"share_target":target::to_hex(&j.share_target()).unwrap(),"desired_solve_time":number(j.desired_solve_time()),"max_solve_time":number(j.maximum_solve_time()),"created":number(j.created()),"shares":j.shares().iter().map(share_json).collect::<Vec<_>>(),"error_shares":j.errors().iter().map(share_json).collect::<Vec<_>>()},"solve_time":number(&j.solve_time()),"submitted":j.is_submitted()})
}
fn make_share(nonce: Part, proof: [u32; 42], created: i64) -> Share {
    Share {
        user: User {
            account: "account".into(),
            worker: "worker".into(),
        },
        miner_nonce: nonce,
        proof,
        validity: None,
        created: created.into(),
    }
}
fn job(id: &str, hash: &str, version: u32, block: BigInt, share: BigInt, created: i64) -> Job {
    Job::new(
        id.into(),
        Work::new(hash.into(), version, block).unwrap(),
        share,
        Timing {
            desired: 30000.into(),
            maximum: 60000.into(),
            created: created.into(),
        },
        Limits::default(),
    )
    .unwrap()
}
fn recalculate(x: &Value) -> Result<BigInt, Error> {
    let previous = x["previous"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (integer(&p[0]), integer(&p[1])))
        .collect::<Vec<_>>();
    target::recalculate(&previous, &integer(&x["desired"]), &integer(&x["maximum"]))
}

#[test]
fn original_nonce_target_job_id_and_bounded_queue_vectors_match() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_primitives.json")).unwrap();
    let mut successes = 0;
    for row in &rows {
        let x = &row["input"];
        let expected = &row["result"];
        let result: Result<Value, Error> = match row["op"].as_str().unwrap() {
            "nonce" => {
                let n = x["bytes"].as_u64().unwrap() as u8;
                let v = x["value"].as_u64().unwrap();
                let e = Part::new(Kind::Extra, v, n).unwrap();
                let m = Part::new(Kind::Miner, Part::maximum(8 - n).unwrap() / 3, 8 - n).unwrap();
                let full = e.merge(m).unwrap();
                let (e1, m1) = full.split(Kind::Extra, n).unwrap();
                let (e2, m2) = full.split(Kind::Miner, 8 - n).unwrap();
                Ok(
                    json!({"extra":part(e),"miner":part(m),"value":full.0,"hex":full.to_hex(),"reverse_value":m.merge(e).unwrap().0,"split_extra":[part(e1),part(m1)],"split_miner":[part(e2),part(m2)],"decoded":Nonce::from_hex(&full.to_hex()).unwrap().0,"decoded_extra":Part::from_hex(Kind::Extra,&e.to_hex(),n).unwrap().value(),"complement":e.complement()}),
                )
            }
            "nonce_bad" => x["value"]
                .as_u64()
                .ok_or(Error::Nonce)
                .and_then(|v| Part::new(Kind::Extra, v, x["bytes"].as_u64().unwrap() as u8))
                .map(part),
            "recalculate" => recalculate(x).map(|v| number(&v)),
            "diff" => {
                target::difference(&integer(&x["new"]), &integer(&x["old"])).map(|d| match d {
                    target::Difference::Unchanged => json!("no_change"),
                    target::Difference::Increase(n) => json!(["increase", n]),
                    target::Difference::Decrease(n) => json!(["decrease", n]),
                })
            }
            "job_id" => Work::new(
                x["hash"].as_str().unwrap().into(),
                x["version"].as_u64().unwrap() as u32,
                integer(&x["target"]),
            )
            .and_then(|w| w.id())
            .map(Value::from),
            "queue" => {
                let cap = x["capacity"].as_u64().unwrap() as usize;
                let mut q = Queue::new(cap).unwrap();
                let values = (0..cap + 3).map(|i| (i % 2, i)).collect::<Vec<_>>();
                for &(k, v) in &values {
                    q.push(k, v);
                }
                let original = q.iter().copied().collect::<Vec<_>>();
                let found = q.find(&0).map(|v| json!([0, v])).unwrap_or(json!(false));
                let front = q.front().copied();
                let rear = q.rear().copied();
                q.replace(&0, 99);
                Ok(
                    json!({"values":values,"queue":original,"replaced":q.iter().collect::<Vec<_>>(),"find":found,"front":["ok",front],"rear":["ok",rear],"length":q.len(),"full":q.is_full()}),
                )
            }
            other => panic!("{other}"),
        };
        if expected["status"] == "ok" {
            successes += 1;
            assert_eq!(result.unwrap(), expected["value"], "{row}");
        } else {
            assert!(result.is_err(), "{row}");
        }
    }
    assert_eq!((rows.len(), successes), (140, 130));
}

fn jobs_case(x: &Value) -> Value {
    let n = x["count"].as_u64().unwrap() as usize;
    let cap = x["capacity"].as_u64().unwrap() as usize;
    let mut jobs = Jobs::new(cap, 8192).unwrap();
    let hash = "01".repeat(32);
    for i in 1..=n {
        let mut j = job(
            &(i % 2).to_string(),
            &hash,
            1,
            100.into(),
            1000.into(),
            1000,
        );
        let nonce = Part::new(Kind::Miner, i as u64, 4).unwrap();
        let proof = std::array::from_fn(|i| i as u32 + 1);
        let mut a = make_share(nonce, proof, 1000 + i as i64);
        a.validity = Some(Validity::ValidShare);
        let mut b = make_share(nonce, proof, 900);
        b.validity = Some(Validity::ValidShare);
        let mut e = make_share(nonce, proof, 999);
        e.validity = Some(Validity::InvalidSolution);
        match i % 3 {
            0 => {}
            1 => j.add_share(a).unwrap(),
            _ => {
                j.add_share(a).unwrap();
                j.add_share(b).unwrap();
                j.add_error(e).unwrap();
            }
        }
        jobs.add(j).unwrap();
    }
    let original = jobs.iter().map(job_json).collect::<Vec<_>>();
    let next = match jobs.next_target(&30000.into(), &10000.into()) {
        Ok(n) => json!(["ok", number(&n)]),
        Err(Error::NotEnoughJobs) => json!(["error", "not_enough_jobs"]),
        r => panic!("{r:?}"),
    };
    jobs.replace("0", job("different", &hash, 1, 90.into(), 900.into(), 10))
        .unwrap();
    json!({"jobs":original,"replaced":jobs.iter().map(job_json).collect::<Vec<_>>(),"found":jobs.find("0").map(job_json),"next_target":next})
}
fn submit_case(x: &Value) -> Value {
    let variant = x["variant"].as_str().unwrap();
    let hash = x["hash"].as_str().unwrap();
    let version = x["version"].as_u64().unwrap() as u32;
    let original: [u32; 42] = x["proof"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_u64().unwrap() as u32)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let target = BigInt::from_bytes_be(
        Sign::Plus,
        &ae_consensus::cuckoo::solution_hash(&original.map(u64::from), 29)
            .unwrap()
            .0,
    );
    let block_target = if matches!(
        variant,
        "share_equal" | "high_target" | "target_before_time"
    ) {
        &target - 1
    } else {
        target.clone()
    };
    let share_target = if matches!(variant, "high_target" | "target_before_time") {
        &target - 1
    } else {
        target.clone()
    };
    let (extra, mut miner) = Nonce(x["nonce"].as_u64().unwrap())
        .split(Kind::Extra, 4)
        .unwrap();
    if matches!(variant, "nonce_size" | "auth_before_nonce") {
        miner = Part::new(Kind::Miner, 0, 3).unwrap();
    }
    let mut proof = original;
    if matches!(variant, "bad_proof" | "duplicate_before_proof") {
        proof[0] = 0;
    }
    let created = match variant {
        "late" | "target_before_time" => 61001,
        "clock_back" => 900,
        _ => 61000,
    };
    let s = make_share(miner, proof, created);
    let mut j = job(
        "id",
        hash,
        version,
        block_target.clone(),
        share_target.clone(),
        1000,
    );
    if matches!(variant, "duplicate" | "duplicate_before_proof") {
        j.add_share(s.clone()).unwrap();
    }
    let mut jobs = Jobs::default();
    if variant != "job_missing" {
        jobs.add(j).unwrap();
    }
    match variant {
        "duplicate_job_keys" => {
            jobs.add(jobs.find("id").unwrap().clone()).unwrap();
        }
        "replacement_id_missing" | "replacement_id_present" => {
            let other = job(
                "other",
                hash,
                version,
                block_target.clone(),
                share_target.clone(),
                1000,
            );
            jobs.replace("id", other.clone()).unwrap();
            if variant == "replacement_id_present" {
                jobs.add(other).unwrap();
            }
        }
        _ => {}
    }
    let authorized = !matches!(variant, "unauthorized" | "auth_before_nonce");
    let receipt = jobs
        .submit(
            "id",
            s,
            &Context {
                extra_nonce: extra,
                authorized,
                maximum_solve_time: &if variant == "session_time_override" {
                    59999.into()
                } else {
                    60000.into()
                },
                edge_bits: 29,
            },
        )
        .unwrap();
    if receipt.decision.validity.accepted() {
        assert_eq!(
            receipt.decision.nonce,
            Some(Nonce(x["nonce"].as_u64().unwrap()))
        );
    } else {
        assert!(receipt.decision.nonce.is_none());
    }
    json!({"tag":if receipt.decision.validity.accepted(){"ok"}else{"error"},"share":share_json(&receipt.share),"retained_jobs":jobs.iter().map(job_json).collect::<Vec<_>>(),"effects":receipt.effects.iter().map(effect).collect::<Vec<_>>(),"response":response(receipt.response),"solution_target":number(&target),"block_target":number(&block_target),"share_target":number(&share_target),"authorized":authorized})
}
#[test]
fn original_work_state_and_submission_validation_match_saved_mainnet_proofs() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_work.json")).unwrap();
    let mut submissions = 0;
    let mut errors = 0;
    for row in &rows {
        let result: Result<Value, Error> = match row["op"].as_str().unwrap() {
            "target_hex" => target::to_hex(&integer(&row["value"])).map(Value::from),
            "target_int" => target::from_hex(row["text"].as_str().unwrap()).map(|v| number(&v)),
            "difference" => {
                target::difference(&integer(&row["new"]), &integer(&row["old"])).map(|d| match d {
                    target::Difference::Unchanged => json!("no_change"),
                    target::Difference::Increase(p) => {
                        json!({"direction":"increase","percent":p,"bits":p.to_bits()})
                    }
                    target::Difference::Decrease(p) => {
                        json!({"direction":"decrease","percent":p,"bits":p.to_bits()})
                    }
                })
            }
            "retarget" => recalculate(row).map(|v| number(&v)),
            "jobs" => Ok(jobs_case(row)),
            "submit" => {
                submissions += 1;
                Ok(submit_case(row))
            }
            other => panic!("{other}"),
        };
        if row["result"]["status"] == "ok" {
            let mut expected = row["result"]["value"].clone();
            if row["op"] == "submit" {
                expected.as_object_mut().unwrap().remove("job");
            }
            assert_eq!(result.unwrap(), expected, "{row}");
        } else {
            errors += 1;
            assert!(result.is_err(), "{row}");
        }
    }
    assert_eq!((rows.len(), submissions, errors), (215, 108, 19));
}

#[test]
fn accepted_effects_are_ordered_and_duplicate_submissions_do_not_repeat_them() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_work.json")).unwrap();
    let row = rows
        .iter()
        .find(|r| r["op"] == "submit" && r["variant"] == "block_equal")
        .unwrap();
    let proof: [u32; 42] = row["proof"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_u64().unwrap() as u32)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let target = integer(&row["result"]["value"]["solution_target"]);
    let (extra, miner) = Nonce(row["nonce"].as_u64().unwrap())
        .split(Kind::Extra, 4)
        .unwrap();
    let mut jobs = Jobs::default();
    jobs.add(job(
        "id",
        row["hash"].as_str().unwrap(),
        row["version"].as_u64().unwrap() as u32,
        target.clone(),
        target,
        1000,
    ))
    .unwrap();
    let ctx = Context {
        extra_nonce: extra,
        authorized: true,
        maximum_solve_time: &60000.into(),
        edge_bits: 29,
    };
    let s = make_share(miner, proof, 1001);
    let first = jobs.submit("id", s.clone(), &ctx).unwrap();
    assert!(matches!(
        first.effects.as_slice(),
        [Effect::SubmitShare { .. }, Effect::SubmitSolution { .. }]
    ));
    assert_eq!(first.response, Response::Result(true));
    let mut duplicate = s;
    duplicate.user.worker = "different worker".into();
    let again = jobs.submit("id", duplicate, &ctx).unwrap();
    assert_eq!(again.decision.validity, Validity::DuplicateShare);
    assert!(again.effects.is_empty());
    assert_eq!(
        again.response,
        Response::Error(Rejection::DuplicateShare, None)
    );
    assert_eq!(jobs.find("id").unwrap().shares().len(), 1);
    assert_eq!(jobs.find("id").unwrap().errors().len(), 1);
}
