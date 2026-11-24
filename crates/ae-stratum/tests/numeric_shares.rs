use ae_stratum::{
    Error,
    job::{Job, Jobs, Limits, Timing, Work},
    nonce::{Kind, Nonce, Part},
    proof::{Number, Proof},
    share::{Context, Effect, Share, User, Validity},
    target,
    wire::{json as wire_json, rpc},
};
use num_bigint::BigInt;
use serde_json::{Value, json};

const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
const ID: &str = "0123456789abcdef";
fn native(t: &Value) -> Value {
    if let Some(s) = t.get("atom").and_then(Value::as_str) {
        return match s {
            "true" => json!(true),
            "false" => json!(false),
            "null" => Value::Null,
            _ => json!(s),
        };
    }
    if let Some(s) = t.get("binary").and_then(Value::as_str) {
        return json!(String::from_utf8(hex::decode(s).unwrap()).unwrap());
    }
    if let Some(s) = t.get("integer").and_then(Value::as_str) {
        return serde_json::from_str(s).unwrap();
    }
    if let Some(s) = t.get("float").and_then(Value::as_str) {
        return json!(f64::from_bits(u64::from_str_radix(s, 16).unwrap()));
    }
    if let Some(a) = t
        .get("list")
        .or_else(|| t.get("tuple"))
        .and_then(Value::as_array)
    {
        return Value::Array(a.iter().map(native).collect());
    }
    let a = t["map"].as_array().unwrap();
    Value::Object(
        a.iter()
            .map(|pair| (native(&pair[0]).as_str().unwrap().into(), native(&pair[1])))
            .collect(),
    )
}
// serde_json equality alone is not sufficient to check numeric type/zero sign.
fn typed(v: Value) -> Value {
    match v {
        Value::Number(n) if n.as_str().contains(['.', 'e', 'E']) => {
            json!({"float_bits":format!("{:016x}",n.as_f64().unwrap().to_bits())})
        }
        Value::Array(a) => Value::Array(a.into_iter().map(typed).collect()),
        Value::Object(m) => Value::Object(m.into_iter().map(|(k, v)| (k, typed(v))).collect()),
        v => v,
    }
}
fn number(n: &BigInt) -> Value {
    serde_json::from_str(&n.to_string()).unwrap()
}
fn validity(v: Option<Validity>) -> &'static str {
    match v {
        None => "undefined",
        Some(Validity::ValidBlock) => "valid_block",
        Some(Validity::ValidShare) => "valid_share",
        Some(Validity::UserNotFound) => "user_not_found",
        Some(Validity::InvalidMinerNonce) => "invalid_miner_nonce",
        Some(Validity::DuplicateShare) => "duplicate_share",
        Some(Validity::InvalidSolution) => "invalid_solution",
        Some(Validity::HighTargetShare) => "high_target_share",
        Some(Validity::MaxSolveTimeExceeded) => "max_solve_time_exceeded",
        Some(Validity::JobNotFound) => "job_not_found",
    }
}
fn share(s: &Share) -> Value {
    json!({"user":[s.user.account,s.user.worker],"miner_nonce":s.miner_nonce.to_hex(),"pow":s.proof.to_json(),"created":number(&s.created),"validity":validity(s.validity)})
}
fn job(j: &Job) -> Value {
    json!({"id":j.id(),"block_hash":j.work().hash_text(),"block_version":j.work().version,"block_target":target::to_hex(&j.work().block_target).unwrap(),"share_target":target::to_hex(&j.share_target()).unwrap(),"created":number(j.created()),"desired_solve_time":number(j.desired_solve_time()),"max_solve_time":number(j.maximum_solve_time()),"shares":j.shares().iter().map(share).collect::<Vec<_>>(),"error_shares":j.errors().iter().map(share).collect::<Vec<_>>()})
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
fn make_share(proof: Proof, miner_nonce: Part, created: i64) -> Share {
    Share {
        user: User {
            account: ACCOUNT.into(),
            worker: "one".into(),
        },
        miner_nonce,
        proof,
        validity: None,
        created: created.into(),
    }
}
fn make_job(hash: &str, version: u32, target: BigInt, limits: Limits) -> Job {
    Job::new(
        ID.into(),
        Work::new(hash.into(), version, target.clone()).unwrap(),
        target,
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
fn original_numeric_submissions_match_retention_wire_responses_and_arithmetic_failures() {
    let corpus: Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_numeric_shares.json")).unwrap();
    let rows = corpus["rows"].as_array().unwrap();
    let mut exceptions = 0;
    for row in rows {
        let variant = row["variant"].as_str().unwrap();
        let hash = row["hash"].as_str().unwrap();
        let pow = native(&row["pow"]);
        let proof = Proof::from_json(&pow).unwrap();
        assert_eq!(typed(proof.to_json()), typed(pow));
        let original = rows
            .iter()
            .find(|r| {
                r["hash"] == row["hash"] && r["mode"] == "integer" && r["variant"] == "normal"
            })
            .unwrap();
        let (extra, miner) = Nonce(row["nonce"].as_u64().unwrap())
            .split(Kind::Extra, 4)
            .unwrap();
        let miner = if variant == "nonce_size" {
            Part::new(Kind::Miner, 0, 3).unwrap()
        } else {
            miner
        };
        let submitted = make_share(proof, miner, if variant == "late" { 61001 } else { 61000 });
        let options = rpc::Options::default();
        let packet = json!({"jsonrpc":"2.0","id":7,"method":"mining.submit","params":[format!("{ACCOUNT}.one"),ID,miner.to_hex(),submitted.proof.to_json()]});
        let bytes = wire_json::encode(&packet, options.limits).unwrap();
        let wire = match rpc::decode(&bytes, &options) {
            Ok(m) => json!(["ok", m]),
            Err(rpc::Error::InvalidParam(p, rpc::MaybeId::Integer(i))) => {
                json!(["error", ["invalid_param", p, number(&i)]])
            }
            other => panic!("unexpected wire outcome {other:?}"),
        };
        assert_eq!(
            typed(wire),
            typed(native(&row["wire"])),
            "wire {} {variant}",
            row["mode"]
        );
        let mut target: BigInt = row["target"].as_str().unwrap().parse().unwrap();
        if variant == "high_target" {
            target -= 1;
        }
        let mut j = make_job(
            hash,
            row["version"].as_u64().unwrap() as u32,
            target,
            Limits::default(),
        );
        if variant == "duplicate_same" {
            j.add_share(submitted.clone()).unwrap();
        }
        if variant == "duplicate_integer" {
            j.add_share(make_share(
                Proof::from_json(&native(&original["pow"])).unwrap(),
                miner,
                61000,
            ))
            .unwrap();
        }
        let mut jobs = Jobs::default();
        if variant != "missing" {
            jobs.add(j).unwrap();
        }
        let before = jobs.clone();
        let ctx = Context {
            extra_nonce: extra,
            authorized: variant != "unauthorized",
            maximum_solve_time: &60000.into(),
            edge_bits: 29,
        };
        let result = match jobs.submit(ID, submitted, &ctx) {
            Ok(receipt) => {
                let reply = rpc::share_response(&7.into(), receipt.response).unwrap();
                let encoded = rpc::encode(&reply, &options).unwrap();
                let response = wire_json::decode(&encoded, options.limits).unwrap();
                json!({"tag":if receipt.decision.validity.accepted(){"ok"}else{"error"},"share":share(&receipt.share),"response":response,"retained":jobs.iter().map(job).collect::<Vec<_>>(),"effects":receipt.effects.iter().map(effect).collect::<Vec<_>>()})
            }
            Err(Error::ProofArithmetic) => {
                exceptions += 1;
                assert_eq!(jobs, before, "arithmetic failure mutated state");
                json!({"exception":["error","badarith"],"retained":jobs.iter().map(job).collect::<Vec<_>>(),"effects":[]})
            }
            other => panic!("unexpected submission outcome {other:?}"),
        };
        assert_eq!(
            typed(result),
            typed(native(&row["result"])),
            "{} {variant} {hash}",
            row["mode"]
        );
    }
    assert_eq!(rows.len(), 816);
    assert_eq!(exceptions, 168);
}

#[test]
fn native_duplicate_matching_is_type_sensitive_but_equates_signed_float_zero() {
    let corpus: Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_numeric_shares.json")).unwrap();
    let make = |mode: &str| {
        let mut p: Vec<_> = (0..42).map(|i| Number::Integer(i.into())).collect();
        match mode {
            "integer" => {}
            "first_float" | "positive_zero" => p[0] = Number::Float(0.0f64.to_bits()),
            "negative_zero" => p[0] = Number::Float((-0.0f64).to_bits()),
            _ => panic!("mode"),
        }
        Proof::new(p).unwrap()
    };
    let nonce = Part::new(Kind::Miner, 42, 4).unwrap();
    for row in corpus["duplicates"].as_array().unwrap() {
        let s = make_share(make(row["stored"].as_str().unwrap()), nonce, 1);
        assert_eq!(
            s.duplicates(nonce, &make(row["submitted"].as_str().unwrap())),
            row["result"].as_bool().unwrap()
        );
    }
    assert_ne!(make("negative_zero"), make("positive_zero"));
    assert_eq!(corpus["duplicates"].as_array().unwrap().len(), 16);
}

#[test]
fn numeric_domains_and_validation_order_do_not_coerce_or_scan_past_native_failures() {
    assert_eq!(
        Proof::new(vec![Number::Float(f64::NAN.to_bits())]),
        Err(Error::Float)
    );
    assert_eq!(
        Proof::new(vec![Number::Float(f64::INFINITY.to_bits())]),
        Err(Error::Float)
    );
    assert_eq!(
        Proof::new(vec![Number::Integer(BigInt::from(1) << 16384)]),
        Err(Error::Limit("integer"))
    );
    assert_eq!(
        Proof::new(vec![Number::Integer(0.into()); 1025]),
        Err(Error::Limit("proof elements"))
    );
    assert_eq!(
        Proof::new(vec![Number::Integer(BigInt::from(1) << 16000); 40]),
        Err(Error::Limit("proof bytes"))
    );
    assert_eq!(Proof::from_json(&json!([true])), Err(Error::Proof));
    assert_eq!(Proof::from_json(&json!(null)), Err(Error::Proof));
    let nonce = Part::new(Kind::Miner, 0, 4).unwrap();
    let ctx = Context {
        extra_nonce: Part::new(Kind::Extra, 0, 4).unwrap(),
        authorized: true,
        maximum_solve_time: &60000.into(),
        edge_bits: 63,
    };
    let j = make_job(&"00".repeat(32), 1, 100.into(), Limits::default());
    for (value, expected) in [
        (json!([0.0]), Err(Error::ProofArithmetic)),
        (json!([0, 0, 0.0]), Ok(Validity::InvalidSolution)),
        (json!([0.0, -1]), Err(Error::ProofArithmetic)),
        (
            json!([9223372036854775808.0]),
            Ok(Validity::InvalidSolution),
        ),
        (json!([9223372036854774784.0]), Err(Error::ProofArithmetic)),
        (json!([-1.0]), Ok(Validity::InvalidSolution)),
        (json!([-0.5]), Err(Error::ProofArithmetic)),
    ] {
        let s = make_share(Proof::from_json(&value).unwrap(), nonce, 1001);
        assert_eq!(
            ae_stratum::share::classify(Some(&j), &s, &ctx).map(|d| d.validity),
            expected
        );
    }
    assert!(
        Proof::from_json(&json!([]))
            .unwrap()
            .integer_edges()
            .is_none()
    );
}

#[test]
fn byte_retention_limits_preflight_duplicate_replacement_and_release_on_eviction() {
    let nonce = Part::new(Kind::Miner, 0, 4).unwrap();
    let s = make_share([0; 42].into(), nonce, 1001);
    let charge = s.retained_bytes();
    let make = || {
        make_job(
            &"00".repeat(32),
            1,
            100.into(),
            Limits {
                bytes: charge,
                ..Limits::default()
            },
        )
    };
    let mut j = make();
    j.add_error(s.clone()).unwrap();
    assert_eq!(j.retained_bytes(), charge);
    let before = j.clone();
    assert_eq!(j.add_share(s.clone()), Err(Error::Limit("job share bytes")));
    assert_eq!(j, before);
    let mut jobs = Jobs::with_limits(2, 100, charge).unwrap();
    jobs.add(make()).unwrap();
    jobs.add(make()).unwrap();
    let before = jobs.clone();
    assert_eq!(
        jobs.replace(ID, j.clone()),
        Err(Error::Limit("retained share bytes"))
    );
    assert_eq!(jobs, before);
    let ctx = Context {
        extra_nonce: Part::new(Kind::Extra, 0, 4).unwrap(),
        authorized: false,
        maximum_solve_time: &60000.into(),
        edge_bits: 29,
    };
    assert_eq!(
        jobs.submit(ID, s.clone(), &ctx),
        Err(Error::Limit("retained share bytes"))
    );
    assert_eq!(jobs, before);
    let mut one = Jobs::with_limits(1, 100, charge).unwrap();
    one.add(j.clone()).unwrap();
    assert_eq!(one.add(j.clone()).unwrap(), Some(j));
    one.add(make()).unwrap();
    one.submit(ID, s, &ctx).unwrap();
    assert_eq!(one.front().unwrap().retained_bytes(), charge);
    assert!(Jobs::with_limits(1, 1, ae_stratum::job::MAX_RETAINED_SHARE_BYTES + 1).is_err());
}
