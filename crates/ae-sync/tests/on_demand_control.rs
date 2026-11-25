use ae_sync::chain::on_demand::control::{self, Command, Error, Host, Kind, Reply};
use ae_types::Hash256;
use serde_json::{Value as J, json};
use std::collections::VecDeque;
fn tuple(xs: Vec<J>) -> J {
    json!(["tuple", xs])
}
fn binary(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode_upper(b)])
}
struct Script {
    top: J,
    leader: bool,
    seq: u64,
    step: u64,
    fail: u64,
    pool: VecDeque<u64>,
    batches: VecDeque<Vec<u8>>,
    trace: Vec<J>,
    committed: Vec<J>,
}
impl Script {
    fn new(c: &J) -> Self {
        Self {
            top: json!({"kind":"initial","id":0,"txs":[]}),
            leader: c["leader"].as_bool().unwrap(),
            seq: 0,
            step: 0,
            fail: c["fail_at"].as_u64().unwrap(),
            pool: c["pool"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_u64().unwrap())
                .collect(),
            batches: c
                .get("batches_override")
                .unwrap_or(&c["batches"])
                .as_array()
                .unwrap()
                .iter()
                .map(|a| {
                    a.as_array()
                        .unwrap()
                        .iter()
                        .map(|n| n.as_u64().unwrap() as u8)
                        .collect()
                })
                .collect(),
            trace: vec![],
            committed: vec![],
        }
    }
    fn event(&mut self, j: J) -> Result<(), ()> {
        self.trace.push(j);
        self.step += 1;
        if self.step == self.fail {
            Err(())
        } else {
            Ok(())
        }
    }
    fn top_hash(&mut self) -> Result<J, ()> {
        self.event(json!("top_hash"))?;
        Ok(tuple(vec![json!("hash"), self.top["id"].clone()]))
    }
    fn commit(&mut self, kind: &str, b: J) -> Result<J, ()> {
        self.event(tuple(vec![json!(kind), b["id"].clone()]))?;
        self.top = b.clone();
        self.committed.push(b.clone());
        if b["kind"] == "key" {
            self.leader = true;
        }
        Ok(b)
    }
}
impl Host for Script {
    type Block = J;
    type Error = ();
    fn is_leader(&mut self) -> Result<bool, ()> {
        self.event(json!("is_leader"))?;
        Ok(self.leader)
    }
    fn key(&mut self) -> Result<J, ()> {
        let parent = self.top_hash()?;
        self.event(json!("beneficiary"))?;
        self.event(json!("candidate_key"))?;
        self.event(tuple(vec![
            json!("create_key"),
            parent.clone(),
            binary(b"beneficiary"),
            binary(b"miner"),
        ]))?;
        self.seq += 1;
        self.commit(
            "add_synced",
            json!({"kind":"key","id":self.seq,"parent":parent,"txs":[]}),
        )
    }
    fn micro(&mut self) -> Result<J, ()> {
        let parent = self.top_hash()?;
        self.event(tuple(vec![
            json!("create_micro"),
            parent.clone(),
            json!("infinity"),
        ]))?;
        self.seq += 1;
        let txs: Vec<_> = self
            .batches
            .pop_front()
            .unwrap_or_default()
            .into_iter()
            .map(|n| binary([n; 32]))
            .collect();
        self.event(tuple(vec![json!("sign_micro"), json!(self.seq)]))?;
        self.commit(
            "post",
            json!({"kind":"micro","id":self.seq,"parent":parent,"txs":txs,"signed":true}),
        )
    }
    fn pool_empty(&mut self) -> Result<bool, ()> {
        self.event(tuple(vec![json!("peek"), json!("infinity")]))?;
        let n = if self.pool.len() > 1 {
            self.pool.pop_front().unwrap()
        } else {
            self.pool.front().copied().unwrap_or(0)
        };
        Ok(n == 0)
    }
    fn top(&mut self) -> Result<J, ()> {
        self.event(json!("top_block"))?;
        Ok(self.top.clone())
    }
    fn transactions(&self, b: &J) -> Vec<Hash256> {
        b["txs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                Hash256(
                    hex::decode(t[1].as_str().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                )
            })
            .collect()
    }
}
fn command(c: &J) -> Command {
    match c["command"].as_str().unwrap() {
        "emit_kb" => Command::Key,
        "emit_mb" => Command::Micro,
        "empty" => Command::EmptyPool,
        "mine_blocks" => Command::Blocks {
            count: c["count"].as_i64().unwrap(),
            kind: match c["type"].as_str().unwrap() {
                "key" => Kind::Key,
                "micro" => Kind::Micro,
                "any" => Kind::Any,
                s => Kind::Unknown(s.as_bytes().to_vec()),
            },
        },
        "until" => Command::Until {
            maximum: c["maximum"].as_u64().unwrap(),
            hashes: c["targets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| {
                    let h = [n.as_u64().unwrap() as u8; 32];
                    if c["encoded"] == true {
                        ae_codec::api::encode(ae_codec::api::ApiType::TxHash, &h).into_bytes()
                    } else {
                        h.to_vec()
                    }
                })
                .collect(),
        },
        _ => panic!(),
    }
}
fn result(r: Result<Reply<J>, Error<()>>) -> J {
    match r {
        Ok(v) => {
            json!({"ok":true,"value":match v{Reply::Block(b)=>b,Reply::Blocks(b)=>tuple(vec![json!("ok"),json!(b)]),Reply::MaxReached=>tuple(vec![json!("error"),json!("max_reached")])}})
        }
        Err(e) => {
            json!({"ok":false,"class":"error","reason":match e{Error::Host(())=>json!("injected_failure"),Error::FunctionClause=>json!("function_clause"),Error::CouldNotMineTransaction=>json!("could_not_mine_tx"),Error::UnknownKind{leader,kind}=>tuple(vec![json!("case_clause"),tuple(vec![json!(leader),json!(String::from_utf8(kind).unwrap())])]),Error::ResourceLimit=>panic!("fixture exceeded budget")}})
        }
    }
}
#[test]
fn original_command_order_results_and_partial_failures_match() {
    let rows: J =
        serde_json::from_str(include_str!("fixtures/erlang_on_demand_commands.json")).unwrap();
    for row in rows.as_array().unwrap() {
        let c = &row["input"];
        let mut script = Script::new(c);
        let r = control::run(command(c), &mut script, Default::default());
        assert_eq!(json!(script.trace), row["trace"], "trace {}", c["name"]);
        assert_eq!(
            json!(script.committed),
            row["committed"],
            "committed {}",
            c["name"]
        );
        assert_eq!(
            r.produced, script.committed,
            "partial receipt {}",
            c["name"]
        );
        assert_eq!(script.top, row["top"]);
        assert_eq!(result(r.result), row["result"], "result {}", c["name"]);
    }
    assert_eq!(rows.as_array().unwrap().len(), 141);
    println!(
        "On-demand commands: 141 original control-flow traces, native results and partial effects"
    );
}
#[test]
fn resource_exhaustion_retains_prior_effects_and_empty_pool_does_not_emit() {
    let c = json!({"leader":true,"pool":[0],"batches":[],"fail_at":0});
    let mut s = Script::new(&c);
    let r = control::run(
        Command::Blocks {
            count: 4,
            kind: Kind::Any,
        },
        &mut s,
        control::Limits {
            blocks: 2,
            ..Default::default()
        },
    );
    assert!(matches!(r.result, Err(Error::ResourceLimit)));
    assert_eq!(r.produced, s.committed);
    assert_eq!(r.produced.len(), 2);
    let mut s = Script::new(&c);
    let r = control::run(
        Command::EmptyPool,
        &mut s,
        control::Limits {
            blocks: 0,
            ..Default::default()
        },
    );
    assert!(matches!(r.result, Ok(Reply::Blocks(_))));
    assert!(r.produced.is_empty());
    for (hashes, limits) in [
        (
            vec![vec![0; 32]; 2],
            control::Limits {
                hashes: 1,
                ..Default::default()
            },
        ),
        (
            vec![vec![0; 33]],
            control::Limits {
                hash_bytes: 32,
                ..Default::default()
            },
        ),
    ] {
        let mut s = Script::new(&c);
        let r = control::run(
            Command::Until {
                hashes,
                maximum: 10,
            },
            &mut s,
            limits,
        );
        assert!(matches!(r.result, Err(Error::ResourceLimit)));
        assert!(r.produced.is_empty());
        assert!(s.trace.is_empty());
    }
}
