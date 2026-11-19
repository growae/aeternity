use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{finalization, payload, *},
    parent_block::ParentBlock,
};
use ae_protocol::{NetworkProfile, contracts::Version};
use ae_state::{
    Trees,
    accounts::Account,
    contracts::{Code, Contract, Nonce},
};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Unsigned};
use num_bigint::{BigInt, Sign};
use serde_json::{Value as J, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock},
};
#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
fn unpack(s: &str) -> J {
    fn visit(v: &J, values: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                visit(&values[v["$ref"].as_u64().unwrap() as usize], values)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), visit(v, values)))
                    .collect(),
            ),
            J::Array(xs) => J::Array(xs.iter().map(|v| visit(v, values)).collect()),
            _ => v.clone(),
        }
    }
    let d: J = serde_json::from_str(s).unwrap();
    if d["format"] == "interned-json-v1" {
        visit(&d["root"], d["values"].as_array().unwrap())
    } else {
        d
    }
}
static DATA: LazyLock<J> = LazyLock::new(|| unpack(include_str!("fixtures/erlang_hc_eoe.json")));
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn num(n: &Unsigned) -> J {
    int(BigInt::from_bytes_be(Sign::Plus, n.as_bytes()))
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn map(v: Vec<(J, J)>) -> J {
    json!(["map", v])
}
fn named(v: Vec<(&str, J)>) -> J {
    map(v.into_iter().map(|(k, v)| (atom(k), v)).collect())
}
fn field<'a>(m: &'a J, key: &str) -> &'a J {
    &m[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .unwrap()[1]
}
fn bytes(b: &J) -> Vec<u8> {
    hex::decode(b[1].as_str().unwrap()).unwrap()
}
fn uint(v: &J) -> Unsigned {
    Unsigned::from_decimal(v[1].as_str().unwrap()).unwrap()
}
fn small(v: &J) -> u64 {
    v[1].as_str().unwrap().parse().unwrap()
}
fn optional<T>(v: Option<T>, f: impl FnOnce(T) -> J) -> J {
    v.map(f).unwrap_or(atom("undefined"))
}
fn normalize(v: &J) -> J {
    if let Some(a) = v.as_array() {
        if a.len() == 2 && a[0] == "map" {
            let mut pairs: Vec<_> = a[1].as_array().unwrap().iter().map(normalize).collect();
            pairs.sort_by_key(|p| p[0].to_string());
            return json!(["map", pairs]);
        }
        J::Array(a.iter().map(normalize).collect())
    } else {
        v.clone()
    }
}
fn key(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}
fn signer(n: u64) -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(n))
}
fn pubkey(n: u64) -> AccountPublicKey {
    signer(n).public_key()
}
fn payload_json(p: &Payload) -> J {
    map(p
        .iter()
        .map(|(k, v)| {
            (
                bin(k),
                match v {
                    Field::Binary(b) => bin(b),
                    Field::Integer(n) => int(n),
                },
            )
        })
        .collect())
}
fn block_json(b: &ParentBlock) -> J {
    tuple(vec![
        atom("block"),
        bin(&b.hash),
        num(&b.height),
        bin(&b.previous_hash),
        num(&b.time),
    ])
}
fn state_json(m: &Machine, stakers: &BTreeSet<AccountPublicKey>, block_time: u64) -> J {
    let client = match m.client() {
        ClientState::Fork { hash } => tuple(vec![atom("data"), optional(hash.as_ref(), bin)]),
        ClientState::Length { length, delta } => tuple(vec![
            atom("data"),
            optional(length.as_ref(), int),
            optional(delta.as_ref(), int),
        ]),
    };
    named(vec![
        (
            "state",
            atom(match m.state() {
                State::AwaitEpoch => "await_eoe",
                State::Proposal => "proposal",
                State::Vote => "vote",
                State::Finalize => "finalize",
                State::Complete => "complete",
            }),
        ),
        ("epoch", optional(m.epoch(), num)),
        ("height", optional(m.height(), num)),
        ("seed", optional(m.seed(), bin)),
        ("leader", optional(m.leader(), |k| bin(k.0))),
        (
            "validators",
            list(
                m.validators()
                    .iter()
                    .map(|(k, v)| tuple(vec![bin(k.0), num(v)]))
                    .collect(),
            ),
        ),
        (
            "remaining",
            map(m
                .remaining()
                .iter()
                .map(|(k, v)| (bin(k.0), num(v)))
                .collect()),
        ),
        ("stakers", list(stakers.iter().map(|k| bin(k.0)).collect())),
        ("majority", int(m.majority())),
        ("proposal", optional(m.proposal(), payload_json)),
        ("block_time", int(block_time)),
        (
            "result",
            optional(m.result(), |r| match r {
                ResultState::Calldata(b) => tuple(vec![atom("ok"), bin(b)]),
                ResultState::NoConsensus => tuple(vec![atom("error"), atom("no_consensus")]),
            }),
        ),
        ("from", optional(m.pending_request(), int)),
        ("nonce_offset", num(m.nonce_offset())),
        (
            "votes",
            map(m
                .votes()
                .iter()
                .map(|(k, v)| (bin(k.0), payload_json(v)))
                .collect()),
        ),
        (
            "parents",
            map(m
                .parents()
                .iter()
                .map(|(e, b)| (num(e), block_json(b)))
                .collect()),
        ),
        (
            "buffered",
            list(m.buffered().iter().rev().map(|s| bin(s.encode())).collect()),
        ),
        ("client", client),
    ])
}
fn trees(nonce: u64) -> Trees {
    let mut trees = Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let mut account = Account::new(
        pubkey(1),
        Unsigned::from_decimal("100000000000000000000").unwrap(),
    );
    account.set_nonce(nonce.into());
    trees.accounts.enter(&account).unwrap();
    let mut contract = Contract::new(
        pubkey(1),
        Nonce::Number(&1.into()),
        Version { vm: 8, abi: 3 },
        Code::Inline(vec![0]),
        0.into(),
    )
    .unwrap();
    contract.public_key = key(101);
    trees
        .contracts
        .insert(&contract, &Default::default())
        .unwrap();
    trees
}
#[derive(Clone)]
struct TestHost {
    keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>,
    published: Vec<Vec<u8>>,
    entropy_heights: Vec<Unsigned>,
    parent_events: Vec<(Unsigned, ParentBlock)>,
    reject: bool,
    chain_error: bool,
    entropy_missing: bool,
}
impl Host for TestHost {
    type Error = String;
    type View = u64;
    type Finalized = J;
    fn is_operational(error: &String) -> bool {
        !error.starts_with("missing")
    }
    fn create_vote(
        &mut self,
        voter: AccountPublicKey,
        epoch: &Unsigned,
        kind: &Unsigned,
        fields: &Payload,
        limits: Limits,
    ) -> Result<SignedTransaction<Transaction>, String> {
        let key = self.keys.get(&voter).ok_or("missing key")?;
        let payload = payload::create_payload(fields, key, limits).map_err(|e| format!("{e:?}"))?;
        payload::create_transaction(
            voter,
            epoch.clone(),
            kind.clone(),
            &payload,
            key,
            &NetworkProfile::mainnet(),
            limits,
        )
        .map_err(|e| format!("{e:?}"))
    }
    fn publish(&mut self, s: SignedTransaction<Transaction>) -> Result<bool, String> {
        self.published.push(s.encode());
        Ok(!self.reject)
    }
    fn finalize(
        &mut self,
        epoch: &Unsigned,
        leader: AccountPublicKey,
        calldata: &[u8],
        nonce: &Unsigned,
        view: Option<&u64>,
    ) -> Result<J, String> {
        if view.is_none() && self.chain_error {
            return Ok(tuple(vec![
                atom("error"),
                tuple(vec![atom("error"), atom("chain_too_short")]),
            ]));
        }
        let height = epoch
            .add(&2.into())
            .checked_sub(&3.into())
            .unwrap_or(0.into())
            .mul_u64(5)
            .add(&11.into());
        self.entropy_heights.push(height.clone());
        if self.entropy_missing {
            return Ok(tuple(vec![atom("error"), atom("not_in_cache")]));
        }
        self.parent_events.push((
            epoch.add(&2.into()),
            ParentBlock {
                hash: key(99).to_vec(),
                height,
                previous_hash: key(98).to_vec(),
                time: 12345.into(),
            },
        ));
        let tx = finalization::transaction(
            &trees(*view.unwrap_or(&20)),
            leader,
            key(101),
            calldata.to_vec(),
            nonce,
        )
        .map_err(|e| format!("{e:?}"))?;
        let key = self.keys.get(&leader).ok_or("missing leader key")?;
        Ok(tuple(vec![
            atom("ok"),
            bin(finalization::sign(tx, key, &NetworkProfile::mainnet()).encode()),
        ]))
    }
}
fn event(input: &J) -> Event<u64> {
    match field(input, "kind")[1].as_str().unwrap() {
        "tx" => Event::Transaction {
            received: field(input, "event") == &atom("tx_received"),
            signed: Box::new(SignedTransaction::decode(&bytes(field(input, "signed"))).unwrap()),
        },
        "negotiate" => Event::Negotiate(Negotiation {
            epoch: uint(field(input, "epoch")),
            height: 50.into(),
            hash: bytes(field(input, "hash")),
            leader: AccountPublicKey(bytes(field(input, "leader")).try_into().unwrap()),
            validators: field(input, "validators")[1]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    (
                        AccountPublicKey(bytes(&p[1][0]).try_into().unwrap()),
                        uint(&p[1][1]),
                    )
                })
                .collect(),
            seed: (field(input, "seed") != &atom("undefined")).then(|| bytes(field(input, "seed"))),
            current_length: uint(field(input, "length")),
        }),
        "parent" => {
            let b = &field(input, "block")[1];
            Event::ParentBlock {
                epoch: uint(field(input, "epoch")),
                block: ParentBlock {
                    hash: bytes(&b[1]),
                    height: uint(&b[2]),
                    previous_hash: bytes(&b[3]),
                    time: uint(&b[4]),
                },
            }
        }
        "get" => Event::GetFinalize {
            request: small(field(input, "request")),
            view: small(field(input, "view")),
            nonce_offset: uint(field(input, "offset")),
        },
        "new_epoch" => Event::NewEpoch,
        "timeout" => Event::Timeout(if field(input, "value") == &atom("no_proposal") {
            Timeout::NoProposal
        } else {
            Timeout::NoQuorum
        }),
        _ => Event::Ignore,
    }
}
fn transition_json(t: Transition<J>) -> J {
    let mut actions = Vec::new();
    if let Some((time, timeout)) = t.timeout {
        actions.push(tuple(vec![
            atom("state_timeout"),
            num(&time),
            atom(match timeout {
                Timeout::NoProposal => "no_proposal",
                Timeout::NoQuorum => "no_quorum",
            }),
        ]));
    }
    for (id, reply) in t.replies {
        actions.push(tuple(vec![
            atom("reply"),
            int(id),
            match reply {
                Reply::NotReady => tuple(vec![atom("error"), atom("not_ready")]),
                Reply::NoConsensus => tuple(vec![atom("error"), atom("no_consensus")]),
                Reply::Finalized(j) => j,
            },
        ]));
    }
    tuple(vec![
        atom(match t.kind {
            TransitionKind::KeepData => "keep_data",
            TransitionKind::Keep => "keep",
            TransitionKind::Next => "next",
        }),
        list(actions),
    ])
}
#[test]
fn original_eoe_state_transitions_publications_and_finalization_match() {
    let order = otp_order::profile(&DATA["profile"]);
    for row in DATA["runs"].as_array().unwrap() {
        let owned: BTreeSet<_> = row["owned"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| pubkey(n.as_u64().unwrap()))
            .collect();
        let get = |name| {
            row["config"][1]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p[0] == atom(name))
                .map(|p| &p[1])
        };
        let block_time = get("block_time").map_or(1000, small);
        let mut machine = Machine::new(
            if row["mode"] == "fork" {
                Mode::Fork
            } else {
                Mode::Length
            },
            owned.clone(),
            block_time.into(),
        );
        // Observers do not own signing keys for publication, but finalization
        // accesses the configured leader key exactly as the reference does.
        // Reference observer finalization has undefined private key: capture it.
        let keys = row["owned"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| {
                let k = signer(n.as_u64().unwrap());
                (k.public_key(), k)
            })
            .collect();
        let mut host = TestHost {
            keys: Arc::new(keys),
            published: vec![],
            entropy_heights: vec![],
            parent_events: vec![],
            reject: get("reject") == Some(&atom("true")),
            chain_error: get("chain_error") == Some(&atom("true")),
            entropy_missing: get("entropy_missing") == Some(&atom("true")),
        };
        assert_eq!(
            normalize(&state_json(&machine, &owned, block_time)),
            normalize(&row["initial"])
        );
        for (i, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            host.published.clear();
            host.entropy_heights.clear();
            host.parent_events.clear();
            let result = machine.step(event(&step["input"]), &mut host, &order, Limits::default());
            if !step["native"].is_null() {
                let reason = match result.unwrap_err() {
                    StepError::Core(Failure::Native(n)) => n,
                    StepError::Host(e) if e.starts_with("missing") => Native::Badarg,
                    e => panic!("unexpected native error {e:?}"),
                };
                assert_eq!(
                    native_json(reason),
                    step["native"],
                    "{} {} step {i}",
                    row["mode"],
                    row["name"]
                );
            } else {
                let result = result
                    .unwrap_or_else(|e| panic!("{} {} step {i}: {e:?}", row["mode"], row["name"]));
                assert_eq!(
                    normalize(&transition_json(result)),
                    normalize(&step["result"]),
                    "{} {} step {i}",
                    row["mode"],
                    row["name"]
                );
            }
            assert_eq!(
                normalize(&state_json(&machine, &owned, block_time)),
                normalize(&step["after_state"]),
                "state {} {} step {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                list(host.published.iter().map(bin).collect()),
                step["published"],
                "published {} {} step {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                list(host.entropy_heights.iter().map(num).collect()),
                step["entropy_heights"],
                "entropy {} {} step {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                list(
                    host.parent_events
                        .iter()
                        .map(|(e, b)| tuple(vec![num(e), block_json(b)]))
                        .collect()
                ),
                step["parent_events"],
                "parents {} {} step {i}",
                row["mode"],
                row["name"]
            );
        }
    }
}
fn native_json(n: Native) -> J {
    tuple(vec![
        atom("error"),
        atom(match n {
            Native::Badarg => "badarg",
            Native::Badarith => "badarith",
            Native::FunctionClause => "function_clause",
        }),
    ])
}

#[path = "service/mod.rs"]
mod service;

#[test]
fn original_epoch_length_rounding_and_overflow_match() {
    let data = unpack(include_str!("fixtures/erlang_hc_eoe_edges.json"));
    for (i, row) in data["delta"].as_array().unwrap().iter().enumerate() {
        let number = |name| Unsigned::from_decimal(row[name].as_str().unwrap()).unwrap();
        let epoch = number("epoch");
        let mut parents = BTreeMap::new();
        for (name, e) in [("first", epoch.clone()), ("second", epoch.add(&1.into()))] {
            if row["missing"] != "both" && row["missing"] != name {
                parents.insert(
                    e,
                    ParentBlock {
                        hash: vec![],
                        height: 1.into(),
                        previous_hash: vec![],
                        time: number(name),
                    },
                );
            }
        }
        let result =
            match calculate_delta(&epoch, &parents, &number("length"), &number("block_time")) {
                Ok(n) => tuple(vec![atom("ok"), int(n)]),
                Err(Failure::Native(n)) => tuple(vec![atom("native"), native_json(n)]),
                Err(e) => panic!("unexpected {e:?}"),
            };
        assert_eq!(result, row["result"], "delta {i} {row}");
    }
}
#[test]
fn original_payload_conversion_and_parent_network_encoding_match() {
    let data = unpack(include_str!("fixtures/erlang_hc_eoe_edges.json"));
    for (i, row) in data["conversion"].as_array().unwrap().iter().enumerate() {
        let mode = if row["mode"] == "fork" {
            Mode::Fork
        } else {
            Mode::Length
        };
        let signed = SignedTransaction::decode(&bytes(&row["signed"])).unwrap();
        let result = match payload::convert_transaction(&signed, mode, Limits::default()) {
            Ok(v) => tuple(vec![
                atom("ok"),
                tuple(vec![
                    atom("ok"),
                    num(&mode.type_number(v.kind)),
                    num(&v.epoch),
                    bin(v.voter.0),
                    payload_json(&v.fields),
                ]),
            ]),
            Err(payload::ConversionError::BadInteger) => {
                tuple(vec![atom("ok"), tuple(vec![atom("error"), atom("badarg")])])
            }
            Err(payload::ConversionError::SignatureVerificationFailed) => tuple(vec![
                atom("ok"),
                tuple(vec![atom("error"), atom("signature_verification_failed")]),
            ]),
            Err(payload::ConversionError::Failure(Failure::Native(n))) => {
                tuple(vec![atom("native"), native_json(n)])
            }
            Err(e) => panic!("unexpected {e:?}"),
        };
        assert_eq!(
            normalize(&result),
            normalize(&row["result"]),
            "conversion {i}"
        );
    }
    for row in data["network"].as_array().unwrap() {
        let b = (row["input"][0] == "binary").then(|| bytes(&row["input"]));
        let result = match ae_hyperchain::parent_block::encode_network_id(b.as_deref()) {
            Ok(encoded) => tuple(vec![atom("ok"), bin(encoded)]),
            Err(b) => tuple(vec![
                atom("native"),
                tuple(vec![atom("error"), tuple(vec![atom("badmatch"), bin(b)])]),
            ]),
        };
        assert_eq!(result, row["result"]);
    }
}
