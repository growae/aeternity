use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    pinning::{self, Event, Host, State},
    signing::Keys,
};
use ae_types::{AccountPublicKey, Unsigned};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
struct NativeHost {
    leader: AccountPublicKey,
    mapped: bool,
    parent_ok: bool,
    leader_error: bool,
    included: bool,
    nonce: Option<Unsigned>,
    price: Unsigned,
    keys: Keys,
    mode: String,
    pushes: usize,
    transactions: Vec<String>,
    posts: Vec<J>,
    queries: usize,
}
impl NativeHost {
    fn push(&mut self, proof: bool, hash: &[u8]) -> Result<(), ()> {
        let input = pinning::TransactionInput {
            leader: self.leader,
            nonce: self.nonce.clone().unwrap_or_else(|| 1.into()),
            minimum_gas_price: self.price.clone(),
            parent_hash: hash,
            network: b"hc_agent",
            keys: &self.keys,
            limits: Default::default(),
        };
        let mut contract = [0; 32];
        contract[31] = 9;
        let tx = if proof {
            pinning::proof(contract, input)
        } else {
            pinning::note(input)
        }
        .map_err(|_| ())?;
        self.transactions.push(hex::encode(tx.encode()));
        self.pushes += 1;
        if (self.mode == "note_throw" && self.pushes == 1)
            || (self.mode == "proof_throw" && self.pushes == 2)
        {
            return Err(());
        }
        if !((self.mode == "note_rejected" && self.pushes == 1)
            || (self.mode == "proof_rejected" && self.pushes == 2))
            && let Some(n) = &mut self.nonce
        {
            *n = n.add(&1.into());
        }
        Ok(())
    }
}
impl Host for NativeHost {
    type Error = ();
    fn operational(_: &()) -> bool {
        false
    }
    fn leader(&mut self, _: &BigInt) -> Result<AccountPublicKey, ()> {
        if self.leader_error {
            Err(())
        } else {
            Ok(self.leader)
        }
    }
    fn mapped(&self, _: AccountPublicKey) -> bool {
        self.mapped
    }
    fn post(&mut self, leader: AccountPublicKey) -> Result<Option<Vec<u8>>, ()> {
        let fee = num_bigint::BigUint::from_bytes_be(self.price.mul_u64(1_000_000).as_bytes())
            .to_string();
        self.posts
            .push(json!({"who":hex::encode(leader.0),"amount":"1","fee":fee}));
        Ok(self.parent_ok.then(|| b"th_parent".to_vec()))
    }
    fn included(&mut self, _: Option<&[u8]>) -> Result<bool, ()> {
        self.queries += 1;
        Ok(self.included)
    }
    fn note(&mut self, _: AccountPublicKey, h: &[u8]) -> Result<(), ()> {
        self.push(false, h)
    }
    fn proof(&mut self, _: AccountPublicKey, h: &[u8]) -> Result<(), ()> {
        self.push(true, h)
    }
}
fn number(v: &J) -> BigInt {
    v.as_str().unwrap().parse().unwrap()
}
fn state(s: &State) -> J {
    match &s.active {
        None => {
            json!({"enabled":s.enabled,"next_last":null,"parent_hash":null,"child_note":null,"leader":null})
        }
        Some(a) => {
            json!({"enabled":s.enabled,"next_last":a.next_last.to_string(),"parent_hash":a.parent_hash.as_ref().map(hex::encode),"child_note":a.child_note,"leader":hex::encode(a.leader.0)})
        }
    }
}
#[test]
fn original_agent_lifecycle_fees_and_signed_child_transactions() {
    let data: J = serde_json::from_str(include_str!("fixtures/erlang_pinning_agent.json")).unwrap();
    assert_eq!(data["rows"].as_array().unwrap().len(), 34);
    assert_eq!(data["disabled_ignored"], true);
    for case in data["rows"].as_array().unwrap() {
        let mut seed = [0; 32];
        seed[31] = 7;
        let key = Ed25519SecretKey::from_seed(&seed);
        assert_eq!(hex::encode(key.public_key().0), data["account"]);
        let protocol =
            ae_protocol::Protocol::try_from(case["protocol"].as_u64().unwrap() as u32).unwrap();
        let mut host = NativeHost {
            leader: key.public_key(),
            mapped: true,
            parent_ok: case["parent_ok"].as_bool().unwrap(),
            leader_error: case["leader_error"].as_bool().unwrap(),
            included: false,
            nonce: None,
            price: Unsigned::from_decimal(case["miner"].as_str().unwrap())
                .unwrap()
                .max(protocol.minimum_gas_price().into()),
            keys: if case["signing"] == true {
                Keys::new([key])
            } else {
                Keys::default()
            },
            mode: case["push_mode"].as_str().unwrap().to_owned(),
            pushes: 0,
            transactions: vec![],
            posts: vec![],
            queries: 0,
        };
        let mut actual = State::default();
        assert_eq!(state(&actual), case["initial"]);
        for row in case["rows"].as_array().unwrap() {
            host.mapped = row["mapped"].as_bool().unwrap();
            host.nonce = row["nonce"]
                .as_str()
                .map(|s| Unsigned::from_decimal(s).unwrap());
            host.included = row["inclusions"]
                .as_array()
                .unwrap()
                .first()
                .is_some_and(|v| !v.is_null() && number(v) != (-1).into());
            host.transactions.clear();
            host.posts.clear();
            host.queries = 0;
            let v = &row["event"];
            let event = if v["kind"] == "epoch" {
                Event::Epoch {
                    epoch: number(&v["epoch"]),
                    first: number(&v["first"]),
                    last: number(&v["last"]),
                }
            } else {
                Event::Top {
                    height: number(&v["height"]),
                }
            };
            assert_eq!(
                actual.apply(&event, &mut host).is_err(),
                row["failed"].as_bool().unwrap(),
                "{} {event:?}",
                case["name"]
            );
            assert_eq!(state(&actual), row["state"], "{} {event:?}", case["name"]);
            assert_eq!(
                json!(host.transactions),
                row["pushed"],
                "{} {event:?}",
                case["name"]
            );
            assert_eq!(json!(host.posts), row["posts"]);
            assert_eq!(host.queries, row["queries"].as_u64().unwrap() as usize);
            assert!(
                row["gas_heights"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|v| *v == 1)
            );
        }
    }
}
