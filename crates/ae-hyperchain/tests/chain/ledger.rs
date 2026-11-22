use super::*;
use ae_chain::{
    block::{Block, DecodeConfig},
    hyperchain as hc, rewards,
};
use ae_codec::headers::{Header, HeaderMode};
use ae_execution::chain_history::{HeaderLink, Snapshot};
use ae_hyperchain::block as cb;
use ae_types::{Hash256, Unsigned};
use num_traits::ToPrimitive;
use std::collections::BTreeMap;
#[path = "ledger/failures.rs"]
mod failures;
#[path = "ledger/storage.rs"]
mod storage;
static LEDGER: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_hc_ledger.json")).unwrap()
});
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Host {
    reads: Vec<BigInt>,
    unavailable: bool,
}
#[derive(Debug)]
struct Unavailable;
impl cb::Host for Host {
    type Error = Unavailable;
    fn is_operational(_: &Self::Error) -> bool {
        true
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        self.reads.push(epoch.clone());
        if self.unavailable {
            return Err(Unavailable);
        }
        let h: BigInt = (epoch - BigInt::from(3)).max(0.into()) * 5 + 1;
        Ok(Some(key(1000 + h.to_u64().unwrap()).to_vec()))
    }
    fn pin(&mut self, _: &[u8]) -> Result<Option<cb::Pin>, Self::Error> {
        Ok(None)
    }
    fn key_hash(&mut self, _: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }
}
fn rules(genesis: Hash256) -> hc::Rules {
    let mut r = hc::Rules::new(
        genesis,
        cb::Config {
            parent_epoch_length: 5.into(),
            parent_start: 1.into(),
            parent_finality: 2.into(),
            pin_sync_margin: 1.into(),
            child_block_time: 4.into(),
            cache_limits: Default::default(),
        },
    );
    r.now_ms = 1_000_000;
    r.reward_delay = 2;
    r.fee_distribution = [10.into(), 70.into(), 20.into()];
    r
}
fn history(
    keys: &rewards::History,
    candidate: Option<&ae_codec::headers::KeyHeader>,
) -> Arc<Snapshot> {
    let mut headers: BTreeMap<_, _> = keys
        .iter()
        .map(|(h, r)| {
            (
                h.0,
                HeaderLink {
                    height: r.header.common.height,
                    previous_key: r.header.common.previous_key_hash,
                },
            )
        })
        .collect();
    if let Some(k) = candidate {
        headers.insert(
            Header::Key(k.clone()).hash().unwrap().0,
            HeaderLink {
                height: k.common.height,
                previous_key: k.common.previous_key_hash,
            },
        );
    }
    Arc::new(Snapshot {
        selected: keys
            .iter()
            .map(|(h, r)| (r.header.common.height, h.0))
            .collect(),
        headers,
    })
}
fn context(
    h: &Header,
    k: &ae_codec::headers::KeyHeader,
    t: &Trees,
    keys: &rewards::History,
) -> chain::View {
    let mut v = view(t.clone(), u64::from(k.common.version), k.common.height);
    v.environment.origin = [0; 32];
    v.environment.network_id = b"ae_hc_reference".to_vec();
    v.environment.key_hash = Header::Key(k.clone()).hash().unwrap().0;
    v.environment.beneficiary = k.beneficiary;
    v.environment.difficulty = k.target.into();
    v.environment.timestamp = match h {
        Header::Key(k) => BigInt::from(k.time) + 1,
        Header::Micro(m) => m.time.into(),
    };
    v.history = history(keys, None);
    v
}
fn number(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(num_bigint::Sign::Plus, n.as_bytes())
}
fn tx_event(e: &transactions::batch::Event) -> J {
    assert_eq!(e.transaction_type, ae_tx::TransactionKind::Spend);
    let (kind, fields, info) = match &e.value {
        transactions::Event::Primitive(ae_execution::Event::Spend { from, to, amount }) => (
            "spend",
            vec![bin(from.0), bin(to.0), int(number(amount))],
            b"Spend.amount".as_slice(),
        ),
        transactions::Event::Primitive(ae_execution::Event::FeeDebit { account, amount }) => (
            "delta",
            vec![bin(account.0), int(-number(amount))],
            b"Chain.fee".as_slice(),
        ),
        other => panic!("unexpected fixture event {other:?}"),
    };
    tuple(vec![
        tuple(vec![atom(kind), tuple(fields)]),
        named(vec![
            ("info", bin(info)),
            ("type", atom("spend_tx")),
            ("tx_hash", bin(e.transaction_hash.0)),
        ]),
    ])
}
type R = Reader<MemoryStore, MemoryStore, Otp26>;
struct Step<'a> {
    block: &'a Block,
    parent: &'a hc::Parent<'a>,
    history: &'a rewards::History,
    reader: &'a R,
    state: &'a cb::State,
    host: &'a Host,
    rules: &'a hc::Rules,
    expected: &'a J,
}
fn walk(profile: &str, row: &J, mut before: impl FnMut(Step<'_>)) {
    let old = genesis::DATA["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == row["protocol"])
        .unwrap();
    let mut reader = genesis::reader(old);
    Arc::make_mut(&mut reader.profile).order = otp_order::profile(&LEDGER[profile]);
    Arc::make_mut(&mut reader.profile).network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        row["activations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| ae_protocol::Activation {
                height: a["height"].as_u64().unwrap(),
                protocol: Protocol::try_from(a["protocol"].as_u64().unwrap() as u32).unwrap(),
            })
            .collect(),
    )
    .unwrap();
    let Header::Key(g) =
        Header::decode(&bytes(&row["genesis"]["header"]), HeaderMode::Hyperchain).unwrap()
    else {
        panic!()
    };
    let gh = Header::Key(g.clone()).hash().unwrap();
    let mut rules = rules(gh);
    rules.fixed_coinbase = Unsigned::from_field_bytes(
        &row["fixed_coinbase"]
            .as_str()
            .unwrap()
            .parse::<BigInt>()
            .unwrap()
            .to_bytes_be()
            .1,
    )
    .unwrap();
    let mut t = trees(&row["genesis"]["trees"]);
    let mut prev = Header::Key(g.clone());
    let mut leader = g.clone();
    let mut fees = Unsigned::from(0u64);
    let mut keys = BTreeMap::from([(
        gh,
        rewards::Record {
            header: g,
            fees: Some(fees.clone()),
            reported_fraud: Some(false),
        },
    )]);
    let mut selected = chain::Selection::default();
    let mut state = cb::State::default();
    let mut host = Host::default();
    for expected in row["rows"].as_array().unwrap() {
        let b = Block::decode(
            &bytes(&expected["block"]),
            DecodeConfig {
                mode: HeaderMode::Hyperchain,
                ..Default::default()
            },
        )
        .unwrap();
        let pv = context(&prev, &leader, &t, &keys);
        let ph = prev.hash().unwrap();
        selected.top = Some(prev.common().height.into());
        if matches!(prev, Header::Key(_)) {
            selected
                .heights
                .insert(prev.common().height.into(), ph.0.to_vec());
        }
        selected.views.insert(ph.0.to_vec(), Arc::new(pv.clone()));
        reader.selected.replace(selected.clone());
        let parent = hc::Parent {
            header: &prev,
            key: &leader,
            view: &pv,
            fees: &fees,
            execution_history: history(
                &keys,
                match &b {
                    Block::Key(k) => Some(k),
                    _ => None,
                },
            ),
        };
        before(Step {
            block: &b,
            parent: &parent,
            history: &keys,
            reader: &reader,
            state: &state,
            host: &host,
            rules: &rules,
            expected,
        });
        let before = t.roots();
        let out = hc::execute(&b, &parent, &keys, &reader, &mut state, &mut host, &rules)
            .unwrap_or_else(|e| {
                panic!(
                    "protocol {} {} height {}: {e:?}",
                    row["protocol"], expected["kind"], expected["height"]
                )
            });
        assert_eq!(before, t.roots());
        check(&out.trees, &expected["trees"]);
        assert_eq!(number(&out.fees).to_string(), expected["fees"]);
        assert_eq!(number(&out.score).to_string(), expected["score"]);
        let events = if matches!(b, Block::Key(_)) {
            assert!(out.transactions.is_empty());
            block::events(out.consensus)
        } else {
            assert!(out.consensus.is_empty());
            list(out.transactions.iter().map(tx_event).collect())
        };
        assert_eq!(normalize(&events), normalize(&expected["events"]));
        assert_eq!(
            normalize(&block::cache(&state)),
            normalize(&expected["cache"])
        );
        assert_eq!(out.wait_ms, None);
        let hash = b.hash().unwrap();
        assert_eq!(hex::encode(hash.0), expected["hash"]);
        fees = out.fees;
        t = out.trees;
        prev = b.header();
        if let Block::Key(k) = b {
            keys.insert(
                hash,
                rewards::Record {
                    header: k.clone(),
                    fees: Some(fees.clone()),
                    reported_fraud: Some(false),
                },
            );
            leader = k;
        }
    }
}

#[test]
fn original_signed_chain_matches_full_hc_execution_roots_fees_scores_events_and_caches() {
    for profile in ["profile", "repeat_profile"] {
        for row in LEDGER["rows"].as_array().unwrap() {
            walk(profile, row, |_| {});
        }
    }
}
