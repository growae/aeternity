use super::*;
use ae_protocol::{Activation, ProfileError, Protocol};
use serde_json::Value as J;
use std::sync::{Arc, LazyLock};

static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_signaling.json")).unwrap()
});
#[derive(Clone, Default, Debug)]
struct Memory(Arc<BTreeMap<Vec<u8>, Vec<u8>>>);
impl NodeReader for Memory {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        Ok(self.0.get(&key(NODES, h.0)).cloned())
    }
}
impl Read for Memory {
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut Visitor<'_>,
    ) -> Result<(), Error> {
        for (i, (k, v)) in self.0.range(start.to_vec()..end.to_vec()).enumerate() {
            if i >= limit {
                return Err(Error::ResourceLimit);
            }
            visit(k, v)?;
        }
        Ok(())
    }
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Entries, Error> {
        let out: Vec<_> = self
            .0
            .range(start.to_vec()..end.to_vec())
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if out.len() > limit {
            Err(Error::ResourceLimit)
        } else {
            Ok(out)
        }
    }
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.0.get(k).cloned())
    }
    fn prefix(&self, p: &[u8], limit: usize) -> Result<Entries, Error> {
        let out: Vec<_> = self
            .0
            .iter()
            .filter(|(k, _)| k.starts_with(p))
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if out.len() > limit {
            Err(Error::ResourceLimit)
        } else {
            Ok(out)
        }
    }
}
impl Memory {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        Arc::make_mut(&mut self.0).insert(k, v);
    }
    fn apply(&mut self, writes: Writes) {
        for (k, v) in writes {
            if let Some(v) = v {
                self.put(k, v);
            } else {
                Arc::make_mut(&mut self.0).remove(&k);
            }
        }
    }
    fn block(&mut self, b: &Block) {
        self.put(key(BLOCKS, b.hash().unwrap().0), b.encode().unwrap());
    }
    fn select(&mut self, blocks: &[Block]) {
        Arc::make_mut(&mut self.0).retain(|k, _| k[0] != KEYS);
        for b in blocks {
            self.put(
                key(KEYS, b.header().common().height.to_be_bytes()),
                b.hash().unwrap().0.to_vec(),
            );
        }
    }
    fn view(&self) -> View<Self> {
        View::load(self.clone()).unwrap()
    }
}
fn fork(v: &J) -> Fork {
    Fork {
        start: v["start"].as_u64().unwrap(),
        end: v["end"].as_u64().unwrap(),
        threshold: v["threshold"].as_u64().unwrap(),
        info: v["info"].as_u64().unwrap() as u32,
        version: v["version"].as_u64().unwrap() as u32,
    }
}
fn profile(base: u32, fork: Fork) -> NetworkProfile {
    NetworkProfile::custom(
        b"ae_signaling".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(base).unwrap(),
        }],
    )
    .unwrap()
    .with_community_fork(Some(fork))
    .unwrap()
}
fn block(v: &J) -> Block {
    Block::decode(
        &hex::decode(v.as_str().unwrap()).unwrap(),
        crate::block::DecodeConfig::default(),
    )
    .unwrap()
}
fn setup(row: &J) -> (Memory, NetworkProfile, Vec<Block>) {
    let p = profile(row["base"].as_u64().unwrap() as u32, fork(&row["fork"]));
    let blocks = vec![block(&row["genesis"]), block(&row["first"])];
    let mut m = Memory::default();
    m.put(
        IDENTITY.to_vec(),
        Identity::new(&p, blocks[0].hash().unwrap(), Hash256::ZERO).encode(),
    );
    m.put(STATUS.to_vec(), Status::default().encode());
    for b in &blocks {
        m.block(b);
    }
    (m, p, blocks)
}
#[test]
fn original_signaling_configuration_and_selected_branch_decisions_match() {
    let mut decisions = 0;
    for row in DATA["scenarios"].as_array().unwrap() {
        let (mut db, p, first) = setup(row);
        let f = p.community_fork().unwrap();
        let mut branches = vec![];
        for rows in row["branches"].as_array().unwrap() {
            let mut branch = first.clone();
            for r in rows.as_array().unwrap() {
                let b = block(&r["block"]);
                db.block(&b);
                if r["insert_count"].as_bool().unwrap() {
                    let Block::Key(k) = &b else { panic!() };
                    let mut writes = Writes::new();
                    db.view().insert_signal(k, f, 10, &mut writes).unwrap();
                    db.apply(writes);
                }
                assert_eq!(
                    db.view().signal_count(b.hash().unwrap()).unwrap(),
                    r["count"].as_u64()
                );
                branch.push(b);
            }
            branches.push(branch);
        }
        let last = [
            branches[0][5].hash().unwrap(),
            branches[1][5].hash().unwrap(),
        ];
        let before_queries = db.view();
        for q in row["queries"].as_array().unwrap() {
            let selected = match q["selection"].as_str().unwrap() {
                "a" | "a_again" => branches[0].as_slice(),
                "b" => branches[1].as_slice(),
                "missing" => &[],
                _ => panic!(),
            };
            db.select(selected);
            let height = q["height"].as_u64().unwrap();
            let view = db.view();
            let readonly = view.protocol_profile(&p, height, 10).unwrap();
            assert_eq!(
                readonly.version_at(height).unwrap(),
                q["version"].as_u64().unwrap() as u32
            );
            let (resolved, writes) = view.resolve_protocol(&p, height, 10).unwrap();
            assert_eq!(readonly, resolved);
            assert!(view.identity().matches(&resolved));
            db.apply(writes);
            assert_eq!(
                db.view().signal_count(last[0]).unwrap(),
                q["a_count"].as_u64()
            );
            assert_eq!(
                db.view().signal_count(last[1]).unwrap(),
                q["b_count"].as_u64()
            );
            if height >= f.end {
                assert_eq!(
                    p.version_at(height),
                    Err(ProfileError::MissingForkDecision { height })
                );
                assert_eq!(
                    resolved.version_at(height + 1),
                    Err(ProfileError::MissingForkDecision { height: height + 1 })
                );
                if q["version"] == 9 {
                    assert_eq!(
                        resolved.protocol_at(height),
                        Err(ProfileError::UnknownProtocol(9))
                    );
                }
            }
            decisions += 1;
        }
        assert!(before_queries.canonical_key(5).unwrap().is_none());
        for h in row["not_last"].as_array().unwrap() {
            assert_eq!(
                db.view().fork_result(
                    &p,
                    branches[0][h.as_u64().unwrap() as usize].hash().unwrap(),
                    10
                ),
                Err(Error::NotLastSignalingBlock)
            );
        }
        for r in row["info"].as_array().unwrap() {
            assert_eq!(
                f.info_at(r["height"].as_u64().unwrap()).map(u64::from),
                r["value"].as_u64()
            );
        }
    }
    assert_eq!(decisions, 4560);
    for r in DATA["configurations"].as_array().unwrap() {
        let f = fork(&r["fork"]);
        assert_eq!(
            f.validate(Activation {
                height: r["previous_height"].as_u64().unwrap(),
                protocol: Protocol::try_from(r["previous"].as_u64().unwrap() as u32).unwrap()
            })
            .is_ok(),
            r["valid"].as_bool().unwrap(),
            "{r}"
        );
    }
    assert_eq!(DATA["configurations"].as_array().unwrap().len(), 5600);
    for r in DATA["selection_configurations"].as_array().unwrap() {
        if r["network"] == "ae_signaling" {
            assert_eq!(!r["result"].is_null(), r["enabled"] == true);
        } else {
            assert!(r["result"].is_null());
        }
    }
}
#[test]
fn signaling_missing_history_corruption_bounds_and_configuration_are_explicit() {
    let row = &DATA["scenarios"][0];
    let (mut db, p, mut chain) = setup(row);
    let f = p.community_fork().unwrap();
    for r in row["branches"][0].as_array().unwrap() {
        let b = block(&r["block"]);
        db.block(&b);
        chain.push(b);
    }
    db.select(&chain);
    let last = chain[5].hash().unwrap();
    let original = db.0.clone();
    assert_eq!(
        db.view().fork_result(&p, last, 3),
        Err(Error::ResourceLimit)
    );
    assert_eq!(db.view().fork_result(&p, last, 4), Ok(false));
    assert_eq!(db.0, original); // public read never warms the immutable snapshot
    assert!(matches!(
        db.view().resolve_protocol(&p, 6, 3),
        Err(Error::ResourceLimit)
    ));
    let pinned = db.view();
    let (resolved, writes) = pinned.resolve_protocol(&p, 6, 4).unwrap();
    db.apply(writes);
    assert_eq!(resolved.protocol_at(6), Ok(Protocol::Minerva));
    assert_eq!(pinned.signal_count(last).unwrap(), None);
    assert_eq!(db.view().signal_count(last).unwrap(), Some(0));
    assert_eq!(db.view().fork_result(&p, last, 0), Ok(false));
    for bad in [
        vec![],
        vec![0xc0],
        encoded_count(5),
        list(vec![number(2), number(0)]).encode(),
        [encoded_count(0), vec![0]].concat(),
    ] {
        let mut corrupted = db.clone();
        corrupted.put(key(SIGNALS, last.0), bad);
        assert!(matches!(
            corrupted.view().fork_result(&p, last, 10),
            Err(Error::Corrupt(_))
        ));
    }
    let mut missing = Memory(original.clone());
    Arc::make_mut(&mut missing.0).remove(&key(BLOCKS, chain[4].hash().unwrap().0));
    assert_eq!(
        missing.view().fork_result(&p, last, 10),
        Err(Error::Missing(chain[4].hash().unwrap()))
    );
    let mut wrong_index = db.clone();
    wrong_index.put(
        key(KEYS, 5u64.to_be_bytes()),
        chain[4].hash().unwrap().0.to_vec(),
    );
    assert!(matches!(
        wrong_index.view().resolve_protocol(&p, 6, 10),
        Err(Error::Corrupt(_))
    ));
    let mut bad_parent = db.clone();
    bad_parent.put(key(SIGNALS, chain[4].hash().unwrap().0), encoded_count(4));
    let Block::Key(k) = &chain[5] else { panic!() };
    let mut writes = Writes::new();
    assert!(matches!(
        bad_parent.view().insert_signal(k, f, 10, &mut writes),
        Err(Error::Corrupt(_))
    ));
    assert!(writes.is_empty());
    let mut short = k.clone();
    short.common.previous_key_hash = chain[3].hash().unwrap().0;
    assert!(matches!(
        Memory(original)
            .view()
            .insert_signal(&short, f, 10, &mut writes),
        Err(Error::Corrupt(_))
    ));
    assert!(writes.is_empty());
    let changed = p
        .clone()
        .with_community_fork(Some(Fork { info: 8, ..f }))
        .unwrap();
    assert_eq!(
        db.view().fork_result(&changed, last, 10),
        Err(Error::Identity)
    );
    assert_eq!(
        NetworkProfile::mainnet().with_community_fork(Some(f)),
        Err(ProfileError::ReservedNetworkId)
    );
    assert_eq!(
        NetworkProfile::uat().with_community_fork(Some(f)),
        Err(ProfileError::ReservedNetworkId)
    );
    assert_eq!(
        p.resolve(6, Prior::Version(3)),
        Err(ProfileError::WrongForkDecision)
    );
    assert_eq!(
        p.resolve(7, Prior::SignalingResult(true)),
        Err(ProfileError::WrongForkDecision)
    );
    assert!(ProfileError::MissingForkDecision { height: 6 }.is_operational());
}
