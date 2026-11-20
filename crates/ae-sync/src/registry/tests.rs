use super::*;
use ae_chain::repository::{Error as RepositoryError, Identity};
use ae_protocol::NetworkProfile;
use ae_state::genesis::{self, Network};
use ae_types::Hash256;
use std::{net::Ipv4Addr, path::PathBuf};
mod native;
mod recovery;

pub(super) struct Temp(PathBuf);
impl Temp {
    pub(super) fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-durable-registry-{}-{}",
            std::process::id(),
            NEXT_OWNER.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::mainnet(),
        genesis::header(Network::Mainnet).unwrap().hash().unwrap(),
        Hash256::ZERO,
    )
}
pub(super) fn db(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn key(n: u64) -> PeerPublicKey {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    PeerPublicKey(bytes)
}
pub(super) fn peer(n: u64, trusted: bool) -> discovery::Peer {
    discovery::Peer {
        id: key(n),
        address: Ipv4Addr::new(10, n as u8, 1, 1),
        source: Ipv4Addr::new(20, n as u8, 2, 2),
        host: format!("peer{n}").into_bytes(),
        port: 3000 + n as u16,
        trusted,
    }
}
pub(super) fn config() -> Config {
    Config {
        policy: policy::Config {
            pool: discovery::Config {
                verified_buckets: 1,
                verified_size: 16,
                unverified_buckets: 1,
                unverified_size: 16,
                maximum_references: 1,
                standby_times: vec![1_000_000],
                maximum_update_lapse: 100_000_000,
                maximum_rejections: 2,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    }
}
pub(super) fn service(db: Arc<Database>, cfg: Config) -> Service {
    let r = Registry::with_seed(key(128), cfg.policy.clone(), &[1, 2, 3, 4], [7; 32]).unwrap();
    Service {
        core: Core::start(r, Writer::start(db, cfg.storage).unwrap(), &cfg).unwrap(),
    }
}
fn durable(db: &Database) -> BTreeMap<PeerPublicKey, Record> {
    records(db.peer_view().unwrap().records(Default::default()).unwrap()).unwrap()
}
fn drain<S: Store>(core: &mut Core<S>) -> Vec<Effect> {
    let mut result = vec![];
    while let Some(delivery) = core.next_effect().unwrap().cloned() {
        result.push(delivery.effect);
        assert!(core.acknowledge(delivery.id));
    }
    result
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Before,
    After,
    Absent,
}
struct FaultStore {
    writer: Writer,
    fault: Fault,
    uncertain: bool,
    tamper: bool,
    wrong_receipt: Option<Version>,
}
impl Store for FaultStore {
    fn recover(&mut self) -> Result<(Version, Vec<Record>), peers::Failure> {
        let (version, mut rows) = self.writer.recover()?;
        self.uncertain = false;
        if self.tamper {
            rows.push(record(&peer(99, false)));
        }
        Ok((version, rows))
    }
    fn commit(
        &mut self,
        version: Version,
        changes: &[peers::Change],
    ) -> Result<Version, peers::Failure> {
        let fault = std::mem::replace(&mut self.fault, Fault::None);
        if matches!(fault, Fault::Before) {
            return Err(peers::Failure::Storage(RepositoryError::ResourceLimit));
        }
        if matches!(fault, Fault::Absent) {
            self.uncertain = true;
            return Err(peers::Failure::Storage(RepositoryError::Backend(
                "ambiguous absent write".into(),
            )));
        }
        let result = self.writer.commit(version, changes)?;
        if matches!(fault, Fault::After) {
            self.uncertain = true;
            return Err(peers::Failure::Storage(RepositoryError::Backend(
                "lost acknowledgement".into(),
            )));
        }
        Ok(self.wrong_receipt.take().unwrap_or(result))
    }
    fn uncertain(&self) -> bool {
        self.uncertain || self.writer.recovery_required()
    }
}
fn faulty(db: Arc<Database>) -> Core<FaultStore> {
    let cfg = config();
    Core::start(
        Registry::with_seed(key(128), cfg.policy.clone(), &[1, 2, 3, 4], [7; 32]).unwrap(),
        FaultStore {
            writer: Writer::start(db, cfg.storage).unwrap(),
            fault: Fault::None,
            uncertain: false,
            tamper: false,
            wrong_receipt: None,
        },
        &cfg,
    )
    .unwrap()
}

#[test]
fn staged_state_effects_and_rng_survive_definite_and_ambiguous_persistence_errors() {
    for fault in [Fault::Before, Fault::Absent, Fault::After] {
        let temp = Temp::new();
        let database = db(&temp);
        let mut core = faulty(database.clone());
        core.apply(1, Command::Add(peer(1, false))).unwrap();
        drain(&mut core);
        let token = match core.apply(2, Command::Connect(Some(key(1)))).unwrap() {
            Outcome::Selected(t) => t,
            _ => panic!(),
        };
        let old = core.registry.revision();
        let old_rows = durable(&database);
        let old_delivery = core.next_effect().unwrap().unwrap().clone();
        core.store.fault = fault;
        assert!(
            core.apply(3, Command::Block(policy::Info::from(&peer(1, false))))
                .is_err()
        );
        assert_eq!(core.registry.revision(), old);
        assert_eq!(core.registry.connection(key(1)).unwrap().token, token);
        if matches!(fault, Fault::Before) {
            assert!(core.pending.is_none());
            assert_eq!(core.next_effect().unwrap(), Some(&old_delivery));
            assert_eq!(durable(&database), old_rows);
            core.apply(3, Command::Block(policy::Info::from(&peer(1, false))))
                .unwrap();
        } else {
            assert!(matches!(core.next_effect(), Err(Error::RecoveryRequired)));
            assert!(matches!(
                core.apply(4, Command::Delete(key(1))),
                Err(Error::RecoveryRequired)
            ));
            if matches!(fault, Fault::After) {
                assert!(durable(&database).is_empty());
                core.store.tamper = true;
                assert!(matches!(core.recover(), Err(Error::Inconsistent)));
                assert!(core.pending.is_some());
                core.store.tamper = false;
            }
            let result = core.recover().unwrap();
            if matches!(fault, Fault::After) {
                assert_eq!(result, Recovery::Committed(Outcome::Done));
            } else {
                assert_eq!(result, Recovery::NotCommitted);
                assert_eq!(core.registry.revision(), old);
                assert_eq!(durable(&database), old_rows);
                core.apply(3, Command::Block(policy::Info::from(&peer(1, false))))
                    .unwrap();
            }
        }
        assert!(core.registry.connection(key(1)).is_none());
        assert!(durable(&database).is_empty());
        let effects = drain(&mut core);
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::Blocked(_)))
                .count(),
            1
        );
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e,Effect::Close(t)if *t==token))
                .count(),
            1
        );
        assert!(!effects.iter().any(|e| matches!(e, Effect::Dial { .. })));
        assert_eq!(core.recover().unwrap(), Recovery::Unchanged);
        assert!(core.queue.is_empty());
        assert_eq!(core.records, durable(&database));
    }
    // Failed preparation preserves the seeded random stream and timer choices.
    let a = Temp::new();
    let b = Temp::new();
    let mut left = faulty(db(&a));
    let mut right = faulty(db(&b));
    left.store.fault = Fault::Before;
    assert!(left.apply(1, Command::Add(peer(1, false))).is_err());
    for c in [&mut left, &mut right] {
        c.apply(1, Command::Add(peer(1, false))).unwrap();
        c.apply(2, Command::Connect(Some(key(1)))).unwrap();
    }
    assert_eq!(
        left.registry
            .timers()
            .map(|t| t.deadline)
            .collect::<Vec<_>>(),
        right
            .registry
            .timers()
            .map(|t| t.deadline)
            .collect::<Vec<_>>()
    );
}

#[test]
fn effect_capacity_and_generation_checks_precede_durable_publication() {
    let temp = Temp::new();
    let database = db(&temp);
    let mut cfg = config();
    cfg.effects = 1;
    let mut s = service(database.clone(), cfg);
    s.apply(1, Command::Add(peer(1, true))).unwrap();
    let dial = s.next_effect().unwrap().unwrap().clone();
    let revision = s.policy().revision();
    let rows = durable(&database);
    assert!(matches!(
        s.apply(2, Command::Block(policy::Info::from(&peer(2, false)))),
        Err(Error::EffectsFull)
    ));
    assert_eq!(s.policy().revision(), revision);
    assert_eq!(durable(&database), rows);
    // A current connecting generation is replaced by shutdown retirement in
    // the same one-slot queue; obsolete creation work must never be delivered.
    s.apply(2, Command::Shutdown).unwrap();
    assert!(!s.acknowledge(dial.id));
    let close = s.next_effect().unwrap().unwrap().clone();
    assert!(matches!(close.effect, Effect::Close(_)));
    assert!(s.acknowledge(close.id));
    assert!(!s.acknowledge(close.id));
    assert_eq!(s.queued_effects(), (0, 0));
    drop(s);
    let mut other = service(database.clone(), config());
    other.apply(3, Command::Add(peer(3, true))).unwrap();
    assert!(!other.acknowledge(close.id));
    assert!(other.next_effect().unwrap().is_some());
    other.core.sequence = u64::MAX;
    let old = other.version();
    assert!(matches!(
        other.apply(4, Command::Block(policy::Info::from(&peer(4, false)))),
        Err(Error::SequenceExhausted)
    ));
    assert_eq!(other.version(), old);
    drop(other);
    let mut cfg = config();
    cfg.effect_bytes = 1;
    let mut small = service(database.clone(), cfg);
    assert!(matches!(
        small.apply(5, Command::Add(peer(1, true))),
        Err(Error::EffectsFull)
    ));
    assert!(small.policy().pool().find(key(1)).is_none());
    assert!(durable(&database).is_empty());
    drop(small);
    let mut cfg = config();
    cfg.storage.records = 1;
    let mut limited = service(database.clone(), cfg);
    limited.apply(6, Command::Add(peer(1, false))).unwrap();
    let before = limited.policy().revision();
    assert!(matches!(
        limited.apply(7, Command::Add(peer(2, false))),
        Err(Error::Storage(peers::Failure::Storage(
            RepositoryError::ResourceLimit
        )))
    ));
    assert_eq!(limited.policy().revision(), before);
    assert!(!limited.recovery_required());
    assert!(limited.policy().pool().find(key(2)).is_none());
    assert_eq!(durable(&database).len(), 1);
    limited.apply(8, Command::Delete(key(1))).unwrap();
    limited.apply(9, Command::Add(peer(2, false))).unwrap();
    assert_eq!(
        durable(&database).into_values().collect::<Vec<_>>(),
        vec![record(&peer(2, false))]
    );
}

#[test]
fn a_foreign_storage_receipt_stops_publication_and_requires_owner_replacement() {
    let temp = Temp::new();
    let other = Temp::new();
    let database = db(&temp);
    let foreign = db(&other);
    let mut core = faulty(database.clone());
    core.apply(1, Command::Add(peer(1, false))).unwrap();
    core.apply(2, Command::Connect(Some(key(1)))).unwrap();
    let revision = core.registry.revision();
    let foreign_version = foreign.peer_view().unwrap().version();
    assert!(!core.version.same_database(foreign_version));
    core.store.wrong_receipt = Some(foreign_version);
    assert!(matches!(
        core.apply(3, Command::Block(policy::Info::from(&peer(1, false)))),
        Err(Error::Inconsistent)
    ));
    assert_eq!(core.registry.revision(), revision);
    assert!(core.registry.connection(key(1)).is_some());
    assert!(durable(&database).is_empty());
    assert!(matches!(core.next_effect(), Err(Error::Inconsistent)));
    assert!(matches!(core.recover(), Err(Error::Inconsistent)));
    drop(core);
    let s = service(database, config());
    assert_eq!(s.saved_peers().count(), 0);
}

#[test]
fn restart_retains_addresses_until_explicit_resolution_and_deletion_removes_unresolved_rows() {
    let temp = Temp::new();
    let database = db(&temp);
    let mut s = service(database.clone(), config());
    s.apply(1, Command::Add(peer(1, false))).unwrap();
    s.apply(2, Command::Add(peer(2, true))).unwrap();
    let old = s.policy().connection(key(2)).unwrap().token;
    drop(s);
    drop(database);
    let database = db(&temp);
    let mut s = service(database.clone(), config());
    assert_eq!(
        s.saved_peers().cloned().collect::<Vec<_>>(),
        vec![record(&peer(1, false))]
    );
    assert!(
        s.policy()
            .pool()
            .all(discovery::Target::Both)
            .next()
            .is_none()
    );
    assert!(s.next_effect().unwrap().is_none());
    assert_eq!(
        s.apply(
            3,
            Command::Connected {
                peer: key(2),
                token: old
            }
        )
        .unwrap(),
        Outcome::Stale
    );
    let saved = s.saved_peers().next().unwrap().clone();
    let std::net::IpAddr::V4(source) = saved.source else {
        panic!()
    };
    s.apply(
        4,
        Command::Resolve {
            peer: policy::Info {
                id: saved.id,
                host: saved.host,
                port: saved.port,
            },
            source: Some(source),
            trusted: false,
        },
    )
    .unwrap();
    let token = match s.next_effect().unwrap().unwrap().effect {
        Effect::Resolve { token, .. } => token,
        _ => panic!(),
    };
    s.apply(
        5,
        Command::Resolved {
            token,
            address: Some(Ipv4Addr::new(55, 66, 77, 88)),
        },
    )
    .unwrap();
    let changed = s.saved_peers().next().unwrap();
    assert_eq!(changed.address, std::net::IpAddr::from([55, 66, 77, 88]));
    assert_eq!(changed.source, std::net::IpAddr::V4(source));
    drop(s);
    let mut s = service(database.clone(), config());
    s.apply(6, Command::Delete(key(1))).unwrap();
    assert!(durable(&database).is_empty());
    drop(s);
    let mut raw = Writer::start(database.clone(), Default::default()).unwrap();
    let mut ipv6 = record(&peer(9, false));
    ipv6.source = "::1".parse().unwrap();
    raw.commit(raw.version().unwrap(), &[peers::Change::Put(ipv6.clone())])
        .unwrap();
    drop(raw);
    let mut s = service(database.clone(), config());
    assert_eq!(s.saved_peers().next(), Some(&ipv6));
    s.apply(7, Command::Block(policy::Info::from(&peer(9, false))))
        .unwrap();
    assert!(durable(&database).is_empty());
}
