use ae_p2p::{
    address,
    registry::{Info, Registry},
};
use ae_storage::peers::Record;
use ae_types::PeerPublicKey;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::{IpAddr, Ipv4Addr},
};
mod seeds;

#[derive(Debug, Clone)]
pub struct Configuration {
    pub peers: Vec<Vec<u8>>,
    pub blocked_peers: Vec<Vec<u8>>,
    pub include_default_peers: bool,
    // Overrides the defaults for the selected database network. None uses the
    // pinned original mainnet/UAT seeds; other networks have no implicit seeds.
    pub defaults: Option<Vec<Vec<u8>>>,
    pub addresses: address::Limits,
}
impl Default for Configuration {
    fn default() -> Self {
        Self {
            peers: vec![],
            blocked_peers: vec![],
            include_default_peers: true,
            defaults: None,
            addresses: Default::default(),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub entries: usize,
    pub bytes: usize,
    pub maximum_step: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            entries: 131072,
            bytes: 64 << 20,
            maximum_step: 64,
        }
    }
}
impl Settings {
    fn validate(self) -> Result<(), Error> {
        if self.entries == 0
            || self.entries > 1_000_000
            || self.bytes == 0
            || self.bytes > 1 << 30
            || self.maximum_step == 0
            || self.maximum_step > 4096
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    ResourceLimit,
    NotFresh,
    NotConfigured,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    Configured(usize),
    Default(usize),
    Blocked(usize),
    Saved(PeerPublicKey),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    Address(address::Error),
    Local,
    Duplicate,
    Shadowed,
    Blocked,
    Configured,
    UnsupportedSource,
    Stale,
    AlreadyPresent,
    NotPooled,
    Administrative,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub origin: Origin,
    pub reason: Reason,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    pub ready: bool,
    pub queued: usize,
    pub protected_trusted: usize,
    pub submitted: usize,
    pub trusted_admitted: usize,
    pub skipped: usize,
    pub diagnostics: usize,
    pub input_entries: usize,
    pub input_bytes: usize,
    pub backpressured: bool,
}
#[derive(Clone)]
pub(super) struct Desired {
    pub info: Info,
    pub origin: Origin,
}
#[derive(Clone)]
pub(super) enum Item {
    Trusted(PeerPublicKey),
    Saved(Record),
}
impl Item {
    pub fn id(&self) -> PeerPublicKey {
        match self {
            Self::Trusted(id) => *id,
            Self::Saved(row) => row.id,
        }
    }
}
pub(super) struct State {
    pub installed: bool,
    pub blocks: Vec<address::Address>,
    pub trusted: BTreeMap<PeerPublicKey, Desired>,
    pub queued_trusted: BTreeSet<PeerPublicKey>,
    pub queue: VecDeque<Item>,
    pub diagnostics: Vec<Diagnostic>,
    pub settings: Settings,
    pub progress: Progress,
}
fn charge(
    count: &mut usize,
    bytes: &mut usize,
    payload: usize,
    settings: Settings,
) -> Result<(), Error> {
    *count = count.checked_add(1).ok_or(Error::ResourceLimit)?;
    *bytes = bytes
        .checked_add(128)
        .and_then(|n| n.checked_add(payload))
        .ok_or(Error::ResourceLimit)?;
    if *count > settings.entries || *bytes > settings.bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}
fn parse(
    raw: &[u8],
    addresses: address::Limits,
    origin: Origin,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<Option<address::Address>, Error> {
    match address::parse(raw, addresses) {
        Ok(info) => Ok(Some(info)),
        Err(address::Error::ResourceLimit) => Err(Error::ResourceLimit),
        Err(address::Error::Configuration) => Err(Error::Configuration),
        Err(error) => {
            diagnostics.push(Diagnostic {
                origin,
                reason: Reason::Address(error),
            });
            Ok(None)
        }
    }
}
impl State {
    pub fn build(
        network: &[u8],
        registry: &Registry,
        records: &BTreeMap<PeerPublicKey, Record>,
        config: Configuration,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.validate()?;
        config
            .addresses
            .validate()
            .map_err(|_| Error::Configuration)?;
        let default_count = if !config.include_default_peers {
            0
        } else {
            config.defaults.as_ref().map_or(
                match network {
                    b"ae_mainnet" => 9,
                    b"ae_uat" => 3,
                    _ => 0,
                },
                Vec::len,
            )
        };
        let count = config
            .peers
            .len()
            .checked_add(config.blocked_peers.len())
            .and_then(|n| n.checked_add(default_count))
            .and_then(|n| n.checked_add(records.len()))
            .ok_or(Error::ResourceLimit)?;
        if count > settings.entries {
            return Err(Error::ResourceLimit);
        }
        let defaults: Vec<&[u8]> = if !config.include_default_peers {
            vec![]
        } else if let Some(v) = &config.defaults {
            v.iter().map(Vec::as_slice).collect()
        } else {
            match network {
                b"ae_mainnet" => seeds::MAINNET.to_vec(),
                b"ae_uat" => seeds::UAT.to_vec(),
                _ => vec![],
            }
        };
        let mut progress = Progress::default();
        for raw in config
            .peers
            .iter()
            .map(Vec::as_slice)
            .chain(config.blocked_peers.iter().map(Vec::as_slice))
            .chain(defaults.iter().copied())
        {
            charge(
                &mut progress.input_entries,
                &mut progress.input_bytes,
                raw.len(),
                settings,
            )?;
        }
        for row in records.values() {
            charge(
                &mut progress.input_entries,
                &mut progress.input_bytes,
                row.host.len(),
                settings,
            )?;
        }
        let local = registry.local_peer();
        let mut diagnostics = vec![];
        let mut blocks = BTreeMap::<PeerPublicKey, address::Address>::new();
        for (i, raw) in config.blocked_peers.iter().enumerate() {
            let origin = Origin::Blocked(i);
            let Some(info) = parse(raw, config.addresses, origin.clone(), &mut diagnostics)? else {
                continue;
            };
            if info.id == local {
                diagnostics.push(Diagnostic {
                    origin,
                    reason: Reason::Local,
                });
                continue;
            }
            if let Some(old) = blocks.get(&info.id) {
                diagnostics.push(Diagnostic {
                    origin,
                    reason: if *old == info {
                        Reason::Duplicate
                    } else {
                        Reason::Shadowed
                    },
                });
                continue;
            }
            if info.host.len() > registry.configuration().pool.maximum_host_bytes {
                return Err(Error::ResourceLimit);
            }
            blocks.insert(info.id, info);
        }
        if blocks.len() > registry.configuration().maximum_blocked {
            return Err(Error::ResourceLimit);
        }
        let mut trusted = BTreeMap::<PeerPublicKey, Desired>::new();
        let mut queue = VecDeque::new();
        let mut queued_trusted = BTreeSet::new();
        let supplied = config
            .peers
            .iter()
            .enumerate()
            .map(|(i, v)| (Origin::Configured(i), v.as_slice()));
        let defaults = defaults
            .iter()
            .enumerate()
            .map(|(i, v)| (Origin::Default(i), *v));
        for (origin, raw) in supplied.chain(defaults) {
            let Some(address) = parse(raw, config.addresses, origin.clone(), &mut diagnostics)?
            else {
                continue;
            };
            let info = match address.registry_info() {
                Ok(info) => info,
                Err(address::Error::ResourceLimit) => return Err(Error::ResourceLimit),
                Err(error) => {
                    diagnostics.push(Diagnostic {
                        origin,
                        reason: Reason::Address(error),
                    });
                    continue;
                }
            };
            let reason = if info.id == local {
                Some(Reason::Local)
            } else if blocks.contains_key(&info.id) {
                Some(Reason::Blocked)
            } else {
                trusted.get(&info.id).map(|old| {
                    if old.info == info {
                        Reason::Duplicate
                    } else {
                        Reason::Shadowed
                    }
                })
            };
            if let Some(reason) = reason {
                diagnostics.push(Diagnostic { origin, reason });
                continue;
            }
            if info.host.len() > registry.configuration().pool.maximum_host_bytes {
                return Err(Error::ResourceLimit);
            }
            let id = info.id;
            trusted.insert(id, Desired { info, origin });
            queue.push_back(Item::Trusted(id));
            queued_trusted.insert(id);
        }
        for row in records.values() {
            let origin = Origin::Saved(row.id);
            let reason = if row.id == local {
                Some(Reason::Local)
            } else if blocks.contains_key(&row.id) {
                Some(Reason::Blocked)
            } else if trusted.contains_key(&row.id) {
                Some(Reason::Configured)
            } else if row.source.is_ipv6() {
                Some(Reason::UnsupportedSource)
            } else {
                address::Address {
                    id: row.id,
                    host: row.host.clone(),
                    port: Some(row.port.into()),
                }
                .registry_info()
                .err()
                .map(Reason::Address)
            };
            if let Some(reason) = reason {
                diagnostics.push(Diagnostic { origin, reason });
                continue;
            }
            if row.host.len() > registry.configuration().pool.maximum_host_bytes {
                return Err(Error::ResourceLimit);
            }
            queue.push_back(Item::Saved(row.clone()));
        }
        progress.skipped = diagnostics.len();
        Ok(Self {
            installed: false,
            blocks: blocks.into_values().collect(),
            trusted,
            queued_trusted,
            queue,
            diagnostics,
            settings,
            progress,
        })
    }
    pub fn status(&self) -> Progress {
        Progress {
            ready: self.installed,
            queued: self.queue.len(),
            protected_trusted: self.trusted.len(),
            diagnostics: self.diagnostics.len(),
            ..self.progress
        }
    }
    pub fn diagnostic(&mut self, origin: Origin, reason: Reason) {
        self.diagnostics.push(Diagnostic { origin, reason });
        self.progress.skipped += 1;
    }
    pub fn reconcile(&mut self, registry: &Registry) {
        let finished: Vec<_> = self
            .trusted
            .iter()
            .filter(|(id, d)| {
                !self.queued_trusted.contains(id) && !registry.resolving_peer(&d.info.host, **id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in finished {
            let desired = self.trusted.remove(&id).unwrap();
            if registry.pool().find(id).is_some_and(|p| p.trusted) {
                self.progress.trusted_admitted += 1;
            } else {
                self.diagnostic(desired.origin, Reason::NotPooled);
            }
        }
    }
    pub fn retire(&mut self, id: PeerPublicKey) {
        let was_queued = self.queue.iter().any(|item| item.id() == id);
        self.queue.retain(|item| item.id() != id);
        self.queued_trusted.remove(&id);
        if let Some(desired) = self.trusted.remove(&id) {
            self.diagnostic(desired.origin, Reason::Administrative);
        } else if was_queued {
            self.diagnostic(Origin::Saved(id), Reason::Administrative);
        }
    }
    pub fn intent(&self, item: &Item) -> (Info, Option<Ipv4Addr>, bool) {
        match item {
            Item::Trusted(id) => (self.trusted[id].info.clone(), None, true),
            Item::Saved(row) => {
                let IpAddr::V4(source) = row.source else {
                    unreachable!("validated source")
                };
                (
                    Info {
                        id: row.id,
                        host: row.host.clone(),
                        port: row.port,
                    },
                    Some(source),
                    false,
                )
            }
        }
    }
}
