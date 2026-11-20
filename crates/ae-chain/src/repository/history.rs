use super::{Error, Read, View};
use ae_codec::headers::{Header, KeyHeader};
use ae_execution::{
    chain_history::{HeaderLink, Snapshot},
    contracts, transactions,
};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

// Pin the selected index and the candidate's branch from the same database
// view. Historical FATE still observes selected-versus-branch differences.
pub fn vm_history<S: Read>(
    view: &View<S>,
    branch: &KeyHeader,
    limit: usize,
) -> Result<Snapshot, Error> {
    if limit == 0 {
        return Err(Error::ResourceLimit);
    }
    let branch_hash = Header::Key(branch.clone())
        .hash()
        .map_err(|_| Error::Corrupt("branch header"))?;
    let tip = view.status().tip.ok_or(Error::Uninitialized)?;
    let top = view.require_block(tip)?.header();
    let top_hash = match top {
        Header::Key(_) => tip,
        Header::Micro(ref m) => Hash256(m.common.previous_key_hash),
    };
    let mut cached = BTreeMap::from([(branch_hash, branch.clone())]);
    fn get<S: Read>(
        view: &View<S>,
        cache: &mut BTreeMap<Hash256, KeyHeader>,
        h: Hash256,
        limit: usize,
    ) -> Result<KeyHeader, Error> {
        if let Some(k) = cache.get(&h) {
            return Ok(k.clone());
        }
        if cache.len() >= limit {
            return Err(Error::ResourceLimit);
        }
        let super::Block::Key(k) = view.require_block(h)? else {
            return Err(Error::Corrupt("key history"));
        };
        cache.insert(h, k.clone());
        Ok(k)
    }
    let mut seen = [BTreeSet::new(), BTreeSet::new()];
    let mut next = [Some(branch_hash), Some(top_hash)];
    'common: loop {
        for side in 0..2 {
            let Some(h) = next[side] else { continue };
            if !seen[side].insert(h) {
                return Err(Error::Corrupt("key history cycle"));
            }
            if seen[1 - side].contains(&h) {
                break 'common;
            }
            let k = get(view, &mut cached, h, limit)?;
            if h == view.identity().genesis {
                next[side] = None;
            } else {
                if k.common.height == 0 {
                    return Err(Error::Corrupt("foreign key genesis"));
                }
                let p = Hash256(k.common.previous_key_hash);
                let previous = get(view, &mut cached, p, limit)?;
                if previous.common.height.checked_add(1) != Some(k.common.height) {
                    return Err(Error::Corrupt("key history height"));
                }
                next[side] = Some(p);
            }
        }
        if next.iter().all(Option::is_none) {
            return Err(Error::Corrupt("no common key ancestor"));
        }
    }
    let floor = branch.common.height.saturating_sub(256);
    let mut cursor = branch_hash;
    loop {
        let k = get(view, &mut cached, cursor, limit)?;
        if k.common.height <= floor {
            break;
        }
        let p = Hash256(k.common.previous_key_hash);
        let previous = get(view, &mut cached, p, limit)?;
        if previous.common.height.checked_add(1) != Some(k.common.height) {
            return Err(Error::Corrupt("key history height"));
        }
        cursor = p;
    }
    let mut selected = BTreeMap::new();
    for height in floor
        ..branch
            .common
            .height
            .min(top.common().height.saturating_add(1))
    {
        let hash = view
            .canonical_key(height)?
            .ok_or(Error::Corrupt("missing canonical generation"))?;
        let k = get(view, &mut cached, hash, limit)?;
        if k.common.height != height {
            return Err(Error::Corrupt("canonical generation height"));
        }
        selected.insert(height, hash.0);
    }
    Ok(Snapshot {
        selected,
        headers: cached
            .into_iter()
            .map(|(h, k)| {
                (
                    h.0,
                    HeaderLink {
                        height: k.common.height,
                        previous_key: k.common.previous_key_hash,
                    },
                )
            })
            .collect(),
    })
}
pub(super) fn with_history<O: MapOrder, R>(
    config: &transactions::Config<'_, O>,
    history: Arc<Snapshot>,
    f: impl FnOnce(&transactions::Config<'_, O>) -> R,
) -> R {
    let fate = config.fate.map(|old| contracts::fate::Config {
        context: old.context,
        order: old.order,
        overrides: old.overrides,
        environment: old.environment,
        history: history.clone(),
        auth_tx: old.auth_tx.clone(),
        metering_floor: old.metering_floor,
        diagnostic_profile: old.diagnostic_profile,
    });
    let aevm = config.aevm.map(|old| {
        let mut a = old.clone();
        a.environment.history = history;
        a
    });
    let bound = transactions::Config {
        network: config.network,
        consensus: config.consensus,
        fate: fate.as_ref(),
        aevm: aevm.as_ref(),
        signature_limits: config.signature_limits,
        channel_limits: config.channel_limits,
        channel_state_atoms: config.channel_state_atoms,
        oracle_limits: config.oracle_limits,
        fate_limits: config.fate_limits,
        maximum_fate_steps: config.maximum_fate_steps,
        aevm_limits: config.aevm_limits,
    };
    f(&bound)
}
