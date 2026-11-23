use super::*;
use ae_storage::pinning_agent as durable;
use ae_types::Hash256;

pub(super) fn limits(c: &Config) -> durable::Limits {
    durable::Limits {
        bytes: c.checkpoint_bytes,
        events: c.maximum_events,
        integer_bytes: c.transaction.integer_bytes,
        hash_bytes: c.transaction.hash_bytes,
    }
}
fn failure(e: repo::Error) -> Error {
    Error::Journal(e.into())
}
pub(super) fn batch(
    database: &Database,
    revision: u64,
    config: &Config,
) -> Result<durable::Batch, Error> {
    let view = database.view().map_err(failure)?;
    project(&view, revision, config)
}
fn project<S: repo::Read>(
    view: &repo::View<S>,
    revision: u64,
    config: &Config,
) -> Result<durable::Batch, Error> {
    let change = view
        .event(revision)
        .map_err(failure)?
        .ok_or(Error::RecoveryGap(revision))?;
    let record = view
        .hyperchain_event(revision)
        .map_err(failure)?
        .ok_or(Error::RecoveryGap(revision))?;
    if record.context.origin == publication::Origin::Rollback {
        return Err(Error::Rollback(revision));
    }
    let a = change.encode();
    let b = record
        .encode(view.config().hyperchain_metadata)
        .map_err(failure)?;
    if a.len()
        .checked_add(b.len())
        .is_none_or(|n| n > config.maximum_event_bytes)
    {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = (a.len() as u64).to_be_bytes().to_vec();
    bytes.extend(a);
    bytes.extend(b);
    let mut events = Vec::new();
    for e in record.emitted() {
        if let ae_hyperchain::block::Event::NewEpoch(info) = e {
            let ae_vm_fate::value::Value::Integer(epoch) = &info.epoch else {
                return Err(Error::State("epoch integer"));
            };
            events.push(core::Event::Epoch {
                epoch: epoch.clone(),
                first: info.first.clone(),
                last: info.last.clone(),
            });
        }
    }
    if matches!(
        record.context.origin,
        publication::Origin::Synced | publication::Origin::Received | publication::Origin::Created
    ) && change.old_tip != Some(change.new_tip)
    {
        let block = record.context.block.ok_or(Error::State("event block"))?;
        events.push(core::Event::Top {
            height: block.height.into(),
        });
    }
    if events.len() > config.maximum_events {
        return Err(Error::ResourceLimit);
    }
    Ok(durable::Batch {
        revision,
        old_tip: change.old_tip,
        tip: change.new_tip,
        source: ae_crypto::blake2b_256(&bytes),
        events,
        next: 0,
        operation: Default::default(),
    })
}
pub(super) fn preflight(
    database: &Database,
    c: &durable::Checkpoint,
    config: &Config,
) -> Result<(), Error> {
    let view = database.view().map_err(failure)?;
    let head = view.status();
    let gap = head
        .revision
        .checked_sub(c.consumed.revision)
        .ok_or(Error::RecoveryGap(c.consumed.revision))?;
    if gap > config.maximum_recovery_records as u64 {
        return Err(Error::ResourceLimit);
    }
    let mut tip = c.consumed.tip;
    for revision in c.consumed.revision.saturating_add(1)..=head.revision {
        let change = view
            .event(revision)
            .map_err(failure)?
            .ok_or(Error::RecoveryGap(revision))?;
        if change.old_tip != tip {
            return Err(Error::RecoveryGap(revision));
        }
        let record = view
            .hyperchain_event(revision)
            .map_err(failure)?
            .ok_or(Error::RecoveryGap(revision))?;
        if record.context.origin == publication::Origin::Rollback {
            return Err(Error::Rollback(revision));
        }
        if !change.detached.is_empty() {
            return Err(Error::RecoveryReorganization(revision));
        }
        if let Some(p) = &c.pending
            && p.revision == revision
        {
            let actual = project(&view, revision, config)?;
            if p.source != actual.source
                || p.events != actual.events
                || p.old_tip != actual.old_tip
                || p.tip != actual.tip
            {
                return Err(Error::State("pending event source mismatch"));
            }
        }
        tip = Some(change.new_tip);
    }
    if tip != head.tip {
        return Err(Error::RecoveryGap(head.revision));
    }
    Ok(())
}
pub(super) fn pending(
    p: &durable::Batch,
    source: &events::Reader,
    payload: Option<Arc<publication::Payload>>,
) -> Result<Pending, Error> {
    Ok(Pending {
        payload,
        after: source.at_revision(p.revision).map_err(Error::Events)?,
        events: p.events[p.next..].iter().cloned().collect(),
    })
}
pub(super) fn parent_intent(
    meta: &parent::Metadata,
    post: &durable::Post,
    maximum: usize,
) -> Result<Hash256, Error> {
    let payload =
        ae_hyperchain::pin::encode_parent(&post.pin, maximum).map_err(|_| Error::ResourceLimit)?;
    ae_storage::parent_posts::Intent {
        chain: match meta.chain {
            parent_pins::Chain::Aeternity => ae_storage::parent_posts::Chain::Aeternity,
            parent_pins::Chain::Dogecoin => ae_storage::parent_posts::Chain::Dogecoin,
            parent_pins::Chain::Bitcoin => ae_storage::parent_posts::Chain::Bitcoin,
        },
        network: meta.network.to_vec(),
        account: post.account,
        payload,
    }
    .id()
    .map_err(failure)
}
