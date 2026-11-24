// Pending-payout evidence is operational data, not a new chain state tree.
// Capture and the GC floor advance share the database writer and atomic batch.
use super::*;
use ae_codec::headers::{Header, HeaderMode};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie};
use std::{cell::Cell, collections::BTreeMap};

pub(crate) const KEY: &[u8] = b"\0stratum-payout-receipts";
const MAX_BYTES: usize = 16 << 20;
const MAX_ENTRIES: usize = 16;
const MAX_NODES: usize = 256;
const MAX_READ_BYTES: usize = 16 << 20;
mod codec;

#[derive(Clone)]
struct Evidence {
    header: Header,
    roots: ae_state::StateRoots,
    nodes: BTreeMap<Hash256, Vec<u8>>,
}
struct Archive {
    identity: Vec<u8>,
    sequence: u64,
    payment: core::PaymentId,
    signed: ae_mempool::pool::Signed,
    entries: BTreeMap<Hash256, Evidence>,
}
impl Evidence {
    fn lookup(&self, key: &Nibbles) -> Result<Option<Vec<u8>>, Failure> {
        let mut store = MemoryStore::default();
        for (hash, raw) in &self.nodes {
            store.put_node(*hash, raw.clone()).map_err(Error::from)?;
        }
        let tree = Trie::from_root(self.roots.calls, store).map_err(Error::from)?;
        let (value, used) = tree.lookup_with_proof(key).map_err(Error::from)?;
        // Reject unrelated, duplicate or incomplete proof material as well as
        // hash corruption. Absence is authenticated but is never success.
        if used.nodes() != &self.nodes {
            return Err(Error::Corrupt("Stratum receipt proof nodes").into());
        }
        match value {
            Some(Value::Bytes(raw)) => Ok(Some(raw)),
            None => Ok(None),
            _ => Err(Error::Corrupt("Stratum payout call trie value").into()),
        }
    }
}
impl Archive {
    fn validate(
        &self,
        view: &repo::View<Snapshot<'_>>,
        config: &Config,
        expected: &Record,
        limits: Limits,
    ) -> Result<(), Failure> {
        if self.identity != view.identity().binding()
            || self.sequence != expected.sequence
            || self.payment != expected.payment.id
            || Some(self.signed.hash()) != expected.hash()?
            || expected
                .signed
                .as_ref()
                .is_some_and(|s| s.transaction != self.signed.encode())
        {
            return Err(Error::Corrupt("Stratum receipt owner binding").into());
        }
        let (id, key) = execution::call_key(config, &self.signed)?;
        for (hash, proof) in &self.entries {
            if !matches!(proof.header, Header::Micro(_))
                || proof
                    .header
                    .hash()
                    .map_err(|_| Error::Corrupt("receipt header"))?
                    != *hash
                || proof.header.common().state_hash != proof.roots.hash().0
            {
                return Err(Error::Corrupt("Stratum receipt state binding").into());
            }
            if let Some(raw) = proof.lookup(&key)? {
                execution::validate_call(
                    config,
                    &self.signed,
                    proof.header.common().height,
                    id,
                    &raw,
                    limits,
                )?;
            }
        }
        Ok(())
    }
}
fn load(source: &Snapshot<'_>, limits: Limits) -> Result<Option<Archive>, Failure> {
    source
        .snapshot
        .get_pinned(KEY)
        .map_err(backend)?
        .map(|raw| Archive::decode(&raw, limits.bytes.min(MAX_BYTES)))
        .transpose()
}
pub(super) fn verify(s: &Stored<'_>, info: Option<&Info>, limits: Limits) -> Result<(), Failure> {
    let Some(archive) = load(&s.source, limits)? else {
        return Ok(());
    };
    let info = info.ok_or(Error::Corrupt("Stratum receipt without owner"))?;
    let expected = info
        .pending
        .as_ref()
        .ok_or(Error::Corrupt("Stratum receipt without pending payout"))?;
    let view = view(s, repo::ReadConfig::default())?;
    archive.validate(&view, &info.config, expected, limits)
}
// The caller has already rechecked the current canonical block and exact
// transaction. A detached proof may remain archived, but cannot be selected.
pub(super) fn receipt(
    view: &repo::View<Snapshot<'_>>,
    config: &Config,
    expected: &Record,
    signed: &ae_mempool::pool::Signed,
    block: Hash256,
    limits: Limits,
) -> Result<Option<Option<Vec<u8>>>, Failure> {
    let Some(archive) = load(view.source(), limits)? else {
        return Ok(None);
    };
    archive.validate(view, config, expected, limits)?;
    let Some(proof) = archive.entries.get(&block) else {
        return Ok(None);
    };
    if archive.signed.encode() != signed.encode()
        || proof.header != view.require_block(block)?.header()
    {
        return Err(Error::Corrupt("Stratum receipt inclusion binding").into());
    }
    let (_, key) = execution::call_key(config, signed)?;
    proof.lookup(&key).map(Some)
}
pub(super) fn release(s: &Stored<'_>) -> Result<Extra, Failure> {
    Ok(Extra {
        key: KEY.to_vec(),
        before: s.source.snapshot.get(KEY).map_err(backend)?,
        after: None,
    })
}
struct Reader<'a, 'db> {
    source: &'a Snapshot<'db>,
    writes: Option<&'a repo::Writes>,
    reads: Cell<usize>,
    bytes: Cell<usize>,
}
impl NodeReader for Reader<'_, '_> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        let reads = self.reads.get() + 1;
        if reads > MAX_NODES * 3 {
            return Err(ae_trie::Error::ResourceLimit);
        }
        self.reads.set(reads);
        if let Some(raw) = self
            .writes
            .and_then(|w| w.get(&repo::key(repo::NODES, hash.0)))
        {
            self.charge(raw.as_ref().map_or(0, Vec::len))?;
            return Ok(raw.clone());
        }
        let raw = self
            .source
            .snapshot
            .get_pinned(repo::key(repo::NODES, hash.0))
            .map_err(|e| ae_trie::Error::Backend(e.to_string()))?;
        self.charge(raw.as_ref().map_or(0, |r| r.len()))?;
        Ok(raw.map(|r| r.to_vec()))
    }
}
impl Reader<'_, '_> {
    fn charge(&self, size: usize) -> Result<(), ae_trie::Error> {
        let bytes = self
            .bytes
            .get()
            .checked_add(size)
            .filter(|n| *n <= MAX_READ_BYTES)
            .ok_or(ae_trie::Error::ResourceLimit)?;
        self.bytes.set(bytes);
        Ok(())
    }
}
// Called only while the chain/GC writer is held; no nested writer acquisition.
// Existing pruned legacy receipts remain unknown. Fresh capture failures stop
// GC before the floor changes, preserving the possibility of a bounded retry.
pub(crate) fn before_gc(
    db: &Database,
    view: &repo::View<Snapshot<'_>>,
    floor: u64,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    capture(db, view, floor, batch).map_err(|e| match e {
        Failure::Storage(e) => e,
        _ => Error::Corrupt("Stratum receipt capture"),
    })
}
fn capture(
    db: &Database,
    view: &repo::View<Snapshot<'_>>,
    floor: u64,
    batch: &mut WriteBatch,
) -> Result<(), Failure> {
    let limits = Limits::maximum();
    let s = db.stratum_snapshot(super::super::Limits::maximum(), view.source().policy)?;
    let Some(info) = s.payouts(limits)? else {
        return Ok(());
    };
    let Some(expected) = &info.pending else {
        return Ok(());
    };
    let Some(hash) = expected.hash()? else {
        return Ok(());
    };
    let Some(block) = view.location(hash)? else {
        return Ok(());
    };
    if !view.is_canonical(block)? {
        return Err(Error::Corrupt("Stratum receipt canonical location").into());
    }
    let Block::Micro(micro) = view.require_block(block)? else {
        return Err(Error::Corrupt("Stratum receipt inclusion kind").into());
    };
    micro
        .transactions
        .iter()
        .find(|tx| tx.hash() == hash)
        .ok_or(Error::Corrupt("Stratum receipt transaction missing"))?;
    let height = micro.header.common.height;
    if height >= floor {
        return Ok(());
    }
    if load(view.source(), limits)?.is_some_and(|a| a.entries.contains_key(&block)) {
        return Ok(());
    }
    match view.require_retained(height) {
        Ok(()) => {}
        Err(Error::Pruned { .. }) => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    let roots = view.require_record(block)?.roots.state;
    if let Some(raw) = append(view, &info, micro, roots, None)? {
        batch.put(KEY, raw);
    }
    Ok(())
}
pub(crate) fn on_insert(
    db: &Database,
    view: &repo::View<Snapshot<'_>>,
    update: &repo::Update,
) -> Result<Option<Vec<u8>>, Error> {
    insert(db, view, update).map_err(|e| match e {
        Failure::Storage(e) => e,
        _ => Error::Corrupt("Stratum receipt insertion"),
    })
}
fn insert(
    db: &Database,
    view: &repo::View<Snapshot<'_>>,
    update: &repo::Update,
) -> Result<Option<Vec<u8>>, Failure> {
    let s = db.stratum_snapshot(super::super::Limits::maximum(), view.source().policy)?;
    let Some(info) = s.payouts(Limits::maximum())? else {
        return Ok(None);
    };
    let Some(expected) = &info.pending else {
        return Ok(None);
    };
    let Some(hash) = expected.hash()? else {
        return Ok(None);
    };
    let Some(block) = update.change().inserted else {
        return Ok(None);
    };
    let Some(Block::Micro(micro)) = update.inserted_block(view)? else {
        return Ok(None);
    };
    if !micro.transactions.iter().any(|tx| tx.hash() == hash) {
        return Ok(None);
    }
    let raw = update
        .writes()
        .get(&repo::key(repo::RECORDS, block.0))
        .and_then(Option::as_ref)
        .ok_or(Error::Corrupt("Stratum receipt staged record"))?;
    let roots = repo::Record::decode(raw)?.roots.state;
    append(view, &info, micro, roots, Some(update.writes()))
}
fn append(
    view: &repo::View<Snapshot<'_>>,
    info: &Info,
    micro: ae_chain::block::MicroBlock,
    roots: ae_state::StateRoots,
    writes: Option<&repo::Writes>,
) -> Result<Option<Vec<u8>>, Failure> {
    let limits = Limits::maximum();
    let expected = info.pending.as_ref().ok_or(Error::Conflict)?;
    let hash = expected.hash()?.ok_or(Error::Conflict)?;
    let signed = micro
        .transactions
        .iter()
        .find(|tx| tx.hash() == hash)
        .ok_or(Error::Corrupt("Stratum receipt transaction missing"))?;
    let header = Header::Micro(micro.header);
    let block = header
        .hash()
        .map_err(|_| Error::Corrupt("receipt header"))?;
    let mut archive = load(view.source(), limits)?.unwrap_or_else(|| Archive {
        identity: view.identity().binding(),
        sequence: expected.sequence,
        payment: expected.payment.id,
        signed: signed.clone(),
        entries: BTreeMap::new(),
    });
    if archive.entries.contains_key(&block) {
        return Ok(None);
    }
    if archive.entries.len() >= MAX_ENTRIES {
        return Err(Error::ResourceLimit.into());
    }
    if roots.hash().0 != header.common().state_hash {
        return Err(Error::Corrupt("Stratum receipt state binding").into());
    }
    let (_, key) = execution::call_key(&info.config, signed)?;
    let reader = Reader {
        source: view.source(),
        writes,
        reads: Cell::new(0),
        bytes: Cell::new(0),
    };
    let (_, proof) = Trie::from_root(roots.calls, reader)
        .map_err(Error::from)?
        .lookup_with_proof(&key)
        .map_err(Error::from)?;
    archive.entries.insert(
        block,
        Evidence {
            header,
            roots,
            nodes: proof.nodes().clone(),
        },
    );
    archive.validate(view, &info.config, expected, limits)?;
    archive.encode().map(Some)
}
