use super::*;
use ae_chain::{
    block::Block,
    interchange::{self, Limits, Manifest},
};
use ae_trie::{Value, scan};
use std::{fs::File, io::Read as IoRead, path::PathBuf};

pub(crate) const MANIFEST: &[u8] = b"\0replay-manifest";
pub(crate) const PROGRESS: &[u8] = b"\0replay-progress";
const MAX_PROGRESS: usize = 4 * 1024 * 1024;
const MARKER: &str = ".ae-replay-";

pub(super) fn has_marker(path: &Path) -> Result<bool, Error> {
    for entry in std::fs::read_dir(path).map_err(|e| Error::Backend(e.to_string()))? {
        let entry = entry.map_err(|e| Error::Backend(e.to_string()))?;
        if entry.file_name().to_string_lossy().starts_with(MARKER) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug)]
pub enum Failure {
    Repository(Error),
    Archive(interchange::Error),
    Io(std::io::Error),
    Validation(String),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl From<interchange::Error> for Failure {
    fn from(e: interchange::Error) -> Self {
        Self::Archive(e)
    }
}
impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Replay,
    Verify,
    Complete,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub manifest: Hash256,
    pub phase: Phase,
    pub chunk: u64,
    pub offset: u64,
    pub blocks: u64,
    cursor: Vec<u8>,
}
fn binary(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
fn number(n: u64) -> Value {
    binary(ae_types::Unsigned::from(n).as_bytes())
}
impl Progress {
    fn encode(&self) -> Vec<u8> {
        Value::List(vec![
            number(1),
            binary(self.manifest.0),
            number(match self.phase {
                Phase::Replay => 0,
                Phase::Verify => 1,
                Phase::Complete => 2,
            }),
            number(self.chunk),
            number(self.offset),
            number(self.blocks),
            binary(&self.cursor),
        ])
        .encode()
    }
    fn decode(raw: &[u8]) -> Result<Self, Error> {
        let value = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: MAX_PROGRESS,
                max_items: 8,
                max_depth: 2,
            },
        )
        .map_err(|_| Error::Corrupt("replay progress"))?;
        let ae_codec::rlp::Rlp::List(f) = value else {
            return Err(Error::Corrupt("replay progress fields"));
        };
        let bytes = |i: usize| match f.get(i) {
            Some(ae_codec::rlp::Rlp::Bytes(b)) => Ok(*b),
            _ => Err(Error::Corrupt("replay progress field")),
        };
        let integer = |i| {
            ae_types::Unsigned::from_field_bytes(bytes(i)?)
                .map_err(|_| Error::Corrupt("replay integer"))?
                .to_u64()
                .map_err(|_| Error::Corrupt("replay integer range"))
        };
        if f.len() != 7 || integer(0)? != 1 {
            return Err(Error::Corrupt("replay progress version"));
        }
        let state = Self {
            manifest: Hash256::try_from(bytes(1)?)
                .map_err(|_| Error::Corrupt("replay manifest hash"))?,
            phase: match integer(2)? {
                0 => Phase::Replay,
                1 => Phase::Verify,
                2 => Phase::Complete,
                _ => return Err(Error::Corrupt("replay phase")),
            },
            chunk: integer(3)?,
            offset: integer(4)?,
            blocks: integer(5)?,
            cursor: bytes(6)?.to_vec(),
        };
        if (state.phase != Phase::Verify && !state.cursor.is_empty()) || state.encode() != raw {
            return Err(Error::Corrupt("replay encoding"));
        }
        Ok(state)
    }
    fn validate(&self, manifest: &Manifest) -> Result<(), Error> {
        if self.manifest != manifest.id() {
            return Err(Error::Identity);
        }
        let index = usize::try_from(self.chunk).map_err(|_| Error::ResourceLimit)?;
        let prior = manifest
            .chunks
            .get(..index)
            .ok_or(Error::Corrupt("replay chunk"))?
            .iter()
            .try_fold(0u64, |sum, c| {
                sum.checked_add(c.blocks).ok_or(Error::ResourceLimit)
            })?;
        if prior.checked_add(self.offset) != Some(self.blocks)
            || match self.phase {
                Phase::Replay => manifest
                    .chunks
                    .get(index)
                    .is_none_or(|c| self.offset >= c.blocks),
                Phase::Verify | Phase::Complete => {
                    index != manifest.chunks.len()
                        || self.offset != 0
                        || self.blocks != manifest.blocks
                }
            }
        {
            return Err(Error::Corrupt("replay position"));
        }
        Ok(())
    }
}

pub struct Archive {
    path: PathBuf,
    manifest: Manifest,
    limits: Limits,
}
fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, Failure> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(Error::ResourceLimit.into());
    }
    let mut bytes = vec![];
    file.take((limit as u64).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(Error::ResourceLimit.into());
    }
    Ok(bytes)
}
impl Archive {
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Failure> {
        let path = path.as_ref().to_path_buf();
        let manifest = Manifest::decode(
            &read_bounded(&path.join("manifest.rlp"), limits.manifest_bytes)?,
            limits,
        )?;
        Ok(Self {
            path,
            manifest,
            limits,
        })
    }
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn chunk(&self, index: usize) -> Result<Vec<u8>, Failure> {
        let descriptor = self
            .manifest
            .chunks
            .get(index)
            .ok_or(Error::Corrupt("archive chunk index"))?;
        let bytes = read_bounded(
            &self.path.join(format!("{index:08}.rlp")),
            self.limits.chunk_bytes,
        )?;
        if bytes.len() as u64 != descriptor.bytes || blake2b_256(&bytes) != descriptor.hash {
            return Err(interchange::Error::Malformed("chunk checksum").into());
        }
        Ok(bytes)
    }
}

// No public Database/view escapes before verification completes. The directory
// is a separate staging database, never an overlay on an existing live chain.
pub struct Replay {
    db: Database,
    manifest: Manifest,
    progress: Progress,
    limits: Limits,
}
impl Replay {
    pub fn open(
        path: impl AsRef<Path>,
        identity: Identity,
        manifest: Manifest,
        config: ReadConfig,
        limits: Limits,
    ) -> Result<Self, Failure> {
        manifest.validate(limits)?;
        if identity.network != manifest.network || identity.genesis != manifest.genesis {
            return Err(Error::Identity.into());
        }
        let path = path.as_ref();
        let mut binding = identity.encode();
        binding.extend(manifest.id().0);
        let name = format!(
            "{MARKER}{}",
            blake2b_256(&binding)
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let marker = path.join(name);
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            if meta.file_type().is_symlink() || !meta.is_dir() {
                return Err(Error::NotEmpty.into());
            }
            if !path.join("CURRENT").is_file()
                && !marker.is_file()
                && std::fs::read_dir(path)?.next().is_some()
            {
                return Err(Error::NotEmpty.into());
            }
        }
        if !path.join("CURRENT").is_file() {
            // A zero-byte file with a bound filename is created atomically;
            // unlike file contents, there is no partially written identity.
            // Persist ownership before RocksDB can create its first files.
            std::fs::create_dir_all(path)?;
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)
            {
                Ok(file) => file.sync_all()?,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let meta = std::fs::symlink_metadata(&marker)?;
                    if !meta.is_file() || meta.len() != 0 {
                        return Err(Error::NotEmpty.into());
                    }
                }
                Err(e) => return Err(e.into()),
            }
            File::open(path)?.sync_all()?;
        }
        let db = Database::open_mode(path, identity, config, Default::default(), true)?;
        let progress = match db.db.get(PROGRESS).map_err(backend)? {
            Some(bytes) => {
                let stored = db
                    .db
                    .get(MANIFEST)
                    .map_err(backend)?
                    .ok_or(Error::Corrupt("missing replay manifest"))?;
                if stored != manifest.encode() {
                    return Err(Error::Identity.into());
                }
                Progress::decode(&bytes)?
            }
            None => {
                if db.view()?.status() != Status::default() {
                    return Err(Error::NotEmpty.into());
                }
                let state = Progress {
                    manifest: manifest.id(),
                    phase: Phase::Replay,
                    chunk: 0,
                    offset: 0,
                    blocks: 0,
                    cursor: vec![],
                };
                let mut batch = WriteBatch::default();
                batch.put(MANIFEST, manifest.encode());
                batch.put(PROGRESS, state.encode());
                db.db.write_opt(batch, &write_options()).map_err(backend)?;
                state
            }
        };
        progress.validate(&manifest)?;
        Ok(Self {
            db,
            manifest,
            progress,
            limits,
        })
    }
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn progress(&self) -> &Progress {
        &self.progress
    }
    pub fn apply_chunk(
        &mut self,
        bytes: &[u8],
        max_blocks: usize,
        mut validate: impl FnMut(&View<Snapshot<'_>>, &Block) -> Result<Update, Failure>,
    ) -> Result<Progress, Failure> {
        if self.progress.phase != Phase::Replay {
            return Err(Error::Conflict.into());
        }
        if max_blocks == 0 {
            return Err(Error::ResourceLimit.into());
        }
        let index = usize::try_from(self.progress.chunk).map_err(|_| Error::ResourceLimit)?;
        let blocks = interchange::decode_chunk(
            bytes,
            self.progress.chunk,
            &self.manifest.chunks[index],
            &self.db.config,
            self.limits,
        )?;
        let offset = usize::try_from(self.progress.offset).map_err(|_| Error::ResourceLimit)?;
        for block in blocks.iter().skip(offset).take(max_blocks) {
            let view = self.db.view()?;
            if view.status().revision != self.progress.blocks {
                return Err(Error::Conflict.into());
            }
            let hash = block
                .hash()
                .map_err(|e| interchange::Error::Block(e.into()))?;
            if let Some(parent) = view.status().tip {
                if block.header().common().previous_hash != parent.0 {
                    return Err(interchange::Error::Malformed("noncontiguous replay").into());
                }
            } else if hash != self.manifest.genesis
                || !matches!(block, Block::Key(_))
                || block.header().common().height != 0
            {
                return Err(interchange::Error::Malformed("replay genesis").into());
            }
            let update = validate(&view, block)?;
            if update.change().inserted != Some(hash) || update.change().new_tip != hash {
                return Err(Error::Corrupt("replay receipt binding").into());
            }
            for (hash, bytes) in interchange::transaction_records(block, &self.db.config)? {
                if update.writes().get(&repo::key(repo::TRANSACTIONS, hash.0)) != Some(&Some(bytes))
                {
                    return Err(Error::Corrupt("replay transaction metadata").into());
                }
            }
            let mut next = self.progress.clone();
            next.blocks = next.blocks.checked_add(1).ok_or(Error::ResourceLimit)?;
            next.offset += 1;
            if next.offset == self.manifest.chunks[index].blocks {
                next.chunk += 1;
                next.offset = 0;
                if next.chunk as usize == self.manifest.chunks.len() {
                    next.phase = Phase::Verify;
                }
            }
            next.validate(&self.manifest)?;
            self.db
                .commit_extra(update, Some((PROGRESS, &next.encode())), || Ok(()))?;
            self.progress = next;
        }
        Ok(self.progress.clone())
    }
    pub fn verify_step(
        &mut self,
        limits: scan::Limits,
        page: scan::PageLimits,
    ) -> Result<Progress, Failure> {
        self.verify_checked(limits, page, || Ok(()))
    }
    fn verify_checked(
        &mut self,
        limits: scan::Limits,
        page: scan::PageLimits,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<Progress, Failure> {
        if self.progress.phase == Phase::Complete {
            return Ok(self.progress.clone());
        }
        if self.progress.phase != Phase::Verify {
            return Err(Error::IncompleteImport.into());
        }
        let view = self.db.view()?;
        if view.status()
            != (Status {
                revision: self.manifest.blocks,
                tip: Some(self.manifest.anchor),
            })
        {
            return Err(Error::Corrupt("replay anchor").into());
        }
        let header = view.require_block(self.manifest.anchor)?.header();
        let key = match header {
            ae_codec::headers::Header::Key(_) => self.manifest.anchor,
            ae_codec::headers::Header::Micro(_) => Hash256(header.common().previous_key_hash),
        };
        if header.common().height != self.manifest.height
            || header.common().version != self.manifest.protocol
            || key != self.manifest.anchor_key
            || view.canonical_key(self.manifest.height)? != Some(key)
            || view.require_record(self.manifest.anchor)?.roots != self.manifest.roots
            || repo::rollback::community_fork_target(&view)?.is_some()
        {
            return Err(Error::Corrupt("replay anchor state").into());
        }
        let cursor = if self.progress.cursor.is_empty() {
            scan::Cursor::new(self.manifest.roots.array().into_iter().flatten(), limits)
        } else {
            scan::Cursor::decode(&self.progress.cursor, limits)
        }
        .map_err(Error::from)?;
        let scanned = cursor.page(view.source(), page).map_err(Error::from)?;
        let mut next = self.progress.clone();
        if scanned.next.is_complete() {
            next.phase = Phase::Complete;
            next.cursor.clear();
        } else {
            next.cursor = scanned.next.encode();
        }
        let encoded = next.encode();
        if encoded.len() > MAX_PROGRESS {
            return Err(Error::ResourceLimit.into());
        }
        let mut batch = WriteBatch::default();
        batch.put(PROGRESS, encoded);
        check()?;
        self.db
            .db
            .write_opt(batch, &write_options())
            .map_err(backend)?;
        self.progress = next;
        Ok(self.progress.clone())
    }
    pub fn into_database(self) -> Result<Database, Failure> {
        if self.progress.phase != Phase::Complete {
            return Err(Error::IncompleteImport.into());
        }
        Ok(self.db)
    }
}
impl Database {
    pub(super) fn validate_replay(&self, importing: bool) -> Result<(), Error> {
        let state = self.db.get(PROGRESS).map_err(backend)?;
        let manifest = self.db.get(MANIFEST).map_err(backend)?;
        match (state, manifest) {
            (None, None) => Ok(()),
            (Some(bytes), Some(manifest)) => {
                let state = Progress::decode(&bytes)?;
                if blake2b_256(&manifest) != state.manifest {
                    return Err(Error::Corrupt("replay manifest binding"));
                }
                // Admission budgets may differ between runs. Check the local
                // journal structurally without imposing today's import quotas.
                let manifest = Manifest::decode(
                    &manifest,
                    Limits {
                        manifest_bytes: manifest.len(),
                        chunk_bytes: usize::MAX,
                        chunks: usize::MAX,
                        blocks_per_chunk: usize::MAX,
                        blocks: u64::MAX,
                        total_bytes: u64::MAX,
                    },
                )
                .map_err(|_| Error::Corrupt("replay manifest"))?;
                if manifest.network != self.identity.network
                    || manifest.genesis != self.identity.genesis
                {
                    return Err(Error::Identity);
                }
                state.validate(&manifest)?;
                let revision = self.view()?.status().revision;
                if (state.phase != Phase::Complete && revision != state.blocks)
                    || revision < state.blocks
                {
                    return Err(Error::Corrupt("replay revision"));
                }
                if !importing && state.phase != Phase::Complete {
                    return Err(Error::IncompleteImport);
                }
                Ok(())
            }
            _ => Err(Error::Corrupt("incomplete replay metadata")),
        }
    }
}
