// aec_peer_connection pre_assembly_check/get_micro_block_txs/fill_txs,
// aeternity fa2a6d7. Copyright (c) 2018, Aeternity Anstalt. ISC: LICENSE.
use crate::block::{Light, MicroBlock};
use ae_codec::headers::{Header, KeyHeader, MicroHeader};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Signature};

mod stored;
pub use stored::{PreparationError, Settings, precheck_stored};

type Signed = SignedTransaction<Transaction>;

// Native Height - 1 =< SyncHeight, including height zero and u64::MAX,
// without overflowing either boundary. None represents Erlang infinity.
pub fn allows_height(height: u64, sync_height: Option<u64>) -> bool {
    sync_height.is_none_or(|sync| height.saturating_sub(1) <= sync)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precheck {
    Known,
    Assemble,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Header(ae_codec::headers::HeaderError),
    MicroExpected,
    PreviousKeyMissing,
    Version,
    Interval,
    Future,
    Orphan,
    TooOld,
    Height,
    PreviousKeyHash,
    Signature,
    Transactions,
}
impl From<ae_codec::headers::HeaderError> for Error {
    fn from(error: ae_codec::headers::HeaderError) -> Self {
        Self::Header(error)
    }
}
// The chain adapter supplies one coherent view of already validated parents.
// known/connected refer to actual stored blocks, never peer announcements.
pub struct Environment<'a> {
    pub known: bool,
    pub previous: Option<&'a Header>,
    pub previous_key: Option<&'a KeyHeader>,
    pub connected: bool,
    pub top_height: Option<u64>,
    pub gossip_depth: u64,
    pub now_ms: u64,
    pub micro_cycle_ms: u64,
    pub hyperchain: bool,
}
pub fn precheck(header: &Header, env: Environment<'_>) -> Result<Precheck, Error> {
    header.hash()?;
    if env.known {
        return Ok(Precheck::Known);
    }
    let Header::Micro(m) = header else {
        return Err(Error::MicroExpected);
    };
    let key = env.previous_key.ok_or(Error::PreviousKeyMissing)?;
    // Original pre-assembly uses the preceding key's version, not the
    // activation schedule. Full block admission must still run afterwards.
    if m.common.version != key.common.version {
        return Err(Error::Version);
    }
    if let Some(previous) = env.previous {
        let interval = if matches!(previous, Header::Micro(_)) {
            env.micro_cycle_ms
        } else {
            1
        };
        if u128::from(m.time)
            < u128::from(ae_consensus::headers::timestamp(previous)) + u128::from(interval)
        {
            return Err(Error::Interval);
        }
    }
    if u128::from(m.time) >= u128::from(env.now_ms) + 540_000 {
        return Err(Error::Future);
    }
    if !env.connected {
        return Err(Error::Orphan);
    }
    if env
        .top_height
        .is_some_and(|top| m.common.height < top.saturating_sub(env.gossip_depth))
    {
        return Err(Error::TooOld);
    }
    let previous = env.previous.ok_or(Error::Orphan)?;
    let expected = u128::from(previous.common().height)
        + u128::from(env.hyperchain && matches!(previous, Header::Key(_)));
    if u128::from(m.common.height) != expected {
        return Err(Error::Height);
    }
    let previous_key = if matches!(previous, Header::Key(_)) {
        m.common.previous_hash
    } else {
        previous.common().previous_key_hash
    };
    if m.common.previous_key_hash != previous_key {
        return Err(Error::PreviousKeyHash);
    }
    if !env.hyperchain
        && !ae_crypto::verify_ed25519(
            &AccountPublicKey(key.miner),
            &header.signature_bytes()?,
            &Signature(m.signature),
        )
    {
        return Err(Error::Signature);
    }
    Ok(Precheck::Assemble)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Found(Box<Signed>),
    Missing(Vec<u8>),
}
// This is a reconstruction plan, not proof of block validity. It retains the
// original order and duplicates. Callers must bound lookup and reply resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    header: MicroHeader,
    proof: Option<Box<crate::fraud::Proof>>,
    entries: Vec<Entry>,
}
impl Fragment {
    pub fn lookup<E: From<Error>>(
        light: Light,
        mut find: impl FnMut(&[u8]) -> Result<Option<Signed>, E>,
    ) -> Result<Self, E> {
        let Header::Micro(header) = light.header else {
            return Err(Error::MicroExpected.into());
        };
        let mut entries = Vec::with_capacity(light.hashes.len());
        for hash in light.hashes {
            entries.push(match find(&hash)? {
                Some(tx) => Entry::Found(Box::new(tx)),
                None => Entry::Missing(hash),
            });
        }
        Ok(Self {
            header,
            proof: light.proof,
            entries,
        })
    }
    pub fn header(&self) -> &MicroHeader {
        &self.header
    }
    pub fn missing(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().filter_map(|e| match e {
            Entry::Missing(hash) => Some(hash.as_slice()),
            Entry::Found(_) => None,
        })
    }
    // All wire transactions, including surplus entries, must be decoded before
    // this call, as in the native response handler. An extra decoded tail is
    // ignored. A missing or out-of-order requested transaction fails assembly.
    pub fn fill(self, replies: Vec<Signed>) -> Result<MicroBlock, Error> {
        let mut replies = replies.into_iter();
        let mut transactions = Vec::with_capacity(self.entries.len());
        for entry in self.entries {
            transactions.push(match entry {
                Entry::Found(tx) => *tx,
                Entry::Missing(hash) => {
                    let tx = replies.next().ok_or(Error::Transactions)?;
                    if tx.hash().0.as_slice() != hash {
                        return Err(Error::Transactions);
                    }
                    tx
                }
            });
        }
        Ok(MicroBlock {
            header: self.header,
            transactions,
            proof: self.proof,
        })
    }
}
