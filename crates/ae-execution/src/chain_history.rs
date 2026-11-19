// aec_vm_chain / aefa_chain_api blockhash ancestry, fa2a6d7.
// Copyright (c) 2018-2019, Aeternity Anstalt. ISC: repository LICENSE.
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    NeedHeight(u64),
    NeedHeader([u8; 32]),
    NoCommonAncestor,
}
pub struct Selection {
    pub selected: [u8; 32],
    pub branch: [u8; 32],
}
#[derive(Debug, Clone, Copy)]
pub struct HeaderLink {
    pub height: u64,
    pub previous_key: [u8; 32],
}
// The caller supplies a coherent read snapshot of validated headers and the
// selected chain index. Missing data is retryable, never a made-up hash.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub selected: BTreeMap<u64, [u8; 32]>,
    pub headers: BTreeMap<[u8; 32], HeaderLink>,
}
struct Read<'a, F> {
    snapshot: &'a Snapshot,
    before_read: F,
}
impl<F: FnMut() -> Result<(), Error>> Read<'_, F> {
    fn header(&mut self, k: [u8; 32]) -> Result<HeaderLink, Error> {
        (self.before_read)()?;
        self.snapshot
            .headers
            .get(&k)
            .copied()
            .ok_or(Error::NeedHeader(k))
    }
    fn ancestor(&mut self, mut a: [u8; 32], mut b: [u8; 32]) -> Result<Option<[u8; 32]>, Error> {
        loop {
            if a == b {
                return Ok(Some(a));
            }
            let x = self.header(a)?;
            let y = self.header(b)?;
            if x.height == 0 && y.height == 0 {
                return Ok(None);
            }
            if x.height >= y.height {
                a = x.previous_key;
            }
            if y.height >= x.height {
                b = y.previous_key;
            }
        }
    }
    fn traverse(&mut self, h: u64, mut k: [u8; 32]) -> Result<[u8; 32], Error> {
        loop {
            let header = self.header(k)?;
            if header.height == h {
                return Ok(k);
            }
            if header.height.checked_sub(1) == Some(h) {
                return Ok(header.previous_key);
            }
            k = header.previous_key;
        }
    }
}
pub fn select(
    snapshot: &Snapshot,
    height: u64,
    key_hash: [u8; 32],
    before_read: impl FnMut() -> Result<(), Error>,
) -> Result<Selection, Error> {
    if key_hash == [0; 32] {
        return Ok(Selection {
            selected: key_hash,
            branch: key_hash,
        });
    }
    let hash = *snapshot
        .selected
        .get(&height)
        .ok_or(Error::NeedHeight(height))?;
    let mut read = Read {
        snapshot,
        before_read,
    };
    let branch = match read.ancestor(hash, key_hash)? {
        Some(ancestor) if ancestor == hash => hash,
        Some(_) => read.traverse(height, key_hash)?,
        None => return Err(Error::NoCommonAncestor),
    };
    Ok(Selection {
        selected: hash,
        branch,
    })
}
