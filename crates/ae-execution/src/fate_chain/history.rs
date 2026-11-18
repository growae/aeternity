// aefa_chain_api:blockhash / traverse_to_key_hash, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use ae_vm_fate::{
    machine::{Exception, program::remote::HostFailure},
    types::Version,
};
use std::collections::BTreeMap;

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
struct Read<'a> {
    snapshot: &'a Snapshot,
    remaining: usize,
}
impl Read<'_> {
    fn header(&mut self, k: [u8; 32]) -> Result<HeaderLink, HostFailure<Error>> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(HostFailure::ResourceLimit)?;
        self.snapshot
            .headers
            .get(&k)
            .copied()
            .ok_or(HostFailure::Backend(Error::NeedHeader(k)))
    }
    fn ancestor(
        &mut self,
        mut a: [u8; 32],
        mut b: [u8; 32],
    ) -> Result<Option<[u8; 32]>, HostFailure<Error>> {
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
    fn traverse(&mut self, h: u64, mut k: [u8; 32]) -> Result<[u8; 32], HostFailure<Error>> {
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
pub fn block_hash(
    snapshot: &Snapshot,
    height: u64,
    key_hash: [u8; 32],
    vm: Version,
    maximum_reads: usize,
) -> Result<[u8; 32], HostFailure<Error>> {
    if key_hash == [0; 32] {
        return Ok(key_hash);
    }
    let hash = *snapshot
        .selected
        .get(&height)
        .ok_or(HostFailure::Backend(Error::NeedHeight(height)))?;
    let mut read = Read {
        snapshot,
        remaining: maximum_reads,
    };
    match read.ancestor(hash, key_hash)? {
        Some(ancestor) if ancestor == hash => Ok(hash),
        Some(_) => {
            let branch = read.traverse(height, key_hash)?;
            if vm <= Version::Two && hash != branch {
                Err(HostFailure::Exception(Exception::Native("badmatch")))
            } else {
                Ok(branch)
            }
        }
        None => Err(HostFailure::Exception(Exception::Native("case_clause"))),
    }
}
