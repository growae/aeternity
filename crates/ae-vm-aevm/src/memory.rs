// aevm_eeevm_memory, fa2a6d7. Copyright (c) 2017, Aeternity Anstalt.
// ISC: repository LICENSE. Sparse words and their logical high-water mark.
use crate::word::Word;
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Memory {
    words: BTreeMap<BigUint, Word>,
    size: BigUint,
}
impl Memory {
    pub fn size_in_words(&self) -> &BigUint {
        &self.size
    }
    pub fn words(&self) -> &BTreeMap<BigUint, Word> {
        &self.words
    }
    fn aligned(address: &BigUint) -> (BigUint, usize) {
        let offset = (address % 32u8).to_usize().expect("remainder below 32");
        (address - offset, offset)
    }
    fn extend(&mut self, aligned: &BigUint) {
        self.size = self.size.clone().max((aligned >> 5usize) + 1u8);
    }
    fn read(&self, aligned: &BigUint) -> Word {
        self.words.get(aligned).copied().unwrap_or(Word::ZERO)
    }
    fn write(&mut self, aligned: BigUint, word: Word) {
        self.extend(&aligned);
        if word.is_zero() {
            self.words.remove(&aligned);
        } else {
            self.words.insert(aligned, word);
        }
    }
    pub fn load(&mut self, address: &BigUint) -> Word {
        let (low, offset) = Self::aligned(address);
        if offset == 0 {
            self.extend(&low);
            return self.read(&low);
        }
        let high = &low + 32u8;
        let a = self.read(&low);
        let b = self.read(&high);
        self.extend(&high);
        let mut out = [0; 32];
        out[..32 - offset].copy_from_slice(&a.0[offset..]);
        out[32 - offset..].copy_from_slice(&b.0[..offset]);
        Word(out)
    }
    pub fn store(&mut self, address: &BigUint, word: Word) {
        self.write_area(address, &word.0);
    }
    pub fn store8(&mut self, address: &BigUint, byte: u8) {
        self.write_area(address, &[byte]);
    }
    pub fn write_area(&mut self, address: &BigUint, mut bytes: &[u8]) {
        let (mut aligned, mut offset) = Self::aligned(address);
        while !bytes.is_empty() {
            let n = (32 - offset).min(bytes.len());
            let mut word = self.read(&aligned);
            word.0[offset..offset + n].copy_from_slice(&bytes[..n]);
            self.write(aligned.clone(), word);
            bytes = &bytes[n..];
            aligned += 32u8;
            offset = 0;
        }
    }
    // Callers choose an operational byte allowance before allocating an area.
    // On exhaustion even the logical high-water mark is unchanged.
    pub fn area(
        &mut self,
        address: &BigUint,
        size: usize,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, crate::execution::Stop> {
        if size > maximum_bytes {
            return Err(crate::execution::Stop::ResourceLimit);
        }
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut out = vec![0; size];
        let (mut aligned, mut offset) = Self::aligned(address);
        let mut dest = 0;
        while dest < size {
            let n = (32 - offset).min(size - dest);
            let word = self.read(&aligned);
            out[dest..dest + n].copy_from_slice(&word.0[offset..offset + n]);
            self.extend(&aligned);
            dest += n;
            aligned += 32u8;
            offset = 0;
        }
        Ok(out)
    }
    pub(crate) fn allocated_bytes(&self) -> Option<usize> {
        let mut n = usize::try_from(self.size.bits().div_ceil(8)).ok()?;
        for address in self.words.keys() {
            n = n
                .checked_add(32)?
                .checked_add(usize::try_from(address.bits().div_ceil(8)).ok()?)?;
        }
        Some(n)
    }
    pub fn is_empty(&self) -> bool {
        self.size.is_zero()
    }
}
