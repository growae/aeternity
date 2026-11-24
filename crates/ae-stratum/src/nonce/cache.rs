use super::{Kind, Part};
use crate::Error;
use std::collections::BTreeSet;

pub const ATTEMPTS: usize = 50;

// The native cache keys numeric values, not (width, value) pairs. Entropy is an
// explicit input; an exhausted random search is distinct from a full namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cache {
    values: BTreeSet<u64>,
    maximum: usize,
}
impl Cache {
    pub fn new(maximum: usize) -> Result<Self, Error> {
        if maximum == 0 || maximum > 1_000_000 {
            return Err(Error::Configuration);
        }
        Ok(Self {
            values: BTreeSet::new(),
            maximum,
        })
    }
    pub fn values(&self) -> &BTreeSet<u64> {
        &self.values
    }
    pub fn allocate(
        &mut self,
        bytes: u8,
        mut draw: impl FnMut(u64) -> Result<u64, Error>,
    ) -> Result<Option<Part>, Error> {
        let maximum = Part::maximum(bytes)?;
        if self.values.len() >= self.maximum {
            return Err(Error::Limit("nonce cache"));
        }
        for _ in 0..ATTEMPTS {
            let value = draw(maximum)?;
            let nonce = Part::new(Kind::Extra, value, bytes)?;
            if self.values.insert(value) {
                return Ok(Some(nonce));
            }
        }
        Ok(None)
    }
    pub fn free(&mut self, nonce: Part) {
        self.values.remove(&nonce.value());
    }
}
