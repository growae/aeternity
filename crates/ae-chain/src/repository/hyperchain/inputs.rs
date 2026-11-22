// Immutable inputs for aec_consensus_hc entropy/pin reads. No service client
// or mutable selected tip is retained by deterministic block execution.
use super::*;
use ae_hyperchain::{
    block::{Host, Pin},
    parent_block::ParentBlock,
    parent_cache::Cache,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    RequestParent(BigInt),
    Parent { epoch: BigInt, block: ParentBlock },
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_entropy_entries: usize,
    pub maximum_entropy_bytes: usize,
    pub maximum_effects: usize,
    pub maximum_effect_bytes: usize,
    pub maximum_pin_entries: usize,
    pub maximum_pin_bytes: usize,
    pub maximum_integer_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_entropy_entries: 32,
            maximum_entropy_bytes: 16 * 1024 * 1024,
            maximum_effects: 4096,
            maximum_effect_bytes: 16 * 1024 * 1024,
            maximum_pin_entries: 4096,
            maximum_pin_bytes: 16 * 1024 * 1024,
            maximum_integer_bytes: 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InputError {
    ResourceLimit,
    EntropyNotCaptured(BigInt),
    InvalidEntropy,
    PinAlreadyCaptured,
    PinNotCaptured(Vec<u8>),
    Selection(Error),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Effects {
    entries: Vec<Effect>,
    bytes: usize,
}
impl Effects {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn entries(&self) -> &[Effect] {
        &self.entries
    }
    pub fn drain(&mut self) -> Vec<Effect> {
        self.bytes = 0;
        std::mem::take(&mut self.entries)
    }
    fn push(&mut self, effect: Effect, limits: Limits) -> Result<(), InputError> {
        let mut size = 1usize;
        let mut add = |n: usize| -> Result<(), InputError> {
            size = size.checked_add(n).ok_or(InputError::ResourceLimit)?;
            Ok(())
        };
        match &effect {
            Effect::RequestParent(h) => add(integer_size(h))?,
            Effect::Parent { epoch, block } => {
                for n in [
                    integer_size(epoch),
                    block.hash.len(),
                    block.previous_hash.as_ref().map_or(0, Vec::len),
                    block.height.as_bytes().len(),
                    block.time.as_bytes().len(),
                ] {
                    add(n)?;
                }
            }
        }
        let bytes = self
            .bytes
            .checked_add(size)
            .ok_or(InputError::ResourceLimit)?;
        if self.entries.len() >= limits.maximum_effects || bytes > limits.maximum_effect_bytes {
            return Err(InputError::ResourceLimit);
        }
        self.entries.push(effect);
        self.bytes = bytes;
        Ok(())
    }
    fn parent(
        &mut self,
        epoch: &BigInt,
        block: &ParentBlock,
        limits: Limits,
    ) -> Result<(), InputError> {
        let mut bytes = self.bytes.checked_add(1).ok_or(InputError::ResourceLimit)?;
        for n in [
            integer_size(epoch),
            block.hash.len(),
            block.previous_hash.as_ref().map_or(0, Vec::len),
            block.height.as_bytes().len(),
            block.time.as_bytes().len(),
        ] {
            bytes = bytes.checked_add(n).ok_or(InputError::ResourceLimit)?;
        }
        if self.entries.len() >= limits.maximum_effects || bytes > limits.maximum_effect_bytes {
            return Err(InputError::ResourceLimit);
        }
        self.entries.push(Effect::Parent {
            epoch: epoch.clone(),
            block: block.clone(),
        });
        self.bytes = bytes;
        Ok(())
    }
}
fn integer_size(n: &BigInt) -> usize {
    usize::try_from(n.bits().div_ceil(8))
        .unwrap_or(usize::MAX)
        .saturating_add(1)
}

// None is an explicitly captured unsuccessful native pin lookup. An absent
// entry is an operational input miss, never an invented incorrect pin proof.
#[derive(Debug, Clone, Default)]
pub struct Pins {
    entries: BTreeMap<Vec<u8>, Option<Pin>>,
    bytes: usize,
}
impl Pins {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn insert(
        &mut self,
        hash: Vec<u8>,
        pin: Option<Pin>,
        limits: Limits,
    ) -> Result<(), InputError> {
        fn size(hash: &[u8], pin: &Option<Pin>) -> Option<usize> {
            let mut size = hash.len().checked_add(1)?;
            if let Some(pin) = pin {
                for n in [
                    integer_size(&pin.epoch),
                    integer_size(&pin.child_height),
                    pin.child_hash.len(),
                    integer_size(&pin.parent_height),
                ] {
                    size = size.checked_add(n)?;
                }
            }
            Some(size)
        }
        if pin.as_ref().is_some_and(|p| {
            [&p.epoch, &p.child_height, &p.parent_height]
                .iter()
                .any(|n| integer_size(n) > limits.maximum_integer_bytes)
        }) {
            return Err(InputError::ResourceLimit);
        }
        let old = self.entries.get(&hash);
        let old_bytes = old
            .map_or(Some(0), |p| size(&hash, p))
            .ok_or(InputError::ResourceLimit)?;
        let bytes = self
            .bytes
            .checked_sub(old_bytes)
            .and_then(|n| n.checked_add(size(&hash, &pin)?))
            .ok_or(InputError::ResourceLimit)?;
        if (old.is_none() && self.entries.len() >= limits.maximum_pin_entries)
            || bytes > limits.maximum_pin_bytes
        {
            return Err(InputError::ResourceLimit);
        }
        self.entries.insert(hash, pin);
        self.bytes = bytes;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Captured<S> {
    child: View<S>,
    cache: Arc<Cache>,
    pins: Arc<Pins>,
    parent_start: Unsigned,
    parent_length: Unsigned,
    effects: Effects,
    limits: Limits,
    entropy: Option<BTreeMap<BigInt, Option<ParentBlock>>>,
    entropy_bytes: usize,
}
impl<S: Read> Captured<S> {
    pub fn new(
        child: View<S>,
        cache: Arc<Cache>,
        pins: Arc<Pins>,
        parent_start: Unsigned,
        parent_length: Unsigned,
        effects: Effects,
        limits: Limits,
    ) -> Result<Self, InputError> {
        if child.identity().consensus != ConsensusType::Pos {
            return Err(InputError::Selection(Error::Identity));
        }
        if parent_start.as_bytes().len() > limits.maximum_integer_bytes
            || parent_length.as_bytes().len() > limits.maximum_integer_bytes
            || pins.entries.len() > limits.maximum_pin_entries
            || pins.bytes > limits.maximum_pin_bytes
            || effects.entries.len() > limits.maximum_effects
            || effects.bytes > limits.maximum_effect_bytes
        {
            return Err(InputError::ResourceLimit);
        }
        if pins.entries.values().flatten().any(|pin| {
            [&pin.epoch, &pin.child_height, &pin.parent_height]
                .into_iter()
                .any(|n| integer_size(n) > limits.maximum_integer_bytes)
        }) {
            return Err(InputError::ResourceLimit);
        }
        Ok(Self {
            child,
            cache,
            pins,
            parent_start,
            parent_length,
            effects,
            limits,
            entropy: None,
            entropy_bytes: 0,
        })
    }
    // Enable demand-driven acquisition by the insertion owner. Execution
    // remains deterministic: a miss carries the request out as an error.
    pub fn require_entropy(&mut self) {
        self.entropy.get_or_insert_with(BTreeMap::new);
    }
    pub fn capture_entropy(
        &mut self,
        height: BigInt,
        block: Option<ParentBlock>,
    ) -> Result<(), InputError> {
        let entries = self.entropy.as_mut().ok_or(InputError::InvalidEntropy)?;
        if entries.contains_key(&height)
            || block
                .as_ref()
                .is_some_and(|b| BigInt::from_bytes_be(Sign::Plus, b.height.as_bytes()) != height)
        {
            return Err(InputError::InvalidEntropy);
        }
        let mut bytes = self
            .entropy_bytes
            .checked_add(integer_size(&height))
            .and_then(|n| n.checked_add(1))
            .ok_or(InputError::ResourceLimit)?;
        if let Some(block) = &block {
            for size in [
                block.hash.len(),
                block.previous_hash.as_ref().map_or(0, Vec::len),
                block.height.as_bytes().len(),
                block.time.as_bytes().len(),
            ] {
                bytes = bytes.checked_add(size).ok_or(InputError::ResourceLimit)?;
            }
        }
        if integer_size(&height) > self.limits.maximum_integer_bytes
            || entries.len() >= self.limits.maximum_entropy_entries
            || bytes > self.limits.maximum_entropy_bytes
        {
            return Err(InputError::ResourceLimit);
        }
        entries.insert(height, block);
        self.entropy_bytes = bytes;
        Ok(())
    }
    pub fn revision(&self) -> u64 {
        self.child.status().revision
    }
    pub fn capture_pin(&mut self, hash: Vec<u8>, pin: Option<Pin>) -> Result<(), InputError> {
        if self.pins.entries.contains_key(&hash) {
            return Err(InputError::PinAlreadyCaptured);
        }
        Arc::make_mut(&mut self.pins).insert(hash, pin, self.limits)
    }
    pub fn effects(&self) -> &Effects {
        &self.effects
    }
    pub fn into_effects(self) -> Effects {
        self.effects
    }
}
impl<S: Read> Host for Captured<S> {
    type Error = InputError;
    fn is_operational(_: &InputError) -> bool {
        true
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, InputError> {
        if integer_size(epoch) > self.limits.maximum_integer_bytes {
            return Err(InputError::ResourceLimit);
        }
        let height = (epoch - BigInt::from(3)).max(0.into())
            * BigInt::from_bytes_be(Sign::Plus, self.parent_length.as_bytes())
            + BigInt::from_bytes_be(Sign::Plus, self.parent_start.as_bytes());
        if integer_size(&height) > self.limits.maximum_integer_bytes {
            return Err(InputError::ResourceLimit);
        }
        let block = match self.cache.lookup(&height) {
            Ok(block) => Some(block),
            Err(_) => match &self.entropy {
                None => None,
                Some(entries) => match entries.get(&height) {
                    Some(block) => block.as_ref(),
                    None => {
                        if entries.len() >= self.limits.maximum_entropy_entries {
                            return Err(InputError::ResourceLimit);
                        }
                        return Err(InputError::EntropyNotCaptured(height));
                    }
                },
            },
        };
        match block {
            Some(block) => {
                // Bound before cloning a block retained by the cache owner.
                self.effects.parent(epoch, block, self.limits)?;
                let hash = block.hash.clone();
                Ok(Some(hash))
            }
            None => {
                self.effects
                    .push(Effect::RequestParent(height), self.limits)?;
                Ok(None)
            }
        }
    }
    fn pin(&mut self, hash: &[u8]) -> Result<Option<Pin>, InputError> {
        if hash.len() > self.limits.maximum_pin_bytes {
            return Err(InputError::ResourceLimit);
        }
        match self.pins.entries.get(hash) {
            Some(pin) => Ok(pin.clone()),
            None if self.pins.entries.len() >= self.limits.maximum_pin_entries => {
                Err(InputError::ResourceLimit)
            }
            None => Err(InputError::PinNotCaptured(hash.to_vec())),
        }
    }
    fn key_hash(&mut self, height: &BigInt) -> Result<Option<Vec<u8>>, InputError> {
        self.child
            .hyperchain_key_hash(height)
            .map(|h| h.map(|h| h.0.to_vec()))
            .map_err(InputError::Selection)
    }
}
