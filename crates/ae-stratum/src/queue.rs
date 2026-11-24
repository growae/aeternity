use crate::Error;
use std::collections::VecDeque;

pub const MAX_CAPACITY: usize = 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queue<K, V> {
    capacity: usize,
    entries: VecDeque<(K, V)>,
}
impl<K: Eq, V> Queue<K, V> {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::Configuration);
        }
        if capacity > MAX_CAPACITY {
            return Err(Error::Limit("jobs"));
        }
        Ok(Self {
            capacity,
            entries: VecDeque::new(),
        })
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(K, V)> {
        self.entries.iter()
    }
    pub fn front(&self) -> Option<&(K, V)> {
        self.entries.front()
    }
    pub fn rear(&self) -> Option<&(K, V)> {
        self.entries.back()
    }
    pub fn find(&self, key: &K) -> Option<&V> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
    pub fn push(&mut self, key: K, value: V) -> Option<(K, V)> {
        let removed = if self.is_full() {
            self.entries.pop_front()
        } else {
            None
        };
        self.entries.push_back((key, value));
        removed
    }
    pub fn pop(&mut self) -> Option<(K, V)> {
        self.entries.pop_front()
    }
    // Native keyreplace replaces every duplicate key; find returns the first.
    pub fn replace(&mut self, key: &K, value: V)
    where
        V: Clone,
    {
        for (k, v) in &mut self.entries {
            if k == key {
                *v = value.clone();
            }
        }
    }
}
