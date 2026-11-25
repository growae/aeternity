// Listener scheduling follows Ranch a692f445, pinned by aeternity fa2a6d7.
// A connection is counted before its acceptor is paused at the threshold.
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub acceptors: usize,
    pub maximum_connections: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            acceptors: 10,
            maximum_connections: 1000,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Acceptor,
    Paused,
    Duplicate,
}
impl Config {
    pub fn validate(self) -> Result<(), Error> {
        // Independent bound on scheduler metadata, not a peer policy limit.
        if self.acceptors > 65_536 {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub struct Listener<T: Ord> {
    config: Config,
    active: BTreeSet<T>,
    paused: Vec<bool>,
    sleeping: Vec<usize>,
}
impl<T: Ord> Listener<T> {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config,
            active: BTreeSet::new(),
            paused: vec![false; config.acceptors],
            sleeping: Vec::new(),
        })
    }
    pub fn config(&self) -> Config {
        self.config
    }
    pub fn active(&self) -> usize {
        self.active.len()
    }
    // Oldest sleeper first, as an inspectable representation of Ranch's stack.
    pub fn sleeping(&self) -> &[usize] {
        &self.sleeping
    }
    pub fn available(&self) -> Option<usize> {
        self.paused.iter().position(|p| !p)
    }
    pub fn started(&mut self, acceptor: usize, connection: T) -> Result<(), Error> {
        match self.paused.get(acceptor) {
            None => return Err(Error::Acceptor),
            Some(true) => return Err(Error::Paused),
            Some(false) => (),
        }
        if !self.active.insert(connection) {
            return Err(Error::Duplicate);
        }
        if self.active.len() >= self.config.maximum_connections {
            self.paused[acceptor] = true;
            self.sleeping.push(acceptor);
        }
        Ok(())
    }
    pub fn closed(&mut self, connection: &T) -> bool {
        let count = self.active.len();
        if !self.active.remove(connection) {
            return false;
        }
        if count <= self.config.maximum_connections
            && let Some(acceptor) = self.sleeping.pop()
        {
            self.paused[acceptor] = false;
        }
        true
    }
    pub fn set_maximum(&mut self, maximum: usize) {
        if maximum > self.config.maximum_connections {
            for acceptor in self.sleeping.drain(..) {
                self.paused[acceptor] = false;
            }
        }
        self.config.maximum_connections = maximum;
    }
}
