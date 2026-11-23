// aehttpc_{aeternity,btc,doge} header reads, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::parent_pins::{self, transport::Transport};
use ae_hyperchain::parent_connector::fetch::{self, BackendError, Context, Query, Remote};
pub use parent_pins::{Chain, Node};
use std::{
    sync::{Arc, Mutex, TryLockError},
    time::Duration,
};
mod lookup;

#[derive(Debug, Clone)]
pub struct Config {
    pub maximum_hosts: usize,
    pub request_timeout: Duration,
    pub maximum_hash_bytes: usize,
    pub maximum_number_digits: usize,
    pub maximum_header_bytes: usize,
    pub maximum_body_bytes: usize,
    pub public_roots: bool,
    pub roots: Vec<Vec<u8>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            maximum_hosts: 32,
            request_timeout: Duration::from_secs(5),
            maximum_hash_bytes: 4096,
            maximum_number_digits: 4096,
            maximum_header_bytes: 16 * 1024,
            maximum_body_bytes: 4 * 1024 * 1024,
            public_roots: true,
            roots: vec![],
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Spawn,
}
struct Host {
    node: Node,
    transport: Transport,
    runtime: Mutex<tokio::runtime::Runtime>,
}
// The connector's fixed fetch workers own execution. Each configured host
// admits one operation, including across multiple callers sharing this backend.
// No task or replacement thread escapes the connector's join boundary.
pub struct Backend {
    hosts: Vec<Host>,
    config: Config,
}
impl Backend {
    pub fn new(nodes: Vec<Node>, config: Config) -> Result<Self, Error> {
        if nodes.is_empty()
            || nodes
                .iter()
                .any(|n| n.chain != Chain::Aeternity && n.credentials.is_none())
        {
            return Err(Error::Configuration);
        }
        Self::configured(nodes, config)
    }
    pub fn configured(nodes: Vec<Node>, config: Config) -> Result<Self, Error> {
        if !(1..=32).contains(&config.maximum_hosts)
            || nodes.len() > config.maximum_hosts
            || nodes.iter().any(|n| n.chain != nodes[0].chain)
            || config.request_timeout.is_zero()
            || config.request_timeout > Duration::from_secs(60)
            || !(1..=65536).contains(&config.maximum_hash_bytes)
            || !(1..=65536).contains(&config.maximum_number_digits)
            || !(512..=65536).contains(&config.maximum_header_bytes)
            || !(1024..=16 * 1024 * 1024).contains(&config.maximum_body_bytes)
            || config.roots.len() > 32
            || config.roots.iter().any(|r| r.len() > 65536)
        {
            return Err(Error::Configuration);
        }
        let transport_config = Arc::new(parent_pins::Config {
            request_timeout: config.request_timeout,
            maximum_header_bytes: config.maximum_header_bytes,
            maximum_body_bytes: config.maximum_body_bytes,
            public_roots: config.public_roots,
            roots: config.roots.clone(),
            ..Default::default()
        });
        let hosts = nodes
            .into_iter()
            .map(|node| {
                Ok(Host {
                    node,
                    transport: Transport::new(transport_config.clone())
                        .map_err(|_| Error::Configuration)?,
                    runtime: Mutex::new(
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|_| Error::Spawn)?,
                    ),
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self { hosts, config })
    }
    pub fn hosts(&self) -> usize {
        self.hosts.len()
    }
}
impl fetch::Backend for Backend {
    fn fetch(
        &self,
        host: usize,
        query: &Query,
        seed: &[u8; 16],
        context: &Context,
    ) -> Result<Remote, BackendError> {
        if context.cancelled() {
            return Err(BackendError::Unavailable);
        }
        let host = self.hosts.get(host).ok_or(BackendError::Unavailable)?;
        let runtime = host.runtime.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => BackendError::ResourceLimit,
            TryLockError::Poisoned(_) => BackendError::Unavailable,
        })?;
        runtime.block_on(async {
            tokio::select! {
                biased;
                _ = async {
                    while !context.cancelled() {
                        tokio::time::sleep(context.remaining().min(Duration::from_millis(10))).await;
                    }
                } => Err(BackendError::Unavailable),
                result = lookup::fetch(&host.transport, &host.node, &self.config, query, seed) => result,
            }
        })
    }
}
