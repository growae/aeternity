use super::{Config, Error, SubmissionLimits};
use crate::configuration::http::startup::Startup;
use std::{net::SocketAddr, time::Duration};

// Rust service budgets, independent of native endpoint registration and
// transport getters. The current transport closes after one request; its
// deadline is not Cowboy's idle_timeout, nor its body budget max_skip_body_length.
#[derive(Debug, Clone)]
pub struct Limits {
    pub connections: usize,
    pub header_bytes: usize,
    pub body_bytes: usize,
    pub submissions: SubmissionLimits,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        let config = Config::default();
        Self {
            connections: config.connections,
            header_bytes: config.header_bytes,
            body_bytes: config.body_bytes,
            submissions: config.submissions,
            timeout: config.timeout,
        }
    }
}
impl Startup {
    pub fn configs(&self, limits: &Limits) -> Result<Vec<Config>, Error> {
        self.endpoints
            .iter()
            .map(|endpoint| {
                let port = endpoint
                    .port
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok())
                    .ok_or(Error::Configuration)?;
                let acceptors = endpoint
                    .acceptors
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or(Error::Configuration)?;
                let config = Config {
                    listen: SocketAddr::new(endpoint.address, port),
                    target: endpoint.target,
                    groups: self.groups.clone(),
                    cors: self.cors.clone(),
                    acceptors,
                    connections: limits.connections,
                    header_bytes: limits.header_bytes,
                    body_bytes: limits.body_bytes,
                    submissions: limits.submissions.clone(),
                    timeout: limits.timeout,
                };
                config.validate()?;
                Ok(config)
            })
            .collect()
    }
}
