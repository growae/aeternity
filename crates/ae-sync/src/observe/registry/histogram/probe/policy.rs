use super::Error;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Restart,
    Disable,
    Delete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Threshold {
    Always,
    Total(u64),
    Within { count: usize, milliseconds: u64 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub threshold: Threshold,
    pub action: Action,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy(pub Vec<Rule>);
impl Default for Policy {
    fn default() -> Self {
        Self(vec![
            Rule {
                threshold: Threshold::Within {
                    count: 3,
                    milliseconds: 60_000,
                },
                action: Action::Restart,
            },
            Rule {
                threshold: Threshold::Always,
                action: Action::Disable,
            },
        ])
    }
}
impl Policy {
    pub(super) fn validate(&self, history_limit: usize) -> Result<(), Error> {
        if self.0.len() > 64 || self.0.iter().any(|r| matches!(r.threshold, Threshold::Within { count, .. } if count >= history_limit)) { Err(Error::Limit) } else { Ok(()) }
    }
}
pub(super) struct History {
    policy: Policy,
    total: u64,
    times: VecDeque<i128>,
    max_time: Option<u64>,
    limit: usize,
}
impl History {
    pub fn new(policy: Policy, limit: usize) -> Self {
        let max_time = policy
            .0
            .iter()
            .filter_map(|r| match r.threshold {
                Threshold::Within { milliseconds, .. } => Some(milliseconds),
                _ => None,
            })
            .max();
        Self {
            policy,
            total: 0,
            times: VecDeque::new(),
            max_time,
            limit,
        }
    }
    pub fn failure(&mut self, now: i128) -> Result<Action, Error> {
        self.total = self.total.checked_add(1).ok_or(Error::Sequence)?;
        if let Some(max) = self.max_time {
            let oldest = now.saturating_sub(i128::from(max));
            self.times.retain(|t| *t > oldest);
            if self.times.len() >= self.limit {
                return Err(Error::Limit);
            }
            self.times.push_front(now);
        }
        for rule in &self.policy.0 {
            let matches = match rule.threshold {
                Threshold::Always => true,
                Threshold::Total(n) => n >= self.total,
                Threshold::Within {
                    count,
                    milliseconds,
                } => {
                    // Native within/3 always accepts the first event, even
                    // count=0. It scans a prefix, not all matching timestamps.
                    let oldest = now.saturating_sub(i128::from(milliseconds));
                    let more = self
                        .times
                        .iter()
                        .skip(1)
                        .take_while(|t| **t > oldest)
                        .count();
                    more == 0 || more < count
                }
            };
            if matches {
                return Ok(rule.action);
            }
        }
        Ok(Action::Delete)
    }
}
