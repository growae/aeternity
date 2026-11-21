use super::*;
use ae_types::Unsigned;

#[derive(Debug, Clone, PartialEq)]
pub enum Number {
    Integer(i128),
    Unsigned(Unsigned),
    Float(f64),
}
impl Number {
    pub fn text(&self) -> Result<String, Error> {
        Ok(match self {
            Self::Integer(n) => n.to_string(),
            Self::Unsigned(n) => {
                if n.as_bytes().len() > 512 {
                    return Err(Error::RecordTooLarge);
                }
                let mut n = n.clone();
                let mut digits = vec![];
                loop {
                    let (q, r) = n.div_rem_u64(10).map_err(|_| Error::Encoding)?;
                    digits.push(b'0' + r as u8);
                    if q == 0.into() {
                        break;
                    }
                    n = q;
                }
                digits.reverse();
                String::from_utf8(digits).map_err(|_| Error::Encoding)?
            }
            Self::Float(n) if n.is_finite() => float_text(*n),
            Self::Float(_) => return Err(Error::Encoding),
        })
    }
    fn json(&self) -> Result<Value, Error> {
        // Retain the unrounded value in the snapshot; only StatsD uses three
        // decimals, as aec_metrics:value does.
        match self {
            Self::Float(n) if n.is_finite() => Ok(json!(n)),
            _ => serde_json::from_str(&self.text()?).map_err(|_| Error::Encoding),
        }
    }
}
fn float_text(n: f64) -> String {
    // OTP 26 ~.3f first uses float_to_list's 21 significant digits, then
    // rounds the decimal mantissa half away from zero. Direct Rust .3 formatting
    // differs both at exact ties and above the 21-digit decimal boundary.
    let scientific = format!("{:.20e}", n.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let position = exponent.parse::<i32>().unwrap() + 1;
    let mut digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    let integer = if position <= 0 {
        let mut leading = vec![b'0'; (1 - position) as usize];
        leading.extend(digits);
        digits = leading;
        1
    } else {
        position as usize
    };
    let keep = integer + 3;
    digits.resize(digits.len().max(keep + 1), b'0');
    let mut carry = digits[keep] >= b'5';
    digits.truncate(keep);
    for digit in digits.iter_mut().rev() {
        if !carry {
            break;
        }
        if *digit == b'9' {
            *digit = b'0';
        } else {
            *digit += 1;
            carry = false;
        }
    }
    if carry {
        digits.insert(0, b'1');
    }
    digits.insert(digits.len() - 3, b'.');
    if n.is_sign_negative() {
        digits.insert(0, b'-');
    }
    String::from_utf8(digits).unwrap()
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Gauge,
    Counter,
    Timer,
    Histogram,
    Meter,
    Set,
}
impl Kind {
    fn code(self) -> &'static str {
        match self {
            Self::Gauge => "g",
            Self::Counter => "c",
            Self::Timer => "ms",
            Self::Histogram => "h",
            Self::Meter => "m",
            Self::Set => "s",
        }
    }
}
pub fn line(name: &str, value: &Number, kind: Kind) -> Result<Vec<u8>, Error> {
    if name.is_empty()
        || name.len() > 1024
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(Error::Encoding);
    }
    Ok(format!("{name}:{}|{}", value.text()?, kind.code()).into_bytes())
}
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: &'static str,
    pub value: Number,
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub chain_revision: u64,
    pub pool_chain_revision: u64,
    pub registry_revision: u64,
    pub metrics: Vec<Metric>,
}
impl Snapshot {
    pub fn get(&self, name: &str) -> Option<&Number> {
        self.metrics
            .iter()
            .find(|m| m.name == name)
            .map(|m| &m.value)
    }
    pub fn json(&self) -> Result<Value, Error> {
        let mut metrics = serde_json::Map::new();
        for m in &self.metrics {
            metrics.insert(m.name.into(), m.value.json()?);
        }
        Ok(
            json!({"chain_revision":self.chain_revision,"pool_chain_revision":self.pool_chain_revision,"registry_revision":self.registry_revision,"metrics":metrics}),
        )
    }
    pub fn datagrams(&self, prefix: &str) -> Result<Vec<Vec<u8>>, Error> {
        self.metrics
            .iter()
            .map(|m| {
                let name = if prefix.is_empty() {
                    m.name.into()
                } else {
                    format!("{prefix}.{}", m.name)
                };
                // Snapshots contain cumulative totals, not increments. Reporting
                // them as StatsD counters would double-count every interval.
                line(&name, &m.value, Kind::Gauge)
            })
            .collect()
    }
}
pub(super) struct Chain {
    pub revision: u64,
    pub height: u64,
    pub difficulty: Unsigned,
}
pub(super) fn chain(db: &Database) -> Result<Chain, Error> {
    let view = db.view().map_err(|_| Error::Storage)?;
    let local = peer::snapshot::local(&view, false).map_err(|_| Error::Storage)?;
    Ok(Chain {
        revision: view.status().revision,
        height: local.height,
        difficulty: local.difficulty,
    })
}
pub(super) fn snapshot(
    sources: &Sources,
    chain: Chain,
    registry: crate::registry::worker::Metrics,
) -> Snapshot {
    let n = sources.network.status();
    let p = sources.peer.status();
    let g = sources.gossip.status();
    let s = sources.sync.status();
    let pool = sources.admission.info();
    let c = registry.registry.counts;
    let mut metrics = vec![];
    let mut add = |name, value| {
        metrics.push(Metric {
            name,
            value: Number::Integer(value),
        })
    };
    add("ae.epoch.aecore.chain.height.value", chain.height.into());
    add(
        "ae.epoch.aecore.peers.count.value",
        (c.inbound + c.outbound) as i128,
    );
    add("ae.epoch.aecore.peers.inbound.value", c.inbound as i128);
    add("ae.epoch.aecore.peers.outbound.value", c.outbound as i128);
    add("ae.epoch.aecore.peers.blocked.value", c.blocked as i128);
    add(
        "ae.epoch.aecore.peers.verified.value",
        registry.verified as i128,
    );
    add(
        "ae.epoch.aecore.peers.unverified.value",
        registry.unverified as i128,
    );
    add(
        "ae.epoch.aecore.peers.errored.value",
        registry.standby as i128,
    );
    add(
        "ae.epoch.aecore.peers.ping.success.value",
        n.counts.pings_completed.into(),
    );
    add(
        "ae.epoch.aecore.peers.ping.failure.value",
        n.counts.pings_failed.into(),
    );
    add(
        "ae.epoch.aecore.tx_pool.pending_txs",
        (pool.pool.active + pool.pool.visited) as i128,
    );
    add("ae.rust.network.sockets", n.sockets as i128);
    add("ae.rust.network.resolutions", n.resolutions as i128);
    add("ae.rust.network.ready", i128::from(n.ready));
    add("ae.rust.network.running", i128::from(n.running));
    add("ae.rust.peer.running", i128::from(p.running));
    add("ae.rust.peer.served", p.counts.served.into());
    add(
        "ae.rust.peer.failed_connections",
        p.counts.failed_connections.into(),
    );
    add("ae.rust.gossip.running", i128::from(g.running));
    add("ae.rust.gossip.admitted", g.counts.ordinary_admitted.into());
    add("ae.rust.gossip.rejected", g.counts.rejected.into());
    add("ae.rust.gossip.sent", g.counts.sent.into());
    add("ae.rust.gossip.send_failed", g.counts.send_failed.into());
    add("ae.rust.pool.running", i128::from(pool.running));
    add("ae.rust.pool.queued", pool.queued as i128);
    add(
        "ae.rust.pool.retained_request_bytes",
        pool.retained_request_bytes as i128,
    );
    add("ae.rust.sync.running", i128::from(s.running));
    add("ae.rust.sync.active", s.active.len() as i128);
    add("ae.rust.sync.queued", s.queued as i128);
    add("ae.rust.sync.completed", s.counts.finished.into());
    add("ae.rust.sync.failed", s.counts.failed.into());
    add("ae.rust.sync.cancelled", s.counts.cancelled.into());
    add(
        "ae.rust.sync.peer_completions",
        s.lifecycle.peer_completions.into(),
    );
    add(
        "ae.rust.sync.transitions",
        s.lifecycle.syncing_transitions.into(),
    );
    add(
        "ae.rust.sync.blocks.committed",
        s.blocks.counts.committed.into(),
    );
    add("ae.rust.sync.blocks.failed", s.blocks.counts.failed.into());
    metrics.push(Metric {
        name: "ae.epoch.aecore.chain.total_difficulty",
        value: Number::Unsigned(chain.difficulty),
    });
    if let Ok(progress) = s.lifecycle.progress {
        metrics.push(Metric {
            name: "ae.epoch.aecore.sync.progress.value",
            value: Number::Float(progress.percent),
        });
        metrics.push(Metric {
            name: "ae.rust.sync.active_chain",
            value: Number::Integer(i128::from(progress.syncing)),
        });
    }
    Snapshot {
        chain_revision: chain.revision,
        pool_chain_revision: pool.pool.chain.revision,
        registry_revision: registry.registry.policy_revision,
        metrics,
    }
}
