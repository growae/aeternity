use super::*;
use rand::Rng;
use std::sync::{Arc, Mutex};

const RECORD_LIMIT: usize = 32;
#[derive(Debug, Clone)]
struct Destination {
    gateway: Gateway,
    wire: Ports,
    mapping: Option<Mapping>,
    uncertain: bool,
}
#[derive(Debug, Clone)]
struct Record {
    id: u64,
    network: Network,
    gateway: Gateway,
    configured: Ports,
    wire: Ports,
    description: String,
    mapping: Option<Mapping>,
    destinations: Vec<Destination>,
    retiring: bool,
}
impl Record {
    fn uncertain(&self) -> bool {
        self.destinations.iter().any(|d| d.uncertain)
    }
}
#[derive(Debug)]
struct State {
    ports: Ports,
    records: Vec<Record>,
    next: u64,
    active: bool,
}
impl State {
    fn retained(&self) -> usize {
        self.records.iter().map(|r| r.destinations.len()).sum()
    }
}

// Host-owned runtime ownership survives child replacement, not process loss.
// Every distinct write destination is charged against the shared 32-slot limit.
#[derive(Debug, Clone)]
pub struct PortsSource(Arc<Mutex<State>>);
impl PortsSource {
    pub fn new(ports: Ports) -> Result<Self, Error> {
        ports.validate()?;
        Ok(Self(Arc::new(Mutex::new(State {
            ports,
            records: vec![],
            next: 1,
            active: false,
        }))))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn get(&self) -> Ports {
        self.lock().ports
    }
    pub fn set(&self, ports: Ports) -> Result<(), Error> {
        ports.validate()?;
        self.lock().ports = ports;
        Ok(())
    }
    pub fn retained_mappings(&self) -> usize {
        self.lock().retained()
    }
    pub fn uncertain_mappings(&self) -> usize {
        self.lock()
            .records
            .iter()
            .map(|r| {
                r.destinations
                    .iter()
                    .filter(|d| d.uncertain || r.retiring)
                    .count()
            })
            .sum()
    }
    pub fn retiring_mappings(&self) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|r| r.retiring)
            .map(|r| r.destinations.len())
            .sum()
    }
    pub(super) fn claim(&self) -> Result<Claim, Error> {
        let mut state = self.lock();
        if state.active {
            return Err(Error::Busy);
        }
        state.active = true;
        Ok(Claim(self.clone()))
    }
    fn find(&self, gateway: &Gateway, configured: Ports) -> Option<Record> {
        self.lock()
            .records
            .iter()
            .find(|r| r.gateway == *gateway && r.configured == configured)
            .cloned()
    }
    fn begin(
        &self,
        network: Network,
        gateway: Gateway,
        configured: Ports,
    ) -> Result<Record, Error> {
        let mut state = self.lock();
        if let Some(index) = state
            .records
            .iter()
            .position(|r| r.gateway == gateway && r.configured == configured)
        {
            if state.records[index].uncertain() || state.records[index].retiring {
                return Err(Error::Busy);
            }
            let mut record = state.records.remove(index);
            if let Some(mapping) = &record.mapping {
                record.wire.external = mapping.external;
            }
            state.records.push(record.clone());
            return Ok(record);
        }
        if state.retained() >= RECORD_LIMIT {
            return Err(Error::Limit);
        }
        let id = state.next;
        state.next = state.next.checked_add(1).ok_or(Error::Limit)?;
        let token: [u8; 16] = rand::random();
        let mut description = String::from("aeternity:");
        for byte in token {
            use std::fmt::Write;
            write!(&mut description, "{byte:02x}").expect("string append");
        }
        let wire = if configured.external == 0 && gateway.protocol() != Protocol::NatPmp {
            Ports {
                external: rand::thread_rng().gen_range(10001..=65535),
                ..configured
            }
        } else {
            configured
        };
        let record = Record {
            id,
            network,
            gateway: gateway.clone(),
            configured,
            wire,
            description,
            mapping: None,
            retiring: false,
            destinations: vec![Destination {
                gateway,
                wire,
                mapping: None,
                uncertain: true,
            }],
        };
        state.records.push(record.clone());
        Ok(record)
    }
    fn track(&self, id: u64, gateway: &Gateway, wire: Ports, adding: bool) -> Result<(), Error> {
        let mut state = self.lock();
        let full = state.retained() >= RECORD_LIMIT;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.id == id)
            .ok_or(Error::Closed)?;
        if adding && record.retiring {
            return Err(Error::Closed);
        }
        if let Some(destination) = record
            .destinations
            .iter_mut()
            .find(|d| d.gateway == *gateway)
        {
            // v1 cannot recover multiple unknown external ports by enumeration.
            if gateway.protocol() == Protocol::Upnp1 && destination.wire != wire {
                return Err(Error::Response);
            }
            destination.wire = wire;
            destination.uncertain = true;
        } else {
            if full {
                return Err(Error::Limit);
            }
            record.destinations.push(Destination {
                gateway: gateway.clone(),
                wire,
                mapping: None,
                uncertain: true,
            });
        }
        Ok(())
    }
    fn confirm(&self, id: u64, gateway: &Gateway, mapping: Mapping) {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|r| r.id == id)
            .expect("owned mapping intent");
        let destination = record
            .destinations
            .iter_mut()
            .find(|d| d.gateway == *gateway)
            .expect("recorded write destination");
        destination.uncertain = gateway.protocol() == Protocol::Upnp2
            && destination
                .mapping
                .as_ref()
                .is_some_and(|old| old.external != mapping.external);
        destination.mapping = Some(mapping.clone());
        record.mapping = Some(mapping);
        // Earlier redirect destinations remain uncertain: a server can have
        // applied the request before responding with a redirect.
    }
    fn remove_destination(&self, id: u64, gateway: &Gateway) {
        let mut state = self.lock();
        if let Some(record) = state.records.iter_mut().find(|r| r.id == id) {
            record.destinations.retain(|d| d.gateway != *gateway);
        }
        state.records.retain(|r| !r.destinations.is_empty());
    }
    pub(super) async fn add_ports(
        &self,
        network: Network,
        gateway: Gateway,
        configured: Ports,
    ) -> Result<Mapping, Error> {
        configured.validate()?;
        if let Some(record) = self
            .find(&gateway, configured)
            .filter(|r| r.uncertain() || r.retiring)
        {
            self.clean_record(record.id).await?;
        }
        let attempts = if configured.external == 0 && gateway.protocol() != Protocol::NatPmp {
            3
        } else {
            1
        };
        for attempt in 0..attempts {
            let record = self.begin(network.clone(), gateway.clone(), configured)?;
            match record
                .network
                .add_tracked(
                    &record.gateway,
                    record.wire,
                    Some(&record.description),
                    &mut |at, ports| self.track(record.id, at, ports, true),
                )
                .await
            {
                Ok((mapping, actual)) => {
                    self.confirm(record.id, &actual, mapping.clone());
                    return Ok(mapping);
                }
                Err(error) if attempt + 1 == attempts => return Err(error),
                Err(_) => self.clean_record(record.id).await?,
            }
        }
        unreachable!("at least one mapping attempt")
    }
    async fn clean_record(&self, id: u64) -> Result<(), Error> {
        if let Some(record) = self.lock().records.iter_mut().find(|r| r.id == id) {
            record.retiring = true;
        }
        // Redirected deletes can add destinations. Read the live ledger after
        // every successful retirement, and bound changing redirect graphs.
        for _ in 0..64 {
            let record = self.lock().records.iter().find(|r| r.id == id).cloned();
            let Some(record) = record else {
                return Ok(());
            };
            let destination = record
                .destinations
                .last()
                .expect("nonempty ownership record");
            let known = match destination.gateway.protocol() {
                Protocol::Upnp1 => Some(destination.wire.external),
                Protocol::Upnp2 if !destination.uncertain => {
                    destination.mapping.as_ref().map(|m| m.external)
                }
                _ => None,
            };
            let ports = record
                .network
                .owned_ports(
                    &destination.gateway,
                    destination.wire.internal,
                    &record.description,
                    known,
                )
                .await?;
            for (at, external) in ports {
                record
                    .network
                    .delete_owned(
                        &at,
                        Ports {
                            external,
                            ..destination.wire
                        },
                        &record.description,
                        &mut |at, ports| self.track(id, at, ports, false),
                    )
                    .await?;
            }
            self.remove_destination(id, &destination.gateway);
        }
        Err(Error::Limit)
    }
    pub(super) async fn cleanup(&self) -> Result<(), Error> {
        let records = self.lock().records.iter().map(|r| r.id).collect();
        self.clean_records(records).await
    }
    pub(super) async fn retire_except(
        &self,
        gateway: &Gateway,
        configured: Ports,
    ) -> Result<(), Error> {
        let records = self
            .lock()
            .records
            .iter()
            .filter(|r| r.gateway != *gateway || r.configured != configured || r.retiring)
            .map(|r| r.id)
            .collect();
        self.clean_records(records).await
    }
    async fn clean_records(&self, records: Vec<u64>) -> Result<(), Error> {
        let mut error = None;
        for id in records.into_iter().rev() {
            if let Err(e) = self.clean_record(id).await {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
}
pub(super) struct Claim(PortsSource);
impl Drop for Claim {
    fn drop(&mut self) {
        self.0.lock().active = false;
    }
}
