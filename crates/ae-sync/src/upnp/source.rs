use super::*;
use rand::Rng;
use std::sync::{Arc, Mutex};

const RECORD_LIMIT: usize = 32;
#[derive(Debug, Clone)]
struct Record {
    id: u64,
    network: Network,
    gateway: Gateway,
    configured: Ports,
    wire: Ports,
    description: String,
    mapping: Option<Mapping>,
    uncertain: bool,
    retiring: bool,
}
#[derive(Debug)]
struct State {
    ports: Ports,
    records: Vec<Record>,
    next: u64,
    active: bool,
}

// This source belongs to the host configuration. Its bounded lease ledger
// outlives child replacement; only the single claimed worker may mutate it.
// It is runtime state, not durable recovery after losing the entire process.
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
        self.lock().records.len()
    }
    pub fn uncertain_mappings(&self) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|r| r.uncertain || r.retiring)
            .count()
    }
    pub fn retiring_mappings(&self) -> usize {
        self.lock().records.iter().filter(|r| r.retiring).count()
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
            if state.records[index].uncertain || state.records[index].retiring {
                return Err(Error::Busy);
            }
            let mut record = state.records.remove(index);
            if let Some(mapping) = &record.mapping {
                record.wire.external = mapping.external;
            }
            record.uncertain = true;
            state.records.push(record.clone());
            return Ok(record);
        }
        if state.records.len() >= RECORD_LIMIT {
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
            gateway,
            configured,
            wire,
            description,
            mapping: None,
            uncertain: true,
            retiring: false,
        };
        // This intent is retained before any add request can reach the router.
        state.records.push(record.clone());
        Ok(record)
    }
    fn confirm(&self, id: u64, mapping: Mapping) {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|r| r.id == id)
            .expect("owned mapping intent");
        // AddAnyPortMapping may have created a second mapping rather than
        // replacing the previous reservation. Enumeration must recover both.
        record.uncertain = record.gateway.protocol() == Protocol::Upnp2
            && record
                .mapping
                .as_ref()
                .is_some_and(|old| old.external != mapping.external);
        record.mapping = Some(mapping);
    }
    fn forget(&self, id: u64) {
        self.lock().records.retain(|r| r.id != id);
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
            .filter(|r| r.uncertain || r.retiring)
        {
            self.clean_record(record).await?;
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
                .add_named(&record.gateway, record.wire, Some(&record.description))
                .await
            {
                Ok(mapping) => {
                    self.confirm(record.id, mapping.clone());
                    return Ok(mapping);
                }
                Err(error) if attempt + 1 == attempts => return Err(error),
                Err(_) => {
                    // Keep the native three random-port attempts, but recover
                    // each uncertain reservation before sending another add.
                    self.clean_record(record).await?;
                }
            }
        }
        unreachable!("at least one mapping attempt")
    }
    async fn clean_record(&self, record: Record) -> Result<(), Error> {
        // Once retirement starts, this record must never be renewed without
        // reconciliation, even if a delete was applied but its reply was lost.
        self.lock()
            .records
            .iter_mut()
            .find(|r| r.id == record.id)
            .expect("owned retirement intent")
            .retiring = true;
        let known = match record.gateway.protocol() {
            Protocol::Upnp1 => Some(record.wire.external),
            Protocol::Upnp2 if !record.uncertain => record.mapping.as_ref().map(|m| m.external),
            _ => None,
        };
        let ports = record
            .network
            .owned_ports(
                &record.gateway,
                record.wire.internal,
                &record.description,
                known,
            )
            .await?;
        for external in ports {
            match record
                .network
                .delete(
                    &record.gateway,
                    Ports {
                        external,
                        ..record.wire
                    },
                )
                .await
            {
                // A concurrent expiry is already a successful removal.
                Err(Error::Soap(714)) => {}
                result => result?,
            }
        }
        self.forget(record.id);
        Ok(())
    }
    pub(super) async fn cleanup(&self) -> Result<(), Error> {
        let records = self.lock().records.clone();
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
            .cloned()
            .collect();
        self.clean_records(records).await
    }
    async fn clean_records(&self, records: Vec<Record>) -> Result<(), Error> {
        let mut error = None;
        // Serialize our deletions with enumeration: removing one mapping must
        // not shift another in-flight scan's table indices. Most recently
        // attempted mappings get the first opportunity within the deadline.
        for record in records.into_iter().rev() {
            if let Err(e) = self.clean_record(record).await {
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
