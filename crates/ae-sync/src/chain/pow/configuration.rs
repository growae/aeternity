// Native Cuckoo settings and conductor instance expansion, aeternity fa2a6d7.
// This is the mining.cuckoo subtree, not the complete node configuration file.
use super::{miner, producer::mining};
use num_bigint::BigInt;
use serde_json::Value;
use std::path::PathBuf;

mod parse;

const MAX_DEFINITIONS: usize = 1024;
const MAX_INSTANCES: usize = 64;
const MAX_TEXT: usize = 65536;
const MAX_INTEGER_BITS: u64 = 16384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Json,
    Field(&'static str),
    Limit(&'static str),
    Conflict,
    Consensus,
    Group,
    Directory,
    Executable,
    Attempts,
    Worker(miner::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Miner {
    pub executable: String,
    pub executable_group: String,
    pub extra_args: String,
    pub hex_encoded_header: bool,
    pub repeats: BigInt,
    pub addressed_instances: Option<Vec<BigInt>>,
}
impl Miner {
    pub fn new(executable: String) -> Self {
        Self {
            executable,
            executable_group: "aecuckoo".into(),
            extra_args: String::new(),
            hex_encoded_header: false,
            repeats: 1.into(),
            addressed_instances: None,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if [&self.executable, &self.executable_group, &self.extra_args]
            .iter()
            .any(|s| s.len() > MAX_TEXT)
        {
            return Err(Error::Limit("miner text"));
        }
        positive(&self.repeats, "repeats")?;
        if let Some(devices) = &self.addressed_instances {
            if devices.len() > MAX_INSTANCES {
                return Err(Error::Limit("instances"));
            }
            for device in devices {
                integer_limit(device)?;
            }
        }
        Ok(())
    }
}

// The caller supplies native sys.config values in their original order.
// User settings take precedence; no process environment is read here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct System {
    pub edge_bits: BigInt,
    pub miners: Vec<Miner>,
}
impl Default for System {
    fn default() -> Self {
        Self {
            edge_bits: 29.into(),
            miners: vec![Miner::new("mean29-generic".into())],
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub edge_bits: BigInt,
    // Order returned by aec_mining:get_miner_configs, before expansion.
    pub miners: Vec<Miner>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instance {
    pub native_id: u64,
    pub configuration_index: usize,
    pub device: Option<BigInt>,
}
#[derive(Debug, Clone, Default)]
pub struct Directories {
    pub cuckoo: Option<PathBuf>,
    pub prebuilt: Option<PathBuf>,
}
pub struct Prepared {
    pub config: mining::Config,
    // Same order as config.instances and the producer's worker status list.
    // These native configuration IDs are distinct from unique worker/job IDs.
    pub instances: Vec<Instance>,
}

fn integer_limit(value: &BigInt) -> Result<(), Error> {
    if value.bits() > MAX_INTEGER_BITS {
        Err(Error::Limit("integer"))
    } else {
        Ok(())
    }
}
fn positive(value: &BigInt, field: &'static str) -> Result<(), Error> {
    integer_limit(value)?;
    if value <= &BigInt::from(0) {
        Err(Error::Field(field))
    } else {
        Ok(())
    }
}
impl Configuration {
    // Parses and validates the native user-facing Cuckoo JSON subtree.
    // JSON/YAML file discovery, environment expansion and other node settings
    // belong to the enclosing node loader.
    pub fn parse(bytes: &[u8], system: &System) -> Result<Self, Error> {
        if bytes.len() > 16 << 20 {
            return Err(Error::Limit("json"));
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Json)?;
        parse::schema(&value)?;
        Self::from_value(&value, system)
    }
    // Normalizes the original function boundary after environment lookup.
    // Unlike parse, this does not apply user-file schema constraints. Native
    // integer values stay exact; operational bounds are explicit errors.
    pub fn from_value(value: &Value, system: &System) -> Result<Self, Error> {
        parse::normalize(value, system)
    }
    fn validate(&self) -> Result<(), Error> {
        positive(&self.edge_bits, "edge_bits")?;
        if self.miners.len() > MAX_DEFINITIONS {
            return Err(Error::Limit("miners"));
        }
        for miner in &self.miners {
            miner.validate()?;
        }
        Ok(())
    }
    pub fn instances(&self) -> Result<Vec<Instance>, Error> {
        self.validate()?;
        let count = self.miners.iter().try_fold(0usize, |n, m| {
            n.checked_add(m.addressed_instances.as_ref().map_or(1, Vec::len))
                .filter(|n| *n <= MAX_INSTANCES)
                .ok_or(Error::Limit("instances"))
        })?;
        let mut result = Vec::with_capacity(count);
        for (configuration_index, miner) in self.miners.iter().enumerate() {
            let devices = miner
                .addressed_instances
                .as_ref()
                .map_or_else(|| vec![None], |ids| ids.iter().cloned().map(Some).collect());
            for device in devices {
                result.push(Instance {
                    native_id: result.len() as u64,
                    configuration_index,
                    device,
                });
            }
        }
        // Native conductor prepends both each addressed instance and each
        // configuration's instance group. Creation IDs remain unchanged.
        result.reverse();
        Ok(result)
    }
    pub fn prepare(
        &self,
        directories: &Directories,
        worker: miner::Config,
        mut config: mining::Config,
    ) -> Result<Prepared, Error> {
        let instances = self.instances()?;
        // Configuration never rewrites canonical mainnet/UAT PoW rules.
        if self.edge_bits != BigInt::from(ae_consensus::cuckoo::MAINNET_EDGE_BITS) {
            return Err(Error::Consensus);
        }
        let mut workers = Vec::with_capacity(instances.len());
        for instance in &instances {
            worker.validate().map_err(Error::Worker)?;
            let definition = &self.miners[instance.configuration_index];
            let repeats = u64::try_from(&definition.repeats).map_err(|_| Error::Attempts)?;
            if repeats > worker.maximum_attempts {
                return Err(Error::Attempts);
            }
            let directory = match definition.executable_group.as_str() {
                "aecuckoo" => &directories.cuckoo,
                "aecuckooprebuilt" => &directories.prebuilt,
                _ => return Err(Error::Group),
            }
            .as_ref()
            .filter(|p| p.is_absolute())
            .ok_or(Error::Directory)?;
            if definition.executable.is_empty() || definition.executable.contains('\0') {
                return Err(Error::Executable);
            }
            // Explicit directories replace Erlang application bin/priv paths.
            // No ambient PATH search. Existence/execute permission is checked
            // by the owned spawn and reported through its original receipt.
            let executable = directory.join(&definition.executable);
            let mut external = miner::external::Config::new(executable, directory.clone());
            external.extra_args =
                miner::external::Config::native_extra_args(&definition.extra_args);
            external.hex_header = definition.hex_encoded_header;
            external.device = instance.device.clone();
            external.validate().map_err(Error::Worker)?;
            workers.push(mining::Instance {
                worker,
                external: Some(external),
                repeats,
            });
        }
        config.instances = workers;
        Ok(Prepared { config, instances })
    }
}

#[cfg(test)]
mod tests;
