// aestratum_config/env/appsup, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::resolution::Resolved;
use ae_codec::api::{self, ApiType};
use ae_crypto::Ed25519SecretKey;
use ae_stratum::{proof::Number, rewards, runtime, services, session};
use ae_types::{AccountPublicKey, ContractPublicKey};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde_json::{Map, Value, json};
use std::{
    io::Read,
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Field(&'static str),
    Limit,
    Keys(std::io::ErrorKind),
    KeyPair,
    Rewards,
    Runtime(runtime::Error),
    Signer(ae_sync::preset_keys::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stratum configuration: {self:?}")
    }
}
impl std::error::Error for Error {}
// Key material is deliberately absent from Debug and public projections.
#[derive(Clone)]
pub struct Plan {
    values: Map<String, Value>,
    directory: PathBuf,
    beneficiaries: Value,
    last_n: usize,
    contract: ContractPublicKey,
}
pub struct Prepared {
    values: Map<String, Value>,
    caller: AccountPublicKey,
    contract: ContractPublicKey,
    weights: rewards::Map,
    last_n: usize,
    keys: ae_hyperchain::signing::Keys,
}
fn integer(value: &Value, field: &'static str) -> Result<BigInt, Error> {
    let text = value.as_number().ok_or(Error::Field(field))?.to_string();
    if text.len() > 4096 {
        return Err(Error::Limit);
    }
    text.parse().map_err(|_| Error::Field(field))
}
fn size(value: &Value, field: &'static str) -> Result<usize, Error> {
    integer(value, field)?.to_usize().ok_or(Error::Field(field))
}
fn number_text(text: &str) -> Result<Number, Error> {
    if text.len() > 4096 {
        return Err(Error::Limit);
    }
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(Number::Integer(text.parse().map_err(|_| Error::Rewards)?));
    }
    // Erlang binary_to_float requires decimal digits on both sides of '.'.
    let (mantissa, exponent) = text
        .find(['e', 'E'])
        .map_or((text, None), |i| (&text[..i], Some(&text[i + 1..])));
    let mantissa = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let (whole, fractional) = mantissa.split_once('.').ok_or(Error::Rewards)?;
    if whole.is_empty()
        || fractional.is_empty()
        || !whole
            .bytes()
            .chain(fractional.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(Error::Rewards);
    }
    if let Some(exp) = exponent {
        let exp = exp.strip_prefix(['+', '-']).unwrap_or(exp);
        if exp.is_empty() || !exp.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::Rewards);
        }
    }
    let f: f64 = text.parse().map_err(|_| Error::Rewards)?;
    if !f.is_finite() {
        return Err(Error::Rewards);
    }
    Ok(Number::Float(f.to_bits()))
}
fn numeric(value: &Number) -> Result<f64, Error> {
    match value {
        Number::Integer(i) => i.to_f64().filter(|f| f.is_finite()).ok_or(Error::Rewards),
        Number::Float(f) => Ok(f64::from_bits(*f)),
    }
}
fn add(a: Number, b: Number) -> Result<Number, Error> {
    match (a, b) {
        (Number::Integer(a), Number::Integer(b)) => Ok(Number::Integer(a + b)),
        (a, b) => {
            let f = numeric(&a)? + numeric(&b)?;
            if !f.is_finite() {
                return Err(Error::Rewards);
            }
            Ok(Number::Float(f.to_bits()))
        }
    }
}
fn projection(n: &Number) -> Value {
    match n {
        Number::Integer(i) => json!({"integer":i.to_string()}),
        Number::Float(f) => json!({"float":format!("{f:016X}")}),
    }
}
impl Plan {
    // Native setup clears application env and reads the cached user section.
    // It does not merge application values or populate JSON schema defaults.
    pub fn resolve(
        resolved: &Resolved<'_>,
        network: &[u8],
        private_directory: &Path,
    ) -> Result<Option<Self>, Error> {
        let Some(section) = resolved.get(&["stratum"]).map_err(Error::Resolution)? else {
            return Ok(None);
        };
        let section = section.as_object().ok_or(Error::Field("stratum"))?;
        match section.get("enabled").and_then(Value::as_bool) {
            Some(false) => return Ok(None),
            Some(true) => (),
            None => return Err(Error::Field("enabled")),
        }
        let mut values = Map::new();
        for (section_name, defaults) in [
            (
                "connection",
                json!({"host":"localhost","port":9999,"transport":"tcp","max_connections":1024,"num_acceptors":100}),
            ),
            (
                "session",
                json!({"extra_nonce_bytes":4,"skip_num_blocks":10,"initial_share_target":serde_json::from_str::<Value>(&ae_stratum::target::maximum().to_string()).unwrap(),"max_share_target":serde_json::from_str::<Value>(&ae_stratum::target::maximum().to_string()).unwrap(),"desired_solve_time":30000,"max_solve_time":60000,"share_target_diff_threshold":5.0,"edge_bits":29,"msg_timeout":15000,"max_jobs":20,"max_workers":20}),
            ),
        ] {
            let supplied = section
                .get(section_name)
                .map(|v| v.as_object().ok_or(Error::Field(section_name)))
                .transpose()?;
            for (key, default) in defaults.as_object().unwrap() {
                values.insert(
                    key.clone(),
                    supplied.and_then(|s| s.get(key)).unwrap_or(default).clone(),
                );
            }
        }
        let reward = section
            .get("reward")
            .and_then(Value::as_object)
            .ok_or(Error::Field("reward"))?;
        let beneficiaries = reward
            .get("beneficiaries")
            .ok_or(Error::Field("beneficiaries"))?
            .clone();
        let keys = reward.get("keys").ok_or(Error::Field("keys"))?;
        let extra = size(&values["extra_nonce_bytes"], "extra_nonce_bytes")?;
        if !(1..=7).contains(&extra) {
            return Err(Error::Field("extra_nonce_bytes"));
        }
        let capacity = (integer(&values["max_connections"], "max_connections")?
            + integer(&values["num_acceptors"], "num_acceptors")?)
            * 2;
        if capacity >= (BigInt::from(1) << (extra * 8)) - 1 {
            return Err(Error::Field("insufficent_extra_nonce_bytes"));
        }
        if !values["transport"].is_string() {
            return Err(Error::Field("transport"));
        }
        for field in ["desired_solve_time", "max_solve_time"] {
            values.insert(
                field.into(),
                serde_json::from_str(&(integer(&values[field], field)? * 1000u32).to_string())
                    .map_err(|_| Error::Limit)?,
            );
        }
        let keys = keys
            .as_object()
            .filter(|k| k.len() == 1)
            .ok_or(Error::Field("keys"))?;
        let directory = PathBuf::from(
            keys.get("dir")
                .and_then(Value::as_str)
                .ok_or(Error::Field("keys"))?,
        );
        let last_n = reward
            .get("reward_last_rounds")
            .map_or(Ok(2), |n| size(n, "reward_last_rounds"))?;
        if !(1..10).contains(&last_n) {
            return Err(Error::Field("reward_last_rounds"));
        }
        let address = if network == b"ae_mainnet" {
            "ct_AhMbfHYPBK8Qu1DkqXwQHcMKZoZAUndyYTNZDnyS1AdWh7X9U"
        } else {
            "ct_jgo43gdpikScyo5munDvsVWBJDugjkMtqPHUfzGJyuWAFFUuB"
        };
        let contract = ContractPublicKey(
            api::decode_as(ApiType::ContractPubkey, address)
                .map_err(|_| Error::Field("contract"))?
                .try_into()
                .map_err(|_| Error::Field("contract"))?,
        );
        values.extend([
            ("enabled".into(), json!(true)),
            ("last_n".into(), json!(last_n)),
            ("contract_address".into(), json!(address)),
            (
                "contract_pubkey".into(),
                json!(
                    contract
                        .0
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>()
                ),
            ),
            ("reward_keyblock_delay".into(), json!(180)),
            ("payout_keyblock_delay".into(), json!(180)),
        ]);
        let directory = if directory.is_absolute() {
            directory
        } else {
            if !private_directory.is_absolute() {
                return Err(Error::Field("private directory"));
            }
            private_directory.join(directory)
        };
        Ok(Some(Self {
            values,
            directory,
            beneficiaries,
            last_n,
            contract,
        }))
    }
    pub fn key_directory(&self) -> &Path {
        &self.directory
    }
    // Native read_keys(..., await) retries I/O errors once per second. Dropping
    // this future cancels the wait; it owns no background task or generated key.
    pub async fn load_when_available(self) -> Result<Prepared, Error> {
        loop {
            match self.clone().load() {
                Err(Error::Keys(_)) => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                result => return result,
            }
        }
    }
    pub fn load(self) -> Result<Prepared, Error> {
        // Bounded regular-file reads; no generation, overwrite or secret logging.
        fn bytes(path: &Path, expected: usize) -> Result<Zeroizing<Vec<u8>>, Error> {
            if !std::fs::metadata(path)
                .map_err(|e| Error::Keys(e.kind()))?
                .is_file()
            {
                return Err(Error::KeyPair);
            }
            let f = std::fs::File::open(path).map_err(|e| Error::Keys(e.kind()))?;
            let meta = f.metadata().map_err(|e| Error::Keys(e.kind()))?;
            if !meta.is_file() || meta.len() != expected as u64 {
                return Err(Error::KeyPair);
            }
            let mut data = Zeroizing::new(Vec::with_capacity(expected + 1));
            f.take((expected + 1) as u64)
                .read_to_end(&mut data)
                .map_err(|e| Error::Keys(e.kind()))?;
            if data.len() != expected {
                return Err(Error::KeyPair);
            }
            Ok(data)
        }
        let public = bytes(&self.directory.join("sign_key.pub"), 32)?;
        let secret = bytes(&self.directory.join("sign_key"), 64)?;
        let key = Ed25519SecretKey::from_seed(secret[..32].try_into().map_err(|_| Error::KeyPair)?);
        let caller = key.public_key();
        if public.as_slice() != caller.0
            || secret[32..] != caller.0
            || !ae_crypto::verify_ed25519(&caller, b"roundtrip", &key.sign(b"roundtrip"))
        {
            return Err(Error::KeyPair);
        }
        let mut weights = rewards::Map::new();
        for b in self
            .beneficiaries
            .as_array()
            .ok_or(Error::Field("beneficiaries"))?
        {
            let text = b.as_str().ok_or(Error::Rewards)?;
            let (account, percent) = text.split_once(':').ok_or(Error::Rewards)?;
            if !account.starts_with("ak_") {
                return Err(Error::Rewards);
            }
            let percent = number_text(percent)?;
            let prior = weights
                .remove(account.as_bytes())
                .unwrap_or(Number::Integer(0.into()));
            weights.insert(account.as_bytes().to_vec(), add(prior, percent)?);
        }
        let percent = rewards::sum(&weights, Default::default()).map_err(|_| Error::Rewards)?;
        let too_high = match &percent {
            Number::Integer(i) => i > &BigInt::from(100),
            Number::Float(f) => f64::from_bits(*f) > 100.0,
        };
        if too_high {
            return Err(Error::Rewards);
        }
        let mut values = self.values;
        values.insert(
            "pool_percent_shares".into(),
            Value::Object(
                weights
                    .iter()
                    .map(|(k, n)| {
                        (
                            String::from_utf8(k.clone()).expect("user UTF-8"),
                            projection(n),
                        )
                    })
                    .collect(),
            ),
        );
        values.insert("pool_percent_sum".into(), projection(&percent));
        values.insert(
            "caller_pubkey".into(),
            json!(
                caller
                    .0
                    .iter()
                    .map(|b| format!("{b:02X}"))
                    .collect::<String>()
            ),
        );
        values.insert(
            "caller_address".into(),
            json!(api::encode(ApiType::AccountPubkey, &caller.0)),
        );
        Ok(Prepared {
            values,
            caller,
            contract: self.contract,
            weights,
            last_n: self.last_n,
            keys: ae_hyperchain::signing::Keys::new([key]),
        })
    }
}
impl Prepared {
    pub fn public_configuration(&self) -> Value {
        Value::Object(self.values.clone())
    }
    pub fn caller(&self) -> AccountPublicKey {
        self.caller
    }
    pub fn transport(&self, mut limits: runtime::Limits) -> Result<runtime::Config, Error> {
        let n = |field| size(&self.values[field], field);
        let port = n("port")?.try_into().map_err(|_| Error::Field("port"))?;
        limits.connections = n("max_connections")?;
        limits.acceptors = n("num_acceptors")?;
        let config = runtime::Config {
            // Native appsup uses ranch_tcp and only the configured port; host
            // and declared transport remain descriptive values, even for ssl.
            bind: std::net::SocketAddr::from(([0, 0, 0, 0], port)),
            session: session::Config {
                port,
                extra_nonce_bytes: n("extra_nonce_bytes")?
                    .try_into()
                    .map_err(|_| Error::Field("extra_nonce_bytes"))?,
                message_timeout_ms: n("msg_timeout")?
                    .try_into()
                    .map_err(|_| Error::Field("msg_timeout"))?,
                skip_blocks: n("skip_num_blocks")?
                    .try_into()
                    .map_err(|_| Error::Field("skip_num_blocks"))?,
                edge_bits: n("edge_bits")?
                    .try_into()
                    .map_err(|_| Error::Field("edge_bits"))?,
                max_jobs: n("max_jobs")?,
                targets: session::Targets {
                    initial: integer(&self.values["initial_share_target"], "initial_share_target")?,
                    maximum: integer(&self.values["max_share_target"], "max_share_target")?,
                    difference_threshold: self.values["share_target_diff_threshold"]
                        .as_f64()
                        .filter(|f| f.is_finite())
                        .ok_or(Error::Field("share_target_diff_threshold"))?,
                    desired_solve_time: integer(
                        &self.values["desired_solve_time"],
                        "desired_solve_time",
                    )?,
                    maximum_solve_time: integer(&self.values["max_solve_time"], "max_solve_time")?,
                },
            },
            services: services::Config {
                connections: limits.connections,
                max_workers: n("max_workers")?,
                ..Default::default()
            },
            limits,
        };
        config.validate().map_err(Error::Runtime)?;
        Ok(config)
    }
    pub fn rounds(&self) -> Result<ae_storage::stratum::rounds::Config, Error> {
        ae_storage::stratum::rounds::Config::from_percentages(
            self.weights.clone(),
            self.last_n,
            180,
            Default::default(),
        )
        .map_err(|_| Error::Rewards)
    }
    pub fn payouts(&self) -> ae_storage::stratum::payouts::Config {
        ae_storage::stratum::payouts::Config {
            caller: self.caller,
            contract: self.contract,
            delay: 180,
        }
    }
    pub fn signer(
        &self,
        config: ae_sync::preset_keys::Config,
    ) -> Result<ae_sync::preset_keys::Worker, Error> {
        ae_sync::preset_keys::Worker::spawn(self.keys.fresh(), config).map_err(Error::Signer)
    }
}
#[cfg(test)]
mod tests;
