// aec_fork_block_settings / aec_block_genesis, aeternity fa2a6d7.
// Copyright (c) Aeternity. ISC: repository LICENSE.
use super::{
    chain::Settings,
    consensus::Kind,
    protocols,
    resolution::{Resolved, Strategy},
};
use ae_codec::api::{self, ApiType};
use ae_types::{AccountPublicKey, Unsigned};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
pub mod hyperchain;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asset {
    Accounts,
    ExtraAccounts,
    Contracts,
}
impl Asset {
    fn name(self) -> &'static str {
        match self {
            Self::Accounts => "accounts",
            Self::ExtraAccounts => "extra_accounts",
            Self::Contracts => "contracts",
        }
    }
    fn field(self) -> &'static str {
        match self {
            Self::Accounts => "accounts_file",
            Self::ExtraAccounts => "extra_accounts_file",
            Self::Contracts => "contracts_file",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Configuration(&'static str),
    Protocol,
    Limit,
    Json,
    Shape,
    Address,
    Balance,
    MissingConfiguration,
    CustomFileNotFound(PathBuf),
    Io {
        path: PathBuf,
        kind: std::io::ErrorKind,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub accounts: usize,
    pub contracts: usize,
    pub calls: usize,
    pub validators: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 16 << 20,
            accounts: 100_000,
            contracts: 4096,
            calls: 65536,
            validators: 65536,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    File {
        path: PathBuf,
        custom_prefunded: bool,
    },
    Empty,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Account {
    pub public_key: AccountPublicKey,
    pub amount: Value,
}
#[derive(Debug, Clone)]
pub struct Plan {
    pub accounts: Source,
    pub contracts: Option<Source>,
    pub configured_accounts: Vec<Account>,
    pub limits: Limits,
}
pub fn directory(protocol: u32) -> Result<&'static str, Error> {
    match protocol {
        1 => Ok(".genesis"),
        2 => Ok(".minerva"),
        3 => Ok(".fortuna"),
        4 => Ok(".lima"),
        5 => Ok(".iris"),
        6 => Ok(".ceres"),
        7 => Ok(".arcus"),
        8 => Ok(".salus"),
        _ => Err(Error::Protocol),
    }
}
pub fn filename(network: &[u8], pos: bool, asset: Asset) -> Result<String, Error> {
    let name = asset.name();
    if pos {
        let network =
            std::str::from_utf8(network).map_err(|_| Error::Configuration("network encoding"))?;
        Ok(format!("{network}_{name}.json"))
    } else {
        Ok(match network {
            b"ae_mainnet" => format!("{name}.json"),
            b"ae_uat" => format!("{name}_uat.json"),
            _ => format!("{name}_test.json"),
        })
    }
}
fn path(value: &Value) -> Result<PathBuf, Error> {
    let text = value.as_str().ok_or(Error::Shape)?;
    if text.is_empty() || text.contains('\0') {
        return Err(Error::Configuration("preset path"));
    }
    Ok(text.into())
}
// Resolution is pure. File existence and bounded regular-file reads happen
// only through Source::read; an empty custom asset is not a missing file.
#[allow(clippy::too_many_arguments)]
pub fn source(
    data: &Path,
    network: &[u8],
    pos: bool,
    protocol: u32,
    asset: Asset,
    forks: Option<&Value>,
    custom: Option<&Value>,
) -> Result<Source, Error> {
    if let Some(forks) = forks {
        let forks = forks
            .as_object()
            .ok_or(Error::Configuration("hard_forks"))?;
        if let Some(value) = forks.get(&protocol.to_string())
            && protocols::integer(value).is_none()
        {
            let spec = value.as_object().ok_or(Error::Configuration("hard fork"))?;
            return match spec.get(asset.field()) {
                Some(value) => {
                    let p = path(value)?;
                    Ok(Source::File {
                        path: if p.is_absolute() { p } else { data.join(p) },
                        custom_prefunded: false,
                    })
                }
                None if asset == Asset::Contracts => Err(Error::MissingConfiguration),
                None => Ok(Source::Empty),
            };
        }
    }
    if asset == Asset::Accounts
        && let Some(custom) = custom
    {
        return Ok(Source::File {
            path: path(custom)?,
            custom_prefunded: true,
        });
    }
    Ok(Source::File {
        path: data
            .join(directory(protocol)?)
            .join(filename(network, pos, asset)?),
        custom_prefunded: false,
    })
}
impl Plan {
    pub fn resolve(
        resolved: &Resolved<'_>,
        settings: &Settings,
        limits: Limits,
    ) -> Result<Self, Error> {
        let data = resolved
            .data_directory("aecore")
            .map_err(Error::Resolution)?;
        let network = settings.profile().network_id();
        let pos = settings.consensus().genesis().kind() == Kind::Hyperchain;
        let protocol = settings
            .genesis_header(ae_types::Hash256([0; 32]))
            .common
            .version;
        let forks = resolved
            .find(
                &["chain", "hard_forks"],
                &[
                    Strategy::User,
                    Strategy::Application {
                        app: "aecore",
                        key: "hard_forks",
                    },
                    Strategy::SchemaDefault,
                ],
            )
            .map_err(Error::Resolution)?;
        let custom = resolved
            .get(&["system", "custom_prefunded_accs_file"])
            .map_err(Error::Resolution)?;
        let accounts = source(
            &data,
            network,
            pos,
            protocol,
            Asset::Accounts,
            forks.as_ref(),
            custom,
        )?;
        let contracts = pos
            .then(|| {
                source(
                    &data,
                    network,
                    pos,
                    protocol,
                    Asset::Contracts,
                    forks.as_ref(),
                    custom,
                )
            })
            .transpose()?;
        let configured_accounts = configured_accounts(
            network,
            resolved
                .get(&["chain", "genesis_accounts"])
                .map_err(Error::Resolution)?,
            limits,
        )?;
        Ok(Self {
            accounts,
            contracts,
            configured_accounts,
            limits,
        })
    }
    pub fn load_accounts(&self) -> Result<Vec<(AccountPublicKey, Unsigned)>, Error> {
        let preset = parse_accounts(&self.accounts.read(self.limits.bytes)?, self.limits)?;
        merge_accounts(preset, self.configured_accounts.clone(), self.limits)?
            .into_iter()
            .map(|a| Ok((a.public_key, balance(&a.amount)?)))
            .collect()
    }
}
impl Source {
    pub fn read(&self, maximum: usize) -> Result<Vec<u8>, Error> {
        self.read_at(Path::new("."), maximum)
    }
    // An explicit working directory permits relative custom paths without
    // changing process-global cwd. Errors retain the configured path.
    pub fn read_at(&self, cwd: &Path, maximum: usize) -> Result<Vec<u8>, Error> {
        use std::io::Read;
        let Self::File {
            path,
            custom_prefunded,
        } = self
        else {
            return if maximum >= 2 {
                Ok(b"{}".to_vec())
            } else {
                Err(Error::Limit)
            };
        };
        let io = |e: std::io::Error| Error::Io {
            path: path.clone(),
            kind: e.kind(),
        };
        let physical = if path.is_absolute() {
            path.clone()
        } else {
            cwd.join(path)
        };
        // Erlang filelib:is_file accepts directories as well as regular files;
        // the subsequent read reports eisdir for a directory.
        if *custom_prefunded && !physical.metadata().is_ok_and(|m| m.is_file() || m.is_dir()) {
            return Err(Error::CustomFileNotFound(path.clone()));
        }
        #[cfg(unix)]
        let file: std::fs::File = rustix::fs::open(
            &physical,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|e| io(e.into()))?
        .into();
        #[cfg(not(unix))]
        let file = std::fs::File::open(&physical).map_err(io)?;
        let meta = file.metadata().map_err(io)?;
        if meta.is_dir() {
            return Err(io(std::io::Error::from(std::io::ErrorKind::IsADirectory)));
        }
        if !meta.is_file() {
            return Err(io(std::io::Error::from(std::io::ErrorKind::InvalidInput)));
        }
        if meta.len() > maximum as u64 {
            return Err(Error::Limit);
        }
        let limit = maximum.checked_add(1).ok_or(Error::Limit)?;
        let mut bytes = vec![];
        file.take(limit as u64)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() > maximum {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
}
pub fn balance(value: &Value) -> Result<Unsigned, Error> {
    let amount = protocols::integer(value).ok_or(Error::Balance)?;
    if amount.sign() == num_bigint::Sign::Minus {
        return Err(Error::Balance);
    }
    Unsigned::from_field_bytes(&amount.to_bytes_be().1).map_err(|_| Error::Balance)
}
fn account(key: &str, amount: Value) -> Result<Account, Error> {
    let bytes = api::decode_as(ApiType::AccountPubkey, key).map_err(|_| Error::Address)?;
    Ok(Account {
        public_key: AccountPublicKey(bytes.try_into().map_err(|_| Error::Address)?),
        amount,
    })
}
pub fn decode_accounts(value: &Value, limits: Limits) -> Result<Vec<Account>, Error> {
    super::budget(value).map_err(|_| Error::Limit)?;
    if value.as_array().is_some_and(Vec::is_empty) {
        return Ok(vec![]);
    }
    let object = value.as_object().ok_or(Error::Shape)?;
    if object.len() > limits.accounts {
        return Err(Error::Limit);
    }
    decode_pairs(object.iter().map(|(k, v)| (k.as_str(), v)), limits)
}
pub fn decode_pairs<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a Value)>,
    limits: Limits,
) -> Result<Vec<Account>, Error> {
    let mut out = vec![];
    for (key, amount) in pairs {
        if out.len() >= limits.accounts {
            return Err(Error::Limit);
        }
        super::budget(amount).map_err(|_| Error::Limit)?;
        out.push(account(key, amount.clone())?);
    }
    Ok(out)
}
pub fn configured_accounts(
    network: &[u8],
    value: Option<&Value>,
    limits: Limits,
) -> Result<Vec<Account>, Error> {
    if matches!(network, b"ae_mainnet" | b"ae_uat") {
        Ok(vec![])
    } else {
        value.map_or(Ok(vec![]), |v| decode_accounts(v, limits))
    }
}
pub fn merge_accounts(
    preset: Vec<Account>,
    configured: Vec<Account>,
    limits: Limits,
) -> Result<Vec<Account>, Error> {
    if preset
        .len()
        .checked_add(configured.len())
        .is_none_or(|n| n > limits.accounts)
    {
        return Err(Error::Limit);
    }
    let mut merged = BTreeMap::new();
    for a in preset.into_iter().chain(configured) {
        merged.insert(a.public_key, a.amount);
    }
    Ok(merged
        .into_iter()
        .map(|(public_key, amount)| Account { public_key, amount })
        .collect())
}
// JSX's default object decoding is an ordered proplist. Decode every entry,
// including overwritten duplicate keys, before the native final map merge.
pub fn parse_accounts(bytes: &[u8], limits: Limits) -> Result<Vec<Account>, Error> {
    if bytes.len() > limits.bytes {
        return Err(Error::Limit);
    }
    let mut at = 0;
    fn space(b: &[u8], at: &mut usize) {
        while b
            .get(*at)
            .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        {
            *at += 1;
        }
    }
    fn value(b: &[u8], at: &mut usize) -> Result<Value, Error> {
        let mut stream = serde_json::Deserializer::from_slice(&b[*at..]).into_iter::<Value>();
        let v = stream.next().ok_or(Error::Json)?.map_err(|_| Error::Json)?;
        *at += stream.byte_offset();
        super::budget(&v).map_err(|_| Error::Limit)?;
        Ok(v)
    }
    space(bytes, &mut at);
    if bytes.get(at) == Some(&b'[') {
        let v = value(bytes, &mut at)?;
        space(bytes, &mut at);
        return if v.as_array().is_some_and(Vec::is_empty) && at == bytes.len() {
            Ok(vec![])
        } else {
            Err(Error::Shape)
        };
    }
    if bytes.get(at) != Some(&b'{') {
        let _ = value(bytes, &mut at)?;
        space(bytes, &mut at);
        return if at == bytes.len() {
            Err(Error::Configuration("accounts shape"))
        } else {
            Err(Error::Json)
        };
    }
    at += 1;
    let mut out = vec![];
    space(bytes, &mut at);
    if bytes.get(at) != Some(&b'}') {
        loop {
            let key = value(bytes, &mut at)?;
            let key = key.as_str().ok_or(Error::Json)?;
            space(bytes, &mut at);
            if bytes.get(at) != Some(&b':') {
                return Err(Error::Json);
            }
            at += 1;
            let amount = value(bytes, &mut at)?;
            if out.len() >= limits.accounts {
                return Err(Error::Limit);
            }
            out.push((key.to_owned(), amount));
            space(bytes, &mut at);
            match bytes.get(at) {
                Some(b',') => {
                    at += 1;
                    space(bytes, &mut at);
                }
                Some(b'}') => break,
                _ => return Err(Error::Json),
            }
        }
    }
    at += 1;
    space(bytes, &mut at);
    if at != bytes.len() {
        return Err(Error::Json);
    }
    let mut out = decode_pairs(out.iter().map(|(k, v)| (k.as_str(), v)), limits)?;
    out.sort_by_key(|a| a.public_key);
    Ok(out)
}
