// aec_fork_block_settings, aeternity fa2a6d7. Copyright (c) Aeternity.
// ISC: repository LICENSE. Filesystem IO stays outside deterministic execution.
use super::{presets, resolution::Resolved};
use ae_codec::api::{self, ApiType};
use ae_types::Hash256;
use ae_vm_fate::{
    Limits as FateLimits,
    maps::{Traversal, listing::Overrides, order::binary_indices},
};
use num_bigint::BigInt;
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Read(presets::Error),
    WorkingDirectory,
    Path,
    Limit,
    Json,
    Object,
    Height,
    Encoding(api::ApiError),
    Prefix { expected: ApiType, actual: ApiType },
    Fate(ae_vm_fate::Error),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub entries: usize,
    pub fate: FateLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 16 << 20,
            entries: 100_000,
            fate: FateLimits::default(),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum Asset {
    Whitelist,
    PreIrisOrdering,
}
pub fn filename(network: &[u8], asset: Asset) -> &'static str {
    match (network, asset) {
        (b"ae_mainnet", Asset::Whitelist) => ".block_whitelist.json",
        (b"ae_uat", Asset::Whitelist) => ".block_whitelist_uat.json",
        (_, Asset::Whitelist) => ".block_whitelist_test.json",
        (b"ae_mainnet", Asset::PreIrisOrdering) => ".pre_iris_map_ordering.json",
        (b"ae_uat", Asset::PreIrisOrdering) => ".pre_iris_map_ordering_uat.json",
        (_, Asset::PreIrisOrdering) => ".pre_iris_map_ordering_test.json",
    }
}
#[derive(Debug, Clone)]
pub struct Plan {
    pub whitelist: PathBuf,
    pub ordering: PathBuf,
    cwd: PathBuf,
    network: Vec<u8>,
}
impl Plan {
    pub fn resolve(resolved: &Resolved<'_>, network: &[u8], cwd: &Path) -> Result<Self, Error> {
        if !cwd.is_absolute() {
            return Err(Error::WorkingDirectory);
        }
        let data = resolved
            .data_directory("aecore")
            .map_err(Error::Resolution)?;
        let default = data.join(filename(network, Asset::Whitelist));
        // Native resolution deliberately excludes schema and application defaults.
        let value = resolved
            .get(&["sync", "whitelist_file"])
            .map_err(Error::Resolution)?
            .cloned()
            .unwrap_or(Value::String(default.to_str().ok_or(Error::Path)?.into()));
        let value = resolved
            .expand_value("aecore", &value)
            .map_err(Error::Resolution)?;
        let whitelist = absolute(&filename_text(&value)?, cwd)?;
        Ok(Self {
            whitelist,
            ordering: data.join(filename(network, Asset::PreIrisOrdering)),
            cwd: cwd.into(),
            network: network.into(),
        })
    }
    pub fn load(&self, limits: Limits) -> Result<Loaded, Error> {
        let (white, white_input) = read_optional(&self.whitelist, &self.cwd, limits.bytes)?;
        let whitelist = white
            .as_deref()
            .map_or(Ok(Whitelist::default()), |b| parse_whitelist(b, limits))?;
        let (order, order_input) = read_optional(&self.ordering, &self.cwd, limits.bytes)?;
        let overrides = order
            .as_deref()
            .map_or(Ok(Overrides::default()), |b| parse_ordering(b, limits))?;
        Ok(Loaded {
            whitelist,
            overrides,
            inputs: [white_input, order_input],
            network: self.network.clone(),
        })
    }
}
fn filename_text(value: &Value) -> Result<String, Error> {
    fn append(v: &Value, out: &mut String, in_list: bool) -> Result<(), Error> {
        match v {
            Value::String(s) => out.push_str(s),
            Value::Bool(true) => out.push_str("true"),
            Value::Bool(false) => out.push_str("false"),
            Value::Null => out.push_str("null"),
            Value::Array(values) => {
                for v in values {
                    append(v, out, true)?;
                }
            }
            Value::Number(n) if in_list => out.push(
                n.as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .and_then(char::from_u32)
                    .ok_or(Error::Path)?,
            ),
            _ => return Err(Error::Path),
        }
        if out.len() > super::MAX_BYTES {
            return Err(Error::Limit);
        }
        Ok(())
    }
    let mut out = String::new();
    append(value, &mut out, false)?;
    Ok(out)
}
fn absolute(text: &str, cwd: &Path) -> Result<PathBuf, Error> {
    let joined = if Path::new(text).is_absolute() {
        text.to_owned()
    } else {
        format!("{}/{text}", cwd.to_str().ok_or(Error::Path)?)
    };
    if joined.len() > super::MAX_BYTES {
        return Err(Error::Limit);
    }
    // Native Unix filename:join removes repeated separators and interior ./,
    // preserves .. and a final dot, and does not consult the filesystem.
    let parts: Vec<_> = joined.split('/').collect();
    let mut out = String::from("/");
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() || (*part == "." && i + 1 < parts.len()) {
            continue;
        }
        if out.len() > 1 {
            out.push('/');
        }
        out.push_str(part);
    }
    Ok(out.into())
}
#[derive(Debug, Clone)]
pub struct Input {
    pub path: PathBuf,
    pub hash: Option<Hash256>,
    pub bytes: usize,
}
#[derive(Debug, Clone)]
pub struct Loaded {
    pub(crate) whitelist: Whitelist,
    pub(crate) overrides: Overrides,
    pub(crate) inputs: [Input; 2],
    network: Vec<u8>,
}
impl Loaded {
    pub fn network(&self) -> &[u8] {
        &self.network
    }
    pub fn whitelist(&self) -> &Whitelist {
        &self.whitelist
    }
    pub fn overrides(&self) -> &Overrides {
        &self.overrides
    }
    pub fn inputs(&self) -> &[Input; 2] {
        &self.inputs
    }
}
fn read_optional(
    path: &Path,
    cwd: &Path,
    maximum: usize,
) -> Result<(Option<Vec<u8>>, Input), Error> {
    let physical = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    // filelib:is_file returns false on missing/inaccessible/special files.
    if !physical.metadata().is_ok_and(|m| m.is_file() || m.is_dir()) {
        return Ok((
            None,
            Input {
                path: path.into(),
                hash: None,
                bytes: 0,
            },
        ));
    }
    let bytes = presets::Source::File {
        path: path.into(),
        custom_prefunded: false,
    }
    .read_at(cwd, maximum)
    .map_err(Error::Read)?;
    let input = Input {
        path: path.into(),
        hash: Some(ae_crypto::blake2b_256(&bytes)),
        bytes: bytes.len(),
    };
    Ok((Some(bytes), input))
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Whitelist {
    entries: BTreeMap<BigInt, Hash256>,
}
impl Whitelist {
    pub fn entries(&self) -> &BTreeMap<BigInt, Hash256> {
        &self.entries
    }
    // Negative and oversized native heights cannot match a wire u64 height.
    // Retain them above; omit only at the typed chain interface boundary.
    pub fn checkpoints(&self) -> BTreeMap<u64, Hash256> {
        self.entries
            .iter()
            .filter_map(|(height, hash)| u64::try_from(height).ok().map(|h| (h, *hash)))
            .collect()
    }
}
fn object(bytes: &[u8], limits: Limits) -> Result<Map<String, Value>, Error> {
    if bytes.len() > limits.bytes {
        return Err(Error::Limit);
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Json)?;
    super::budget(&value).map_err(|_| Error::Limit)?;
    let Value::Object(object) = value else {
        return Err(Error::Object);
    };
    if object.len() > limits.entries {
        return Err(Error::Limit);
    }
    Ok(object)
}
fn order(object: &Map<String, Value>, limits: Limits) -> Result<Vec<usize>, Error> {
    // JSX return_maps keeps the last duplicate JSON key, then native
    // maps:to_list controls collisions after height/FATE decoding.
    binary_indices(
        &object.keys().map(|s| s.as_bytes()).collect::<Vec<_>>(),
        Traversal::ToList,
        limits.fate,
    )
    .map_err(Error::Fate)
}
fn decode(text: &str, expected: ApiType, limit: usize) -> Result<Vec<u8>, Error> {
    let (actual, bytes) = api::decode_with_limit(text, limit).map_err(Error::Encoding)?;
    if actual != expected {
        return Err(Error::Prefix { expected, actual });
    }
    Ok(bytes)
}
pub fn parse_whitelist(bytes: &[u8], limits: Limits) -> Result<Whitelist, Error> {
    let object = object(bytes, limits)?;
    let indices = order(&object, limits)?;
    let rows: Vec<_> = object.iter().collect();
    let mut entries = BTreeMap::new();
    for i in indices {
        let (key, value) = rows[i];
        // Native decodes the hash before converting the height.
        let hash = decode(
            value.as_str().ok_or(Error::Object)?,
            ApiType::KeyBlockHash,
            limits.bytes,
        )?;
        let digits = key.strip_prefix(['+', '-']).unwrap_or(key);
        if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return Err(Error::Height);
        }
        let height = key.parse::<BigInt>().map_err(|_| Error::Height)?;
        entries.insert(
            height,
            Hash256(
                hash.try_into()
                    .map_err(|_| Error::Encoding(api::ApiError::IncorrectSize))?,
            ),
        );
    }
    Ok(Whitelist { entries })
}
pub fn parse_ordering(bytes: &[u8], limits: Limits) -> Result<Overrides, Error> {
    let object = object(bytes, limits)?;
    let indices = order(&object, limits)?;
    let rows: Vec<_> = object.iter().collect();
    let mut overrides = Overrides::default();
    let mut remaining = limits.fate;
    for i in indices {
        let (key, value) = rows[i];
        let key = decode(key, ApiType::ContractBytearray, limits.bytes)?;
        let value = decode(
            value.as_str().ok_or(Error::Object)?,
            ApiType::ContractBytearray,
            limits.bytes,
        )?;
        overrides
            .insert_encoded(&key, &value, &mut remaining)
            .map_err(Error::Fate)?;
    }
    Ok(overrides)
}
