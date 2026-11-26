// aehttp_dispatch_ext:GetCurrency and aeu_env, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::resolution::{Resolved, Strategy};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Map, Value, json};
use std::{io::Read, path::Path};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    WorkingDirectory,
    Field(&'static str),
    ResourceLimit,
    FileType,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub logo_bytes: usize,
    pub output_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            logo_bytes: 1 << 20,
            output_bytes: 8 << 20,
        }
    }
}
#[derive(Clone)]
pub struct Plan {
    fields: Map<String, Value>,
    network_name: Vec<u8>,
    logo: Option<(std::path::PathBuf, Option<String>)>,
}
pub struct Metadata {
    // The HTTP encoder must clean raw network identity bytes at emission.
    pub network_name: Vec<u8>,
    pub fields: Map<String, Value>,
}
impl Plan {
    pub fn resolve(resolved: &Resolved<'_>, network: &[u8], cwd: &Path) -> Result<Self, Error> {
        if !cwd.is_absolute() {
            return Err(Error::WorkingDirectory);
        }
        if network.len() > super::MAX_BYTES {
            return Err(Error::ResourceLimit);
        }
        let mut fields = Map::new();
        for (section, names) in [
            (
                "currency",
                &["name", "symbol", "subunit", "subunits_per_unit"][..],
            ),
            ("display", &["primary_colour", "secondary_colour"][..]),
        ] {
            for &name in names {
                let value = resolved
                    .find(
                        &["chain", section, name],
                        &[Strategy::User, Strategy::SchemaDefault],
                    )
                    .map_err(Error::Resolution)?
                    .ok_or(Error::Field(name))?;
                fields.insert(name.into(), value);
            }
        }
        let mut fiat = None;
        for name in ["fiat_converstion_url", "fiat_conversion_url"] {
            if let Some(value) = resolved
                .get(&["chain", "currency", name])
                .map_err(Error::Resolution)?
            {
                fiat = Some(value.clone());
            }
        }
        if let Some(value) = fiat {
            fields.insert("fiat_converstion_url".into(), value.clone());
            fields.insert("fiat_conversion_url".into(), value);
        }
        let network_name = match resolved
            .get(&["chain", "display", "network_name"])
            .map_err(Error::Resolution)?
        {
            Some(value) => value
                .as_str()
                .ok_or(Error::Field("network_name"))?
                .as_bytes(),
            None => match network {
                b"ae_mainnet" => b"Mainnet",
                b"ae_uat" => b"Testnet",
                other => other,
            },
        }
        .to_vec();
        let logo = resolved
            .get(&["chain", "display", "logo_file"])
            .map_err(Error::Resolution)?
            .map(|value| {
                let path = value.as_str().ok_or(Error::Field("logo_file"))?;
                Ok((cwd.join(path), extension(path)))
            })
            .transpose()?;
        Ok(Self {
            fields,
            network_name,
            logo,
        })
    }
    // Synchronous bounded I/O: call only on an owned blocking worker. Resolve
    // retains the configured path; each read observes the current file content.
    pub fn read(&self, limits: Limits) -> Result<Metadata, Error> {
        if limits.logo_bytes > 16 << 20 || limits.output_bytes > 64 << 20 {
            return Err(Error::ResourceLimit);
        }
        let mut budget = Budget(limits.output_bytes);
        // Include the network field, six-byte JSON escaping and JSX's at-most
        // three-byte replacement. Six bytes per input byte bounds both paths.
        budget.spend(self.network_name.len().saturating_mul(6).saturating_add(32))?;
        serde_json::to_writer(&mut budget, &self.fields).map_err(|_| Error::ResourceLimit)?;
        let logo = match &self.logo {
            Some((path, kind)) => match read_file(path, limits.logo_bytes)? {
                Some(bytes) if kind.is_some() => {
                    let kind = kind.as_ref().expect("extension present");
                    let encoded = bytes.len().div_ceil(3).saturating_mul(4);
                    budget.spend(
                        encoded
                            .saturating_add(kind.len().saturating_mul(6))
                            .saturating_add(40),
                    )?;
                    Some(json!({"type":kind,"data":STANDARD.encode(&bytes)}))
                }
                _ => None,
            },
            None => None,
        };
        let mut fields = self.fields.clone();
        if let Some(logo) = logo {
            fields.insert("logo".into(), logo);
        }
        Ok(Metadata {
            fields,
            network_name: self.network_name.clone(),
        })
    }
}
// filename:extension/1 on the configured POSIX path, including dotfiles.
fn extension(path: &str) -> Option<String> {
    let name = path.rsplit('/').next().unwrap_or_default();
    name.rfind('.')
        .filter(|&index| index > 0)
        .map(|index| name[index + 1..].into())
}
struct Budget(usize);
impl Budget {
    fn spend(&mut self, n: usize) -> Result<(), Error> {
        self.0 = self.0.checked_sub(n).ok_or(Error::ResourceLimit)?;
        Ok(())
    }
}
impl std::io::Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.spend(bytes.len())
            .map_err(|_| std::io::Error::other("currency output limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn read_file(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>, Error> {
    #[cfg(unix)]
    let opened = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map(std::fs::File::from);
    #[cfg(not(unix))]
    let opened = std::fs::File::open(path);
    let Ok(file) = opened else { return Ok(None) };
    let Ok(meta) = file.metadata() else {
        return Ok(None);
    };
    if meta.is_dir() {
        return Ok(None);
    }
    if !meta.is_file() {
        return Err(Error::FileType);
    }
    if meta.len() > maximum as u64 {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = vec![];
    if file
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Ok(None);
    }
    if bytes.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests;
