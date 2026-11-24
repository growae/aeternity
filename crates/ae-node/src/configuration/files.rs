use super::{Configuration, Error, Format, MAX_BYTES};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct Discovery {
    pub command_line: Option<PathBuf>,
    pub application_config: Option<PathBuf>,
    pub home: PathBuf,
    pub release_home: PathBuf,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub path: PathBuf,
    pub deprecated: bool,
}
impl Discovery {
    // Native order: --config, AETERNITY_CONFIG, aecore.config, current
    // user/release defaults, EPOCH_CONFIG, deprecated user/release defaults.
    pub fn select(&self, env: &BTreeMap<OsString, OsString>) -> Option<Source> {
        if let Some(path) = [
            self.command_line.as_ref().cloned(),
            env.get(&OsString::from("AETERNITY_CONFIG"))
                .map(PathBuf::from),
            self.application_config.as_ref().cloned(),
        ]
        .into_iter()
        .flatten()
        .next()
        {
            return Some(Source {
                path,
                deprecated: false,
            });
        }
        for (deprecated, prefix, home) in [
            (false, "aeternity", self.home.join(".aeternity/aeternity")),
            (true, "epoch", self.home.join(".epoch/epoch")),
        ] {
            if deprecated && let Some(path) = env.get(&OsString::from("EPOCH_CONFIG")) {
                return Some(Source {
                    path: path.into(),
                    deprecated,
                });
            }
            for dir in [&home, &self.release_home] {
                for extension in ["json", "yaml"] {
                    let path = dir.join(format!("{prefix}.{extension}"));
                    if path.try_exists().unwrap_or(true) {
                        return Some(Source { path, deprecated });
                    }
                }
            }
        }
        None
    }
    pub fn load(
        &self,
        env: &BTreeMap<OsString, OsString>,
    ) -> Result<(Configuration, Option<Source>), Error> {
        let source = self.select(env);
        let configuration = source
            .as_ref()
            .map_or_else(|| Ok(Configuration::empty()), Source::read)?;
        let mut overrides = BTreeMap::new();
        let mut names = BTreeMap::new();
        super::schema::names(super::schema::original(), "AE", &[], &mut names);
        let mut bytes = 0usize;
        for name in names.keys() {
            if let Some(value) = env.get(&OsString::from(name)) {
                let value = value
                    .to_str()
                    .ok_or_else(|| Error::Environment { name: name.clone() })?;
                bytes = bytes
                    .checked_add(value.len())
                    .filter(|n| *n <= MAX_BYTES)
                    .ok_or(Error::Limit("environment bytes"))?;
                overrides.insert(name.clone(), value.into());
            }
        }
        Ok((configuration.with_environment(&overrides)?, source))
    }
}
impl Source {
    pub fn read(&self) -> Result<Configuration, Error> {
        read(&self.path)
    }
}
pub fn read(path: &Path) -> Result<Configuration, Error> {
    let format = match path.extension().and_then(|s| s.to_str()) {
        Some("json") => Format::Json,
        Some("yaml") => Format::Yaml,
        _ => return Err(Error::Format),
    };
    // Nonblocking open prevents named pipes from hanging the config checker.
    #[cfg(unix)]
    let file: File = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|e| Error::Io(std::io::Error::from(e).kind()))?
    .into();
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|e| Error::Io(e.kind()))?;
    let meta = file.metadata().map_err(|e| Error::Io(e.kind()))?;
    if !meta.is_file() {
        return Err(Error::Io(std::io::ErrorKind::InvalidInput));
    }
    if meta.len() > MAX_BYTES as u64 {
        return Err(Error::Limit("input bytes"));
    }
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::Io(e.kind()))?;
    Configuration::parse(&bytes, format)
}
