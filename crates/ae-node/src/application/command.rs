use super::*;
use configuration::{Discovery, resolution::Directories};
use std::{collections::BTreeMap, ffi::OsString, path::Path};

pub const USAGE: &str = "Usage: ae-node --execution-profile PROFILE.json [--config FILE] [--database DIR] [--resource-dir DIR] [--release-home DIR] [--network-id ID] [--dry-run-worker FILE]\nRun the Rust node in the foreground. SIGINT or SIGTERM requests joined shutdown.\nThe execution profile and release resources are explicit inputs; mainnet compatibility validation remains incomplete.";

// Parsing is pure; --help and invalid arguments cannot open a database.
#[derive(Debug)]
pub enum Command {
    Help,
    Run(Box<Arguments>),
}
#[derive(Debug)]
pub struct Arguments {
    pub profile: PathBuf,
    pub config: Option<PathBuf>,
    pub database: Option<PathBuf>,
    pub resources: Option<PathBuf>,
    pub release_home: Option<PathBuf>,
    pub network: Option<Vec<u8>>,
    pub worker: Option<PathBuf>,
}
impl Command {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, Error> {
        let args: Vec<_> = args.into_iter().collect();
        if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
            return Ok(Self::Help);
        }
        let mut values = BTreeMap::new();
        let mut args = args.into_iter();
        while let Some(key) = args.next() {
            let key = key
                .to_str()
                .filter(|k| {
                    matches!(
                        *k,
                        "--execution-profile"
                            | "--config"
                            | "--database"
                            | "--resource-dir"
                            | "--release-home"
                            | "--network-id"
                            | "--dry-run-worker"
                    )
                })
                .ok_or_else(|| Error::at("arguments", "unknown option"))?
                .to_owned();
            let value = args
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::at("arguments", "missing option value"))?;
            if values.insert(key, value).is_some() {
                return Err(Error::at("arguments", "duplicate option"));
            }
        }
        let profile = values
            .remove("--execution-profile")
            .ok_or_else(|| Error::at("arguments", "execution profile required"))?
            .into();
        let network = values
            .remove("--network-id")
            .map(|v| {
                v.into_string()
                    .map(String::into_bytes)
                    .map_err(|_| Error::at("network id", "UTF-8 required"))
            })
            .transpose()?;
        Ok(Self::Run(Box::new(Arguments {
            profile,
            network,
            config: values.remove("--config").map(Into::into),
            database: values.remove("--database").map(Into::into),
            resources: values.remove("--resource-dir").map(Into::into),
            release_home: values.remove("--release-home").map(Into::into),
            worker: values.remove("--dry-run-worker").map(Into::into),
        })))
    }
}
impl Arguments {
    pub fn prepare(
        self,
        cwd: &Path,
        environment: &BTreeMap<OsString, OsString>,
        executable: &Path,
    ) -> Result<Prepared, Error> {
        let home = environment
            .get(&OsString::from("HOME"))
            .map(PathBuf::from)
            .ok_or_else(|| Error::at("home directory", "HOME missing"))?;
        let release = self.release_home.map_or_else(
            || executable.parent().unwrap_or(cwd).to_path_buf(),
            |p| cwd.join(p),
        );
        let resources = self
            .resources
            .map_or_else(|| release.join("data"), |p| cwd.join(p));
        let discovery = Discovery {
            command_line: self.config.map(|p| cwd.join(p)),
            application_config: None,
            home,
            release_home: release.clone(),
        };
        let (configuration, _) = discovery
            .load(environment)
            .map_err(|e| Error::at("configuration file", e))?;
        let profile = configuration::presets::Source::File {
            path: cwd.join(self.profile),
            custom_prefunded: false,
        }
        .read(1 << 20)
        .map_err(|e| Error::at("execution profile file", e))?;
        let profile =
            serde_json::from_slice(&profile).map_err(|e| Error::at("execution profile JSON", e))?;
        let specification =
            Specification::from_value(&profile).map_err(|e| Error::at("execution profile", e))?;
        let directory = |p: &Path| {
            p.to_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::at("resource directories", "UTF-8 required"))
        };
        let context = Arc::new(
            Context::new(Directories {
                home: directory(&release)?,
                data: directory(&resources)?,
                log: directory(&release.join("log"))?,
            })
            .map_err(|e| Error::at("release directories", e))?,
        );
        let database = self.database.unwrap_or_else(|| {
            configuration
                .get(&["chain", "db_path"])
                .and_then(serde_json::Value::as_str)
                .map_or_else(|| cwd.join("data/chain"), PathBuf::from)
        });
        let database = cwd.join(database);
        let worker = self.worker.map_or_else(
            || {
                executable.with_file_name(if cfg!(windows) {
                    "ae-dry-run-worker.exe"
                } else {
                    "ae-dry-run-worker"
                })
            },
            |p| cwd.join(p),
        );
        let mut options = Options::new(
            cwd.into(),
            database,
            http::DryRunConfig::new(worker, std::env::temp_dir()),
        );
        options.network = self.network;
        options.miner_directories = ae_sync::chain::pow::configuration::Directories {
            cuckoo: Some(release.join("lib/aecuckoo/bin")),
            prebuilt: Some(release.join("lib/aecuckooprebuilt/bin")),
        };
        Prepared::load(&configuration, context, &specification, options)
    }
}
