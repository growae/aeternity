use super::*;
use ae_sync::{
    chain::{hyperchain, pow},
    keys,
    registry::startup,
};
use serde_json::json;
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mode {
    pub maintenance: bool,
    pub offline: bool,
    pub development: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationMode {
    Normal,
    Offline,
    Maintenance,
    Development,
}
impl Mode {
    // aecore_app:set_app_ctrl_mode/0, with maintenance taking precedence.
    pub fn application(self) -> ApplicationMode {
        if self.maintenance {
            ApplicationMode::Maintenance
        } else if self.offline {
            ApplicationMode::Offline
        } else if self.development {
            ApplicationMode::Development
        } else {
            ApplicationMode::Normal
        }
    }
}
impl Resolved<'_> {
    pub fn mode(&self) -> Result<Mode, Error> {
        let flag = |name| {
            self.get(&["system", name])?.map_or(Ok(false), |v| {
                v.as_bool().ok_or(Error::Field("system mode"))
            })
        };
        let development = match self.get(&["system", "dev_mode"])? {
            Some(v) => v.as_bool().ok_or(Error::Field("dev_mode"))?,
            None => self.get(&["fork_management", "network_id"])? == Some(&json!("ae_dev")),
        };
        Ok(Mode {
            maintenance: flag("maintenance_mode")?,
            offline: flag("offline_mode")?,
            development,
        })
    }
    pub fn network_id(&self, command_line: Option<&[u8]>) -> Result<Vec<u8>, Error> {
        if let Some(bytes) = command_line {
            if bytes.len() > 65536 {
                return Err(Error::Limit);
            }
            return Ok(bytes.to_vec());
        }
        if let Some(v) = self.find(
            &["fork_management", "network_id"],
            &[
                Strategy::User,
                Strategy::Application {
                    app: "aecore",
                    key: "network_id",
                },
            ],
        )? {
            let s = v.as_str().ok_or(Error::Field("network_id"))?;
            if s.len() > 65536 {
                return Err(Error::Limit);
            }
            return Ok(s.as_bytes().to_vec());
        }
        Ok(if self.mode()?.development {
            b"ae_dev".to_vec()
        } else {
            b"ae_mainnet".to_vec()
        })
    }
    // Returns the actual key worker configuration; no files are created until
    // its existing owned worker is spawned. Password diagnostics stay redacted.
    pub fn keys(&self) -> Result<keys::Config, Error> {
        let data = self.context.expand("setup", &json!("$DATA_DIR"))?;
        let default = path(&data)?.join("aecore").join("keys");
        let directory = self
            .find(
                &["keys", "dir"],
                &[
                    Strategy::User,
                    Strategy::Application {
                        app: "aecore",
                        key: "keys_dir",
                    },
                ],
            )?
            .map_or(Ok(default), |v| path(&v))?;
        let pwd = self.user_or_application(
            &["keys", "peer_password"],
            "aecore",
            "peer_password",
            &json!(""),
        )?;
        let password = bytes(&pwd)?;
        if password.len() > 65536 {
            return Err(Error::Limit);
        }
        Ok(keys::Config::new(directory, password))
    }
    pub fn peers(&self) -> Result<startup::Configuration, Error> {
        let configured = self.get(&["peers"])?;
        let blocked =
            self.user_or_application(&["blocked_peers"], "aecore", "blocked_peers", &json!([]))?;
        let defaults = self
            .find(
                &["include_default_peers"],
                &[
                    Strategy::User,
                    Strategy::SchemaDefault,
                    Strategy::Value(&json!(true)),
                ],
            )?
            .ok_or(Error::Lookup)?;
        fn peers(value: Option<&Value>) -> Result<Vec<Vec<u8>>, Error> {
            value.map_or(Ok(Vec::new()), |v| {
                v.as_array()
                    .ok_or(Error::Field("peers"))?
                    .iter()
                    .map(bytes)
                    .collect()
            })
        }
        Ok(startup::Configuration {
            peers: peers(configured)?,
            blocked_peers: peers(Some(&blocked))?,
            include_default_peers: defaults
                .as_bool()
                .ok_or(Error::Field("include_default_peers"))?,
            ..Default::default()
        })
    }
    pub fn mining(
        &self,
        system: &pow::configuration::System,
    ) -> Result<pow::configuration::Configuration, Error> {
        pow::configuration::Configuration::from_value(
            self.get(&["mining", "cuckoo"])?.unwrap_or(&json!({})),
            system,
        )
        .map_err(Error::Mining)
    }
    pub fn hyperchain(
        &self,
        height: &str,
    ) -> Result<hyperchain::configuration::Configuration, Error> {
        let value = self
            .get(&["chain", "consensus", height])?
            .ok_or(Error::Field("consensus"))?;
        if value.get("type").and_then(Value::as_str) != Some("hyperchain") {
            return Err(Error::Field("consensus type"));
        }
        let value = value
            .get("config")
            .ok_or(Error::Field("consensus config"))?;
        let data = Zeroizing::new(serde_json::to_vec(value).map_err(|_| Error::Encoding)?);
        hyperchain::configuration::Configuration::parse(&data).map_err(Error::Hyperchain)
    }
}
