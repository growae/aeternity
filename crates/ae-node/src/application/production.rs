use super::*;
use ae_sync::chain::{
    hyperchain::conductor,
    pow::{configuration as mining, generator, group, producer},
};
use ae_types::AccountPublicKey;
use configuration::resolution::{Resolved, Strategy};
use serde_json::{Value, json};
use std::time::Duration;

pub(super) struct Plan {
    beneficiary: Option<AccountPublicKey>,
    autostart: bool,
    strict: bool,
    timeout: Option<Duration>,
    mining: Option<mining::Configuration>,
}
impl Plan {
    pub fn resolve(
        resolved: &Resolved<'_>,
        family: runtime::Family,
        stratum: bool,
    ) -> Result<Self, Error> {
        let read = |name, application, fallback| {
            resolved
                .user_or_application(&["mining", name], "aecore", application, &fallback)
                .map_err(|e| Error::at("production configuration", e))
        };
        let flag = |value: Value| {
            value
                .as_bool()
                .ok_or_else(|| Error::at("production configuration", "boolean required"))
        };
        let mode = resolved
            .mode()
            .map_err(|e| Error::at("application mode", e))?;
        let autostart = flag(read("autostart", "autostart", json!(false))?)? && !mode.maintenance;
        let strict = if mode.development
            && resolved
                .get(&["mining", "strictly_follow_top"])
                .map_err(|e| Error::at("production configuration", e))?
                .is_none()
        {
            true
        } else {
            flag(read(
                "strictly_follow_top",
                "strictly_follow_top",
                json!(false),
            )?)?
        };
        let beneficiary = if family == runtime::Family::Hyperchain || stratum {
            None
        } else {
            resolved
                .find(
                    &["mining", "beneficiary"],
                    &[
                        Strategy::User,
                        Strategy::Application {
                            app: "aecore",
                            key: "beneficiary",
                        },
                    ],
                )
                .map_err(|e| Error::at("beneficiary", e))?
                .map(|v| {
                    let text = v
                        .as_str()
                        .ok_or_else(|| Error::at("beneficiary", "account address required"))?;
                    let key = ae_codec::api::decode_as(ae_codec::api::ApiType::AccountPubkey, text)
                        .map_err(|e| Error::at("beneficiary", e))?;
                    Ok(AccountPublicKey(key.try_into().map_err(|_| {
                        Error::at("beneficiary", "account key length")
                    })?))
                })
                .transpose()?
        };
        let (timeout, mining) = if family == runtime::Family::Cuckoo && !stratum {
            let timeout = read(
                "attempt_timeout",
                "mining_attempt_timeout",
                json!(3_600_000),
            )?;
            let timeout = if timeout == json!("infinity") {
                None
            } else {
                Some(Duration::from_millis(
                    timeout.as_u64().filter(|n| *n > 0).ok_or_else(|| {
                        Error::at("mining attempt timeout", "positive milliseconds required")
                    })?,
                ))
            };
            (
                timeout,
                Some(
                    resolved
                        .mining(&mining::System::default())
                        .map_err(|e| Error::at("miner configuration", e))?,
                ),
            )
        } else {
            (None, None)
        };
        Ok(Self {
            beneficiary,
            autostart,
            strict,
            timeout,
            mining,
        })
    }
    pub fn settings(
        &self,
        node: &runtime::Prepared,
        directories: &mining::Directories,
        cache_size: usize,
        stratum: Option<crate::stratum::configured::Config>,
    ) -> Result<runtime::Settings, Error> {
        Ok(match node.family() {
            runtime::Family::Cuckoo => {
                let mut config: crate::pow::Config = crate::pow::Config {
                    stratum: stratum.map(Arc::new),
                    ..Default::default()
                };
                config.production.producer.external_key_work = config.stratum.is_some();
                let cycle = node.configured().chain().settings().micro_cycle_ms();
                config.production.producer.micro_cycle_ms = cycle;
                config.production.generator.packing.window_ms =
                    i64::try_from(cycle).map_err(|e| Error::at("micro cycle", e))?;
                config.production.producer.enabled = self.autostart;
                config.production.producer.strictly_follow_top = self.strict;
                if let Some(beneficiary) = self.beneficiary {
                    let mut mining = producer::mining::Config::new(beneficiary);
                    mining.attempt_timeout = self.timeout;
                    config.production.mining = Some(
                        self.mining
                            .as_ref()
                            .expect("Cuckoo configuration")
                            .prepare(directories, Default::default(), mining)
                            .map_err(|e| Error::at("miner executable configuration", e))?
                            .config,
                    );
                }
                runtime::Settings::Cuckoo(Box::new(runtime::Cuckoo {
                    config,
                    clocks: Arc::new(group::SystemClocks::default()),
                }))
            }
            runtime::Family::OnDemand => {
                if self.autostart {
                    return Err(Error::at(
                        "automatic on-demand production integration",
                        "command runtime requires explicit block requests",
                    ));
                }
                let beneficiary = self.beneficiary.ok_or_else(|| {
                    Error::at(
                        "on-demand beneficiary",
                        "beneficiary required by command runtime",
                    )
                })?;
                runtime::Settings::OnDemand(Box::new(runtime::OnDemand {
                    config: crate::pow::Config::with_production(crate::pow::Commands::new(
                        beneficiary,
                    )),
                    clock: Arc::new(generator::SystemClock),
                }))
            }
            runtime::Family::Hyperchain => {
                let (gossip, cache) =
                    ae_storage::pool::worker::GossipCache::managed(cache_size, 100_000)
                        .map_err(|e| Error::at("gossip cache", e))?;
                let mut runtime = node.hyperchain().expect("checked family").runtime();
                runtime.gossip = gossip;
                runtime.producing = self.autostart;
                runtime.block_producer = node
                    .configured()
                    .chain()
                    .settings()
                    .consensus()
                    .genesis()
                    .config()
                    .get("stakers")
                    .and_then(Value::as_array)
                    .is_some_and(|s| !s.is_empty());
                runtime::Settings::Hyperchain(Box::new(runtime::Hyperchain {
                    config: Default::default(),
                    runtime,
                    services: Default::default(),
                    cache,
                    identity: crate::hyperchain::Identity::Managed,
                    clocks: Arc::new(|_| Ok(conductor::runtime::SystemClock)),
                }))
            }
        })
    }
}
