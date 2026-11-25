use super::resolution::{self, Resolved, Strategy};
use ae_sync::{
    observe::{self, statsd},
    routing,
};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(resolution::Error),
    Range(&'static str),
    Field(&'static str),
    Routing(routing::Error),
    Output(observe::Error),
    Family,
    NotConfigured,
}
impl From<resolution::Error> for Error {
    fn from(e: resolution::Error) -> Self {
        Self::Resolution(e)
    }
}
#[derive(Debug, Clone)]
pub struct Metrics {
    pub host: statsd::Host,
    pub port: u64,
    pub reconnect_ms: u64,
    pub rules: Vec<routing::Rule>,
}
impl Metrics {
    pub fn resolve(r: &Resolved<'_>) -> Result<Self, Error> {
        let host = match r.find(
            &["metrics", "host"],
            &[
                Strategy::User,
                Strategy::Application {
                    app: "aecore",
                    key: "metrics_host",
                },
            ],
        )? {
            None => statsd::Host::System,
            Some(Value::String(s)) => statsd::Host::Name(s),
            _ => return Err(Error::Field("host")),
        };
        let number = |name, application, fallback| {
            r.user_or_application(&["metrics", name], "aecore", application, &json!(fallback))?
                .as_u64()
                .ok_or(Error::Range(name))
        };
        let rules = match r.get(&["metrics", "rules"])? {
            None => vec![],
            Some(value) => value
                .as_array()
                .ok_or(Error::Field("rules"))?
                .iter()
                .map(|value| {
                    let field = |key, default: &[u8]| match value.get(key) {
                        None => Ok(default.to_vec()),
                        Some(v) => v
                            .as_str()
                            .map(|s| s.as_bytes().to_vec())
                            .ok_or(Error::Field("rule field")),
                    };
                    Ok(routing::Rule {
                        name: field("name", b"*")?,
                        kind: field("type", b"*")?,
                        datapoints: field("datapoints", b"default")?,
                        actions: field("actions", b"log,send")?,
                    })
                })
                .collect::<Result<_, Error>>()?,
        };
        Ok(Self {
            host,
            port: number("port", "metrics_port", 8125)?,
            reconnect_ms: number("reconnect_interval", "metrics_reconnect_interval", 10000)?,
            rules,
        })
    }
    pub fn apply(
        &self,
        routing: &mut Option<routing::Setup>,
        observer: &mut observe::Config,
    ) -> Result<statsd::Settings, Error> {
        let output = statsd::Settings {
            target: statsd::Config {
                host: self.host.clone(),
                port: u16::try_from(self.port).map_err(|_| Error::Range("port"))?,
                reconnect_interval: Duration::from_millis(self.reconnect_ms),
            },
            records: observer.sink_records,
            bytes: observer.sink_bytes,
            pipeline: Some(observe::pipeline::Config::default()),
        };
        output.validate().map_err(Error::Output)?;
        let mut setup = routing.clone().unwrap_or_default();
        setup.config.rules = self.rules.clone();
        for entry in observe::registry::predefined::entries() {
            setup.entries.retain(|old| old.name != entry.name);
            setup.entries.push(entry);
        }
        routing::Engine::new(
            setup.config.clone(),
            setup.entries.clone(),
            setup.atoms.clone(),
        )
        .map_err(Error::Routing)?;
        // Resolve and validate every consumer before changing caller settings.
        *routing = Some(setup);
        observer.interval = Duration::from_secs(10);
        observer.statsd = None;
        Ok(output)
    }
}
