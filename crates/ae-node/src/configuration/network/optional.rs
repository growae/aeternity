use super::{Error, Resolved, Strategy, flag, user_limits};
use ae_sync::{
    analytics,
    runtime::{connection, workers},
};
use serde_json::json;

// aec_peer_analytics:enabled/0 and aec_upnp:is_enabled/0 use user/schema
// values, not similarly named application settings. Activation is separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Optional {
    pub analytics: bool,
    pub upnp: bool,
    user_limits: analytics::NetworkLimits,
}
impl Optional {
    pub fn resolve(r: &Resolved<'_>) -> Result<Self, Error> {
        let read = |name| {
            let value = r
                .find(
                    &["sync", name],
                    &[
                        Strategy::User,
                        Strategy::SchemaDefault,
                        Strategy::Value(&json!(false)),
                    ],
                )?
                .ok_or(Error::Field(name))?;
            flag(&value, name)
        };
        Ok(Self {
            analytics: read("peer_analytics")?,
            upnp: read("upnp_enabled")?,
            user_limits: user_limits(r)?,
        })
    }
    pub fn apply(&self, workers: &mut workers::Config, connections: &mut connection::Config) {
        self.apply_options(&mut workers.analytics, &mut workers.upnp, connections);
    }
    pub fn apply_hyperchain(
        &self,
        workers: &mut workers::hyperchain::Config,
        connections: &mut connection::Config,
    ) {
        self.apply_options(&mut workers.analytics, &mut workers.upnp, connections);
    }
    fn apply_options(
        &self,
        analytics: &mut Option<analytics::Config>,
        upnp: &mut Option<ae_sync::upnp::Config>,
        connections: &mut connection::Config,
    ) {
        // Native analytics remains an actual worker even when disabled.
        analytics.get_or_insert_default().enabled = self.analytics;
        if self.upnp {
            upnp.get_or_insert_default();
        } else {
            *upnp = None;
        }
        // The retained analytics owner applies its environment effects when
        // the worker actually starts. Preserve the provenance of user limits.
        connections.analytics_limits = self.user_limits;
    }
}
