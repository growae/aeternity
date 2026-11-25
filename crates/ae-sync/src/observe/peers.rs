use super::*;
use crate::registry::worker::Metrics;

pub(super) type Read = Pin<Box<dyn Future<Output = Result<Metrics, Error>> + Send>>;

pub(super) async fn wait(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
impl Service {
    pub(super) fn count_peers(&mut self) {
        let network = self.sources.network.clone();
        let timeout = self.config.sample_timeout;
        self.peer_counts = Some(Box::pin(async move {
            tokio::time::timeout(timeout, async {
                network
                    .metrics()
                    .map_err(|_| Error::Sample)?
                    .wait()
                    .await
                    .map_err(|_| Error::Sample)
            })
            .await
            .map_err(|_| Error::Timeout)?
        }));
    }
    pub(super) fn peer_counted(&mut self, result: Result<Metrics, Error>) {
        match result {
            Ok(metrics) => {
                self.current.peer_count_samples = self.current.peer_count_samples.saturating_add(1);
                let counts = metrics.registry.counts;
                self.record("peers", json!({
                    "outbound": counts.outbound, "maximum_outbound": metrics.maximum_outbound,
                    "inbound": counts.inbound, "maximum_inbound": metrics.maximum_inbound,
                    "verified": metrics.verified, "unverified": metrics.unverified, "standby": metrics.standby,
                    "message": format!("Peer connections outbound: {}/{}, inbound: {}/{}, verified {}, unverified {}, standby {}",
                        counts.outbound, metrics.maximum_outbound, counts.inbound, metrics.maximum_inbound,
                        metrics.verified, metrics.unverified, metrics.standby)
                }));
            }
            Err(error) => {
                self.current.peer_count_errors = self.current.peer_count_errors.saturating_add(1);
                self.current.last_error = Some(error);
            }
        }
    }
}
