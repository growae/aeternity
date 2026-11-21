use super::*;
use ae_chain::resilience;

impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub(super) async fn policy(
        &self,
        event: resilience::Event,
        config: resilience::Config,
    ) -> Result<resilience::Snapshot, Error> {
        let context = self.context.clone();
        tokio::time::timeout(self.context.config.timeout, async {
            self.jobs
                .submit_wait(1, move |cancel| context.apply_policy(event, config, cancel))
                .await?
                .await?
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}
