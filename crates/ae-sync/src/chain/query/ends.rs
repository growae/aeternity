use super::*;

impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    // Return the complete persisted key-branch end set or reject the request.
    // The same job owns the snapshot, rendering and retained response credits.
    pub fn chain_ends<T: Send + 'static>(
        &self,
        limit: usize,
        rendering_bytes: usize,
        render: impl FnOnce(Vec<Hash256>) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.check_open()?;
        let weight = limit
            .checked_mul(128)
            .and_then(|n| n.checked_add(64 << 10))
            .and_then(|n| n.checked_add(rendering_bytes))
            .ok_or(Error::Busy)?;
        let reservation = self.jobs.reserve(weight)?;
        let context = self.context.clone();
        let pending = reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let ends = view.chain_ends(limit).map_err(|e| match e {
                repository::Error::ResourceLimit => Error::Busy,
                other => Error::Storage(other),
            })?;
            drop(view);
            context.check(&cancel, None)?;
            let result = render(ends);
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.context.config.timeout,
        })
    }
}
