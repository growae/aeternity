use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentHeader {
    pub selected: Current,
    pub version: u32,
    pub network: ae_protocol::NetworkProfile,
}

impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    // Capture the accepted top header (including micro headers), then render
    // within the same bounded job. The returned ticket retains all credits.
    pub fn current_header<T: Send + 'static>(
        &self,
        rendering_bytes: usize,
        render: impl FnOnce(CurrentHeader) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.check_open()?;
        let weight = self
            .context
            .db
            .read_config()
            .header_read_weight()?
            .checked_add(rendering_bytes)
            .ok_or(Error::Busy)?;
        let reservation = self.jobs.reserve(weight)?;
        let context = self.context.clone();
        let pending = reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let (view, cursor) = context.db.event_snapshot()?;
            let status = view.status();
            let top = status.tip.ok_or(repository::Error::Uninitialized)?;
            let block = view.require_header(top)?;
            let height = block.common().height;
            let version = block.common().version;
            let key = match block {
                Header::Key(_) => top,
                Header::Micro(micro) => Hash256(micro.common.previous_key_hash),
            };
            drop(view);
            context.check(&cancel, None)?;
            context.height.seed(cursor, height);
            let result = render(CurrentHeader {
                selected: Current {
                    revision: status.revision,
                    top,
                    key,
                    height,
                },
                version,
                network: context.strategy.network().clone(),
            });
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.context.config.timeout,
        })
    }
}
