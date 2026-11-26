use super::*;
use ae_chain::channel::{self, Reader};

pub struct Permit<'a, O> {
    importer: &'a Importer<O>,
    reservation: blocking::Reservation,
    limits: channel::Limits,
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn channel_query(&self, rendering_bytes: usize) -> Result<Permit<'_, O>, Error> {
        self.check_open()?;
        let limits = channel::Limits::default();
        let weight = limits
            .reads
            .read_bytes
            .checked_add(limits.retained_bytes)
            .and_then(|n| n.checked_add(limits.reads.rlp.max_items.checked_mul(128)?))
            .and_then(|n| n.checked_add(rendering_bytes))
            .ok_or(Error::Busy)?;
        Ok(Permit {
            importer: self,
            reservation: self.jobs.reserve(weight)?,
            limits,
        })
    }
}
impl<O: MapOrder + Send + Sync + 'static> Permit<'_, O> {
    pub fn submit<T: Send + 'static>(
        self,
        key: [u8; 32],
        render: impl FnOnce(Result<channel::Channel, channel::Error>) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.importer.check_open()?;
        let context = self.importer.context.clone();
        let limits = self.limits;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let reply = Reader::new(&view, limits).and_then(|reader| reader.query(key));
            drop(view);
            context.check(&cancel, None)?;
            let result = render(reply);
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.importer.context.config.timeout,
        })
    }
}
