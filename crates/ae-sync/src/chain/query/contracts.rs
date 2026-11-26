use super::*;
use ae_chain::contract::{self, Kind, Reader, Reply};

pub struct Permit<'a, O> {
    importer: &'a Importer<O>,
    reservation: blocking::Reservation,
    limits: contract::Limits,
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn contract_query(&self, rendering_bytes: usize) -> Result<Permit<'_, O>, Error> {
        self.check_open()?;
        let limits = contract::Limits::default();
        let weight = limits
            .reads
            .read_bytes
            .checked_add(limits.proof.max_work_bytes)
            .and_then(|n| n.checked_add(limits.reads.rlp.max_items.checked_mul(128)?))
            .and_then(|n| n.checked_add(limits.proof.max_entries.checked_mul(128)?))
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
        kind: Kind,
        render: impl FnOnce(Result<Reply, contract::Error>) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.importer.check_open()?;
        let context = self.importer.context.clone();
        let limits = self.limits;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let reply = Reader::new(&view, limits).and_then(|reader| reader.query(key, kind));
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
