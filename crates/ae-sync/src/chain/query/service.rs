use super::*;

// Non-chain service work shares the captured owner's bounded blocking executor.
// No database snapshot is acquired, and replacement/cancellation is checked
// before and after I/O/rendering. The returned ticket retains capacity normally.
pub struct ServiceWork<O> {
    context: Arc<Context<O>>,
    reservation: blocking::Reservation,
    timeout: Duration,
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn service_work(&self, retained_bytes: usize) -> Result<ServiceWork<O>, Error> {
        self.check_open()?;
        Ok(ServiceWork {
            context: self.context.clone(),
            reservation: self.jobs.reserve(retained_bytes)?,
            timeout: self.context.config.timeout,
        })
    }
}
impl<O: MapOrder + Send + Sync + 'static> ServiceWork<O> {
    pub fn submit<T: Send + 'static>(
        self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let context = self.context;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let result = work();
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.timeout,
        })
    }
}
