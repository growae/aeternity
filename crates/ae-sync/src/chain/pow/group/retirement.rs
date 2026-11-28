use super::*;
pub struct GeneratorExit {
    pub status: generator::Status,
    pub last_retirement: Option<packing::CancelReport>,
    pub final_retirement: Option<packing::CancelReport>,
}
pub struct ProducerExit {
    pub commands: Option<Box<chain::on_demand::service::Released>>,
    pub status: producer::Status,
    pub outputs: Vec<Arc<producer::Output>>,
    pub completions: Vec<producer::Completion>,
    pub generator_results: Vec<Result<(), generator::RequestError>>,
    pub generator_stop_rejected: Option<generator::RequestError>,
}
pub struct Retired {
    pub generator: Option<(u64, GeneratorExit)>,
    pub producer: Option<(u64, ProducerExit)>,
}
pub struct Retirement {
    // An absent report is not proof that accepted work did not happen. Keep
    // the original panic payload and prevent automatic replacement in that case.
    pub raw: supervisor::Retired<generator::Report, producer::Report>,
    final_retirement: Option<packing::CancelReport>,
}
impl Retirement {
    pub(super) fn new(raw: supervisor::Retired<generator::Report, producer::Report>) -> Self {
        Self {
            raw,
            final_retirement: None,
        }
    }
    pub(super) fn empty(&self) -> bool {
        self.raw.generator.is_none() && self.raw.conductor.is_none()
    }
    pub fn poll(&mut self, pool: &pool::Client) -> Result<bool, Error> {
        let mut ready = true;
        if let Some((_, result)) = &mut self.raw.conductor {
            let report = result
                .as_mut()
                .map_err(|_| Error::MissingRetirement(supervisor::Member::Conductor))?;
            ready &= report.poll_owned().map_err(Error::Commands)?;
        }
        if let Some((_, result)) = &mut self.raw.generator {
            let report = result
                .as_mut()
                .map_err(|_| Error::MissingRetirement(supervisor::Member::Generator))?;
            if let Some(pending) = &mut report.retirement {
                if pool.info().joined {
                    pending.retire_pool(pool).map_err(Error::Packing)?;
                }
                if pending.step().map_err(Error::Packing)? {
                    self.final_retirement =
                        report.retirement.take().map(packing::Cancelled::finish);
                } else {
                    ready = false;
                }
            }
        }
        Ok(ready)
    }
    pub(super) fn finish(self) -> Retired {
        // Only called after poll returned true. Extract observations, then
        // drop both lease-owning reports before the next constructors run.
        let generator = self.raw.generator.map(|(n, r)| {
            let mut r = r.unwrap_or_else(|_| unreachable!("checked retirement"));
            assert!(r.retirement.is_none());
            let out = GeneratorExit {
                status: r.status.clone(),
                last_retirement: r.last_retirement.take(),
                final_retirement: self.final_retirement,
            };
            drop(r);
            (n, out)
        });
        let producer = self.raw.conductor.map(|(n, r)| {
            let mut r = r.unwrap_or_else(|_| unreachable!("checked retirement"));
            assert!(r.is_settled());
            let out = ProducerExit {
                commands: r.commands.take().map(|r| {
                    Box::new(
                        r.release()
                            .unwrap_or_else(|_| unreachable!("checked command retirement")),
                    )
                }),
                status: r.status.clone(),
                outputs: std::mem::take(&mut r.outputs),
                completions: std::mem::take(&mut r.completions),
                generator_results: std::mem::take(&mut r.generator_results),
                generator_stop_rejected: r.generator_stop_rejected.take(),
            };
            drop(r);
            (n, out)
        });
        Retired {
            generator,
            producer,
        }
    }
}
pub struct Report {
    pub status: Status,
    pub outputs: Vec<Arc<Output>>,
    pub unpublished: Vec<Event>,
    pub retirements: Vec<Retirement>,
    pub settled: Vec<Retired>,
    pub(super) pool: pool::Client,
    pub(super) _lease: GroupLease,
}
// Settled observations can outlive this group without retaining its exclusive
// construction lease. Unresolved accepted work must keep the original Report.
pub struct Released {
    pub status: Status,
    pub outputs: Vec<Arc<Output>>,
    pub unpublished: Vec<Event>,
    pub settled: Vec<Retired>,
}
impl Report {
    pub fn commands_status(&self) -> Option<chain::on_demand::service::Status> {
        let pending = self.retirements.iter().filter_map(|r| {
            let (generation, report) = r.raw.conductor.as_ref()?;
            Some((
                *generation,
                report.as_ref().ok()?.commands.as_ref()?.status.clone(),
            ))
        });
        let settled = self.settled.iter().filter_map(|r| {
            let (generation, report) = r.producer.as_ref()?;
            Some((*generation, report.commands.as_ref()?.status.clone()))
        });
        pending
            .chain(settled)
            .max_by_key(|(generation, _)| *generation)
            .map(|(_, status)| status)
    }
    pub fn release(self) -> Result<Released, Box<Self>> {
        if !self.is_settled() {
            return Err(Box::new(self));
        }
        Ok(Released {
            status: self.status,
            outputs: self.outputs,
            unpublished: self.unpublished,
            settled: self.settled,
        })
    }
    // Settlement never constructs services or retries accepted requests. A
    // matching pool may have to be joined by its independent owner first.
    pub fn poll(&mut self) -> Result<bool, Error> {
        let mut index = 0;
        while index < self.retirements.len() {
            if self.retirements[index].poll(&self.pool)? {
                self.settled.push(self.retirements.remove(index).finish());
            } else {
                index += 1;
            }
        }
        self.status.retiring = !self.is_settled();
        Ok(self.is_settled())
    }
    pub fn is_settled(&self) -> bool {
        self.retirements.is_empty()
    }
}
