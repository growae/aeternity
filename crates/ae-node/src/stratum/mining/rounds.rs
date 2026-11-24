// The mining/accounting actor owns both callbacks and their single Writer.
use super::*;
use storage::rounds::{Config, Limits, Receipt};
pub struct Inputs {
    pub config: Config,
    pub limits: Limits,
}
impl Inputs {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            limits: Default::default(),
        }
    }
}
pub(super) fn start(writer: &mut storage::Writer, inputs: Inputs) -> Result<Limits, Error> {
    let info = retry(writer, |w| {
        w.begin_rounds(inputs.config.clone(), inputs.limits)
    })?;
    // Resume the second half of an interrupted callback before admitting any
    // new share. Already committed round keys must never be allocated again.
    if info.computing() {
        retry(writer, |w| w.step_rounds(info.next, inputs.limits))?;
    }
    Ok(inputs.limits)
}
pub(super) fn tick(writer: &mut storage::Writer, limits: Limits) -> Result<Option<Receipt>, Error> {
    let info = retry(writer, |w| w.capture_rounds(limits))?;
    if !info.pending.is_empty() {
        return Ok(Some(retry(writer, |w| w.step_rounds(info.next, limits))?));
    }
    Ok(None)
}
