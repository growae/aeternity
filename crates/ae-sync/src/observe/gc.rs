use super::*;
use ae_storage::gc::events::{self, Kind};
impl Service {
    pub(super) fn drain_gc(&mut self) -> Result<(), Error> {
        self.gc_pending = false;
        match self.gc.after(&self.gc_cursor, 16) {
            Ok(batch) => {
                self.gc_pending = batch.records.len() == 16;
                for record in &batch.records {
                    self.current.gc_events = self.current.gc_events.saturating_add(1);
                    let event = match record.kind {
                        Kind::Switch => "gc_switch",
                        Kind::ScansComplete => "scans_complete",
                        Kind::CollectionComplete => "collection_complete",
                        Kind::Cancelled => "cancelled",
                    };
                    self.record("gc",json!({"event":event,"sequence":record.sequence,"epoch":record.state.epoch,"height":record.state.height,"revision":record.chain.revision,"state":progress(&record.state)}));
                }
                self.gc_cursor = batch.cursor;
                if batch.closed && batch.records.is_empty() {
                    return Err(Error::GcClosed);
                }
            }
            Err(events::Error::Gap { missed, resume }) => {
                let (info, latest) = self.gc.snapshot();
                self.current.gc_missed = self.current.gc_missed.saturating_add(missed);
                self.record("gc",json!({"event":"history_gap","missed":missed,"resume_after":resume.sequence(),"snapshot_sequence":latest.sequence(),"state":progress(&info.state)}));
                self.gc_cursor = resume;
                self.gc_pending = true;
            }
            Err(error) => return Err(Error::Gc(error)),
        }
        Ok(())
    }
}
fn progress(state: &ae_chain::repository::retention::State) -> Value {
    json!({"epoch":state.epoch,"height":state.height,"phase":format!("{:?}",state.phase),"pending":state.pending,"records":state.records,"nodes":state.nodes,"removed":state.removed,"bytes":state.bytes})
}
