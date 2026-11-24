use super::*;
use ae_chain::{
    block::Block,
    publication::{Effect, Kind},
};
// This cursor is process-local and owned by the exclusive Writer. Starting a
// new mining epoch clears all active authority before binding a new cursor.
// Neither cached work nor a historical publication receipt grants authority.
#[derive(Debug, Clone)]
pub struct Frame {
    pub revision: u64,
    pub reset_revision: u64,
    pub top: Option<Hash256>,
    pub generation: Option<Hash256>,
    pub invalidated: bool,
    pub gap: bool,
    pub info: Info,
}
impl Writer {
    pub(in crate::stratum) fn finish_work(
        &mut self,
        result: Result<super::super::Commit, Failure>,
        revision: u64,
        reset: u64,
    ) -> Result<super::super::Commit, Failure> {
        if result.is_ok() {
            self.work_revision = Some(revision);
            self.work_reset = reset;
        } else if let Some(pending) = self.pending.as_mut() {
            pending.work_after = Some((revision, reset));
        }
        result
    }

    pub(super) fn work_frame(
        &self,
        stored: &Stored<'_>,
        journal: &Journal<'_>,
    ) -> Result<Frame, Failure> {
        let view = repo::View::load_with_config(stored.source.clone(), self.db.config.clone())?;
        let status = view.status();
        let generation = status
            .tip
            .map(|hash| -> Result<Hash256, Error> {
                match view
                    .block(hash)?
                    .ok_or(Error::Corrupt("Stratum work top missing"))?
                {
                    Block::Key(_) => Ok(hash),
                    Block::Micro(block) => Ok(Hash256(block.header.common.previous_key_hash)),
                }
            })
            .transpose()?;
        let mut frame = Frame {
            revision: status.revision,
            reset_revision: self.work_reset,
            top: status.tip,
            generation,
            invalidated: false,
            gap: false,
            info: journal.info.clone(),
        };
        let Some(revision) = self.work_revision else {
            // Only a successfully begun mining epoch may use work authority.
            return Err(Error::Conflict.into());
        };
        if revision > status.revision {
            return Err(Error::Corrupt("Stratum work revision").into());
        }
        let source = self.db.events();
        let mut cursor = source.at_revision(revision).map_err(Failure::Events)?;
        while cursor.revision() < status.revision {
            let batch = match source.after(&cursor, 128) {
                Ok(batch) => batch,
                Err(crate::events::Error::Gap { .. }) => {
                    frame.invalidated = true;
                    frame.gap = true;
                    break;
                }
                Err(error) => return Err(Failure::Events(error)),
            };
            let before = cursor.revision();
            for event in &batch.records {
                if event.change().revision > status.revision {
                    break;
                }
                for effect in event.effects() {
                    match effect {
                        Effect::TopChanged(block) if block.kind == Kind::Key => {
                            frame.invalidated = true
                        }
                        Effect::TopChanged(block) => {
                            let key = view
                                .block(block.hash)?
                                .map(|b| Hash256(b.header().common().previous_key_hash));
                            if key.is_none()
                                || frame.info.active.iter().any(|c| {
                                    Some(Hash256(c.header().common.previous_key_hash)) != key
                                })
                            {
                                frame.invalidated = true;
                            }
                        }
                        Effect::Reverted(_) => frame.invalidated = true,
                        _ => {}
                    }
                }
                cursor = source
                    .at_revision(event.change().revision)
                    .map_err(Failure::Events)?;
            }
            if frame.invalidated {
                break;
            }
            if cursor.revision() == before {
                return Err(Error::Corrupt("Stratum work event continuity").into());
            }
        }
        if frame
            .info
            .active
            .iter()
            .any(|c| Some(Hash256(c.header().common.previous_key_hash)) != generation)
        {
            frame.invalidated = true;
        }
        if frame.invalidated {
            frame.reset_revision = frame.revision;
            frame.info.active.clear();
        }
        Ok(frame)
    }
    pub fn refresh_work(&mut self, epoch: &Epoch, limits: Limits) -> Result<Frame, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        self.check_mining_epoch(epoch, &journal)?;
        let frame = self.work_frame(&stored, &journal)?;
        let extra = if frame.info != journal.info {
            vec![journal.extra(&frame.info)?]
        } else {
            vec![]
        };
        let checks = vec![status_check(&stored)?];
        drop(journal);
        drop(stored);
        let result = self.commit_checked(version, vec![], extra, checks, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        });
        self.finish_work(result, frame.revision, frame.reset_revision)?;
        Ok(frame)
    }
    pub fn cleanup_work(
        &mut self,
        epoch: &Epoch,
        before: core::Date,
        limits: Limits,
    ) -> Result<super::super::Commit, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        self.check_mining_epoch(epoch, &stored.mining(limits)?)?;
        drop(stored);
        // Native cache expiry does not retire conductor authority. A later
        // missing-cache solution records its share but never reaches admission.
        self.commit(version, vec![Command::DeleteCandidatesBefore(before)])
    }
}
