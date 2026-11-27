use super::{Operation, Termination, error};
use process_wrap::std::{ChildWrapper, CommandWrap, JobObject};
use std::{
    io,
    process::{Command, ExitStatus},
};
pub(crate) struct Child {
    child: Option<Box<dyn ChildWrapper>>,
    stopped: bool,
}
impl Child {
    pub(crate) fn spawn(command: Command) -> io::Result<Self> {
        let mut command = CommandWrap::from(command);
        command.wrap(JobObject);
        Ok(Self {
            child: Some(command.spawn()?),
            stopped: false,
        })
    }
    pub(crate) fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }
    pub(crate) fn exited(&mut self) -> Result<bool, super::Error> {
        // Inspect the native leader, not the wrapper's all-job wait. The job
        // handle keeps descendant ownership independent of PID reuse.
        self.child
            .as_mut()
            .unwrap()
            .inner_mut()
            .try_wait()
            .map(|s| s.is_some())
            .map_err(|e| error(Operation::Inspect, e))
    }
    pub(crate) fn stop(&mut self) -> Result<(), super::Error> {
        if !self.stopped {
            self.child
                .as_mut()
                .unwrap()
                .start_kill()
                .map_err(|e| error(Operation::Kill, e))?;
            self.stopped = true;
        }
        Ok(())
    }
    pub(crate) fn termination(&self) -> Option<Termination> {
        self.stopped.then_some(Termination::Signalled)
    }
    pub(crate) fn finish(&mut self) -> Result<ExitStatus, super::Error> {
        self.stop()?;
        let status = self
            .child
            .as_mut()
            .unwrap()
            .wait()
            .map_err(|e| error(Operation::Wait, e))?;
        self.child = None;
        Ok(status)
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = self.finish();
        }
    }
}
