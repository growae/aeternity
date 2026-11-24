use super::{Operation, Termination, error};
use process_wrap::std::{ChildWrapper, CommandWrap, JobObject};
use std::{
    fs::OpenOptions,
    io,
    process::{Command, ExitStatus, Stdio},
};
use tokio::{
    io::AsyncReadExt,
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
};

pub(super) struct Reader {
    pipe: NamedPipeServer,
    connected: bool,
}
impl Reader {
    pub(super) async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if !self.connected {
            self.pipe.connect().await?;
            self.connected = true;
        }
        self.pipe.read(bytes).await
    }
}
pub(super) fn pipe(id: crate::chain::pow::miner::Id) -> io::Result<(Reader, Stdio, Stdio)> {
    let name = format!(
        r"\\.\pipe\ae-pow-{}-{}-{}-{:032x}",
        std::process::id(),
        id.worker,
        id.sequence,
        rand::random::<u128>()
    );
    let pipe = ServerOptions::new()
        .first_pipe_instance(true)
        .access_inbound(true)
        .access_outbound(false)
        .create(&name)?;
    let writer = OpenOptions::new().write(true).open(&name)?;
    Ok((
        Reader {
            pipe,
            connected: false,
        },
        writer.try_clone()?.into(),
        writer.into(),
    ))
}
pub(super) struct Child {
    child: Option<Box<dyn ChildWrapper>>,
    stopped: bool,
}
impl Child {
    pub(super) fn spawn(command: Command) -> io::Result<Self> {
        let mut command = CommandWrap::from(command);
        command.wrap(JobObject);
        Ok(Self {
            child: Some(command.spawn()?),
            stopped: false,
        })
    }
    pub(super) fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }
    pub(super) fn exited(&mut self) -> Result<bool, super::super::Error> {
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
    pub(super) fn stop(&mut self) -> Result<(), super::super::Error> {
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
    pub(super) fn termination(&self) -> Option<Termination> {
        self.stopped.then_some(Termination::Signalled)
    }
    pub(super) fn finish(&mut self) -> Result<ExitStatus, super::super::Error> {
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
