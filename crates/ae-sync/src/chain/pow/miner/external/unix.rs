use super::{Operation, Termination, error};
use rustix::{
    fs::{OFlags, fcntl_getfl, fcntl_setfl},
    process::{Pid, Signal, WaitIdOptions, kill_process_group, waitid},
};
use std::{
    io::{self, PipeReader, Read},
    os::unix::process::CommandExt,
    process::{Command, ExitStatus, Stdio},
};
use tokio::io::unix::AsyncFd;

pub(super) struct Reader(AsyncFd<PipeReader>);
impl Reader {
    pub(super) async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut ready = self.0.readable().await?;
            match ready.try_io(|pipe| pipe.get_ref().read(bytes)) {
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }
}
pub(super) fn pipe(_: crate::chain::pow::miner::Id) -> io::Result<(Reader, Stdio, Stdio)> {
    let (read, write) = io::pipe()?;
    fcntl_setfl(&read, fcntl_getfl(&read)? | OFlags::NONBLOCK)?;
    Ok((
        Reader(AsyncFd::new(read)?),
        write.try_clone()?.into(),
        write.into(),
    ))
}
pub(super) struct Child {
    child: Option<std::process::Child>,
    pid: Pid,
    termination: Option<Termination>,
    owned: bool,
}
impl Child {
    pub(super) fn spawn(mut command: Command) -> io::Result<Self> {
        command.process_group(0);
        let child = command.spawn()?;
        let pid = Pid::from_raw(child.id() as i32).expect("positive OS child PID");
        Ok(Self {
            child: Some(child),
            pid,
            termination: None,
            owned: true,
        })
    }
    pub(super) fn id(&self) -> u32 {
        self.pid.as_raw_nonzero().get() as u32
    }
    pub(super) fn exited(&mut self) -> Result<bool, super::super::Error> {
        match waitid(
            rustix::process::WaitId::Pid(self.pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(value) => Ok(value.is_some()),
            Err(rustix::io::Errno::INTR) => Ok(false),
            Err(rustix::io::Errno::CHILD) => {
                self.owned = false;
                Err(super::super::Error::External(super::Error::OwnershipLost))
            }
            Err(e) => Err(error(Operation::Inspect, e)),
        }
    }
    pub(super) fn stop(&mut self) -> Result<(), super::super::Error> {
        if !self.owned {
            return Err(super::super::Error::External(super::Error::OwnershipLost));
        }
        if self.termination.is_none() {
            match kill_process_group(self.pid, Signal::KILL) {
                Ok(()) => self.termination = Some(Termination::Signalled),
                Err(rustix::io::Errno::SRCH) => self.termination = Some(Termination::AlreadyGone),
                #[cfg(target_os = "macos")]
                Err(rustix::io::Errno::PERM) if self.exited()? => {
                    self.termination = Some(Termination::PermissionAfterLeaderExit);
                }
                Err(e) => return Err(error(Operation::Kill, e)),
            }
        }
        Ok(())
    }
    pub(super) fn termination(&self) -> Option<Termination> {
        self.termination
    }
    pub(super) fn finish(&mut self) -> Result<ExitStatus, super::super::Error> {
        self.stop()?;
        let status = match self.child.as_mut().expect("owned child").wait() {
            Ok(status) => status,
            Err(e) => {
                if e.raw_os_error() == Some(rustix::io::Errno::CHILD.raw_os_error()) {
                    self.owned = false;
                }
                return Err(error(Operation::Wait, e));
            }
        };
        self.child = None;
        Ok(status)
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        if self.child.is_some() && self.owned {
            let _ = self.stop();
            if self.owned
                && let Some(mut child) = self.child.take()
            {
                // Even a group-signal failure must not leak the owned leader.
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}
