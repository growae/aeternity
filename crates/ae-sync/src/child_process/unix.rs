use super::{Operation, Termination, error};
use rustix::process::{Pid, Signal, WaitIdOptions, kill_process_group, waitid};
use std::os::unix::process::CommandExt;
use std::{
    io,
    process::{Command, ExitStatus},
};
pub(crate) struct Child {
    child: Option<std::process::Child>,
    pid: Pid,
    termination: Option<Termination>,
    owned: bool,
}
impl Child {
    pub(crate) fn spawn(mut command: Command) -> io::Result<Self> {
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
    pub(crate) fn id(&self) -> u32 {
        self.pid.as_raw_nonzero().get() as u32
    }
    pub(crate) fn exited(&mut self) -> Result<bool, super::Error> {
        match waitid(
            rustix::process::WaitId::Pid(self.pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(value) => Ok(value.is_some()),
            Err(rustix::io::Errno::INTR) => Ok(false),
            Err(rustix::io::Errno::CHILD) => {
                self.owned = false;
                Err(super::Error::OwnershipLost)
            }
            Err(e) => Err(error(Operation::Inspect, e)),
        }
    }
    pub(crate) fn stop(&mut self) -> Result<(), super::Error> {
        if !self.owned {
            return Err(super::Error::OwnershipLost);
        }
        if self.termination.is_none() {
            #[cfg(target_os = "macos")]
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
            #[cfg_attr(not(target_os = "macos"), allow(clippy::never_loop))]
            loop {
                match kill_process_group(self.pid, Signal::KILL) {
                    Ok(()) => self.termination = Some(Termination::Signalled),
                    Err(rustix::io::Errno::SRCH) => {
                        self.termination = Some(Termination::AlreadyGone)
                    }
                    #[cfg(target_os = "macos")]
                    Err(rustix::io::Errno::PERM) => {
                        // Darwin can deny the signal during exit before waitid
                        // exposes the zombie. Keep the leader unreaped while
                        // retrying; never treat a persistent denial as success.
                        if self.exited()? {
                            self.termination = Some(Termination::PermissionAfterLeaderExit);
                        } else if std::time::Instant::now() < deadline {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                            continue;
                        } else {
                            return Err(error(Operation::Kill, rustix::io::Errno::PERM));
                        }
                    }
                    Err(e) => return Err(error(Operation::Kill, e)),
                }
                break;
            }
        }
        Ok(())
    }
    pub(crate) fn termination(&self) -> Option<Termination> {
        self.termination
    }
    pub(crate) fn finish(&mut self) -> Result<ExitStatus, super::Error> {
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
