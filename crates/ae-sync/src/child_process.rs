// Shared ownership of trusted external workers. An unreaped Unix leader keeps
// its PID/process-group identity reserved until descendants have been signalled.
// Windows uses a job object whose wait includes zero active job processes.
use std::io;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
#[cfg(unix)]
pub(crate) use unix::Child;
#[cfg(windows)]
pub(crate) use windows::Child;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Runtime,
    Pipe,
    Spawn,
    Read,
    Inspect,
    Kill,
    Wait,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Io {
        operation: Operation,
        kind: io::ErrorKind,
    },
    OwnershipLost,
}
fn error(operation: Operation, error: impl Into<io::Error>) -> Error {
    Error::Io {
        operation,
        kind: error.into().kind(),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Signalled,
    AlreadyGone,
    // Darwin may report EPERM for a group containing only zombies. This proves
    // leader exit, not a successful signal to every descendant.
    PermissionAfterLeaderExit,
}
