use super::{Outcome, Receipt, Shared, Work, cuckoo, lock};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    ffi::OsString, io, path::PathBuf, process::ExitStatus, sync::atomic::Ordering, time::Duration,
};

#[cfg(unix)]
#[path = "external/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "external/windows.rs"]
mod platform;
mod protocol;

#[derive(Debug, Clone)]
pub struct Config {
    #[cfg(test)]
    pub(crate) prefix_args: Vec<OsString>,
    pub executable: PathBuf,
    pub directory: PathBuf,
    pub extra_args: Vec<OsString>,
    pub device: Option<u32>,
    pub hex_header: bool,
    pub line_bytes: usize,
    pub output_bytes: u64,
    pub poll: Duration,
    // Drain buffered output after the leader exits; inherited pipes cannot
    // postpone retirement indefinitely. This is not a mining-attempt timer.
    pub drain: Duration,
}
impl Config {
    pub fn new(executable: PathBuf, directory: PathBuf) -> Self {
        Self {
            #[cfg(test)]
            prefix_args: vec![],
            executable,
            directory,
            extra_args: vec![],
            device: None,
            hex_header: false,
            line_bytes: 8192,
            output_bytes: 16 * 1024 * 1024,
            poll: Duration::from_millis(5),
            drain: Duration::from_millis(100),
        }
    }
    pub fn native_extra_args(value: &str) -> Vec<OsString> {
        value
            .split(' ')
            .filter(|v| !v.is_empty())
            .map(OsString::from)
            .collect()
    }
    pub(super) fn validate(&self) -> Result<(), super::Error> {
        if !self.executable.is_absolute()
            || !self.directory.is_absolute()
            || !(1..=1024 * 1024).contains(&self.line_bytes)
            || !(1..=64 * 1024 * 1024).contains(&self.output_bytes)
            || self.poll.is_zero()
            || self.poll > Duration::from_millis(100)
            || self.drain.is_zero()
            || self.drain > Duration::from_secs(5)
        {
            return Err(super::Error::Configuration);
        }
        Ok(())
    }
    fn arguments(&self, work: Work) -> Vec<OsString> {
        let hash = STANDARD.encode(work.hash.0);
        let hash = if self.hex_header {
            hash.bytes().map(|b| format!("{b:02X}")).collect()
        } else {
            hash
        };
        let mut args = Vec::new();
        #[cfg(test)]
        args.extend(self.prefix_args.iter().cloned());
        args.extend([
            "-h".into(),
            hash.into(),
            "-n".into(),
            work.nonces.start().to_string().into(),
            "-r".into(),
            work.nonces.repeats().to_string().into(),
        ]);
        args.extend(self.extra_args.iter().cloned());
        if let Some(device) = self.device {
            args.extend(["-d".into(), device.to_string().into()]);
        }
        args
    }
}
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
    LineLimit,
    OutputLimit,
    BadNonce,
    BadEdge,
    BadSize,
    NonceOutsideReservation,
    DrainTimeout,
    OwnershipLost,
}
fn error(operation: Operation, error: impl Into<io::Error>) -> super::Error {
    super::Error::External(Error::Io {
        operation,
        kind: error.into().kind(),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}
impl From<ExitStatus> for Exit {
    fn from(status: ExitStatus) -> Self {
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        #[cfg(not(unix))]
        let signal = None;
        Self {
            code: status.code(),
            signal,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Starting,
    Reading,
    Verifying,
    Draining,
    Reaped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Signalled,
    AlreadyGone,
    // Darwin also reports EPERM for a group containing only zombies. The
    // leader's exit is verified, but this is not a successful group signal.
    PermissionAfterLeaderExit,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    pub pid: Option<u32>,
    pub bytes: u64,
    pub lines: u64,
    pub exit: Option<Exit>,
    pub termination: Option<Termination>,
    pub phase: Phase,
}
fn progress(shared: &Shared, report: Report) {
    lock(&shared.state).status.external = Some(report);
    #[cfg(test)]
    if let Some(hook) = lock(&shared.external_hook).as_mut() {
        hook(report);
    }
}
pub(super) fn execute(
    shared: &Shared,
    receipt: &Receipt,
    config: &Config,
) -> (Report, Result<Outcome, super::Error>) {
    let mut report = Report::default();
    let result = (|| {
        if receipt.cancelled.load(Ordering::Acquire) {
            return Err(super::Error::Cancelled);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| error(Operation::Runtime, e))?;
        runtime.block_on(run(shared, receipt, config, &mut report))
    })();
    progress(shared, report);
    (report, result)
}
async fn run(
    shared: &Shared,
    receipt: &Receipt,
    config: &Config,
    report: &mut Report,
) -> Result<Outcome, super::Error> {
    let (mut reader, stdout, stderr) =
        platform::pipe(receipt.id).map_err(|e| error(Operation::Pipe, e))?;
    let mut command = std::process::Command::new(&config.executable);
    command
        .current_dir(&config.directory)
        .args(config.arguments(receipt.work))
        .stdin(std::process::Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    let mut child = platform::Child::spawn(command).map_err(|e| error(Operation::Spawn, e))?;
    report.pid = Some(child.id());
    report.phase = Phase::Reading;
    progress(shared, *report);
    // Child owns cleanup during unwinding as well as ordinary completion.
    let result = scan(shared, receipt, config, report, &mut reader, &mut child).await;
    drop(reader);
    let exit = child.finish();
    report.termination = child.termination();
    match exit {
        Ok(exit) => {
            report.exit = Some(exit.into());
            report.phase = Phase::Reaped;
        }
        Err(e) => return Err(e),
    }
    match result? {
        Some(found) => Ok(found),
        None => Ok(Outcome::NoSolution),
    }
}
async fn scan(
    shared: &Shared,
    receipt: &Receipt,
    config: &Config,
    report: &mut Report,
    reader: &mut platform::Reader,
    child: &mut platform::Child,
) -> Result<Option<Outcome>, super::Error> {
    let mut framing = protocol::Framing::new(config.line_bytes);
    let mut buffer = [0; 4096];
    let mut eof = false;
    let mut deadline = None;
    loop {
        if receipt.cancelled.load(Ordering::Acquire) {
            return Err(super::Error::Cancelled);
        }
        if deadline.is_none() && child.exited()? {
            // Kill inherited writers before reaping the leader. On Unix the
            // unreaped child prevents reuse of its PID/process-group identity.
            child.stop()?;
            deadline = Some(tokio::time::Instant::now() + config.drain);
            report.phase = Phase::Draining;
            progress(shared, *report);
        }
        if deadline.is_some() && eof {
            return Ok(None);
        }
        if deadline.is_some_and(|at| tokio::time::Instant::now() >= at) {
            return Err(super::Error::External(Error::DrainTimeout));
        }
        tokio::select! {
            read = reader.read(&mut buffer), if !eof => {
                let n = read.map_err(|e| error(Operation::Read, e))?;
                if n == 0 { eof = true; continue; }
                report.bytes += n as u64;
                if report.bytes > config.output_bytes { return Err(super::Error::External(Error::OutputLimit)); }
                let found = framing.feed(&buffer[..n], |line| {
                    report.lines += 1;
                    protocol::parse(line, receipt.work)
                }).map_err(super::Error::External)?;
                progress(shared, *report);
                if let Some(Outcome::Found { nonce, proof }) = found {
                    report.phase = Phase::Verifying;
                    progress(shared, *report);
                    let index = nonce.checked_sub(receipt.work.nonces.start());
                    if index.and_then(|i| receipt.work.nonces.at(i)) != Some(nonce) { return Err(super::Error::External(Error::NonceOutsideReservation)); }
                    if !cuckoo::verify_proof_from_hash(receipt.work.hash, nonce, proof.as_ref(), receipt.work.edge_bits) { return Err(super::Error::InvalidProof); }
                    return Ok(Some(Outcome::Found { nonce, proof }));
                }
            }
            _ = tokio::time::sleep(config.poll) => {}
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
