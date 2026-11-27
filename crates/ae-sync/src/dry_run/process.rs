// A blocking admission job owns an isolated process and its immutable broker.
// This is a trusted local-worker protocol, not a sandbox for arbitrary programs.
use super::{Client, Control, Ticket, Timeout};
use crate::child_process;
use ae_worker_heap::{Limit, Source};
use std::{
    ffi::OsString,
    io,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::Ordering,
    time::Duration,
};
mod monitor;
#[cfg(unix)]
#[path = "process/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "process/windows.rs"]
mod platform;
pub mod trie;
pub mod wire;
pub use monitor::{Cause, Observation};

#[derive(Debug, Clone)]
pub struct Config {
    pub executable: PathBuf,
    pub directory: PathBuf,
    pub arguments: Vec<OsString>,
    pub heap: Limit,
    pub frame_bytes: usize,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub read_requests: usize,
    pub read_bytes: u64,
    pub process_overhead: usize,
    pub poll: Duration,
    pub drain: Duration,
    #[cfg(test)]
    stderr: bool,
    #[cfg(test)]
    reaped: Option<std::sync::mpsc::Sender<u32>>,
}
impl Config {
    pub fn new(executable: PathBuf, directory: PathBuf) -> Self {
        Self {
            executable,
            directory,
            arguments: vec![],
            heap: Limit {
                bytes: 128 << 20,
                source: Source::Service,
            },
            frame_bytes: 8 << 20,
            input_bytes: 5 << 20,
            output_bytes: 8 << 20,
            read_requests: 65536,
            read_bytes: 64 << 20,
            process_overhead: 32 << 20,
            poll: Duration::from_millis(5),
            drain: Duration::from_millis(100),
            #[cfg(test)]
            stderr: false,
            #[cfg(test)]
            reaped: None,
        }
    }
    pub fn reservation(&self, input: usize) -> Result<usize, super::Error> {
        if !self.executable.is_absolute()
            || !self.directory.is_absolute()
            || self.poll.is_zero()
            || self.poll > Duration::from_millis(100)
            || self.drain.is_zero()
            || self.drain > Duration::from_secs(5)
            || input > self.input_bytes
            || self.input_bytes > self.frame_bytes
            || self.output_bytes > self.frame_bytes
            || self.read_requests == 0
            || self.read_requests > 1_000_000
            || self.read_bytes > 1 << 30
            || self.process_overhead == 0
            || wire::bootstrap(self.heap, self.frame_bytes).is_err()
        {
            return Err(super::Error::Configuration);
        }
        // Reserve the child heap and explicit overhead plus input, output and
        // simultaneously retained request/reply/framing storage. This weight
        // is not a measurement or hard bound on native allocations or total RSS.
        [
            self.heap.bytes,
            self.process_overhead,
            input,
            self.output_bytes,
            self.frame_bytes,
            self.frame_bytes,
            self.frame_bytes,
        ]
        .into_iter()
        .try_fold(0_usize, |a, b| {
            a.checked_add(b).ok_or(super::Error::Configuration)
        })
    }
}
pub trait Broker {
    type Error;
    fn read(&mut self, query: &[u8]) -> Result<Vec<u8>, Self::Error>;
}
#[derive(Debug)]
pub enum MonitorError {
    Process(child_process::Error),
    Panicked,
}
#[derive(Debug)]
pub enum Error<E> {
    Control(super::Error),
    Configuration,
    Io(io::ErrorKind),
    Protocol,
    Capacity,
    Broker(E),
    Monitor(MonitorError),
    Exit(std::process::ExitStatus),
    DrainTimeout,
}
pub struct Completed<B: Broker> {
    pub broker: B,
    pub result: Result<Vec<u8>, Error<B::Error>>,
    pub observation: Option<Observation>,
}
pub fn submit<B: Broker + Send + 'static>(
    client: &Client,
    config: Config,
    input: Vec<u8>,
    broker: B,
    timeout: Timeout,
) -> Result<Ticket<Completed<B>>, super::Error>
where
    B::Error: Send + 'static,
{
    let weight = config.reservation(input.len())?;
    client.submit(weight, timeout, move |control| {
        run(config, input, broker, control)
    })
}
pub fn run<B: Broker>(
    config: Config,
    input: Vec<u8>,
    mut broker: B,
    control: Control,
) -> Completed<B> {
    let mut observation = None;
    let result = (|| {
        config
            .reservation(input.len())
            .map_err(|_| Error::Configuration)?;
        control.check().map_err(Error::Control)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Io(e.kind()))?;
        runtime.block_on(async {
            let (mut reader, mut writer, stdin, stdout) =
                platform::pipes().map_err(|e| Error::Io(e.kind()))?;
            let mut command = Command::new(&config.executable);
            command
                .current_dir(&config.directory)
                .args(&config.arguments)
                .stdin(stdin);
            #[cfg(test)]
            if config.stderr {
                command.stdout(Stdio::null()).stderr(stdout);
            } else {
                command.stdout(stdout).stderr(Stdio::null());
            }
            #[cfg(not(test))]
            command.stdout(stdout).stderr(Stdio::null());
            control.check().map_err(Error::Control)?;
            let child = child_process::Child::spawn(command).map_err(|e| Error::Io(e.kind()))?;
            let monitor = monitor::Monitor::start(child, control.clone(), &config)
                .map_err(|e| Error::Io(e.kind()))?;
            let result = exchange(
                &config,
                &input,
                &mut broker,
                &mut reader,
                &mut writer,
                &monitor,
                &control,
            )
            .await;
            // Close both pipe ends before joining on a protocol failure; the
            // monitor also signals the entire owned group/job before reaping.
            drop(writer);
            drop(reader);
            let stopped = monitor.join(result.is_err()).map_err(Error::Monitor)?;
            let cause = stopped.cause;
            let status = stopped.status;
            observation = Some(stopped);
            if let Cause::Control(e) = cause {
                return Err(Error::Control(e));
            }
            match result {
                Err(Error::Io(_) | Error::DrainTimeout)
                    if !status.success() && cause == Cause::Exited =>
                {
                    Err(Error::Exit(status))
                }
                Err(e) => Err(e),
                Ok(_) if !status.success() => Err(Error::Exit(status)),
                Ok(value) => Ok(value),
            }
        })
    })();
    Completed {
        broker,
        result,
        observation,
    }
}
fn wire_error<E>(e: wire::Error) -> Error<E> {
    match e {
        wire::Error::Io(e) => Error::Io(e.kind()),
        wire::Error::Protocol | wire::Error::Arm(_) => Error::Protocol,
        wire::Error::Capacity => Error::Capacity,
    }
}
async fn write<E>(writer: &mut platform::Writer, mut bytes: &[u8]) -> Result<(), Error<E>> {
    while !bytes.is_empty() {
        let n = writer.write(bytes).await.map_err(|e| Error::Io(e.kind()))?;
        if n == 0 {
            return Err(Error::Io(io::ErrorKind::WriteZero));
        }
        bytes = &bytes[n..];
    }
    Ok(())
}
async fn read_exact<E>(reader: &mut platform::Reader, bytes: &mut [u8]) -> Result<bool, Error<E>> {
    let mut at = 0;
    while at < bytes.len() {
        let n = reader
            .read(&mut bytes[at..])
            .await
            .map_err(|e| Error::Io(e.kind()))?;
        if n == 0 {
            return if at == 0 {
                Ok(false)
            } else {
                Err(Error::Io(io::ErrorKind::UnexpectedEof))
            };
        }
        at += n;
    }
    Ok(true)
}
async fn frame<E>(
    reader: &mut platform::Reader,
    maximum: usize,
) -> Result<Option<(wire::Header, Vec<u8>)>, Error<E>> {
    let mut raw = [0; wire::HEADER_BYTES];
    if !read_exact(reader, &mut raw).await? {
        return Ok(None);
    }
    let h = wire::Header::decode(raw, maximum).map_err(wire_error)?;
    let mut data = vec![0; h.bytes];
    if !data.is_empty() && !read_exact(reader, &mut data).await? {
        return Err(Error::Io(io::ErrorKind::UnexpectedEof));
    }
    Ok(Some((h, data)))
}
// Pin each I/O operation across monitor ticks. Dropping a partially completed
// read_exact on every tick would lose framing progress and corrupt the stream.
async fn watched<T, E>(
    operation: impl std::future::Future<Output = Result<T, Error<E>>>,
    config: &Config,
    monitor: &monitor::Monitor,
    mut deadline: Option<tokio::time::Instant>,
) -> Result<T, Error<E>> {
    tokio::pin!(operation);
    loop {
        if monitor.finished.load(Ordering::Acquire) && deadline.is_none() {
            deadline = Some(tokio::time::Instant::now() + config.drain)
        }
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            return Err(Error::DrainTimeout);
        }
        tokio::select! {value=&mut operation=>return value,_=tokio::time::sleep(config.poll)=>{}}
    }
}
async fn exchange<B: Broker>(
    config: &Config,
    input: &[u8],
    broker: &mut B,
    reader: &mut platform::Reader,
    writer: &mut platform::Writer,
    monitor: &monitor::Monitor,
    control: &Control,
) -> Result<Vec<u8>, Error<B::Error>> {
    watched(
        async {
            write(
                writer,
                &wire::bootstrap(config.heap, config.frame_bytes).map_err(wire_error)?,
            )
            .await?;
            write(
                writer,
                &wire::Header {
                    kind: wire::INIT,
                    id: 0,
                    bytes: input.len(),
                }
                .encode(config.frame_bytes)
                .map_err(wire_error)?,
            )
            .await?;
            write(writer, input).await
        },
        config,
        monitor,
        None,
    )
    .await?;
    let (mut result, mut eof, mut next, mut calls, mut bytes, mut drain) =
        (None, false, 1_u64, 0_usize, 0_u64, None);
    loop {
        control.check().map_err(Error::Control)?;
        if monitor.finished.load(Ordering::Acquire) && drain.is_none() {
            drain = Some(tokio::time::Instant::now() + config.drain)
        }
        if eof && monitor.finished.load(Ordering::Acquire) {
            return result.ok_or(Error::Io(io::ErrorKind::UnexpectedEof));
        }
        if drain.is_some_and(|d| tokio::time::Instant::now() >= d) {
            return Err(Error::DrainTimeout);
        }
        if eof {
            tokio::time::sleep(config.poll).await;
            continue;
        }
        let Some((h, data)) =
            watched(frame(reader, config.frame_bytes), config, monitor, drain).await?
        else {
            eof = true;
            if drain.is_none() {
                drain = Some(tokio::time::Instant::now() + config.drain)
            }
            continue;
        };
        if result.is_some() {
            return Err(Error::Protocol);
        }
        match h.kind {
            wire::READ if h.id == next => {
                calls += 1;
                bytes = bytes
                    .checked_add(data.len() as u64)
                    .ok_or(Error::Capacity)?;
                if calls > config.read_requests || bytes > config.read_bytes {
                    return Err(Error::Capacity);
                }
                next = next.checked_add(1).ok_or(Error::Capacity)?;
                control.check().map_err(Error::Control)?;
                let reply = broker.read(&data).map_err(Error::Broker)?;
                control.check().map_err(Error::Control)?;
                bytes = bytes
                    .checked_add(reply.len() as u64)
                    .ok_or(Error::Capacity)?;
                if reply.len() > config.frame_bytes || bytes > config.read_bytes {
                    return Err(Error::Capacity);
                }
                watched(
                    async {
                        write(
                            writer,
                            &wire::Header {
                                kind: wire::VALUE,
                                id: h.id,
                                bytes: reply.len(),
                            }
                            .encode(config.frame_bytes)
                            .map_err(wire_error)?,
                        )
                        .await?;
                        write(writer, &reply).await
                    },
                    config,
                    monitor,
                    None,
                )
                .await?;
            }
            wire::RESULT if h.id == 0 => {
                if data.len() > config.output_bytes {
                    return Err(Error::Capacity);
                }
                result = Some(data);
                drain = Some(tokio::time::Instant::now() + config.drain);
            }
            _ => return Err(Error::Protocol),
        }
    }
}
#[cfg(test)]
mod tests;
