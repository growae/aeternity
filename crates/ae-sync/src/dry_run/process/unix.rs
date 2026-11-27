use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use std::{
    io::{self, PipeReader, PipeWriter, Read, Write},
    process::Stdio,
};
use tokio::io::unix::AsyncFd;
pub(super) struct Reader(AsyncFd<PipeReader>);
pub(super) struct Writer(AsyncFd<PipeWriter>);
impl Reader {
    pub(super) async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut ready = self.0.readable().await?;
            match ready.try_io(|p| p.get_ref().read(bytes)) {
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                Ok(v) => return v,
                Err(_) => continue,
            }
        }
    }
}
impl Writer {
    pub(super) async fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            let mut ready = self.0.writable().await?;
            match ready.try_io(|p| p.get_ref().write(bytes)) {
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                Ok(v) => return v,
                Err(_) => continue,
            }
        }
    }
}
pub(super) fn pipes() -> io::Result<(Reader, Writer, Stdio, Stdio)> {
    let (output_read, output_write) = io::pipe()?;
    let (input_read, input_write) = io::pipe()?;
    fcntl_setfl(&output_read, fcntl_getfl(&output_read)? | OFlags::NONBLOCK)?;
    fcntl_setfl(&input_write, fcntl_getfl(&input_write)? | OFlags::NONBLOCK)?;
    Ok((
        Reader(AsyncFd::new(output_read)?),
        Writer(AsyncFd::new(input_write)?),
        input_read.into(),
        output_write.into(),
    ))
}
