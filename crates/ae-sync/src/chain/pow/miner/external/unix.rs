use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use std::{
    io::{self, PipeReader, Read},
    process::Stdio,
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

pub(super) use crate::child_process::Child;
