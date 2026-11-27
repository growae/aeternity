use std::{fs::OpenOptions, io, process::Stdio};
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

pub(super) use crate::child_process::Child;
