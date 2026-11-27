use std::{fs::OpenOptions, io, process::Stdio};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
};
pub(super) struct Reader {
    pipe: NamedPipeServer,
    connected: bool,
}
pub(super) struct Writer {
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
impl Writer {
    pub(super) async fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.connected {
            self.pipe.connect().await?;
            self.connected = true;
        }
        self.pipe.write(bytes).await
    }
}
pub(super) fn pipes() -> io::Result<(Reader, Writer, Stdio, Stdio)> {
    let name = format!(
        r"\\.\pipe\ae-dry-run-{}-{:032x}",
        std::process::id(),
        rand::random::<u128>()
    );
    let outname = format!("{name}-out");
    let inname = format!("{name}-in");
    let output = ServerOptions::new()
        .first_pipe_instance(true)
        .access_inbound(true)
        .access_outbound(false)
        .create(&outname)?;
    let input = ServerOptions::new()
        .first_pipe_instance(true)
        .access_inbound(false)
        .access_outbound(true)
        .create(&inname)?;
    let output_write = OpenOptions::new().write(true).open(outname)?;
    let input_read = OpenOptions::new().read(true).open(inname)?;
    Ok((
        Reader {
            pipe: output,
            connected: false,
        },
        Writer {
            pipe: input,
            connected: false,
        },
        input_read.into(),
        output_write.into(),
    ))
}
