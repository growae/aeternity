use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::watch,
    time::Instant,
};

pub(super) struct Socket {
    stream: TcpStream,
    activity: watch::Sender<Instant>,
}
impl Socket {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            activity: watch::channel(Instant::now()).0,
        }
    }
    pub fn inactivity(&self, timeout: Duration) -> impl Future<Output = ()> + Send + 'static {
        let mut activity = self.activity.subscribe();
        async move {
            loop {
                let deadline = *activity.borrow_and_update() + timeout;
                tokio::select! {
                    biased;
                    changed = activity.changed() => if changed.is_err() { return; },
                    _ = tokio::time::sleep_until(deadline) => return,
                }
            }
        }
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.activity.send_replace(Instant::now());
        }
        result
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
