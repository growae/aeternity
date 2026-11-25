use super::*;
use ae_codec::api::{self, ApiType};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::atomic::{AtomicI64, Ordering},
    thread::{self, JoinHandle},
};
pub(super) struct Parent {
    pub(super) url: String,
    pub(super) requests: Arc<Mutex<Vec<i64>>>,
    missing: Arc<AtomicI64>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Parent {
    pub(super) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let requests = Arc::new(Mutex::new(vec![]));
        let seen = requests.clone();
        let missing = Arc::new(AtomicI64::new(-1));
        let absent = missing.clone();
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                let mut stream = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("{e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut raw = vec![];
                let mut chunk = [0; 1024];
                loop {
                    let n = match stream.read(&mut chunk) {
                        Ok(n) => n,
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::TimedOut
                                    | std::io::ErrorKind::ConnectionReset
                            ) =>
                        {
                            break;
                        }
                        Err(e) => panic!("{e}"),
                    };
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                    assert!(raw.len() < 8192);
                    if raw.windows(4).any(|p| p == b"\r\n\r\n") {
                        break;
                    }
                }
                if !raw.windows(4).any(|p| p == b"\r\n\r\n") {
                    continue;
                }
                let head = std::str::from_utf8(&raw).unwrap();
                assert!(head.starts_with("GET "));
                assert!(!head.contains("Authorization:"));
                let path = head.split_whitespace().nth(1).unwrap();
                let height = if path == "/v3/key-blocks/current" {
                    16
                } else {
                    path.strip_prefix("/v3/key-blocks/height/")
                        .unwrap()
                        .parse::<i64>()
                        .unwrap()
                };
                seen.lock().unwrap().push(height);
                let (status, body) = if height < 0 || height == absent.load(Ordering::Acquire) {
                    (404, serde_json::json!({"reason":"Block not found"}))
                } else {
                    let h = height as u64;
                    (
                        200,
                        serde_json::json!({"hash":api::encode(ApiType::KeyBlockHash,&key(1000+h)),"prev_key_hash":api::encode(ApiType::KeyBlockHash,&key(999+h)),"height":h,"time":h*100}),
                    )
                };
                let body = serde_json::to_vec(&body).unwrap();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(&body);
            }
        });
        Self {
            url,
            requests,
            missing,
            stop,
            worker: Some(worker),
        }
    }
    pub(super) fn deny(&self, height: i64) {
        self.missing.store(height, Ordering::Release);
    }
}
impl Drop for Parent {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
    }
}
