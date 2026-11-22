use super::*;
use ae_codec::api::{self, ApiType};
use ae_sync::parent_blocks::{Backend, Chain, Config as HttpConfig, Node as HttpNode};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::atomic::Ordering,
    thread::{self, JoinHandle},
};
struct Parent {
    requests: Arc<Mutex<Vec<i64>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Parent {
    fn start() -> (Self, Arc<Backend>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let node = HttpNode::new(
            Chain::Aeternity,
            &format!("http://{}", listener.local_addr().unwrap()),
            None,
        )
        .unwrap();
        let backend = Arc::new(Backend::new(vec![node], HttpConfig::default()).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let requests = Arc::new(Mutex::new(vec![]));
        let seen = requests.clone();
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
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut raw = vec![];
                let mut chunk = [0; 1024];
                loop {
                    let n = stream.read(&mut chunk).unwrap();
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&chunk[..n]);
                    assert!(raw.len() < 8192);
                    if raw.windows(4).any(|p| p == b"\r\n\r\n") {
                        break;
                    }
                }
                if raw.is_empty() {
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
                let (status, body) = if height < 0 {
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
        (
            Self {
                requests,
                stop,
                worker: Some(worker),
            },
            backend,
        )
    }
}
impl Drop for Parent {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
#[test]
fn live_http_parent_drives_automatic_targets_entropy_and_signed_epoch_production() {
    let node = Node::new(1);
    let (parent, backend) = Parent::start();
    let mut cfg = config(&node, true);
    cfg.parent.hosts = backend.hosts();
    let mut c = Conductor::start(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        cfg,
        backend,
    )
    .unwrap();
    let mut wall = 1000;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(30);
    let mut keys = vec![];
    let mut epochs = 0;
    while keys.len() < 7 {
        assert!(
            Instant::now() < end,
            "HTTP parent production stalled {:?}",
            c.status()
        );
        tick(&mut c, &mut wall, &mut clock);
        if let Some(out) = c.take_output() {
            let Output::Attempt(done) = out else {
                panic!("{out:?}")
            };
            if let production::Outcome::Produced { key, is_eoe, .. } = done.outcome {
                keys.push(key.hash);
                epochs += usize::from(is_eoe);
            }
        }
    }
    assert!(epochs >= 2);
    while !matches!(
        c.parent().snapshot().cache.blocks().get(&26.into()),
        Some(parent_cache::Entry::Block(_))
    ) {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
        c.take_output();
    }
    for hash in keys {
        let view = node.db.view().unwrap();
        let Block::Key(k) = view.require_block(hash).unwrap() else {
            panic!()
        };
        ae_hyperchain::signing::verify_key(&k).unwrap();
        assert_eq!(
            view.require_record(hash).unwrap().roots.state.hash().0,
            k.common.state_hash
        );
    }
    let report = shutdown(c);
    assert!(report.owner.errors.is_empty());
    for height in [-4, 1, 6, 16, 21, 26] {
        assert!(
            parent.requests.lock().unwrap().contains(&height),
            "missing HTTP parent height {height}"
        );
    }
}
