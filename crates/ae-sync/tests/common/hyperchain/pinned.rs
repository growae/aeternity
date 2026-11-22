use super::*;
use ae_chain::repository::hyperchain::inputs::Effects;
use ae_sync::{
    chain::{Importer, hyperchain::scoped},
    parent_pins,
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};
pub static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!(
        "../../../../ae-hyperchain/tests/fixtures/erlang_hc_pinned.json"
    ))
    .unwrap()
});
pub type Accepted = scoped::Accepted<Otp26, Effects>;
pub struct Parent {
    pub node: parent_pins::Node,
    service: Option<parent_pins::Service>,
    server: Option<thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    pub mode: Arc<AtomicUsize>,
    pub requests: Arc<AtomicUsize>,
    pub parents: parents::Pair,
}
impl Parent {
    pub fn new(index: usize) -> Self {
        let row = &DATA["rows"][index]["parent_pin"];
        let payload =
            ae_codec::api::encode(ae_codec::api::ApiType::Bytearray, &bytes(&row["payload"]));
        let height: J = serde_json::from_str(row["parent_height"].as_str().unwrap()).unwrap();
        let body = serde_json::json!({"tx":{"payload":payload},"block_height":height}).to_string();
        let status = row["status"].as_u64().unwrap();
        let tx = String::from_utf8(bytes(&row["transaction_hash"])).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let node = parent_pins::Node::new(
            parent_pins::Chain::Aeternity,
            &format!("http://{}", listener.local_addr().unwrap()),
            None,
        )
        .unwrap();
        let service = parent_pins::Service::spawn(
            vec![node.clone()],
            parent_pins::Config {
                request_timeout: Duration::from_secs(2),
                lookup_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mode = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (s, m, r) = (stop.clone(), mode.clone(), requests.clone());
        let server = thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
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
                let mut raw = vec![];
                while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut b = [0; 512];
                    let n = stream.read(&mut b).unwrap();
                    assert_ne!(n, 0);
                    raw.extend_from_slice(&b[..n]);
                    assert!(raw.len() < 8192);
                }
                assert!(
                    raw.starts_with(format!("GET /v3/transactions/{tx} HTTP/1.1\r\n").as_bytes())
                );
                r.fetch_add(1, Ordering::AcqRel);
                while m.load(Ordering::Acquire) == 3 && !s.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(1));
                }
                if s.load(Ordering::Acquire) {
                    break;
                }
                let reply = match m.load(Ordering::Acquire) {
                    0 => format!(
                        "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    ),
                    1 => "HTTP/1.1 404 MISSING\r\nContent-Length: 0\r\n\r\n".to_string(),
                    2 => "HTTP/1.1 200 OK\r\nContent-Length: 4194305\r\n\r\n".to_string(),
                    n => panic!("unexpected parent mode {n}"),
                };
                let _ = stream.write_all(reply.as_bytes());
            }
        });
        Self {
            node,
            service: Some(service),
            server: Some(server),
            stop,
            mode,
            requests,
            parents: parents::Pair::new(false),
        }
    }
    pub fn client(&self) -> parent_pins::Client {
        self.service.as_ref().unwrap().client()
    }
    pub fn importer(&self, node: &Node) -> (Importer, Accepted) {
        Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            scoped::Cached {
                cache: self.parents.client.clone(),
                pins: scoped::RemotePins {
                    execution: node.config.clone(),
                    client: self.client(),
                    limits: Default::default(),
                },
                parent_start: 1.into(),
                parent_length: 5.into(),
                limits: Default::default(),
            },
            Default::default(),
        )
        .unwrap()
    }
    pub fn close(&mut self) {
        drop(self.service.take());
    }
}
impl Drop for Parent {
    fn drop(&mut self) {
        self.close();
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}
pub fn check_pin(node: &Node, index: usize) {
    use ae_hyperchain::block::Event;
    use ae_vm_fate::value::{ObjectPayload, Value};
    let row = &node.corpus["rows"][node.index]["rows"][index];
    if row["pin"].is_null() {
        return;
    }
    let b = block(row);
    let Block::Key(k) = b else { panic!() };
    let view = node.db.view().unwrap();
    let events = view
        .hyperchain_event(view.status().revision)
        .unwrap()
        .unwrap()
        .events;
    if row["pin"]["accepted"] == true {
        assert!(events.contains(&Event::PinAccepted {
            reward: 14.into(),
            recipient: ObjectPayload::Bytes(k.beneficiary.to_vec()),
            epoch: Value::Integer(2.into()),
            height: 6.into()
        }));
        assert!(!events.contains(&Event::IncorrectPin));
    } else {
        assert!(events.contains(&Event::IncorrectPin));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::PinAccepted { .. }))
        );
    }
    assert!(
        events
            .iter()
            .any(|e| matches!(e,Event::NewEpoch(info)if info.epoch==Value::Integer(3.into())))
    );
}
