use super::*;
use crate::observe::statsd::supervision::{Owner as Supervisor, Phase};
use crate::routing::{Key, Part};
use std::time::Duration;
use tokio::net::UdpSocket;

#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);
impl Write for Log {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn key(name: &str) -> Key {
    Key {
        name: vec![Part::atom("ae"), Part::atom("pipeline"), Part::atom(name)],
        datapoint: Part::atom("value"),
    }
}
fn event(name: &str, n: i64) -> report::Event {
    report::Options::default()
        .event(
            key(name),
            report::Value::Integer(n.into()),
            report::Extra::Undefined,
            report::Limits::default(),
        )
        .unwrap()
}
async fn advance(owner: &mut Supervisor, condition: impl Fn(&Supervisor) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            owner.poll().await;
            if condition(owner) {
                break;
            }
            owner.changed().await;
        }
    })
    .await
    .unwrap();
}
async fn packet(socket: &UdpSocket) -> (Vec<u8>, std::net::SocketAddr) {
    let mut b = [0; 4096];
    let (n, from) = tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    (b[..n].to_vec(), from)
}
async fn setup(config: Config) -> (Supervisor, UdpSocket) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut owner = Supervisor::prepare(statsd::Settings {
        target: statsd::Config {
            host: statsd::Host::Name("127.0.0.1".into()),
            port: socket.local_addr().unwrap().port(),
            ..Default::default()
        },
        pipeline: Some(config),
        ..Default::default()
    })
    .unwrap();
    owner.activate();
    advance(&mut owner, |o| o.handle().status().phase == Phase::Running).await;
    (owner, socket)
}
#[tokio::test]
async fn native_routes_and_staged_disable_join_input_but_keep_logger_and_socket_until_retirement() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_pipeline.json"
    ))
    .unwrap();
    let routing = routing::Owner::new(routing::Setup::default()).unwrap();
    let worker = routing::Worker::spawn(routing.store()).unwrap();
    let routes = worker.client();
    for (name, dests) in [
        ("send_only", vec![routing::Destination::Send]),
        ("log_only", vec![routing::Destination::Log]),
        (
            "both",
            vec![routing::Destination::Log, routing::Destination::Send],
        ),
        ("none", vec![routing::Destination::Log]),
    ] {
        for d in dests {
            routes.add(key(name), d).unwrap().await.unwrap();
        }
    }
    routes
        .delete(key("none"), routing::Destination::Log)
        .unwrap()
        .await
        .unwrap();
    let config = Config::default();
    config.set_routing(Some(Routing::Fixed(routes)));
    let log = Log::default();
    config.set_log(Some(Box::new(log.clone())));
    let (mut owner, socket) = setup(config).await;
    let client = owner.client();
    let input = client.input().unwrap();
    let old = owner.handle().current().unwrap();
    let mut from = None;
    for row in native["routes"].as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        let mut value = event(name, 0);
        value.value = if let Some(n) = row["value"].as_i64() {
            report::Value::Integer(n.into())
        } else {
            report::Value::Float(row["value"].as_f64().unwrap())
        };
        log.0.lock().unwrap().clear();
        client.report(value).unwrap();
        client.report(event("barrier", 0)).unwrap();
        advance(&mut owner, |_| {
            log.0
                .lock()
                .unwrap()
                .ends_with(b"ae.pipeline.barrier.value:0|g\n")
        })
        .await;
        let mut packets = vec![];
        loop {
            let (p, source) = packet(&socket).await;
            from = Some(source);
            if p == b"ae.pipeline.barrier.value:0|g" {
                break;
            }
            packets.push(String::from_utf8(p).unwrap());
        }
        let lines = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        let logs = lines
            .lines()
            .filter(|s| *s != "ae.pipeline.barrier.value:0|g")
            .collect::<Vec<_>>();
        assert_eq!(serde_json::json!(packets), row["packets"]);
        assert_eq!(serde_json::json!(logs), row["logs"]);
    }
    // A filtered malformed value never reaches either formatter callback.
    let mut hidden = event("none", 0);
    hidden.kind = "invalid".into();
    client.report(hidden).unwrap();
    client.report(event("barrier", 0)).unwrap();
    packet(&socket).await;
    let first = input.prepare_stop().unwrap();
    let second = input.prepare_stop().unwrap();
    first.wait().await.unwrap();
    second.wait().await.unwrap();
    assert!(input.info().joined && input.info().disabled && !input.info().running);
    assert!(client.info().running && client.info().connected && !client.info().joined);
    assert_eq!(client.report(event("after", 1)), Err(Error::Closed));
    assert!(!client.submit(vec![b"after:1|g".to_vec()]));
    owner.poll().await;
    assert_eq!(owner.handle().status().generation, old.number);
    assert_eq!(owner.handle().status().child_restart_attempts, 0);
    let address = from.unwrap();
    assert_eq!(
        std::net::UdpSocket::bind(address).unwrap_err().kind(),
        io::ErrorKind::AddrInUse
    );
    assert!(
        native["prep_stop"]["socket_retained_until_logger_exit"]
            .as_bool()
            .unwrap()
    );
    owner.finish().await;
    assert!(client.info().joined);
    assert!(std::net::UdpSocket::bind(address).is_ok());
    drop(worker);
    drop(routing);
}
#[tokio::test]
async fn callback_failure_and_abnormal_input_exit_replace_the_entire_owned_logger_generation() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_pipeline.json"
    ))
    .unwrap();
    assert_eq!(
        native["linked_input_failure_replaces_logger_and_input"],
        true
    );
    let log = Log::default();
    let config = Config::default();
    config.set_log(Some(Box::new(log)));
    let (mut owner, socket) = setup(config).await;
    let stable = owner.client();
    for fail_input in [false, true] {
        let old = owner.handle().current().unwrap();
        let input = old.client.input().unwrap();
        if fail_input {
            input.panic_for_test();
        } else {
            let mut e = event("invalid", 1);
            e.kind = "unknown".into();
            stable.report(e).unwrap();
        }
        advance(&mut owner, |o| {
            o.handle().status().generation > old.number
                && o.handle().status().phase == Phase::Running
        })
        .await;
        assert!(old.client.info().joined);
        assert!(input.info().joined && !input.info().running);
        assert_eq!(input.report(event("old", 1)), Err(Error::Closed));
        stable.report(event("fresh", 2)).unwrap();
        assert_eq!(packet(&socket).await.0, b"ae.pipeline.fresh.value:2|g");
    }
    assert_eq!(owner.handle().status().child_restart_attempts, 2);
    owner.finish().await;
}
#[tokio::test]
async fn cached_routes_survive_withdrawn_worker_without_restarting_logger_or_socket() {
    let node = crate::test_support::Node::new();
    let routing = routing::Owner::new(routing::Setup::default()).unwrap();
    let worker = routing::Worker::spawn(routing.store()).unwrap();
    let old = worker.client();
    old.add(key("cached"), routing::Destination::Send)
        .unwrap()
        .await
        .unwrap();
    old.add(key("none"), routing::Destination::Log)
        .unwrap()
        .await
        .unwrap();
    old.delete(key("none"), routing::Destination::Log)
        .unwrap()
        .await
        .unwrap();
    let (publisher, binding) = bindings::Owner::new(&node.db);
    publisher.install(old.clone(), 1);
    let config = Config::default();
    config.set_routing(Some(Routing::Managed(binding.clone())));
    let (mut owner, socket) = setup(config).await;
    let client = owner.client();
    client.report(event("cached", 1)).unwrap();
    let (_, source) = packet(&socket).await;
    publisher.withdraw();
    worker.shutdown().unwrap();
    assert!(binding.current().is_none() && !old.info().running);
    let mut filtered = event("none", 0);
    filtered.kind = "unknown".into();
    client.report(filtered).unwrap();
    client.report(event("cached", 2)).unwrap();
    let (line, same_source) = packet(&socket).await;
    assert_eq!(line, b"ae.pipeline.cached.value:2|g");
    assert_eq!(source, same_source);
    let worker = routing::Worker::spawn(routing.store()).unwrap();
    publisher.install(worker.client(), 2);
    client.report(event("cached", 3)).unwrap();
    let (line, same_source) = packet(&socket).await;
    assert_eq!(line, b"ae.pipeline.cached.value:3|g");
    assert_eq!(source, same_source);
    owner.poll().await;
    assert_eq!(owner.handle().status().generation, 1);
    assert_eq!(owner.handle().status().child_restart_attempts, 0);
    assert!(client.input().unwrap().info().last_error.is_none());
    owner.finish().await;
}
struct BlockingLog {
    gate: Option<Gate>,
    log: Log,
}
impl Write for BlockingLog {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if let Some(gate) = self.gate.take() {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }
        self.log.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.log.flush()
    }
}
#[tokio::test]
async fn cancelling_disable_wait_preserves_queued_output_and_one_shared_acknowledgement() {
    let config = Config::default();
    let log = Log::default();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    config.set_log(Some(Box::new(BlockingLog {
        gate: Some(Gate {
            entered,
            release: held,
        }),
        log: log.clone(),
    })));
    let (mut owner, socket) = setup(config).await;
    let release = Release(Some(release));
    let client = owner.client();
    let input = client.input().unwrap();
    client.report(event("held", 1)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap();
    client.report(event("queued", 2)).unwrap();
    let ticket = input.prepare_stop().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), ticket.wait())
            .await
            .is_err()
    );
    assert!(!input.info().disabled && !client.info().joined);
    assert!(client.info().connected);
    let next = input.prepare_stop().unwrap();
    drop(release);
    tokio::time::timeout(Duration::from_secs(3), next.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(input.info().disabled && input.info().joined);
    assert_eq!(packet(&socket).await.0, b"ae.pipeline.held.value:1|g");
    assert_eq!(packet(&socket).await.0, b"ae.pipeline.queued.value:2|g");
    assert_eq!(
        *log.0.lock().unwrap(),
        b"ae.pipeline.held.value:1|g\nae.pipeline.queued.value:2|g\n"
    );
    owner.poll().await;
    assert_eq!(owner.handle().status().generation, 1);
    owner.finish().await;
}
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            let _ = s.send(());
        }
    }
}
#[tokio::test]
async fn cancelled_replacement_keeps_blocked_input_and_its_credits_owned_without_replay() {
    let config = Config::default();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut owner = Supervisor::prepare(statsd::Settings {
        target: statsd::Config {
            host: statsd::Host::Name("127.0.0.1".into()),
            port: socket.local_addr().unwrap().port(),
            ..Default::default()
        },
        records: 2,
        bytes: 8192,
        pipeline: Some(config),
    })
    .unwrap();
    owner.activate();
    advance(&mut owner, |o| o.handle().status().phase == Phase::Running).await;
    let input = owner.client().input().unwrap();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    input.0.lock().gate = Some(Gate {
        entered,
        release: held,
    });
    input.report(event("old", 1)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap();
    input.report(event("old", 2)).unwrap();
    assert_eq!(input.report(event("old", 3)), Err(Error::Busy));
    assert_eq!(input.info().pending, 2);
    let retained = input.info().retained_bytes;
    let old = owner.handle().current().unwrap();
    let ticket = owner.handle().restart(old.number).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), owner.poll())
            .await
            .is_err()
    );
    assert_eq!(owner.handle().status().phase, Phase::Retiring);
    assert!(!input.info().joined);
    assert_eq!(input.info().pending, 2);
    assert_eq!(input.info().retained_bytes, retained);
    // Consume the logger's exit notification before releasing its input. The
    // owner must observe input completion even though the logger is gone.
    tokio::time::timeout(Duration::from_secs(3), async {
        while old.client.info().running {
            owner.changed().await;
        }
    })
    .await
    .unwrap();
    while tokio::time::timeout(Duration::from_millis(10), owner.changed())
        .await
        .is_ok()
    {}
    drop(release);
    tokio::time::timeout(Duration::from_secs(3), owner.changed())
        .await
        .expect("input exit must wake the retiring owner");
    advance(&mut owner, |o| o.handle().status().phase == Phase::Running).await;
    assert_eq!(ticket.wait().await, Ok(old.number + 1));
    assert!(input.info().joined && old.client.info().joined);
    assert_eq!(input.info().pending, 0);
    assert_eq!(input.info().retained_bytes, 0);
    owner.client().report(event("fresh", 4)).unwrap();
    assert_eq!(packet(&socket).await.0, b"ae.pipeline.fresh.value:4|g");
    owner.finish().await;
}
