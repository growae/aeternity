#![cfg(unix)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
#[path = "common/execution_profile.rs"]
mod profile;

struct Lab(PathBuf);
impl Lab {
    fn new(kind: &str, normal: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ae-application-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.join("resources/aecore")).unwrap();
        let lab = Self(path);
        lab.write("profile.json", &profile::default_value());
        lab.write("alloc.json", &json!({}));
        lab.write("node.json", &json!({
            "fork_management":{"network_id":"ae_application_tests"},
            "system":{"dev_mode":!normal,"custom_prefunded_accs_file":"alloc.json"},
            "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":kind}}},
            "mining":{"autostart":false,"beneficiary":ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey,&[42;32]),"cuckoo":{"miners":[]}},
            "keys":{"dir":"keys","peer_password":"local-test"},
            "include_default_peers":false,
            "sync":{"listen_address":"127.0.0.1","port":0,"max_outbound":0,"upnp_enabled":false,"peer_analytics":false},
            "metrics":{"host":"127.0.0.1","rules":[{"name":"ae.**","actions":"none"}]},
            "http":{"external":{"listen_address":"127.0.0.1","port":0},"internal":{"port":0},"rosetta":{"port":0},"rosetta_offline":{"port":0},"endpoints":{"dry-run":true}}
        }));
        lab
    }
    fn write(&self, name: &str, value: &Value) {
        std::fs::write(self.0.join(name), serde_json::to_vec(value).unwrap()).unwrap();
    }
    fn config(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.0.join("node.json")).unwrap()).unwrap()
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ae-node"));
        command
            .current_dir(&self.0)
            .env_clear()
            .env("HOME", &self.0)
            .args([
                "--config",
                "node.json",
                "--execution-profile",
                "profile.json",
                "--database",
                "chain",
                "--resource-dir",
                "resources",
                "--dry-run-worker",
                env!("CARGO_BIN_EXE_ae-dry-run-worker"),
            ]);
        command
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Process {
    child: Child,
    lines: mpsc::Receiver<Value>,
    reader: Option<thread::JoinHandle<()>>,
    log: PathBuf,
}
impl Process {
    fn spawn(lab: &Lab, mut command: Command, name: &str) -> Self {
        let log = lab.0.join(format!("{name}.log"));
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, lines) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if let Ok(value) = serde_json::from_str(&line) {
                    let _ = send.send(value);
                }
            }
        });
        Self {
            child,
            lines,
            reader: Some(reader),
            log,
        }
    }
    fn event(&self, name: &str) -> Value {
        let value = self
            .lines
            .recv_timeout(Duration::from_secs(20))
            .unwrap_or_else(|e| panic!("{e}: {}", std::fs::read_to_string(&self.log).unwrap()));
        assert_eq!(
            value["event"],
            name,
            "{value}: {}",
            std::fs::read_to_string(&self.log).unwrap()
        );
        value
    }
    fn signal(&self, signal: rustix::process::Signal) {
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(self.child.id() as i32).unwrap(),
            signal,
        )
        .unwrap();
    }
    fn wait(&mut self, success: bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert_eq!(
                    status.success(),
                    success,
                    "{}",
                    std::fs::read_to_string(&self.log).unwrap()
                );
                break;
            }
            assert!(Instant::now() < deadline, "node did not exit");
            thread::sleep(Duration::from_millis(10));
        }
        self.reader.take().unwrap().join().unwrap();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
fn request(address: SocketAddr, text: &str) -> (u16, Value) {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket.write_all(text.as_bytes()).unwrap();
    let mut bytes = vec![];
    socket.read_to_end(&mut bytes).unwrap();
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut headers);
    let end = response.parse(&bytes).unwrap().unwrap();
    (
        response.code.unwrap(),
        serde_json::from_slice(&bytes[end..]).unwrap_or(Value::Null),
    )
}
fn addresses(ready: &Value) -> Vec<SocketAddr> {
    ready["http"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().parse().unwrap())
        .collect()
}

#[test]
fn foreground_node_serves_captured_configuration_and_reopens_after_both_signals() {
    let lab = Lab::new("on_demand", false);
    let mut identity = None;
    for (round, signal) in [rustix::process::Signal::TERM, rustix::process::Signal::INT]
        .into_iter()
        .enumerate()
    {
        let mut command = lab.command();
        if round == 0 {
            command.env("AE__HTTP__ENDPOINTS__CHAIN", "false");
        }
        let mut process = Process::spawn(&lab, command, &format!("run{round}"));
        let ready = process.event("ready");
        let addresses = addresses(&ready);
        assert_eq!(ready["family"], "OnDemand");
        assert_eq!(addresses.len(), 4);
        let (code, status) = request(
            addresses[0],
            "GET /v3/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(code, 200);
        assert_eq!(status["network_id"], "ae_application_tests");
        let code = request(
            addresses[0],
            "GET /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .0;
        assert_eq!(code, if round == 0 { 404 } else { 200 });
        let (code, result) = request(
            addresses[0],
            "POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n{\"txs\":[]}",
        );
        assert_eq!((code, result), (200, json!({"results":[]})));
        let peer = request(
            addresses[0],
            "GET /v3/peers/pubkey HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(peer.0, 200);
        let current = (ready["genesis"].clone(), peer.1);
        if let Some(old) = &identity {
            assert_eq!(old, &current);
        } else {
            identity = Some(current);
        }
        let mut stalled = TcpStream::connect(addresses[0]).unwrap();
        stalled
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stalled.write_all(b"POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 1000\r\nExpect: 100-continue\r\n\r\n").unwrap();
        let mut ready_body = [0; 25];
        stalled.read_exact(&mut ready_body).unwrap();
        assert_eq!(&ready_body, b"HTTP/1.1 100 Continue\r\n\r\n");
        process.signal(signal);
        assert_eq!(
            process.event("stopped"),
            json!({"event":"stopped","joined":true,"failed":false})
        );
        process.wait(true);
        assert_eq!(stalled.read(&mut [0]).unwrap(), 0);
        for address in addresses {
            drop(TcpListener::bind(address).unwrap());
        }
    }
    // Reopening under a different execution profile is refused before HTTP.
    let mut value = profile::default_value();
    value["diagnostic"] = json!("test");
    lab.write("profile.json", &value);
    let mut process = Process::spawn(&lab, lab.command(), "mismatch");
    process.wait(false);
    assert!(
        std::fs::read_to_string(&process.log)
            .unwrap()
            .contains("database")
    );
}

#[test]
fn foreground_cuckoo_node_starts_network_and_joins_failed_http_startup() {
    let lab = Lab::new("pow_cuckoo", true);
    let mut reservations = Vec::new();
    let mut config = lab.config();
    for target in ["external", "internal", "rosetta", "rosetta_offline"] {
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        config["http"][target]["port"] = json!(socket.local_addr().unwrap().port());
        reservations.push(socket);
    }
    let ports: Vec<_> = reservations
        .iter()
        .map(|s| s.local_addr().unwrap())
        .collect();
    let occupied = reservations.pop().unwrap();
    drop(reservations);
    lab.write("node.json", &config);
    let mut process = Process::spawn(&lab, lab.command(), "bind-failure");
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":true})
    );
    process.wait(false);
    for address in &ports[..3] {
        drop(TcpListener::bind(address).unwrap());
    }
    drop(occupied);
    let mut process = Process::spawn(&lab, lab.command(), "retry");
    let ready = process.event("ready");
    assert_eq!(ready["family"], "Cuckoo");
    assert_eq!(addresses(&ready), ports);
    let sync: SocketAddr = ready["sync"].as_str().unwrap().parse().unwrap();
    assert!(sync.ip().is_loopback());
    let pending_peer = TcpStream::connect(sync).unwrap();
    assert_eq!(
        request(
            ports[0],
            "GET /v3/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .0,
        200
    );
    process.signal(rustix::process::Signal::TERM);
    assert_eq!(process.event("stopped")["joined"], true);
    process.wait(true);
    drop(pending_peer);
    drop(TcpListener::bind(sync).unwrap());
}

#[test]
fn foreground_arguments_and_native_database_guard_fail_without_mutation() {
    let lab = Lab::new("on_demand", false);
    for (args, code) in [
        (vec!["--help"], 0),
        (vec![], 2),
        (vec!["--unknown"], 2),
        (
            vec!["--execution-profile", "a", "--execution-profile", "b"],
            2,
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_ae-node"))
            .current_dir(&lab.0)
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code));
        assert!(!lab.0.join("chain").exists());
    }
    let original = lab.config();
    for (name, phase) in [
        ("stratum", "Stratum configuration"),
        ("temporary", "nonpersistent storage integration"),
    ] {
        let mut config = original.clone();
        if name == "stratum" {
            config["stratum"] = json!({"enabled":true});
        } else {
            config["chain"]["persist"] = json!(false);
        }
        lab.write("node.json", &config);
        let mut process = Process::spawn(&lab, lab.command(), name);
        process.wait(false);
        assert!(
            std::fs::read_to_string(&process.log)
                .unwrap()
                .contains(phase)
        );
        assert!(!lab.0.join("chain").exists());
    }
    lab.write("node.json", &original);
    std::fs::create_dir(lab.0.join("chain")).unwrap();
    std::fs::write(lab.0.join("chain/schema.DAT"), b"native-reference").unwrap();
    let mut process = Process::spawn(&lab, lab.command(), "native-db");
    process.wait(false);
    assert!(
        std::fs::read_to_string(&process.log)
            .unwrap()
            .contains("database format")
    );
    assert_eq!(std::fs::read_dir(lab.0.join("chain")).unwrap().count(), 1);
    assert_eq!(
        std::fs::read(lab.0.join("chain/schema.DAT")).unwrap(),
        b"native-reference"
    );
}

#[test]
fn foreground_http_reuses_connections_and_preserves_pipelined_dry_run_requests() {
    let lab = Lab::new("on_demand", false);
    let mut process = Process::spawn(&lab, lab.command(), "persistent");
    let ready = process.event("ready");
    let address = addresses(&ready)[0];
    let mut socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket.write_all(b"POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n{\"txs\":[]}GET /v3/status HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    let mut socket = BufReader::new(socket);
    fn response(socket: &mut BufReader<TcpStream>) -> (u16, bool, Value) {
        let mut text = String::new();
        loop {
            let mut line = String::new();
            assert!(socket.read_line(&mut line).unwrap() > 0);
            text.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let mut fields = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut fields);
        response.parse(text.as_bytes()).unwrap().unwrap();
        let length: usize = std::str::from_utf8(
            response
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                .unwrap()
                .value,
        )
        .unwrap()
        .parse()
        .unwrap();
        let closed = response.headers.iter().any(|h| {
            h.name.eq_ignore_ascii_case("connection") && h.value.eq_ignore_ascii_case(b"close")
        });
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        (
            response.code.unwrap(),
            closed,
            serde_json::from_slice(&body).unwrap(),
        )
    }
    assert_eq!(response(&mut socket), (200, false, json!({"results":[]})));
    let status = response(&mut socket);
    assert_eq!((status.0, status.1), (200, false));
    assert_eq!(status.2["network_id"], "ae_application_tests");
    // Reusing the same connection must release the first job's credits.
    socket.get_mut().write_all(b"POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{\"txs\":[]}").unwrap();
    assert_eq!(response(&mut socket), (200, true, json!({"results":[]})));
    assert_eq!(socket.read(&mut [0]).unwrap(), 0);
    // A fully idle persistent connection remains owned and closes on shutdown.
    let mut idle = TcpStream::connect(address).unwrap();
    idle.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    idle.write_all(b"GET /v3/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut idle = BufReader::new(idle);
    assert_eq!(response(&mut idle).0, 200);
    process.signal(rustix::process::Signal::TERM);
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":false})
    );
    process.wait(true);
    assert_eq!(idle.read(&mut [0]).unwrap(), 0);
    drop(TcpListener::bind(address).unwrap());
}

#[test]
fn foreground_stratum_creates_and_reuses_reward_keys_serves_tcp_and_joins_on_signal() {
    let lab = Lab::new("pow_cuckoo", false);
    let reservation = TcpListener::bind("0.0.0.0:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let mut config = lab.config();
    config["mining"]
        .as_object_mut()
        .unwrap()
        .remove("beneficiary");
    config["stratum"] = json!({
        "enabled":true,"connection":{"port":port,"max_connections":4,"num_acceptors":2},
        "reward":{"keys":{"dir":"pool-keys"},"beneficiaries":[]}
    });
    lab.write("node.json", &config);
    let directory = lab.0.join("resources/aestratum/pool-keys");
    let mut saved = None;
    for attempt in 0..2 {
        let mut process = Process::spawn(&lab, lab.command(), &format!("stratum-{attempt}"));
        let ready = process.event("ready");
        assert_eq!(ready["family"], "Cuckoo");
        let address: SocketAddr = ready["stratum"].as_str().unwrap().parse().unwrap();
        assert!(address.ip().is_unspecified());
        let local = SocketAddr::from(([127, 0, 0, 1], address.port()));
        let public = std::fs::read(directory.join("sign_key.pub")).unwrap();
        let secret = std::fs::read(directory.join("sign_key")).unwrap();
        assert_eq!((public.len(), secret.len()), (32, 64));
        if let Some((p, s)) = &saved {
            assert!(p == &public && s == &secret);
        }
        saved = Some((public.clone(), secret));
        let (code, pending) = request(
            addresses(&ready)[0],
            "GET /v3/key-blocks/pending HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(code, 200, "{pending}");
        assert_eq!(
            pending["beneficiary"],
            ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &public)
        );
        assert_ne!(pending["miner"], pending["beneficiary"]);
        let mut socket = TcpStream::connect_timeout(&local, Duration::from_secs(5)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        writeln!(socket,"{}",json!({"jsonrpc":"2.0","id":1,"method":"mining.subscribe","params":["test/1",null,null,port]})).unwrap();
        let mut socket = BufReader::new(socket);
        let mut line = String::new();
        socket.read_line(&mut line).unwrap();
        let reply: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(reply["id"], 1);
        assert!(reply["error"].is_null(), "{reply}");
        process.signal(rustix::process::Signal::TERM);
        assert_eq!(
            process.event("stopped"),
            json!({"event":"stopped","joined":true,"failed":false})
        );
        process.wait(true);
        line.clear();
        assert_eq!(socket.read_line(&mut line).unwrap(), 0);
        drop(TcpListener::bind(address).unwrap());
    }
}
