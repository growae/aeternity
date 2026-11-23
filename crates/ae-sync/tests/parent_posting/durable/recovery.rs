use super::*;
use std::{
    io::BufRead,
    process::{Command, Stdio},
};
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_PARENT_HTTP_CRASH_PATH") else {
        return;
    };
    let endpoint = std::env::var("AE_PARENT_HTTP_CRASH_URL").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let s = service(
        db,
        vec![Node::new(Chain::Aeternity, &endpoint, None).unwrap()],
        Default::default(),
        Some(signing()),
    );
    println!("AE_POST_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "post\n");
    let result = wait(&mut s.client().post(post()).unwrap());
    assert!(matches!(result, Outcome::Submitted { .. }));
    println!("AE_POST_RESPONSE_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}
#[test]
fn killed_http_owners_reopen_without_repeating_possible_parent_submissions() {
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn marker(r: &mut impl BufRead, needle: &str) {
        loop {
            let mut line = String::new();
            assert_ne!(
                r.read_line(&mut line).unwrap(),
                0,
                "child exited before {needle}"
            );
            if line.contains(needle) {
                break;
            }
        }
    }
    for phase in ["before", "broadcast", "response"] {
        let mut store = Store::new();
        store.db.take();
        let l = listener();
        let endpoint = format!("http://{}", l.local_addr().unwrap());
        let mut child = Worker(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "durable::recovery::crash_worker", "--nocapture"])
                .env("AE_PARENT_HTTP_CRASH_PATH", &store.path)
                .env("AE_PARENT_HTTP_CRASH_URL", &endpoint)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
        marker(&mut output, "AE_POST_READY");
        child
            .0
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"post\n")
            .unwrap();
        child.0.stdin.as_mut().unwrap().flush().unwrap();
        let mut nonce = accept(&l);
        assert!(read_request(&mut nonce).0.contains("/next-nonce "));
        let mut transaction = None;
        let mut broadcast = None;
        if phase != "before" {
            reply(&mut nonce, 200, &json!({"next_nonce":1}));
            let mut stream = accept(&l);
            let (head, body) = read_request(&mut stream);
            assert!(head.starts_with("POST /v3/transactions "));
            transaction = Some(
                ae_codec::api::decode(body["tx"].as_str().unwrap())
                    .unwrap()
                    .1,
            );
            if phase == "response" {
                reply(&mut stream, 200, &json!({"tx_hash":"accepted"}));
                marker(&mut output, "AE_POST_RESPONSE_DURABLE");
            }
            broadcast = Some(stream);
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(broadcast);
        drop(nonce);
        store.reopen();
        let records = store.records();
        if phase == "before" {
            assert!(records.is_empty())
        } else {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].transaction, transaction.unwrap());
            assert_eq!(
                records[0].response,
                (phase == "response").then(|| b"accepted".to_vec())
            );
        }
        let s = service(
            store.db(),
            vec![Node::new(Chain::Aeternity, &endpoint, None).unwrap()],
            Default::default(),
            Some(signing()),
        );
        if phase == "before" {
            let thread = thread::spawn(move || {
                first_two(&l, Some("accepted"));
                l
            });
            assert!(matches!(
                wait(&mut s.client().post(post()).unwrap()),
                Outcome::Submitted { .. }
            ));
            s.shutdown().unwrap();
            let l = thread.join().unwrap();
            assert!(l.accept().is_err());
        } else {
            let outcome = wait(&mut s.client().post(post()).unwrap());
            assert!(match phase {
                "broadcast" => matches!(
                    outcome,
                    Outcome::Unknown {
                        reason: Failure::Recovered,
                        ..
                    }
                ),
                _ => matches!(outcome, Outcome::Submitted { .. }),
            });
            s.shutdown().unwrap();
            assert!(l.accept().is_err());
        }
        eprintln!("Parent HTTP crash recovery {phase}: one maximum broadcast");
    }
}
