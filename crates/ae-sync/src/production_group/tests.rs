use super::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

const GENERATOR: &str = "aec_block_generator";
const CONDUCTOR: &str = "aec_conductor";
const SIBLING: &str = "stable_consensus";
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_conductor_group.json"
    ))
    .unwrap()
}
enum Command {
    Exit(&'static str),
    Read(mpsc::Sender<u64>),
    Work(u64, mpsc::Sender<u64>),
    Hold(mpsc::Receiver<()>, mpsc::Sender<()>),
}
#[derive(Clone)]
struct Client(mpsc::SyncSender<Command>);
impl Client {
    fn exit(&self, reason: &'static str) {
        self.0.send(Command::Exit(reason)).unwrap();
    }
    fn read(&self) -> Option<u64> {
        let (send, receive) = mpsc::channel();
        self.0.send(Command::Read(send)).ok()?;
        receive.recv_timeout(Duration::from_secs(3)).ok()
    }
}
struct Worker {
    client: Client,
    thread: Option<thread::JoinHandle<Vec<u64>>>,
}
impl Child for Worker {
    type Exit = Vec<u64>;
    fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }
    fn shutdown(mut self) -> thread::Result<Self::Exit> {
        let _ = self.client.0.send(Command::Exit("shutdown"));
        self.thread.take().unwrap().join()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = self.client.0.send(Command::Exit("shutdown"));
            let _ = thread.join();
        }
    }
}
#[derive(Clone, Default)]
struct Lab {
    trace: Arc<Mutex<Vec<Value>>>,
    faults: Arc<Mutex<BTreeMap<(&'static str, u64), &'static str>>>,
}
impl Lab {
    fn event(&self, child: &str, generation: u64, event: &str, extra: Option<(&str, &str)>) {
        let mut row = json!({"child":child,"generation":generation,"event":event});
        if let Some((key, value)) = extra {
            row[key] = json!(value);
        }
        self.trace.lock().unwrap().push(row);
    }
    fn take(&self) -> Value {
        json!(std::mem::take(&mut *self.trace.lock().unwrap()))
    }
    fn spawn(&self, child: &'static str, generation: u64) -> Result<Worker, &'static str> {
        self.event(child, generation, "attempt", None);
        let fault = self.faults.lock().unwrap().remove(&(child, generation));
        if let Some(mode) = fault {
            self.event(child, generation, "failed", Some(("mode", mode)));
            if mode == "error" {
                panic!("constructor failure");
            }
            return Err("constructor failure");
        }
        let lab = self.clone();
        let (send, receive) = mpsc::sync_channel(8);
        let (ready, started) = mpsc::channel();
        let thread = thread::spawn(move || {
            lab.event(child, generation, "started", None);
            ready.send(()).unwrap();
            let mut work = vec![];
            while let Ok(command) = receive.recv() {
                match command {
                    Command::Read(reply) => {
                        let _ = reply.send(generation);
                    }
                    Command::Work(id, reply) => {
                        work.push(id);
                        let _ = reply.send(id);
                    }
                    Command::Hold(release, held) => {
                        held.send(()).unwrap();
                        release.recv().unwrap();
                    }
                    Command::Exit(reason) => {
                        // "killed" models an abrupt departure without a
                        // terminate callback; this is not thread force-kill.
                        if reason == "killed" {
                            panic!("abrupt child departure");
                        }
                        lab.event(child, generation, "stopping", Some(("reason", reason)));
                        lab.event(child, generation, "stopped", Some(("reason", reason)));
                        if reason == "boom" {
                            panic!("child failure");
                        }
                        break;
                    }
                }
            }
            work
        });
        started.recv_timeout(Duration::from_secs(3)).unwrap();
        Ok(Worker {
            client: Client(send),
            thread: Some(thread),
        })
    }
    fn fail(&self, child: &'static str, generation: u64, mode: &'static str) {
        self.faults
            .lock()
            .unwrap()
            .insert((child, generation), mode);
    }
}
impl Factory for Lab {
    type Generator = Worker;
    type Conductor = Worker;
    type Error = &'static str;
    fn generator(&self, generation: u64) -> Result<Worker, Self::Error> {
        self.spawn(GENERATOR, generation)
    }
    fn conductor(&self, generation: u64, generator: &Worker) -> Result<Worker, Self::Error> {
        assert!(generator.client.read().is_some());
        self.spawn(CONDUCTOR, generation)
    }
}
fn owner(lab: &Lab) -> Owner<Lab> {
    Owner::start(lab.clone()).unwrap_or_else(|_| panic!("startup"))
}
fn departed(owner: &Owner<Lab>, name: &str, reason: &'static str) -> Client {
    let (generator, conductor) = owner.children().unwrap();
    let child = if name == GENERATOR {
        generator
    } else {
        conductor
    };
    let old = child.client.clone();
    old.exit(reason);
    let until = Instant::now() + Duration::from_secs(3);
    while !child.is_finished() {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(1));
    }
    old
}
fn replacement(owner: &mut Owner<Lab>, second: u64) -> Vec<Output<Lab>> {
    let mut notices = vec![];
    for _ in 0..16 {
        let result = owner.poll(second);
        if let Some(notice) = owner.take_notice() {
            notices.push(notice);
        }
        if owner.status().failure.is_some() || owner.status().phase == Phase::Running {
            assert!(result.is_ok());
            return notices;
        }
        assert!(result.is_ok());
    }
    panic!("replacement did not finish");
}

#[test]
fn real_child_lifecycles_match_fourteen_native_cases() {
    let fixture = fixture();
    assert_eq!(fixture["policy"]["strategy"], "one_for_all");
    assert_eq!(fixture["policy"]["intensity"], 5);
    assert_eq!(fixture["policy"]["period"], 50);
    let mut compared = 0;
    for case in fixture["lifecycle"].as_array().unwrap() {
        let kind = case["kind"].as_str().unwrap();
        if kind == "forced_shutdown" {
            continue;
        }
        let lab = Lab::default();
        let sibling = lab.spawn(SIBLING, 1).unwrap();
        let name = if case["child"] == GENERATOR {
            GENERATOR
        } else {
            CONDUCTOR
        };
        if kind == "startup_failure" {
            lab.fail(name, 1, "stop");
            let failure = match Owner::start(lab.clone()) {
                Err(failure) => failure,
                Ok(_) => panic!("startup should fail"),
            };
            assert_eq!(failure.failure.generation, 1);
            assert_eq!(failure.retired.generator.is_some(), name == CONDUCTOR);
            assert!(failure.retired.conductor.is_none());
            assert_eq!(lab.take(), case["trace"]);
        } else {
            let mut owner = owner(&lab);
            match kind {
                "shutdown" => (),
                "normal" | "boom" | "killed" => {
                    let reason = match kind {
                        "normal" => "normal",
                        "boom" => "boom",
                        _ => "killed",
                    };
                    let old = departed(&owner, name, reason);
                    let reports = replacement(&mut owner, 0);
                    assert_eq!(reports.len(), 2);
                    let Notice::Retired(retired) = &reports[0] else {
                        panic!("retirement");
                    };
                    assert_eq!(
                        retired.generator.as_ref().unwrap().1.is_err(),
                        name == GENERATOR && kind != "normal"
                    );
                    assert_eq!(
                        retired.conductor.as_ref().unwrap().1.is_err(),
                        name == CONDUCTOR && kind != "normal"
                    );
                    assert_eq!(old.read(), None);
                    let (g, c) = owner.children().unwrap();
                    assert_eq!(g.client.read(), Some(2));
                    assert_eq!(c.client.read(), Some(2));
                }
                "failed_restart" => {
                    let mode = if case["mode"] == "stop" {
                        "stop"
                    } else {
                        "error"
                    };
                    lab.fail(name, 2, mode);
                    departed(&owner, CONDUCTOR, "normal");
                    let reports = replacement(&mut owner, 0);
                    assert_eq!(reports.len(), 4);
                    let Notice::StartFailed(failure) = &reports[1] else {
                        panic!("start failure")
                    };
                    assert_eq!(failure.generation, 2);
                    assert_eq!(
                        matches!(failure.error, StartError::Panicked),
                        mode == "error"
                    );
                    let (g, c) = owner.children().unwrap();
                    assert_eq!(g.client.read(), Some(3));
                    assert_eq!(c.client.read(), Some(if name == GENERATOR { 2 } else { 3 }));
                }
                "shared_exhaustion" => {
                    for index in 0..6 {
                        departed(
                            &owner,
                            if index % 2 == 0 { GENERATOR } else { CONDUCTOR },
                            "normal",
                        );
                        replacement(&mut owner, 0);
                    }
                    assert_eq!(owner.status().attempts, 6);
                    assert_eq!(owner.poll(0), Err(Error::RestartLimit));
                    assert!(owner.children().is_none());
                }
                _ => panic!("unhandled {kind}"),
            }
            assert_eq!(lab.take(), case["trace"], "{kind} {name}");
            let exit = owner.shutdown();
            assert_eq!(exit.status.phase, Phase::Stopped);
            if let Some(expected) = case.get("shutdown_trace") {
                assert_eq!(lab.take(), *expected, "shutdown {kind} {name}");
            }
        }
        assert_eq!(sibling.client.read(), Some(1));
        sibling.shutdown().unwrap();
        compared += 1;
    }
    assert_eq!(compared, 14);
}

#[test]
fn owned_replacements_follow_all_native_fifty_second_budget_traces() {
    for trace in fixture()["budgets"].as_array().unwrap() {
        let lab = Lab::default();
        let mut owner = owner(&lab);
        for (i, row) in trace.as_array().unwrap().iter().enumerate() {
            departed(
                &owner,
                if i % 2 == 0 { GENERATOR } else { CONDUCTOR },
                "normal",
            );
            let second = row["now"].as_u64().unwrap();
            replacement(&mut owner, second);
            assert_eq!(
                owner.status().failure.is_none(),
                row["allowed"].as_bool().unwrap()
            );
            if owner.status().failure.is_some() {
                assert_eq!(owner.poll(second), Err(Error::RestartLimit));
            }
        }
        owner.shutdown();
    }
}

#[test]
fn unread_retirement_prevents_replacement_and_preserves_accepted_results() {
    let lab = Lab::default();
    let mut owner = owner(&lab);
    let client = owner.children().unwrap().1.client.clone();
    let (send, receive) = mpsc::channel();
    client.0.send(Command::Work(42, send)).unwrap();
    departed(&owner, GENERATOR, "normal");
    assert_eq!(owner.poll(10), Ok(Progress::Output));
    let frozen = owner.status();
    for _ in 0..64 {
        assert_eq!(owner.poll(10), Ok(Progress::Output));
    }
    assert_eq!(owner.status(), frozen);
    assert!(owner.children().is_none());
    assert_eq!(receive.recv_timeout(Duration::from_secs(3)).unwrap(), 42);
    let Some(Notice::Retired(report)) = owner.take_notice() else {
        panic!("retirement")
    };
    assert_eq!(report.conductor.unwrap().1.unwrap(), vec![42]);
    replacement(&mut owner, 10);
    assert_eq!(client.read(), None);
    let exit = owner.shutdown();
    assert!(exit.retired.conductor.unwrap().1.unwrap().is_empty());
}

#[test]
fn shutdown_during_partial_start_preserves_pending_failure_and_joins_generator() {
    let lab = Lab::default();
    let mut owner = owner(&lab);
    lab.fail(CONDUCTOR, 2, "error");
    departed(&owner, CONDUCTOR, "normal");
    owner.poll(0).unwrap();
    owner.take_notice();
    owner.poll(0).unwrap();
    assert_eq!(owner.status().phase, Phase::Retry);
    assert!(owner.children().is_none());
    let exit = owner.shutdown();
    assert!(matches!(exit.pending, Some(Notice::StartFailed(_))));
    assert_eq!(exit.retired.generator.unwrap().0, 2);
    assert!(exit.retired.conductor.is_none());
    assert_eq!(exit.status.attempts, 1);
}

#[test]
fn repeated_constructor_failures_consume_shared_budget_without_publishing_partial_pair() {
    let lab = Lab::default();
    let mut owner = owner(&lab);
    for generation in 2..=6 {
        lab.fail(CONDUCTOR, generation, "stop");
    }
    departed(&owner, GENERATOR, "normal");
    for _ in 0..20 {
        owner.poll(0).unwrap();
        assert!(owner.children().is_none());
        owner.take_notice();
        if owner.status().failure.is_some() {
            break;
        }
    }
    assert_eq!(owner.status().attempts, 6);
    assert_eq!(owner.status().conductor_generation, 6);
    assert_eq!(owner.status().generator_generation, 6);
    assert_eq!(owner.poll(0), Err(Error::RestartLimit));
    assert!(owner.generator.is_none());
    assert!(owner.conductor.is_none());
}

#[test]
fn backwards_clock_does_not_mutate_children_budget_or_pending_result() {
    let lab = Lab::default();
    let mut owner = owner(&lab);
    assert_eq!(owner.poll(51), Ok(Progress::Idle));
    let before = owner.status();
    assert_eq!(owner.poll(50), Err(Error::Time));
    assert_eq!(owner.status(), before);
    assert_eq!(owner.children().unwrap().0.client.read(), Some(1));
    owner.shutdown();
}

#[test]
fn cooperative_shutdown_waits_for_conductor_before_stopping_generator() {
    let lab = Lab::default();
    let owner = owner(&lab);
    let (generator, conductor) = owner.children().unwrap();
    let generator_client = generator.client.clone();
    let (release, wait) = mpsc::channel();
    let (held, holding) = mpsc::channel();
    conductor.client.0.send(Command::Hold(wait, held)).unwrap();
    holding.recv_timeout(Duration::from_secs(3)).unwrap();
    let (done, joined) = mpsc::channel();
    let thread = thread::spawn(move || {
        done.send(owner.shutdown())
            .unwrap_or_else(|_| panic!("report"));
    });
    assert!(joined.recv_timeout(Duration::from_millis(30)).is_err());
    assert_eq!(generator_client.read(), Some(1));
    release.send(()).unwrap();
    let report = joined.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(report.retired.generator.unwrap().1.is_ok());
    assert!(report.retired.conductor.unwrap().1.is_ok());
    thread.join().unwrap();
    assert_eq!(generator_client.read(), None);
    // The native forced-kill fixture is intentionally not claimed by this
    // cooperative test. Rust cannot safely kill this held thread at 5000ms.
}

#[test]
fn dropping_owner_joins_both_generations_in_reverse_order() {
    let lab = Lab::default();
    let owner = owner(&lab);
    let (g, c) = owner.children().unwrap();
    let (g, c) = (g.client.clone(), c.client.clone());
    lab.take();
    drop(owner);
    assert_eq!(g.read(), None);
    assert_eq!(c.read(), None);
    assert_eq!(lab.take(), fixture()["lifecycle"][0]["shutdown_trace"]);
}
