use ae_worker_heap::{Exit, Guard, Limit, Source};
use std::{
    hint::black_box,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

// Only child_entry arms this allocator, in a fresh disposable subprocess.
#[global_allocator]
static HEAP: Guard = Guard::new();

fn arm(extra: usize, source: Source) {
    HEAP.arm(Limit {
        bytes: HEAP.snapshot().live + extra,
        source,
    })
    .unwrap();
}
#[test]
#[ignore = "bounded subprocess entrypoint; invoked by the parent tests"]
fn child_entry() {
    let case = std::env::var("AE_WORKER_HEAP_TEST_CASE").unwrap();
    match case.as_str() {
        "phase_recover" => {
            arm(8 << 20, Source::Service);
            let scope = HEAP
                .begin_native(Some(HEAP.snapshot().live + (128 << 10)))
                .unwrap();
            let core = black_box(vec![7_u8; 64 << 10]);
            drop(scope);
            let render = black_box(vec![9_u8; 2 << 20]);
            assert_eq!(core[0], 7);
            assert_eq!(render[0], 9);
            assert_eq!(HEAP.native_limit(), None);
            assert!(HEAP.snapshot().live > 2 << 20);
        }
        "phase_native" | "phase_equal" | "phase_service" => {
            arm(
                if case == "phase_native" {
                    8 << 20
                } else {
                    256 << 10
                },
                Source::Service,
            );
            let bytes = if case == "phase_equal" {
                HEAP.snapshot().limit.unwrap()
            } else {
                HEAP.snapshot().live
                    + if case == "phase_service" {
                        1 << 20
                    } else {
                        128 << 10
                    }
            };
            let _scope = HEAP.begin_native(Some(bytes)).unwrap();
            black_box(vec![3_u8; black_box(2 << 20)]);
            panic!("active core phase did not enforce the tighter cap");
        }
        "phase_render_service" | "phase_disabled_service" => {
            arm(256 << 10, Source::Service);
            let scope = HEAP
                .begin_native(if case == "phase_disabled_service" {
                    None
                } else {
                    Some(HEAP.snapshot().live + (128 << 10))
                })
                .unwrap();
            let core = black_box(vec![2_u8; 64 << 10]);
            drop(scope);
            black_box(vec![3_u8; black_box(1 << 20)]);
            black_box(core);
            panic!("native phase completion disabled the service cap");
        }
        "phase_already_live" => {
            arm(8 << 20, Source::Service);
            let value = black_box(vec![2_u8; 1 << 20]);
            let _scope = HEAP.begin_native(Some(128 << 10)).unwrap();
            black_box(value);
            panic!("native phase ignored decoded live allocations");
        }
        "phase_arming_race" => {
            arm(16 << 20, Source::Service);
            let barrier = std::sync::Barrier::new(9);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        let bytes = black_box(vec![6_u8; 256 << 10]);
                        barrier.wait();
                        black_box(bytes);
                    });
                }
                let cap = HEAP.snapshot().live + (1 << 20);
                barrier.wait();
                let _native = HEAP.begin_native(Some(cap)).unwrap();
                barrier.wait();
            });
            panic!("allocations raced past the native phase cap");
        }
        "recover" => {
            arm(8 << 20, Source::Service);
            let mut bytes = Vec::with_capacity(16 << 10);
            bytes.resize(16 << 10, 17_u8);
            bytes.reserve_exact(32 << 10);
            assert!(bytes.iter().all(|x| *x == 17));
            drop(black_box(bytes));
        }
        "native" | "service" => {
            arm(
                256 << 10,
                if case == "native" {
                    Source::Native
                } else {
                    Source::Service
                },
            );
            let bytes = vec![black_box(7_u8); black_box(1 << 20)];
            black_box(bytes);
            panic!("guard did not stop one oversized allocation");
        }
        "growth" => {
            let bytes = vec![3_u8; 256 << 10];
            arm(384 << 10, Source::Native);
            let mut bytes = black_box(bytes);
            bytes.reserve_exact(256 << 10);
            black_box(bytes);
            panic!("guard did not account for old and new realloc blocks");
        }
        "already_live" => {
            let bytes = black_box(vec![4_u8; 1 << 20]);
            HEAP.arm(Limit {
                bytes: 128 << 10,
                source: Source::Native,
            })
            .unwrap();
            black_box(bytes);
            panic!("guard ignored allocations made before arming");
        }
        "concurrent" => {
            let barrier = std::sync::Barrier::new(9);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        let bytes = black_box(vec![6_u8; 256 << 10]);
                        barrier.wait();
                        black_box(bytes);
                    });
                }
                // Account for thread setup first; the following limit must
                // aggregate payloads retained concurrently by the eight workers.
                arm(1 << 20, Source::Service);
                barrier.wait();
                barrier.wait();
            });
            panic!("guard did not aggregate thread allocations");
        }
        "arming_race" => {
            let barrier = std::sync::Barrier::new(9);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        let bytes = black_box(vec![6_u8; 256 << 10]);
                        barrier.wait();
                        black_box(bytes);
                    });
                }
                let cap = HEAP.snapshot().live + (1 << 20);
                barrier.wait();
                HEAP.arm(Limit {
                    bytes: cap,
                    source: Source::Native,
                })
                .unwrap();
                barrier.wait();
            });
            panic!("allocations raced past the newly armed cap");
        }
        "fate" => {
            let value = ae_vm_fate::value::Value::String(vec![9; 1 << 20]);
            arm(128 << 10, Source::Native);
            black_box(ae_vm_fate::encoding::encode(&value, Default::default()).unwrap());
            panic!("guard did not reach FATE encoding allocations");
        }
        "aevm" => {
            let bytes = vec![9; 1 << 20];
            let mut memory = ae_vm_aevm::memory::Memory::default();
            arm(128 << 10, Source::Native);
            memory.write_area(&0_u8.into(), &bytes);
            black_box(memory);
            panic!("guard did not reach AEVM memory allocations");
        }
        _ => panic!("unknown child case"),
    }
}
#[test]
fn native_core_scope_preserves_service_enforcement_and_allocation_ownership() {
    assert!(child("phase_recover").success());
    for (case, expected) in [
        ("phase_native", Exit::NativeHeap),
        ("phase_equal", Exit::NativeHeap),
        ("phase_service", Exit::ServiceHeap),
        ("phase_render_service", Exit::ServiceHeap),
        ("phase_disabled_service", Exit::ServiceHeap),
        ("phase_already_live", Exit::NativeHeap),
        ("phase_arming_race", Exit::NativeHeap),
    ] {
        assert_eq!(
            child(case).code().and_then(Exit::from_code),
            Some(expected),
            "{case}"
        );
        assert!(child("phase_recover").success(), "fresh core after {case}");
    }
}
fn child(case: &str) -> std::process::ExitStatus {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--ignored", "--test-threads=1"])
        .env("AE_WORKER_HEAP_TEST_CASE", case)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if start.elapsed() > Duration::from_secs(10) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("bounded worker probe hung: {case}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn isolated_exhaustion_preserves_parent_and_allows_fresh_work() {
    for (case, expected) in [
        ("native", Exit::NativeHeap),
        ("service", Exit::ServiceHeap),
        ("growth", Exit::NativeHeap),
        ("already_live", Exit::NativeHeap),
        ("concurrent", Exit::ServiceHeap),
        ("arming_race", Exit::NativeHeap),
    ] {
        assert_eq!(
            child(case).code().and_then(Exit::from_code),
            Some(expected),
            "{case}"
        );
        assert!(child("recover").success(), "fresh work after {case}");
    }
    assert_eq!(HEAP.snapshot().limit, None);
}
#[test]
fn actual_vm_allocations_are_bounded_without_vm_cooperation() {
    for case in ["fate", "aevm"] {
        assert_eq!(
            child(case).code().and_then(Exit::from_code),
            Some(Exit::NativeHeap),
            "{case}"
        );
        assert!(child("recover").success());
    }
}
