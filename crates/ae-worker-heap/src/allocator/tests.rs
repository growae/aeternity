use super::*;
use crate::Source;

#[test]
fn native_phase_is_single_use_and_never_resets_live_allocations_or_service_limit() {
    let guard = Guard::new();
    assert!(matches!(
        guard.begin_native(None),
        Err(PhaseError::NotArmed)
    ));
    guard
        .arm(Limit {
            bytes: 4096,
            source: Source::Service,
        })
        .unwrap();
    for limit in [0, usize::MAX] {
        assert!(matches!(
            guard.begin_native(Some(limit)),
            Err(PhaseError::InvalidLimit)
        ));
    }
    // SAFETY: This temporary decode buffer is freed before the core phase.
    // Its historical peak must not be mistaken for current live allocation.
    unsafe {
        let layout = Layout::from_size_align(2048, 64).unwrap();
        let value = guard.alloc_zeroed(layout);
        guard.dealloc(value, layout);
    }
    assert_eq!(guard.snapshot().peak, 2048);
    let scope = guard.begin_native(Some(1024)).unwrap();
    assert_eq!(guard.native_limit(), Some(1024));
    assert!(matches!(
        guard.begin_native(None),
        Err(PhaseError::AlreadyStarted)
    ));
    let layout = Layout::from_size_align(512, 64).unwrap();
    // SAFETY: The live allocation crosses the phase boundary, then is freed
    // once using its original allocator and matching layout.
    unsafe {
        let value = guard.alloc_zeroed(layout);
        value.write(19);
        drop(scope);
        assert_eq!(guard.native_limit(), None);
        assert_eq!(guard.snapshot().limit, Some(4096));
        assert_eq!(guard.snapshot().live, 512);
        assert_eq!(value.read(), 19);
        guard.dealloc(value, layout);
    }
    assert_eq!(guard.snapshot().live, 0);
    assert!(matches!(
        guard.begin_native(Some(2048)),
        Err(PhaseError::AlreadyStarted)
    ));
    let guard = Guard::new();
    guard
        .arm(Limit {
            bytes: 4096,
            source: Source::Service,
        })
        .unwrap();
    drop(guard.begin_native(None).unwrap());
    assert!(matches!(
        guard.begin_native(Some(2048)),
        Err(PhaseError::AlreadyStarted)
    ));
}

#[test]
fn aligned_zeroed_growth_shrink_and_release_preserve_data_and_peak() {
    let guard = Guard::new();
    guard
        .arm(Limit {
            bytes: 4096,
            source: Source::Native,
        })
        .unwrap();
    let original = Layout::from_size_align(256, 128).unwrap();
    // SAFETY: All layouts are nonzero and valid. Every pointer is used only
    // while owned, with its current layout, then freed exactly once.
    unsafe {
        let p = guard.alloc_zeroed(original);
        assert_eq!(p.addr() % 128, 0);
        for i in 0..256 {
            assert_eq!(*p.add(i), 0);
            *p.add(i) = (i % 251) as u8;
        }
        let p = guard.realloc(p, original, 512);
        assert_eq!(p.addr() % 128, 0);
        for i in 0..256 {
            assert_eq!(*p.add(i), (i % 251) as u8);
        }
        assert_eq!(guard.snapshot().live, 512);
        assert_eq!(guard.snapshot().peak, 768);
        let grown = Layout::from_size_align(512, 128).unwrap();
        let p = guard.realloc(p, grown, 64);
        for i in 0..64 {
            assert_eq!(*p.add(i), (i % 251) as u8);
        }
        assert_eq!(guard.snapshot().live, 64);
        guard.dealloc(p, Layout::from_size_align(64, 128).unwrap());
    }
    assert_eq!(
        guard.snapshot(),
        Snapshot {
            live: 0,
            peak: 768,
            limit: Some(4096)
        }
    );
    assert_eq!(
        guard.arm(Limit {
            bytes: 8192,
            source: Source::Service
        }),
        Err(ArmError::AlreadyArmed)
    );
}

#[test]
fn concurrent_allocation_and_cross_thread_release_share_one_counter() {
    let guard = Guard::new();
    assert_eq!(
        guard.arm(Limit {
            bytes: 0,
            source: Source::Service
        }),
        Err(ArmError::InvalidLimit)
    );
    assert_eq!(
        guard.arm(Limit {
            bytes: usize::MAX,
            source: Source::Service
        }),
        Err(ArmError::InvalidLimit)
    );
    guard
        .arm(Limit {
            bytes: 1 << 20,
            source: Source::Service,
        })
        .unwrap();
    let layout = Layout::from_size_align(1024, 64).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let g = &guard;
            scope.spawn(move || {
                for _ in 0..1000 {
                    // SAFETY: Each nonzero allocation is exclusively owned by
                    // this thread and freed once using the identical layout.
                    unsafe {
                        let p = g.alloc(layout);
                        p.write(42);
                        assert_eq!(p.read(), 42);
                        g.dealloc(p, layout);
                    }
                }
            });
        }
    });
    assert_eq!(guard.snapshot().live, 0);
    assert!(guard.snapshot().peak <= 8 * 1024);
    // Transfer allocation ownership to a different thread without converting
    // through an integer address (which would discard pointer provenance).
    struct Owned(*mut u8);
    // SAFETY: This wrapper uniquely owns its allocation and has no shared
    // accesses. The receiver consumes it once and deallocates with the same guard.
    unsafe impl Send for Owned {}
    impl Owned {
        fn free(self, guard: &Guard, layout: Layout) {
            // SAFETY: Unique ownership is transferred to this call; the original
            // allocator and exact layout are supplied and no further use occurs.
            unsafe { guard.dealloc(self.0, layout) }
        }
    }
    // SAFETY: The valid allocation is moved into a unique Owned wrapper.
    let owned = Owned(unsafe { guard.alloc(layout) });
    std::thread::scope(|scope| {
        scope.spawn(|| owned.free(&guard, layout)).join().unwrap();
    });
    assert_eq!(guard.snapshot().live, 0);
}
