use crate::{ArmError, Exit, Limit, Snapshot};
use core::{
    alloc::{GlobalAlloc, Layout},
    ptr,
    sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering::SeqCst},
};
use std::alloc::System;

pub struct Guard {
    armed: AtomicBool,
    limit: AtomicUsize,
    exit: AtomicI32,
    live: AtomicUsize,
    peak: AtomicUsize,
}
fn terminate(code: i32) -> ! {
    // SAFETY: _exit has no pointer preconditions. It never returns and skips
    // atexit/stdout/stack cleanup that could allocate or deadlock in an allocator.
    unsafe { libc::_exit(code) }
}
impl Default for Guard {
    fn default() -> Self {
        Self::new()
    }
}
impl Guard {
    pub const fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            limit: AtomicUsize::new(usize::MAX),
            exit: AtomicI32::new(Exit::Accounting as i32),
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
    // One job per process. The absolute cap includes allocations made before
    // arming; lowering below current use terminates immediately. No reset exists.
    pub fn arm(&self, limit: Limit) -> Result<(), ArmError> {
        if limit.bytes == 0 || limit.bytes == usize::MAX {
            return Err(ArmError::InvalidLimit);
        }
        if self.armed.swap(true, SeqCst) {
            return Err(ArmError::AlreadyArmed);
        }
        self.exit.store(limit.source.exit() as i32, SeqCst);
        self.limit.store(limit.bytes, SeqCst);
        if self.live.load(SeqCst) > limit.bytes {
            terminate(self.exit.load(SeqCst));
        }
        Ok(())
    }
    pub fn snapshot(&self) -> Snapshot {
        let limit = self.limit.load(SeqCst);
        Snapshot {
            live: self.live.load(SeqCst),
            peak: self.peak.load(SeqCst),
            limit: (limit != usize::MAX).then_some(limit),
        }
    }
    fn reserve(&self, bytes: usize) {
        let mut old = self.live.load(SeqCst);
        loop {
            let Some(next) = old.checked_add(bytes) else {
                terminate(Exit::Accounting as i32);
            };
            if next > self.limit.load(SeqCst) {
                terminate(self.exit.load(SeqCst));
            }
            match self.live.compare_exchange_weak(old, next, SeqCst, SeqCst) {
                Ok(_) => {
                    // Arming may race with a reservation that saw no limit.
                    // With sequential consistency, this check or arm's live
                    // check observes every reservation crossing the new cap.
                    if next > self.limit.load(SeqCst) {
                        terminate(self.exit.load(SeqCst));
                    }
                    self.peak.fetch_max(next, SeqCst);
                    return;
                }
                Err(now) => old = now,
            }
        }
    }
    fn release(&self, bytes: usize) {
        let mut old = self.live.load(SeqCst);
        loop {
            let Some(next) = old.checked_sub(bytes) else {
                terminate(Exit::Accounting as i32);
            };
            match self.live.compare_exchange_weak(old, next, SeqCst, SeqCst) {
                Ok(_) => return,
                Err(now) => old = now,
            }
        }
    }
}
// SAFETY: All requests and matching layouts are forwarded to System. Realloc
// allocates disjoint storage, copies only the common valid range, then frees the
// original. Atomic bookkeeping neither accesses client memory nor allocates,
// locks, formats or unwinds. Exhaustion exits the disposable process.
unsafe impl GlobalAlloc for Guard {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.reserve(layout.size());
        // SAFETY: The caller supplies the nonzero valid Layout required by
        // GlobalAlloc. System receives that exact Layout unchanged.
        let result = unsafe { System.alloc(layout) };
        if result.is_null() {
            terminate(Exit::SystemAllocation as i32);
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.reserve(layout.size());
        // SAFETY: Same layout preconditions as alloc; System initializes the
        // entire requested allocation before it is returned to the caller.
        let result = unsafe { System.alloc_zeroed(layout) };
        if result.is_null() {
            terminate(Exit::SystemAllocation as i32);
        }
        result
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller supplies the still-live block and its exact Layout.
        // This allocator obtained that block from System without adding a header.
        unsafe { System.dealloc(pointer, layout) };
        self.release(layout.size());
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let Ok(next) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        // Reserve the full destination while the original remains live. This
        // deliberately accounts for peak relocation, even for a shrinking block.
        // SAFETY: GlobalAlloc's realloc contract supplies a positive new size,
        // valid alignment and size bound; next is therefore a valid alloc layout.
        let result = unsafe { self.alloc(next) };
        // SAFETY: System's new allocation is disjoint from the live original.
        // Both ranges contain at least min(old,new) bytes. Raw copying preserves
        // initialization state, including any uninitialized bytes in that range.
        unsafe { ptr::copy_nonoverlapping(pointer, result, layout.size().min(new_size)) };
        // SAFETY: The original is still live with its original Layout. We have
        // finished copying, and it is never accessed after this deallocation.
        unsafe { self.dealloc(pointer, layout) };
        result
    }
}

#[cfg(test)]
mod tests;
