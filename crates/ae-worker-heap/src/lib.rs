// A hard bound on outstanding allocation layouts in an isolated Rust worker.
// Install only in a disposable process, never in the node: exhaustion exits
// that entire process without unwinding, logging, allocating or running drops.
// This counts requested Rust heap bytes, including realloc's old and new blocks.
// It does not count allocator metadata, stacks, mappings or foreign allocations.
#[allow(unsafe_code)]
mod allocator;
pub use allocator::Guard;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Exit {
    NativeHeap = 90,
    ServiceHeap = 91,
    SystemAllocation = 92,
    Accounting = 93,
}
impl Exit {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            90 => Some(Self::NativeHeap),
            91 => Some(Self::ServiceHeap),
            92 => Some(Self::SystemAllocation),
            93 => Some(Self::Accounting),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Native,
    Service,
}
impl Source {
    fn exit(self) -> Exit {
        match self {
            Self::Native => Exit::NativeHeap,
            Self::Service => Exit::ServiceHeap,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub bytes: usize,
    pub source: Source,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmError {
    InvalidLimit,
    AlreadyArmed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    // Counters are individually atomic; a concurrent snapshot is observational,
    // not one transactional reading of all fields.
    pub live: usize,
    pub peak: usize,
    pub limit: Option<usize>,
}
