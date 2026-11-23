use super::{keys, siphash, solution_hash};
use crate::target::Target;
use ae_types::Hash256;

// Independent CPU graph search for Aeternity's Cuckoo variant. Leaves cannot
// belong to a cycle; after trimming, enumerate simple 42-edge cycles with
// their smallest edge fixed. No truncated search is reported as exhausted.
#[derive(Debug, Clone, Copy)]
pub struct Input {
    pub hash: Hash256,
    pub nonce: u64,
    pub target: Target,
    pub edge_bits: u8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Nonces {
    start: u64,
    repeats: u64,
}
impl Nonces {
    // aeminer_pow:trim_nonce/next_nonce, c6b6a6d. The strict comparison is
    // intentional: a range ending exactly at MAX_NONCE starts again at zero.
    pub fn reserve(requested: u128, repeats: u64) -> Result<Self, Error> {
        if repeats == 0 {
            return Err(Error::Configuration);
        }
        let start = if requested
            .checked_add(u128::from(repeats))
            .is_some_and(|end| end < u128::from(u64::MAX))
        {
            requested as u64
        } else {
            0
        };
        Ok(Self { start, repeats })
    }
    pub fn start(self) -> u64 {
        self.start
    }
    pub fn repeats(self) -> u64 {
        self.repeats
    }
    pub fn next(self) -> u64 {
        self.start + self.repeats
    }
    pub fn at(self, index: u64) -> Option<u64> {
        (index < self.repeats).then(|| self.start + index)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    // Owned buffer sizes, including a fixed allowance for hashing; excludes
    // allocator bookkeeping, thread stacks and the caller's own storage.
    pub memory_bytes: usize,
    pub work: u64,
    pub trim_rounds: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 256 * 1024 * 1024,
            work: 20_000_000_000,
            trim_rounds: 32,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Allocate,
    Trim,
    Build,
    Sort,
    Search,
    Complete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub phase: Phase,
    pub work: u64,
    pub alive_edges: u64,
    pub peak_buffer_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Memory { required: u128, limit: usize },
    Allocation,
    WorkLimit,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    pub error: Error,
    pub progress: Progress,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub solution: Option<[u64; 42]>,
    pub progress: Progress,
}
struct Budget<F> {
    limits: Limits,
    progress: Progress,
    last_check: u64,
    cancel: F,
}
impl<F: FnMut(Progress) -> bool> Budget<F> {
    fn fail(&self, error: Error) -> Failure {
        Failure {
            error,
            progress: self.progress,
        }
    }
    fn check(&mut self) -> Result<(), Failure> {
        self.last_check = self.progress.work;
        if (self.cancel)(self.progress) {
            Err(self.fail(Error::Cancelled))
        } else {
            Ok(())
        }
    }
    fn phase(&mut self, phase: Phase) -> Result<(), Failure> {
        self.progress.phase = phase;
        self.check()
    }
    fn charge(&mut self, n: u64) -> Result<(), Failure> {
        if n > self.limits.work - self.progress.work {
            return Err(self.fail(Error::WorkLimit));
        }
        if self.progress.work - self.last_check + n > 1024 {
            self.check()?;
        }
        self.progress.work += n;
        if self.progress.work - self.last_check >= 1024 {
            self.check()?;
        }
        Ok(())
    }
    fn memory(&mut self, bytes: u128) -> Result<(), Failure> {
        if bytes > self.limits.memory_bytes as u128 {
            return Err(self.fail(Error::Memory {
                required: bytes,
                limit: self.limits.memory_bytes,
            }));
        }
        self.progress.peak_buffer_bytes = self.progress.peak_buffer_bytes.max(bytes as usize);
        self.check()
    }
    fn complete(mut self, solution: Option<[u64; 42]>) -> Result<Report, Failure> {
        self.phase(Phase::Complete)?;
        Ok(Report {
            solution,
            progress: self.progress,
        })
    }
}
const HASH_BUFFERS: usize = 1024;

fn words<F: FnMut(Progress) -> bool>(
    n: usize,
    fill: u64,
    budget: &mut Budget<F>,
) -> Result<Vec<u64>, Failure> {
    budget.check()?;
    let mut v = Vec::new();
    v.try_reserve_exact(n)
        .map_err(|_| budget.fail(Error::Allocation))?;
    while v.len() < n {
        let chunk = (n - v.len()).min(1024);
        budget.charge(chunk as u64)?;
        v.resize(v.len() + chunk, fill);
    }
    Ok(v)
}
fn clear<F: FnMut(Progress) -> bool>(
    words: &mut [u64],
    budget: &mut Budget<F>,
) -> Result<(), Failure> {
    for chunk in words.chunks_mut(1024) {
        budget.charge(chunk.len() as u64)?;
        chunk.fill(0);
    }
    Ok(())
}
fn vertex(keys: [u64; 4], edge: u64, side: u64, mask: u64) -> u64 {
    siphash(keys, 2 * edge + side) & mask
}
fn position(bit: u64) -> (usize, u64) {
    ((bit / 64) as usize, 1 << (bit % 64))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Arc {
    vertex: u64,
    edge: u64,
    other: u64,
}

// In-place heap sort permits cancellation and work accounting during sorting,
// without allocating another graph or waiting for a large library sort.
fn sift<F: FnMut(Progress) -> bool>(
    arcs: &mut [Arc],
    mut root: usize,
    budget: &mut Budget<F>,
) -> Result<(), Failure> {
    while root < arcs.len() / 2 {
        budget.charge(1)?;
        let mut child = root * 2 + 1;
        if child + 1 < arcs.len() && arcs[child] < arcs[child + 1] {
            child += 1;
        }
        if arcs[root] >= arcs[child] {
            break;
        }
        arcs.swap(root, child);
        root = child;
    }
    Ok(())
}
fn sort<F: FnMut(Progress) -> bool>(
    arcs: &mut [Arc],
    budget: &mut Budget<F>,
) -> Result<(), Failure> {
    for root in (0..arcs.len() / 2).rev() {
        sift(arcs, root, budget)?;
    }
    for end in (1..arcs.len()).rev() {
        budget.charge(1)?;
        arcs.swap(0, end);
        sift(&mut arcs[..end], 0, budget)?;
    }
    Ok(())
}
fn lower<F: FnMut(Progress) -> bool>(
    arcs: &[Arc],
    vertex: u64,
    budget: &mut Budget<F>,
) -> Result<usize, Failure> {
    let (mut start, mut end) = (0, arcs.len());
    while start < end {
        budget.charge(1)?;
        let middle = start + (end - start) / 2;
        if arcs[middle].vertex < vertex {
            start = middle + 1;
        } else {
            end = middle;
        }
    }
    Ok(start)
}
fn cycles<F: FnMut(Progress) -> bool>(
    arcs: &[Arc],
    target: Target,
    edge_bits: u8,
    budget: &mut Budget<F>,
) -> Result<Option<[u64; 42]>, Failure> {
    for first in arcs {
        budget.charge(1)?;
        if first.vertex & 1 != 0 {
            continue;
        }
        let mut path = [0; 42];
        let mut vertices = [0; 42];
        let mut cursors = [0; 42];
        path[0] = first.edge;
        vertices[0] = first.vertex;
        vertices[1] = first.other;
        cursors[1] = lower(arcs, first.other, budget)?;
        let mut depth = 1;
        loop {
            budget.charge(1)?;
            let cursor = cursors[depth];
            if cursor == arcs.len() || arcs[cursor].vertex != vertices[depth] {
                if depth == 1 {
                    break;
                }
                depth -= 1;
                continue;
            }
            cursors[depth] += 1;
            let next = arcs[cursor];
            if next.edge <= first.edge {
                continue;
            }
            if next.other == vertices[0] {
                if depth == 41 {
                    path[depth] = next.edge;
                    let mut proof = path;
                    proof.sort_unstable();
                    budget.charge(42)?;
                    if solution_hash(&proof, edge_bits).is_some_and(|h| target.accepts(&h)) {
                        return Ok(Some(proof));
                    }
                }
                continue;
            }
            if depth == 41 {
                continue;
            }
            budget.charge((depth + 1) as u64)?;
            if vertices[..=depth].contains(&next.other) {
                continue;
            }
            path[depth] = next.edge;
            depth += 1;
            vertices[depth] = next.other;
            cursors[depth] = lower(arcs, next.other, budget)?;
        }
    }
    Ok(None)
}

// The callback returns true to cancel. It runs at phase boundaries and at
// most 1024 charged operations apart. This code reads no clock, I/O or config.
pub fn solve(
    input: Input,
    limits: Limits,
    cancel: impl FnMut(Progress) -> bool,
) -> Result<Report, Failure> {
    let mut budget = Budget {
        limits,
        progress: Progress {
            phase: Phase::Allocate,
            work: 0,
            alive_edges: 0,
            peak_buffer_bytes: 0,
        },
        last_check: 0,
        cancel,
    };
    if !(1..=63).contains(&input.edge_bits) {
        return Err(budget.fail(Error::Configuration));
    }
    budget.check()?;
    let count = 1u64 << input.edge_bits;
    budget.progress.alive_edges = count;
    if count < 42 {
        return budget.complete(None);
    }
    let word_count = count.div_ceil(64);
    budget.memory(u128::from(word_count) * 24 + HASH_BUFFERS as u128)?;
    // The memory guard above proves word_count and every vertex index fit
    // usize, including platforms with a narrower address space.
    let mut alive = words(word_count as usize, u64::MAX, &mut budget)?;
    let mut once = words(word_count as usize, 0, &mut budget)?;
    let mut twice = words(word_count as usize, 0, &mut budget)?;
    let keys = keys(input.hash, input.nonce);
    let mask = count - 1;
    budget.phase(Phase::Trim)?;
    for _ in 0..limits.trim_rounds {
        let before = budget.progress.alive_edges;
        for side in 0..2 {
            clear(&mut once, &mut budget)?;
            clear(&mut twice, &mut budget)?;
            for (i, word) in alive.iter().enumerate() {
                budget.charge(1)?;
                let mut bits = *word;
                while bits != 0 {
                    budget.charge(1)?;
                    let edge = i as u64 * 64 + u64::from(bits.trailing_zeros());
                    bits &= bits - 1;
                    let (index, bit) = position(vertex(keys, edge, side, mask));
                    twice[index] |= once[index] & bit;
                    once[index] |= bit;
                }
            }
            for (i, word) in alive.iter_mut().enumerate() {
                budget.charge(1)?;
                let mut bits = *word;
                while bits != 0 {
                    budget.charge(1)?;
                    let offset = bits.trailing_zeros();
                    bits &= bits - 1;
                    let edge = i as u64 * 64 + u64::from(offset);
                    let (index, bit) = position(vertex(keys, edge, side, mask));
                    if twice[index] & bit == 0 {
                        *word &= !(1 << offset);
                        budget.progress.alive_edges -= 1;
                    }
                }
            }
        }
        if budget.progress.alive_edges < 42 {
            return budget.complete(None);
        }
        if before == budget.progress.alive_edges {
            break;
        }
    }
    drop(once);
    drop(twice);
    budget.phase(Phase::Build)?;
    let arc_count = u128::from(budget.progress.alive_edges) * 2;
    budget.memory(
        u128::from(word_count) * 8 + arc_count * size_of::<Arc>() as u128 + HASH_BUFFERS as u128,
    )?;
    let mut arcs = Vec::new();
    arcs.try_reserve_exact(arc_count as usize)
        .map_err(|_| budget.fail(Error::Allocation))?;
    for (i, word) in alive.iter().enumerate() {
        budget.charge(1)?;
        let mut bits = *word;
        while bits != 0 {
            budget.charge(2)?;
            let edge = i as u64 * 64 + u64::from(bits.trailing_zeros());
            bits &= bits - 1;
            let u = vertex(keys, edge, 0, mask) << 1;
            let v = (vertex(keys, edge, 1, mask) << 1) | 1;
            arcs.push(Arc {
                vertex: u,
                edge,
                other: v,
            });
            arcs.push(Arc {
                vertex: v,
                edge,
                other: u,
            });
        }
    }
    drop(alive);
    budget.phase(Phase::Sort)?;
    sort(&mut arcs, &mut budget)?;
    budget.phase(Phase::Search)?;
    let solution = cycles(&arcs, input.target, input.edge_bits, &mut budget)?;
    budget.complete(solution)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cycle(arcs: &mut Vec<Arc>, length: u64, edge: u64, base: u64) {
        for n in 0..length {
            let u = if n == 0 { 0 } else { base + n };
            let v = if n + 1 == length { 0 } else { base + n + 1 };
            arcs.push(Arc {
                vertex: u,
                edge: edge + n,
                other: v,
            });
            arcs.push(Arc {
                vertex: v,
                edge: edge + n,
                other: u,
            });
        }
    }
    #[test]
    fn exact_cycle_search_handles_shared_vertices_short_cycles_and_target_rejections() {
        let mut arcs = Vec::new();
        cycle(&mut arcs, 40, 0, 0);
        cycle(&mut arcs, 44, 100, 100);
        let mut budget = Budget {
            limits: Limits::default(),
            progress: Progress {
                phase: Phase::Search,
                work: 0,
                alive_edges: 84,
                peak_buffer_bytes: 0,
            },
            last_check: 0,
            cancel: |_| false,
        };
        arcs.sort_unstable();
        assert_eq!(
            cycles(&arcs, Target(0x2100ffff), 15, &mut budget).unwrap(),
            None
        );
        cycle(&mut arcs, 42, 200, 200);
        cycle(&mut arcs, 42, 300, 300);
        arcs.sort_unstable();
        let proof = cycles(&arcs, Target(0x2100ffff), 15, &mut budget)
            .unwrap()
            .unwrap();
        assert_eq!(proof, std::array::from_fn(|i| 200 + i as u64));
        assert_eq!(cycles(&arcs, Target(0), 15, &mut budget).unwrap(), None);
        // The two valid cycles share their start vertex. They must not be
        // concatenated, nor may a short cycle be counted repeatedly.
        assert!(budget.progress.work < budget.limits.work);
    }
}
