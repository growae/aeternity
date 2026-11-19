// Iterative aevm_data conversion. Map values use separate heaps/visited sets.
use super::{Error, Format, Heap, MapSerialization, Type, TypeRef};
use crate::{
    execution::Budget,
    maps::{self, Bytes, Data, Entries, Map, Maps, Store},
    memory::Memory,
    word::Word,
};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::{
    collections::{BTreeSet, btree_map::IntoIter},
    sync::Arc,
};

#[derive(Clone)]
pub(super) enum Buffer<'a> {
    Borrowed(&'a [u8]),
    Shared(Bytes),
}
impl Buffer<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Shared(b) => b,
        }
    }
}
pub(super) enum Source<'a> {
    Sparse(&'a Memory),
    Binary {
        bytes: Buffer<'a>,
        start: usize,
        offset: BigUint,
    },
}
impl<'a> Source<'a> {
    pub(super) fn binary(bytes: &'a [u8], start: usize, offset: BigUint) -> Self {
        Self::Binary {
            bytes: Buffer::Borrowed(bytes),
            start,
            offset,
        }
    }
    fn word(&self, ptr: &BigUint) -> Result<BigUint, Error> {
        match self {
            Self::Sparse(m) => {
                let remainder = (ptr % 32u8).to_u8().expect("remainder below 32");
                if remainder != 0 {
                    return Err(Error::NativeBadAlignment(remainder));
                }
                Ok(m.words().get(ptr).copied().unwrap_or(Word::ZERO).unsigned())
            }
            Self::Binary {
                bytes,
                start,
                offset,
            } => {
                let range = range(ptr, &32u8.into(), offset, bytes.bytes().len() - start)?;
                Ok(BigUint::from_bytes_be(
                    &bytes.bytes()[start + range.start..start + range.end],
                ))
            }
        }
    }
    fn chunk(&self, ptr: &BigUint, size: &BigUint, b: &mut Budget) -> Result<Vec<u8>, Error> {
        match self {
            Self::Binary {
                bytes,
                start,
                offset,
            } => {
                let r = range(ptr, size, offset, bytes.bytes().len() - start)?;
                b.bytes(r.len())?;
                b.work(r.len().div_ceil(32))?;
                Ok(bytes.bytes()[start + r.start..start + r.end].to_vec())
            }
            Self::Sparse(_) => {
                for n in [ptr, size] {
                    let remainder = (n % 32u8).to_u8().expect("remainder below 32");
                    if remainder != 0 {
                        return Err(Error::NativeBadAlignment(remainder));
                    }
                }
                let n = size.to_usize().ok_or(Error::ResourceLimit)?;
                b.bytes(n)?;
                b.work(n / 32)?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n / 32 {
                    out.extend(Word::from_unsigned(&self.word(&(ptr + i * 32))?).0);
                }
                Ok(out)
            }
        }
    }
    fn len(&self) -> Result<usize, Error> {
        match self {
            Self::Binary { bytes, start, .. } => Ok(bytes.bytes().len() - start),
            Self::Sparse(_) => Err(Error::InvalidValue),
        }
    }
}
fn range(
    ptr: &BigUint,
    size: &BigUint,
    offset: &BigUint,
    len: usize,
) -> Result<std::ops::Range<usize>, Error> {
    if ptr < offset {
        return Err(Error::AddressBeforeOffset);
    }
    let from = ptr - offset;
    let end = &from + size;
    if end > len.into() {
        return Err(Error::InvalidValue);
    }
    Ok(from.to_usize().ok_or(Error::InvalidValue)?..end.to_usize().ok_or(Error::InvalidValue)?)
}
#[derive(Clone)]
enum Plan {
    Native(TypeRef),
    Word,
    Rep,
    RepList,
    RepLists,
}
enum Task {
    Visit {
        plan: Plan,
        ptr: BigUint,
        dest: usize,
        depth: usize,
    },
    Leave(BigUint),
    Map(Box<MapWork>),
}
struct MapWork {
    map: Map,
    remaining: Option<IntoIter<Bytes, Option<Bytes>>>,
    done: Entries,
    id: BigUint,
    dest: usize,
    depth: usize,
    value_type: TypeRef,
}
struct Frame<'a> {
    source: Source<'a>,
    out: Vec<u8>,
    pointer: BigUint,
    offset: BigUint,
    maximum: Option<BigUint>,
    active: BTreeSet<BigUint>,
    tasks: Vec<Task>,
    pending: Option<(Box<MapWork>, Bytes)>,
}
impl<'a> Frame<'a> {
    fn new(
        source: Source<'a>,
        ty: TypeRef,
        ptr: BigUint,
        offset: BigUint,
        maximum: Option<BigUint>,
        depth: usize,
        b: &mut Budget,
    ) -> Result<Self, Error> {
        b.bytes(32 + std::mem::size_of::<Self>() + std::mem::size_of::<Task>())?;
        Ok(Self {
            source,
            out: vec![0; 32],
            pointer: BigUint::zero(),
            offset,
            maximum,
            active: BTreeSet::new(),
            tasks: vec![Task::Visit {
                plan: Plan::Native(ty),
                ptr,
                dest: 0,
                depth,
            }],
            pending: None,
        })
    }
    fn size(&self, size: &BigUint) -> Result<(), Error> {
        if self
            .maximum
            .as_ref()
            .is_some_and(|m| BigUint::from(self.out.len() - 32) + size > *m)
        {
            return Err(Error::SizeLimit);
        }
        Ok(())
    }
    fn allocate(&mut self, n: usize, b: &mut Budget) -> Result<usize, Error> {
        self.size(&n.into())?;
        b.bytes(n)?;
        b.work(n.div_ceil(32))?;
        let base = self.out.len();
        self.out
            .resize(base.checked_add(n).ok_or(Error::ResourceLimit)?, 0);
        Ok(base)
    }
    fn set(&mut self, dest: usize, ptr: &BigUint) {
        if dest == 0 {
            self.pointer = ptr.clone();
        }
        self.out[dest..dest + 32].copy_from_slice(&Word::from_unsigned(ptr).0);
    }
    fn point(&mut self, dest: usize, base: usize) {
        self.set(dest, &(&self.offset + (base - 32)));
    }
}
fn map_from_binary(
    source: &Source<'_>,
    ptr: &BigUint,
    key_type: TypeRef,
    value_type: TypeRef,
    b: &mut Budget,
) -> Result<Map, Error> {
    let count = source
        .word(ptr)?
        .min((source.len()? / 64).into())
        .to_usize()
        .ok_or(Error::ResourceLimit)?;
    b.work(count)?;
    let mut data = Entries::new();
    let mut pos = ptr + 32u8;
    for _ in 0..count {
        let n = source.word(&pos)?;
        pos += 32u8;
        let key = source.chunk(&pos, &n, b)?;
        pos += n;
        let n = source.word(&pos)?;
        pos += 32u8;
        let value = source.chunk(&pos, &n, b)?;
        pos += n;
        b.bytes(key.len() + value.len())?;
        data.insert(Arc::from(key), Some(Arc::from(value)));
    }
    Ok(Map {
        key_type,
        value_type,
        parent: None,
        size: data.len().into(),
        data: Data::Local(data),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    ty: &TypeRef,
    ptr: BigUint,
    source: Source<'_>,
    input_maps: &Maps,
    store: &Store,
    input: Format,
    output: Format,
    profile: MapSerialization,
    offset: BigUint,
    maximum: Option<BigUint>,
    b: &mut Budget,
) -> Result<Heap, Error> {
    let mut result_maps = Maps {
        next_id: input_maps.next_id.clone(),
        ..Maps::default()
    };
    let mut frames = vec![Frame::new(source, ty.clone(), ptr, offset, maximum, 0, b)?];
    while let Some(mut frame) = frames.pop() {
        b.work(1)?;
        let Some(task) = frame.tasks.pop() else {
            if let Some(parent) = frames.last_mut() {
                let (mut work, key) = parent.pending.take().ok_or(Error::InvalidValue)?;
                b.bytes(frame.out.len())?;
                work.done.insert(key, Some(Arc::from(frame.out)));
                parent.tasks.push(Task::Map(work));
                continue;
            }
            frame.out.drain(..32);
            return Ok(Heap {
                pointer: frame.pointer,
                offset: frame.offset,
                memory: frame.out,
                maps: result_maps,
            });
        };
        let (plan, ptr, dest, depth) = match task {
            Task::Leave(ptr) => {
                frame.active.remove(&ptr);
                frames.push(frame);
                continue;
            }
            Task::Map(mut work) => {
                if let Some((key, value)) = work.remaining.as_mut().and_then(Iterator::next) {
                    if let Some(value) = value {
                        let ptr =
                            BigUint::from_bytes_be(value.get(..32).ok_or(Error::FunctionClause)?);
                        let child = Frame::new(
                            Source::Binary {
                                bytes: Buffer::Shared(value),
                                start: 32,
                                offset: 32u8.into(),
                            },
                            work.value_type.clone(),
                            ptr,
                            32u8.into(),
                            None,
                            work.depth + 1,
                            b,
                        )?;
                        frame.pending = Some((work, key));
                        frames.push(frame);
                        frames.push(child);
                    } else {
                        work.done.insert(key, None);
                        frame.tasks.push(Task::Map(work));
                        frames.push(frame);
                    }
                    continue;
                }
                if work.remaining.is_some() {
                    work.map.data = Data::Local(work.done);
                }
                if output == Format::Binary && profile == MapSerialization::Legacy {
                    work.map = maps::flatten_inner(&work.id, &work.map, store, b)?;
                }
                match output {
                    Format::Heap => {
                        let id = if input == Format::Binary {
                            let id = result_maps.next_id.clone();
                            result_maps.next_id += 1u8;
                            id
                        } else {
                            work.id
                        };
                        frame.set(work.dest, &id);
                        maps::reserve_map(&work.map, b)?;
                        result_maps.entries.insert(id, work.map);
                    }
                    Format::Binary => {
                        let Data::Local(data) = work.map.data else {
                            return Err(Error::InvalidValue);
                        };
                        let mut n = 32usize;
                        for (k, v) in &data {
                            let v = v.as_ref().ok_or(Error::InvalidValue)?;
                            n = n
                                .checked_add(64)
                                .and_then(|n| n.checked_add(k.len()))
                                .and_then(|n| n.checked_add(v.len()))
                                .ok_or(Error::ResourceLimit)?;
                        }
                        let base = frame.allocate(n, b)?;
                        frame.point(work.dest, base);
                        frame.set(base, &data.len().into());
                        let mut pos = base + 32;
                        for (k, v) in data {
                            let v = v.ok_or(Error::InvalidValue)?;
                            frame.set(pos, &k.len().into());
                            pos += 32;
                            frame.out[pos..pos + k.len()].copy_from_slice(&k);
                            pos += k.len();
                            frame.set(pos, &v.len().into());
                            pos += 32;
                            frame.out[pos..pos + v.len()].copy_from_slice(&v);
                            pos += v.len();
                        }
                    }
                }
                frames.push(frame);
                continue;
            }
            Task::Visit {
                plan,
                ptr,
                dest,
                depth,
            } => (plan, ptr, dest, depth),
        };
        if depth > b.depth {
            return Err(Error::ResourceLimit);
        }
        let fields: Vec<Plan> = match &plan {
            Plan::Word => {
                frame.set(dest, &ptr);
                frames.push(frame);
                continue;
            }
            Plan::Native(t) => match t.as_ref() {
                Type::Function | Type::ContractBytearray => return Err(Error::FunctionClause),
                Type::Word => {
                    frame.set(dest, &ptr);
                    frames.push(frame);
                    continue;
                }
                Type::String => {
                    let size = (frame.source.word(&ptr)? + 31u8) / 32u8 * 32u8 + 32u8;
                    frame.size(&size)?;
                    let bytes = frame.source.chunk(&ptr, &size, b)?;
                    let base = frame.allocate(bytes.len(), b)?;
                    frame.out[base..].copy_from_slice(&bytes);
                    frame.point(dest, base);
                    frames.push(frame);
                    continue;
                }
                Type::Map(key_type, value_type) => {
                    let mut map = match input {
                        Format::Binary => map_from_binary(
                            &frame.source,
                            &ptr,
                            key_type.clone(),
                            value_type.clone(),
                            b,
                        )?,
                        Format::Heap => {
                            let m = input_maps.get_map(&ptr)?;
                            maps::reserve_map(m, b)?;
                            m.clone()
                        }
                    };
                    if output == Format::Binary && profile == MapSerialization::Corrected {
                        map = maps::flatten_inner(&ptr, &map, store, b)?;
                    }
                    let remaining = match std::mem::replace(&mut map.data, Data::Stored) {
                        Data::Stored => None,
                        Data::Local(d) => Some(d.into_iter()),
                    };
                    b.bytes(std::mem::size_of::<MapWork>())?;
                    frame.tasks.push(Task::Map(Box::new(MapWork {
                        map,
                        remaining,
                        done: Entries::new(),
                        id: ptr,
                        dest,
                        depth,
                        value_type: value_type.clone(),
                    })));
                    frames.push(frame);
                    continue;
                }
                Type::List(item) => {
                    if ptr == Word([255; 32]).unsigned() {
                        frame.set(dest, &ptr);
                        frames.push(frame);
                        continue;
                    }
                    vec![Plan::Native(item.clone()), plan.clone()]
                }
                Type::Tuple(ts) => {
                    if ts.is_empty() {
                        frame.set(dest, &BigUint::zero());
                        frames.push(frame);
                        continue;
                    }
                    b.bytes(
                        ts.len()
                            .checked_mul(std::mem::size_of::<Plan>())
                            .ok_or(Error::ResourceLimit)?,
                    )?;
                    ts.iter().cloned().map(Plan::Native).collect()
                }
                Type::Variant(cs) => {
                    let tag = frame
                        .source
                        .word(&ptr)?
                        .to_usize()
                        .ok_or(Error::FunctionClause)?;
                    let ts = cs.get(tag).ok_or(Error::FunctionClause)?;
                    b.bytes(
                        ts.len()
                            .checked_add(1)
                            .and_then(|n| n.checked_mul(std::mem::size_of::<Plan>()))
                            .ok_or(Error::ResourceLimit)?,
                    )?;
                    std::iter::once(Plan::Word)
                        .chain(ts.iter().cloned().map(Plan::Native))
                        .collect()
                }
                Type::TypeRep => rep_fields(&frame.source, &ptr)?,
            },
            Plan::Rep => rep_fields(&frame.source, &ptr)?,
            Plan::RepList | Plan::RepLists => {
                if ptr == Word([255; 32]).unsigned() {
                    frame.set(dest, &ptr);
                    frames.push(frame);
                    continue;
                }
                vec![
                    if matches!(plan, Plan::RepList) {
                        Plan::Rep
                    } else {
                        Plan::RepList
                    },
                    plan.clone(),
                ]
            }
        };
        if frame.active.contains(&ptr) {
            return Err(Error::CircularReferences);
        }
        // Native tuple chunks are fetched before conversion. Binary views allow
        // unaligned reads; sparse VM memory requires word alignment.
        let cell_size = fields.len().checked_mul(32).ok_or(Error::ResourceLimit)?;
        let bytes = frame.source.chunk(&ptr, &cell_size.into(), b)?;
        b.bytes(32)?;
        b.bytes(
            fields
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(std::mem::size_of::<Task>()))
                .ok_or(Error::ResourceLimit)?,
        )?;
        let base = frame.allocate(cell_size, b)?;
        frame.point(dest, base);
        frame.active.insert(ptr.clone());
        frame.tasks.push(Task::Leave(ptr));
        for (i, plan) in fields.into_iter().enumerate().rev() {
            frame.tasks.push(Task::Visit {
                plan,
                ptr: BigUint::from_bytes_be(&bytes[i * 32..i * 32 + 32]),
                dest: base + i * 32,
                depth: depth + 1,
            });
        }
        frames.push(frame);
    }
    Err(Error::InvalidValue)
}
fn rep_fields(source: &Source<'_>, ptr: &BigUint) -> Result<Vec<Plan>, Error> {
    Ok(
        match source.word(ptr)?.to_u8().ok_or(Error::InvalidValue)? {
            0 | 1 | 5 => vec![Plan::Word],
            2 => vec![Plan::Word, Plan::Rep],
            3 => vec![Plan::Word, Plan::RepList],
            4 => vec![Plan::Word, Plan::RepLists],
            6 => vec![Plan::Word, Plan::Rep, Plan::Rep],
            _ => return Err(Error::InvalidValue),
        },
    )
}
