// Iterative aeb_heap type-metadata encoding. ISC: repository LICENSE.
use super::{Budget, Error, Type, TypeRef, Word};

enum Task<'a> {
    Type(&'a TypeRef, usize, usize),
    Types(&'a [TypeRef], usize, usize),
    Cases(&'a [Vec<TypeRef>], usize, usize),
}
fn allocate(out: &mut Vec<u8>, n: usize, b: &mut Budget) -> Result<usize, Error> {
    b.bytes(n)?;
    let base = out.len();
    out.resize(base.checked_add(n).ok_or(Error::ResourceLimit)?, 0);
    Ok(base)
}
fn set(out: &mut [u8], pos: usize, n: usize) {
    out[pos..pos + 32].copy_from_slice(&Word::from_unsigned(&n.into()).0);
}
pub(super) fn encode(types: &[TypeRef], pair: bool, b: &mut Budget) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    allocate(&mut out, 32, b)?;
    let mut tasks = Vec::new();
    if pair {
        assert_eq!(types.len(), 2);
        allocate(&mut out, 64, b)?;
        set(&mut out, 0, 32);
        tasks.push(Task::Type(&types[1], 64, 0));
        tasks.push(Task::Type(&types[0], 32, 0));
    } else {
        assert_eq!(types.len(), 1);
        tasks.push(Task::Type(&types[0], 0, 0));
    }
    while let Some(task) = tasks.pop() {
        b.work(1)?;
        b.bytes(3 * std::mem::size_of::<Task<'_>>())?;
        let depth = match task {
            Task::Type(_, _, d) | Task::Types(_, _, d) | Task::Cases(_, _, d) => d,
        };
        if depth > b.depth {
            return Err(Error::ResourceLimit);
        }
        match task {
            Task::Type(t, dest, d) => {
                let tag = match t.as_ref() {
                    Type::Word => 0,
                    Type::String => 1,
                    Type::List(_) => 2,
                    Type::Tuple(_) => 3,
                    Type::Variant(_) => 4,
                    Type::TypeRep => 5,
                    Type::Map(_, _) => 6,
                    Type::Function => 7,
                    Type::ContractBytearray => 8,
                };
                let n = match tag {
                    2..=4 => 64,
                    6 => 96,
                    _ => 32,
                };
                let base = allocate(&mut out, n, b)?;
                set(&mut out, dest, base);
                set(&mut out, base, tag);
                match t.as_ref() {
                    Type::List(t) => tasks.push(Task::Type(t, base + 32, d + 1)),
                    Type::Tuple(ts) => tasks.push(Task::Types(ts, base + 32, d + 1)),
                    Type::Variant(cs) => tasks.push(Task::Cases(cs, base + 32, d + 1)),
                    Type::Map(k, v) => {
                        tasks.push(Task::Type(v, base + 64, d + 1));
                        tasks.push(Task::Type(k, base + 32, d + 1));
                    }
                    _ => {}
                }
            }
            Task::Types(ts, dest, d) => {
                if let Some((head, tail)) = ts.split_first() {
                    let base = allocate(&mut out, 64, b)?;
                    set(&mut out, dest, base);
                    tasks.push(Task::Types(tail, base + 32, d + 1));
                    tasks.push(Task::Type(head, base, d + 1));
                } else {
                    out[dest..dest + 32].fill(255);
                }
            }
            Task::Cases(cs, dest, d) => {
                if let Some((head, tail)) = cs.split_first() {
                    let base = allocate(&mut out, 64, b)?;
                    set(&mut out, dest, base);
                    tasks.push(Task::Cases(tail, base + 32, d + 1));
                    tasks.push(Task::Types(head, base, d + 1));
                } else {
                    out[dest..dest + 32].fill(255);
                }
            }
        }
    }
    Ok(out)
}
