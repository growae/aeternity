// aevm_eeevm_state call transport, fa2a6d7; aeb_primops, 009e036.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    execution::{self, Budget, Limits, State, Stop},
    gas, heap,
    opcode::Version,
    runtime::{self, Fault, Native},
    runtime_heap::{self, Error, MapContext, MapSerialization, Type, TypeRef},
    word::Word,
};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use std::sync::Arc;

// aeb_heap decoding of a tuple of words, including unaligned binary pointers.
pub(crate) fn words(data: &[u8], count: usize, b: &mut Budget) -> Result<Vec<Word>, Fault> {
    b.work(count)?;
    b.bytes(count.checked_mul(32).ok_or(Stop::ResourceLimit)?)?;
    let decode = || {
        let header = data.get(..32).ok_or(heap::Error::BinaryTooShort)?;
        let start = BigUint::from_bytes_be(header)
            .to_usize()
            .ok_or(heap::Error::Bounds)?;
        let size = count.checked_mul(32).ok_or(heap::Error::Bounds)?;
        let end = start.checked_add(size).ok_or(heap::Error::Bounds)?;
        let body = data.get(start..end).ok_or(heap::Error::Bounds)?;
        Ok(body
            .chunks_exact(32)
            .map(|v| Word(v.try_into().expect("word")))
            .collect())
    };
    decode().map_err(|reason| {
        if let Err(e) = b.bytes(data.len()) {
            return Fault::from(e);
        }
        Fault::Native(Native::BadPrimopData {
            reason,
            data: data.to_vec(),
        })
    })
}
pub(crate) fn is_local_inner(data: &[u8], b: &mut Budget) -> Result<bool, Fault> {
    let op = words(data, 1, b)?[0].unsigned();
    Ok(op >= 300u16.into() && op < 500u16.into())
}
pub fn is_local_primop(data: &[u8], limits: Limits) -> Result<bool, Fault> {
    is_local_inner(data, &mut Budget::new(limits))
}
pub struct Return<'a> {
    pub target: Word,
    pub input: &'a [u8],
    pub output_offset: Word,
    pub output_size: Word,
    pub data: &'a [u8],
    pub ty: Option<&'a TypeRef>,
}
pub(crate) fn conversion_error(error: Error) -> Fault {
    match error {
        Error::ResourceLimit | Error::Stop(_) => error.into(),
        _ => Fault::Native(Native::OutOfGas),
    }
}
pub(crate) fn return_inner(
    state: &State,
    result: Return<'_>,
    b: &mut Budget,
) -> Result<(BigUint, State), Fault> {
    let mut s = execution::snapshot(state, b, true)?;
    if s.version == Version::Solidity1 {
        let address = result.output_offset.unsigned();
        b.memory(&s.memory, &address, result.data.len())?;
        b.work(result.data.len().div_ceil(32))?;
        s.memory.write_area(&address, result.data);
        return Ok((1u8.into(), s));
    }
    let offset = s.memory.size_in_words() * 32u8;
    let heap = if result.target.is_zero() && is_local_inner(result.input, b)? {
        let Some(header) = result.data.get(..32) else {
            b.bytes(result.data.len())?;
            return Err(Fault::Native(Native::BadMatchBinary(result.data.to_vec())));
        };
        let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)?
            * if s.version == Version::Sophia1 {
                1u8
            } else {
                32u8
            };
        let ty = result.ty.ok_or(Fault::Native(Native::OutOfGas))?;
        let h = runtime_heap::copy_binary(
            ty,
            BigUint::from_bytes_be(header),
            &result.data[32..],
            &s.maps,
            32u8.into(),
            offset,
            Some(maximum),
            b,
        )
        .map_err(conversion_error)?;
        // The original computes but discards this charge at this boundary.
        gas::memory(&(h.memory.len() / 32).into(), &s.gas_table, s.float_profile)?;
        h
    } else {
        if !result.data.is_empty() && result.data.len() < 32 {
            return Err(Fault::Native(Native::Heap(Error::NativeFunctionClause)));
        }
        let ty = result.ty.ok_or(Fault::Native(Native::OutOfGas))?;
        runtime_heap::binary_to_heap_inner(ty, result.data, s.maps.next_id.clone(), offset, b)
            .map_err(|e| {
                if e == Error::NativeFunctionClause {
                    e.into()
                } else {
                    conversion_error(e)
                }
            })?
    };
    let ptr = runtime::write_heap(&mut s, heap, b)?;
    Ok((ptr, s))
}
pub fn return_result(
    state: &State,
    result: Return<'_>,
    limits: Limits,
) -> Result<(BigUint, State), Fault> {
    return_inner(state, result, &mut Budget::new(limits))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractVersion {
    pub vm: Version,
    pub abi: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeError {
    NoSuchContract(Word),
    WrongVmVersion { vm: u16, abi: u16 },
    UnknownFunction,
    BadTypeData,
}
pub type FunctionTypes<E> = Result<Result<(TypeRef, TypeRef), TypeError>, Fault<E>>;
pub trait InputHost {
    type Error;
    fn oracle_format(
        &self,
        _oracle: Word,
        response: bool,
        _b: &mut Budget,
    ) -> crate::primops::HostResult<TypeRef, Self::Error> {
        Err(Stop::UnimplementedPrimop(Word::from(if response { 102 } else { 101 })).into())
    }
    fn function_types(
        &self,
        target: Word,
        version: ContractVersion,
        function: Word,
        budget: &mut Budget,
    ) -> FunctionTypes<Self::Error>;
}
pub struct Input {
    pub target: Word,
    pub offset: Word,
    pub size: Word,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub data: Vec<u8>,
    pub output_type: Option<TypeRef>,
    pub state: Box<State>,
}
fn argument_word<E>(s: &State, offset: &BigUint) -> Result<Word, Fault<E>> {
    if offset % 32u8 != 0u8.into() {
        return Err(Fault::Native(Native::OutOfGas));
    }
    Ok(s.memory.words().get(offset).copied().unwrap_or(Word::ZERO))
}
fn primitive_types<H: InputHost>(
    op: Word,
    s: &State,
    offset: &BigUint,
    host: &H,
    b: &mut Budget,
) -> Result<(Vec<TypeRef>, TypeRef), Fault<H::Error>> {
    let word = Arc::new(Type::Word);
    let unit = || Arc::new(Type::Tuple(vec![]));
    let sign = || Arc::new(Type::Tuple(vec![word.clone(); 2]));
    let ttl = || Arc::new(Type::Variant(vec![vec![word.clone()]; 2]));
    let types = match op.unsigned().to_u16() {
        Some(100) => (
            vec![
                word.clone(),
                sign(),
                word.clone(),
                ttl(),
                Arc::new(Type::TypeRep),
                Arc::new(Type::TypeRep),
            ],
            word,
        ),
        Some(103) => (vec![word.clone(), sign(), ttl()], unit()),
        Some(106) => (vec![word.clone()], word),
        Some(110) => (
            vec![
                word.clone(),
                Arc::new(Type::TypeRep),
                Arc::new(Type::TypeRep),
            ],
            word,
        ),
        Some(111) => (
            vec![
                word.clone(),
                word.clone(),
                Arc::new(Type::TypeRep),
                Arc::new(Type::TypeRep),
            ],
            word,
        ),
        Some(id @ (101 | 102 | 104 | 105)) => {
            let oracle = argument_word(s, &(offset + 32u8))?;
            match host.oracle_format(oracle, matches!(id, 102 | 104), b)? {
                Err(_) => (vec![], unit()),
                Ok(t) => match id {
                    101 => (vec![word.clone(), t, ttl(), ttl()], word),
                    102 => (vec![word.clone(), word.clone(), sign(), t], unit()),
                    104 => (
                        vec![word.clone(); 2],
                        Arc::new(Type::Variant(vec![vec![], vec![t]])),
                    ),
                    _ => (vec![word; 2], t),
                },
            }
        }
        Some(200) => {
            let t = Arc::new(Type::Tuple(vec![
                word.clone(),
                word.clone(),
                word.clone(),
                Arc::new(Type::TypeRep),
            ]));
            let bytes = runtime_heap::serialize_limit(
                &t,
                Word::from_unsigned(offset),
                &s.memory,
                MapContext {
                    maps: &s.maps,
                    store: &s.map_store,
                    profile: if s.version == Version::Sophia4 {
                        MapSerialization::Corrected
                    } else {
                        MapSerialization::Legacy
                    },
                },
                None,
                b,
            )?;
            let args = words(&bytes, 4, b).map_err(Fault::widen)?;
            let out = crate::store::parse_type(
                &bytes,
                args[3].unsigned(),
                Some(BigUint::from_bytes_be(&bytes[..32])),
                b,
            )?;
            (
                vec![
                    Arc::new(Type::String),
                    Arc::new(Type::String),
                    Arc::new(Type::TypeRep),
                ],
                Arc::new(Type::Variant(vec![vec![], vec![out]])),
            )
        }
        Some(201) => (vec![word.clone(), word.clone(), sign()], unit()),
        Some(202) => {
            let mut ts = vec![word.clone(), Arc::new(Type::String), word.clone()];
            if s.version == Version::Sophia4 {
                ts.push(word.clone());
            }
            ts.push(sign());
            (ts, unit())
        }
        Some(204 | 205) => {
            let mut ts = vec![word.clone()];
            if op == Word::from(204) {
                ts.push(word.clone());
            }
            ts.push(if s.version == Version::Sophia4 {
                Arc::new(Type::String)
            } else {
                word.clone()
            });
            ts.push(sign());
            (ts, unit())
        }
        Some(1) => (vec![word], Arc::new(Type::Tuple(vec![]))),
        Some(600 | 601 | 610) => (vec![word.clone()], word),
        Some(600..=699) => (vec![], Arc::new(Type::Tuple(vec![]))),
        Some(300) => {
            let rep = Arc::new(Type::TypeRep);
            (vec![rep; 2], word)
        }
        Some(302) => (vec![word.clone(); 3], word),
        Some(303) => (vec![word.clone(); 2], word),
        Some(304) => (vec![word.clone()], word),
        Some(id @ (301 | 305)) => {
            b.work(1)?;
            let map_id = argument_word(s, &(offset + 32u8))?.unsigned();
            let map = s
                .maps
                .entries
                .get(&map_id)
                .ok_or(Fault::Native(Native::MapNotFound))?;
            let output = if id == 301 {
                Type::Variant(vec![vec![], vec![map.value_type.clone()]])
            } else {
                Type::List(Arc::new(Type::Tuple(vec![
                    map.key_type.clone(),
                    map.value_type.clone(),
                ])))
            };
            (vec![word; if id == 301 { 2 } else { 1 }], Arc::new(output))
        }
        Some(306..=399) => (vec![], Arc::new(Type::Tuple(vec![]))),
        Some(400) => (
            vec![
                word.clone(),
                word.clone(),
                Arc::new(Type::Tuple(vec![word.clone(); 2])),
            ],
            word,
        ),
        Some(401..=403) => (vec![Arc::new(Type::TypeRep), word.clone()], word),
        Some(404 | 405) => (vec![Arc::new(Type::String)], word),
        Some(410) => {
            let bytes = Arc::new(Type::Tuple(vec![word.clone(); 2]));
            (vec![word.clone(), bytes.clone(), bytes], word)
        }
        Some(420) => (
            vec![
                word.clone(),
                word.clone(),
                Arc::new(Type::Tuple(vec![word.clone(); 3])),
            ],
            word,
        ),
        Some(421) => (
            vec![word.clone(), Arc::new(Type::Tuple(vec![word.clone(); 3]))],
            Arc::new(Type::Variant(vec![vec![], vec![word]])),
        ),
        Some(500) => (vec![], Arc::new(Type::Variant(vec![vec![], vec![word]]))),
        Some(400..=599) => (vec![], Arc::new(Type::Tuple(vec![]))),
        _ => (vec![], unit()),
    };
    b.bytes(256)?;
    Ok(types)
}
pub(crate) fn input_inner<H: InputHost>(
    s: &State,
    input: Input,
    host: &H,
    b: &mut Budget,
) -> Result<Prepared, Fault<H::Error>> {
    let mut next = execution::snapshot(s, b, true)?;
    if s.version == Version::Solidity1 {
        let n = input
            .size
            .unsigned()
            .to_usize()
            .ok_or(Stop::ResourceLimit)?;
        b.bytes(n)?;
        b.work(n.div_ceil(32))?;
        let data = next.memory.area(&input.offset.unsigned(), n, n)?;
        return Ok(Prepared {
            data,
            output_type: None,
            state: Box::new(next),
        });
    }
    let offset = input.offset.unsigned();
    let (types, out) = if input.target.is_zero() {
        let op = argument_word(s, &offset)?;
        let (types, out) = primitive_types(op, s, &offset, host, b)?;
        if matches!(op.unsigned().to_u16(), Some(101 | 102 | 104 | 105 | 200)) {
            let arg = crate::store::encode_type(&Arc::new(Type::Tuple(types.clone())), b)?;
            let ret = crate::store::encode_type(&out, b)?;
            b.bytes(
                arg.len()
                    .checked_add(ret.len())
                    .and_then(|n| n.checked_add(32))
                    .ok_or(Stop::ResourceLimit)?,
            )?;
            let mut bytes = op.unsigned().to_bytes_be();
            bytes.extend(arg);
            bytes.extend(ret);
            if ae_crypto::blake2b_256(&bytes).0 != input.size.0 {
                return Err(Fault::Native(Native::OutOfGas));
            }
        }
        (types, out)
    } else {
        if s.auth_tx_hash.is_some() {
            return Err(Fault::Native(Native::OutOfGas));
        }
        let function = argument_word(s, &offset)?;
        let (argument, out) = host
            .function_types(
                input.target,
                ContractVersion {
                    vm: s.version,
                    abi: s.abi_version,
                },
                function,
                b,
            )?
            .map_err(|_| Fault::Native(Native::OutOfGas))?;
        (vec![argument], out)
    };
    b.bytes(
        (types.len() + 1)
            .checked_mul(std::mem::size_of::<TypeRef>())
            .ok_or(Stop::ResourceLimit)?,
    )?;
    let mut full = vec![Arc::new(Type::Word)];
    full.extend(types);
    let ty = Arc::new(Type::Tuple(full));
    runtime_heap::check_type(&ty, b)?;
    let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)? * 32u8;
    let profile = if s.version == Version::Sophia4 {
        MapSerialization::Corrected
    } else {
        MapSerialization::Legacy
    };
    let data = runtime_heap::serialize(
        &ty,
        input.offset,
        &s.memory,
        MapContext {
            maps: &s.maps,
            store: &s.map_store,
            profile,
        },
        &maximum,
        b,
    )
    .map_err(|e| conversion_error(e).widen())?;
    let charge = gas::memory(&(data.len() / 32).into(), &s.gas_table, s.float_profile)?;
    if charge > s.gas {
        return Err(Fault::Native(Native::OutOfGas));
    }
    next.gas -= charge;
    Ok(Prepared {
        data,
        output_type: Some(out),
        state: Box::new(next),
    })
}
pub fn prepare_input<H: InputHost>(
    s: &State,
    input: Input,
    host: &H,
    limits: Limits,
) -> Result<Prepared, Fault<H::Error>> {
    input_inner(s, input, host, &mut Budget::new(limits))
}
