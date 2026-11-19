// aeb_fate_code.erl, aebytecode 009e036.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    Error, Limits, encoding,
    maps::{MapOrder, Traversal},
    native::{self, Term as NativeTerm},
    opcodes::Opcode,
    types::Type,
    value::Value,
};
use ae_codec::rlp::{self, Rlp};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::collections::BTreeMap;

pub const INIT_ID: [u8; 4] = [68, 214, 68, 31];
pub fn symbol_identifier(name: &[u8]) -> [u8; 4] {
    ae_crypto::blake2b_256(name).0[..4]
        .try_into()
        .expect("four bytes")
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeError {
    Data(Error),
    InvalidContainer,
    InvalidFunctionId,
    InvalidAttributes,
    InvalidSignature,
    NoncontiguousBlocks,
    EmptyBlock,
    WrongArity,
    WrongTerminator,
    InvalidModifier,
    CodeWithoutFunction,
    UnknownOpcode(u8),
    SymbolCollision,
}
impl From<Error> for CodeError {
    fn from(e: Error) -> Self {
        Self::Data(e)
    }
}
pub type NativeFailure = native::Failure<CodeError>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attribute {
    Payable,
    Private,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    Stack,
    Argument(Value),
    Variable(Value),
    Immediate(Value),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instruction {
    pub opcode: Opcode,
    pub operands: Vec<Operand>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Function {
    pub attributes: Vec<Attribute>,
    pub arguments: Vec<Type>,
    pub returns: Type,
    pub blocks: BTreeMap<usize, Vec<Instruction>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Code {
    pub functions: BTreeMap<Vec<u8>, Function>,
    // FATE_MAP_VALUE / MAKE_FATE_MAP are identity macros in the reference.
    // Decoding and serialization admit non-maps here; map operations do not.
    pub symbols: Value,
    pub annotations: Value,
}
impl Default for Code {
    fn default() -> Self {
        Self {
            functions: BTreeMap::new(),
            symbols: Value::Map(BTreeMap::new()),
            annotations: Value::Map(BTreeMap::new()),
        }
    }
}
impl Code {
    pub fn insert_symbol(&mut self, name: &[u8]) -> Result<[u8; 4], CodeError> {
        let id = symbol_identifier(name);
        let key = Value::String(id.to_vec());
        let value = Value::String(name.to_vec());
        let Value::Map(symbols) = &mut self.symbols else {
            return Err(CodeError::InvalidContainer);
        };
        if let Some(existing) = symbols.get(&key)
            && existing != &value
        {
            return Err(CodeError::SymbolCollision);
        }
        symbols.insert(key, value);
        Ok(id)
    }
    pub fn insert_function(
        &mut self,
        name: &[u8],
        function: Function,
    ) -> Result<[u8; 4], CodeError> {
        let id = self.insert_symbol(name)?;
        self.functions.insert(id.to_vec(), function);
        Ok(id)
    }
    pub fn insert_comment(&mut self, line: Value, comment: &[u8]) -> Result<(), CodeError> {
        let Value::Map(annotations) = &mut self.annotations else {
            return Err(CodeError::InvalidContainer);
        };
        annotations.insert(
            Value::Tuple(vec![Value::String(b"comment".to_vec()), line]),
            Value::String(comment.to_vec()),
        );
        Ok(())
    }
    pub fn strip_init(&mut self) -> Result<(), CodeError> {
        let Value::Map(symbols) = &mut self.symbols else {
            return Err(CodeError::InvalidContainer);
        };
        self.functions.remove(INIT_ID.as_slice());
        symbols.remove(&Value::String(INIT_ID.to_vec()));
        Ok(())
    }
    pub fn validate(&self) -> Result<(), CodeError> {
        for (id, f) in &self.functions {
            if id.len() != 4 {
                return Err(CodeError::InvalidFunctionId);
            }
            for (expected, (label, block)) in f.blocks.iter().enumerate() {
                if *label != expected {
                    return Err(CodeError::NoncontiguousBlocks);
                }
                if block.is_empty() {
                    return Err(CodeError::EmptyBlock);
                }
                for (i, instruction) in block.iter().enumerate() {
                    let info = instruction.opcode.info();
                    if instruction.operands.len() != info.arity {
                        return Err(CodeError::WrongArity);
                    }
                    if info.end_block != (i + 1 == block.len()) {
                        return Err(CodeError::WrongTerminator);
                    }
                }
            }
        }
        Ok(())
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, CodeError> {
        self.validate()?;
        self.encode_inner(&mut Meter::new(limits))
    }
    // Native preparation re-encodes values admitted by the bytecode decoder.
    pub fn encode_native(
        &self,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Vec<u8>, NativeFailure> {
        self.encode_native_bounded(order, limits)
            .map(|(bytes, _)| bytes)
    }
    pub fn encode_native_bounded(
        &self,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<(Vec<u8>, Limits), NativeFailure> {
        let mut meter = Meter::new(limits);
        let result = self
            .validate_native(order, &mut meter)
            .and_then(|_| self.encode_inner(&mut meter));
        result
            .map(|bytes| {
                let remaining = Limits {
                    max_nodes: meter.remaining(),
                    max_bytes: limits.max_bytes - bytes.len(),
                    ..limits
                };
                (bytes, remaining)
            })
            .map_err(|e| meter.failure(e))
    }
    fn encode_inner(&self, meter: &mut Meter) -> Result<Vec<u8>, CodeError> {
        let limits = meter.limits;
        let mut functions = vec![];
        for (id, f) in &self.functions {
            meter.nodes(1)?;
            append(&mut functions, &[0xfe], limits)?;
            append(&mut functions, id, limits)?;
            let mut attributes = BigInt::from(0);
            for attr in &f.attributes {
                meter.nodes(1)?;
                attributes += match attr {
                    Attribute::Private => 1,
                    Attribute::Payable => 2,
                };
            }
            append(
                &mut functions,
                &meter.encode_value(&Value::Integer(attributes))?,
                limits,
            )?;
            append(
                &mut functions,
                &meter.encode_arguments(&f.arguments)?,
                limits,
            )?;
            append(&mut functions, &meter.encode_type(&f.returns)?, limits)?;
            for block in f.blocks.values() {
                meter.nodes(1)?;
                for op in block {
                    meter.nodes(1)?;
                    append(&mut functions, &[op.opcode as u8], limits)?;
                    if !op.operands.is_empty() {
                        let mut modifiers = 0u16;
                        for (i, arg) in op.operands.iter().enumerate() {
                            modifiers |= (match arg {
                                Operand::Stack => 0,
                                Operand::Argument(_) => 1,
                                Operand::Variable(_) => 2,
                                Operand::Immediate(_) => 3,
                            }) << (2 * i);
                        }
                        if op.operands.len() > 4 {
                            append(&mut functions, &modifiers.to_be_bytes(), limits)?;
                        } else {
                            append(&mut functions, &[modifiers as u8], limits)?;
                        }
                        for arg in &op.operands {
                            match arg {
                                Operand::Stack => meter.nodes(1)?,
                                Operand::Argument(v)
                                | Operand::Variable(v)
                                | Operand::Immediate(v) => {
                                    append(&mut functions, &meter.encode_value(v)?, limits)?
                                }
                            }
                        }
                    }
                }
            }
        }
        let symbols = meter.encode_value(&self.symbols)?;
        let annotations = meter.encode_value(&self.annotations)?;
        let mut bytes = vec![];
        for section in [&functions, &symbols, &annotations] {
            append(&mut bytes, &rlp::encode(&Rlp::Bytes(section)), limits)?;
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], limits: Limits) -> Result<Self, CodeError> {
        Self::decode_bounded(bytes, limits).map(|(code, _)| code)
    }
    // Carry shared invocation work/byte budgets across code and calldata.
    pub fn decode_bounded(bytes: &[u8], limits: Limits) -> Result<(Self, Limits), CodeError> {
        Self::decode_using(bytes, limits, |code, _| code.validate()).map_err(|e| e.error)
    }
    pub fn decode_native_bounded(
        bytes: &[u8],
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<(Self, Limits), NativeFailure> {
        Self::decode_using(bytes, limits, |code, meter| {
            code.validate_native(order, meter)
        })
    }
    fn decode_using(
        bytes: &[u8],
        limits: Limits,
        validate: impl FnOnce(&Self, &mut Meter) -> Result<(), CodeError>,
    ) -> Result<(Self, Limits), NativeFailure> {
        let mut meter = Meter::new(limits);
        let result = (|| {
            if bytes.len() > limits.max_bytes {
                return Err(Error::ResourceLimit.into());
            }
            let (functions, rest) = section(bytes, &mut meter)?;
            let (symbols, rest) = section(rest, &mut meter)?;
            let (annotations, rest) = section(rest, &mut meter)?;
            if !rest.is_empty() {
                return meter.fail(
                    Error::TrailingBytes.into(),
                    NativeTerm::tagged(
                        "badmatch",
                        vec![NativeTerm::Tuple(vec![
                            encoding::rlp_term(annotations),
                            NativeTerm::Binary(rest.to_vec()),
                        ])],
                    ),
                );
            }
            let Rlp::Bytes(functions) = functions else {
                return Err(CodeError::InvalidContainer);
            };
            let functions = decode_functions(functions, &mut meter)?;
            // Record declaration order wins over the constructor's field order.
            let symbols = meter.whole(symbols)?;
            let annotations = meter.whole(annotations)?;
            let code = Self {
                functions,
                symbols,
                annotations,
            };
            validate(&code, &mut meter)?;
            Ok((
                code,
                Limits {
                    max_nodes: meter.remaining(),
                    max_bytes: limits.max_bytes - bytes.len(),
                    ..limits
                },
            ))
        })();
        result.map_err(|e| meter.failure(e))
    }
    pub fn strip_init_native(&mut self, limits: Limits) -> Result<Limits, NativeFailure> {
        let mut meter = Meter::new(limits);
        if !matches!(&self.symbols, Value::Map(_)) {
            crate::value::check_limits(&self.symbols, limits)
                .map_err(|e| meter.failure(e.into()))?;
            meter.exception = Some(native::Exception::error(NativeTerm::tagged(
                "badmap",
                vec![NativeTerm::Data(Box::new(self.symbols.clone()))],
            )));
            return Err(meter.failure(CodeError::InvalidContainer));
        }
        meter.nodes(2).map_err(|e| meter.failure(e))?;
        self.strip_init().map_err(|e| meter.failure(e))?;
        Ok(Limits {
            max_nodes: meter.remaining(),
            ..limits
        })
    }
    fn validate_native(&self, order: &impl MapOrder, meter: &mut Meter) -> Result<(), CodeError> {
        meter.nodes(self.functions.len())?;
        let bytes = self.functions.keys().try_fold(0usize, |n, id| {
            n.checked_add(id.len()).ok_or(Error::ResourceLimit)
        })?;
        if bytes > meter.limits.max_bytes {
            return Err(Error::ResourceLimit.into());
        }
        let keys: Vec<_> = self
            .functions
            .keys()
            .map(|id| Value::String(id.clone()))
            .collect();
        let refs: Vec<_> = keys.iter().collect();
        let mut budget = crate::Budget::new(meter.limits());
        let indices = crate::maps::ordered_indices::<std::convert::Infallible>(
            &refs,
            order,
            Traversal::ToList,
            &mut budget,
        )
        .map_err(|e| {
            meter.exception = None;
            match e {
                crate::maps::MapError::Data(e) => CodeError::Data(e),
                _ => CodeError::Data(Error::InvalidOrderProfile),
            }
        })?;
        meter.nodes(budget.nodes)?;
        let functions: Vec<_> = self.functions.iter().collect();
        for i in indices {
            let (id, f) = functions[i];
            if id.len() != 4 {
                return meter.fail(
                    CodeError::InvalidFunctionId,
                    NativeTerm::tagged("illegal_function_id", vec![NativeTerm::Binary(id.clone())]),
                );
            }
            for (expected, (label, block)) in f.blocks.iter().enumerate() {
                meter.nodes(1)?;
                if *label != expected {
                    return meter.fail(
                        CodeError::NoncontiguousBlocks,
                        NativeTerm::tagged(
                            "not_contiguous_labels",
                            vec![NativeTerm::List(
                                f.blocks.keys().map(|n| NativeTerm::number(*n)).collect(),
                            )],
                        ),
                    );
                }
                if block.is_empty() {
                    return meter.fail(
                        CodeError::EmptyBlock,
                        NativeTerm::tagged("empty_code_block", vec![NativeTerm::number(*label)]),
                    );
                }
                for (n, op) in block.iter().enumerate() {
                    meter.nodes(1)?;
                    let info = op.opcode.info();
                    if op.operands.len() != info.arity {
                        return meter.fail(
                            CodeError::WrongArity,
                            NativeTerm::tagged(
                                "wrong_nr_args_opcode",
                                vec![NativeTerm::number(op.opcode as u8)],
                            ),
                        );
                    }
                    if info.end_block != (n + 1 == block.len()) {
                        return meter.fail(
                            CodeError::WrongTerminator,
                            NativeTerm::tagged(
                                "wrong_opcode_in_bb",
                                vec![NativeTerm::number(op.opcode as u8)],
                            ),
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
fn append(out: &mut Vec<u8>, bytes: &[u8], limits: Limits) -> Result<(), CodeError> {
    if out
        .len()
        .checked_add(bytes.len())
        .is_none_or(|n| n > limits.max_bytes)
    {
        return Err(Error::ResourceLimit.into());
    }
    out.extend_from_slice(bytes);
    Ok(())
}
struct Meter {
    limits: Limits,
    nodes: usize,
    exception: Option<native::Exception>,
}
impl Meter {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            nodes: 0,
            exception: Some(native::Exception::error(NativeTerm::Atom(
                "function_clause",
            ))),
        }
    }
    fn fail<T>(&mut self, error: CodeError, reason: NativeTerm) -> Result<T, CodeError> {
        self.exception = Some(native::Exception::error(reason));
        Err(error)
    }
    fn failure(&mut self, error: CodeError) -> NativeFailure {
        if error == CodeError::Data(Error::ResourceLimit) {
            return NativeFailure {
                error,
                exception: None,
            };
        }
        if self
            .exception
            .as_ref()
            .is_some_and(|e| e.reason.check_limits(self.limits()).is_err())
        {
            return NativeFailure {
                error: CodeError::Data(Error::ResourceLimit),
                exception: None,
            };
        }
        NativeFailure {
            error,
            exception: self.exception.take(),
        }
    }
    fn whole(&mut self, rlp: Rlp<'_>) -> Result<Value, CodeError> {
        let Rlp::Bytes(bytes) = rlp else {
            return Err(CodeError::InvalidContainer);
        };
        let (v, rest) = self.value(bytes)?;
        if !rest.is_empty() {
            return self.fail(
                Error::TrailingBytes.into(),
                NativeTerm::tagged(
                    "badmatch",
                    vec![NativeTerm::Tuple(vec![
                        NativeTerm::Data(Box::new(v)),
                        NativeTerm::Binary(rest.to_vec()),
                    ])],
                ),
            );
        }
        Ok(v)
    }
    fn remaining(&self) -> usize {
        self.limits.max_nodes.saturating_sub(self.nodes)
    }
    fn limits(&self) -> Limits {
        Limits {
            max_nodes: self.remaining(),
            ..self.limits
        }
    }
    fn nodes(&mut self, n: usize) -> Result<(), CodeError> {
        self.nodes = self.nodes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if self.nodes > self.limits.max_nodes {
            return Err(Error::ResourceLimit.into());
        }
        Ok(())
    }
    fn value<'a>(&mut self, bytes: &'a [u8]) -> Result<(Value, &'a [u8]), CodeError> {
        let (v, rest, n) =
            encoding::decode_one_detailed_counted(bytes, self.limits()).map_err(|e| {
                self.exception = e.exception;
                CodeError::Data(e.error)
            })?;
        self.nodes(n)?;
        Ok((v, rest))
    }
    fn ty<'a>(&mut self, bytes: &'a [u8]) -> Result<(Type, &'a [u8]), CodeError> {
        let (v, rest, n) =
            encoding::decode_type_detailed_counted(bytes, self.limits()).map_err(|e| {
                self.exception = e.exception;
                CodeError::Data(e.error)
            })?;
        self.nodes(n)?;
        Ok((v, rest))
    }
    fn encode_value(&mut self, v: &Value) -> Result<Vec<u8>, CodeError> {
        let (bytes, n) = encoding::encode_vm_counted(v, self.limits()).map_err(|e| {
            self.exception = (e.native_exception == "function_clause")
                .then(|| native::Exception::error(NativeTerm::Atom("function_clause")));
            CodeError::Data(e.error)
        })?;
        self.nodes(n)?;
        Ok(bytes)
    }
    fn encode_type(&mut self, v: &Type) -> Result<Vec<u8>, CodeError> {
        let (bytes, n) = encoding::encode_type_counted(v, self.limits())?;
        self.nodes(n)?;
        Ok(bytes)
    }
    fn encode_arguments(&mut self, arguments: &[Type]) -> Result<Vec<u8>, CodeError> {
        let (bytes, n) = encoding::encode_tuple_type_counted(arguments, self.limits())?;
        self.nodes(n)?;
        Ok(bytes)
    }
}
fn section<'a>(bytes: &'a [u8], meter: &mut Meter) -> Result<(Rlp<'a>, &'a [u8]), CodeError> {
    let decoded = rlp::decode_one_detailed(
        bytes,
        rlp::DecodeLimits {
            max_input_bytes: meter.limits.max_bytes,
            max_items: meter.remaining(),
            max_depth: meter.limits.max_depth,
        },
    )
    .map_err(|e| {
        let reason = match e.error {
            rlp::DecodeError::EmptyInput => NativeTerm::Atom("function_clause"),
            rlp::DecodeError::LeadingZeroLength => NativeTerm::Atom("leading_zeroes_in_size"),
            _ => NativeTerm::tagged(
                "badmatch",
                vec![NativeTerm::Binary(e.remainder.unwrap_or_default().to_vec())],
            ),
        };
        meter.exception = Some(native::Exception::error(reason));
        if e.error == rlp::DecodeError::ResourceLimit {
            CodeError::Data(Error::ResourceLimit)
        } else {
            CodeError::InvalidContainer
        }
    })?;
    meter.nodes(decoded.items)?;
    Ok((decoded.value, decoded.tail))
}

fn finish(
    current: &mut Option<(Vec<u8>, Function)>,
    block: &mut Vec<Instruction>,
    functions: &mut BTreeMap<Vec<u8>, Function>,
) {
    if let Some((id, mut function)) = current.take() {
        if !block.is_empty() {
            function
                .blocks
                .insert(function.blocks.len(), std::mem::take(block));
        }
        functions.insert(id, function);
    }
}
fn decode_functions(
    mut bytes: &[u8],
    meter: &mut Meter,
) -> Result<BTreeMap<Vec<u8>, Function>, CodeError> {
    let mut functions = BTreeMap::new();
    let mut current: Option<(Vec<u8>, Function)> = None;
    let mut block = vec![];
    while !bytes.is_empty() {
        let byte = bytes[0];
        bytes = &bytes[1..];
        meter.nodes(1)?;
        if byte == 0xfe && bytes.len() >= 4 {
            let id = bytes.get(..4).ok_or(Error::Truncated)?.to_vec();
            bytes = &bytes[4..];
            let (attrs, rest) = meter.value(bytes)?;
            let Value::Integer(attrs) = attrs else {
                return meter.fail(CodeError::InvalidAttributes, NativeTerm::Atom("badarith"));
            };
            let n = attrs
                .magnitude()
                .to_u8()
                .filter(|n| *n <= 3)
                .ok_or(CodeError::InvalidAttributes)?;
            let mut attributes = vec![];
            if n & 2 != 0 {
                attributes.push(Attribute::Payable);
            }
            if n & 1 != 0 {
                attributes.push(Attribute::Private);
            }
            let (args, rest) = meter.ty(rest)?;
            let Type::Tuple(arguments) = args else {
                return meter.fail(
                    CodeError::InvalidSignature,
                    NativeTerm::tagged(
                        "badmatch",
                        vec![NativeTerm::Tuple(vec![
                            NativeTerm::Type(Box::new(args)),
                            NativeTerm::Binary(rest.to_vec()),
                        ])],
                    ),
                );
            };
            let (returns, rest) = meter.ty(rest)?;
            bytes = rest;
            finish(&mut current, &mut block, &mut functions);
            current = Some((
                id,
                Function {
                    attributes,
                    arguments,
                    returns,
                    blocks: BTreeMap::new(),
                },
            ));
            continue;
        }
        let Some((_, function)) = &mut current else {
            return meter.fail(
                CodeError::CodeWithoutFunction,
                NativeTerm::tagged("code_without_function", vec![]),
            );
        };
        let opcode = match Opcode::from_byte(byte) {
            Some(op) => op,
            None => {
                meter.exception = Some(native::Exception::exit(NativeTerm::tagged(
                    "bad_opcode",
                    vec![NativeTerm::number(byte)],
                )));
                return Err(CodeError::UnknownOpcode(byte));
            }
        };
        let info = opcode.info();
        let mut operands = vec![];
        if info.arity > 0 {
            let count = if info.arity > 4 { 2 } else { 1 };
            let mods = bytes.get(..count).ok_or(Error::Truncated)?;
            let bits = mods.iter().fold(0u16, |n, b| (n << 8) | u16::from(*b));
            bytes = &bytes[count..];
            if info.arity < 8 && bits >> (info.arity * 2) != 0 {
                return meter.fail(
                    CodeError::InvalidModifier,
                    NativeTerm::Atom("argument_defined_outside_range"),
                );
            }
            for i in 0..info.arity {
                let modifier = (bits >> (2 * i)) & 3;
                let arg = if modifier == 0 {
                    meter.nodes(1)?;
                    Operand::Stack
                } else {
                    let (v, rest) = meter.value(bytes)?;
                    bytes = rest;
                    match modifier {
                        1 => Operand::Argument(v),
                        2 => Operand::Variable(v),
                        _ => Operand::Immediate(v),
                    }
                };
                operands.push(arg);
            }
        }
        block.push(Instruction { opcode, operands });
        if info.end_block {
            meter.nodes(1)?;
            function
                .blocks
                .insert(function.blocks.len(), std::mem::take(&mut block));
        }
    }
    finish(&mut current, &mut block, &mut functions);
    Ok(functions)
}
