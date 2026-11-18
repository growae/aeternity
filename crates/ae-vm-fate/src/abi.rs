// aeb_fate_abi.erl, aebytecode 009e036.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    Error, Limits,
    code::{Code, CodeError, symbol_identifier},
    encoding,
    types::Type,
    value::Value,
};
pub const ABI_VERSION: u16 = 3;
// FATE_TUPLE_ELEMENTS in the ABI library reads element(2) without checking
// the outer tag. Typereps can therefore expose raw Erlang type terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbiTerm {
    Data(Value),
    Atom(&'static str),
    Type(Type),
    Types(Vec<Type>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    BadData,
    WrongFunction,
    ResourceLimit,
}
pub fn create_calldata(
    name: &[u8],
    arguments: Vec<Value>,
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    encoding::encode(
        &Value::Tuple(vec![
            Value::String(symbol_identifier(name).to_vec()),
            Value::Tuple(arguments),
        ]),
        limits,
    )
}
pub fn decode_calldata(name: &[u8], bytes: &[u8], limits: Limits) -> Result<Vec<AbiTerm>, Error> {
    let mut parts = tuple_elements(encoding::decode(bytes, limits)?)?;
    if parts.len() != 2
        || parts[0] != AbiTerm::Data(Value::String(symbol_identifier(name).to_vec()))
    {
        return Err(Error::InvalidValue);
    }
    let AbiTerm::Data(args) = parts.pop().expect("two fields") else {
        return Err(Error::InvalidValue);
    };
    tuple_elements(args)
}
// The reference only checks the outer tuple's arity, not hash/argument types.
pub fn function_hash_from_calldata(bytes: &[u8], limits: Limits) -> Result<AbiTerm, Error> {
    let mut parts = tuple_elements(encoding::decode(bytes, limits)?)?;
    if parts.len() != 2 {
        return Err(Error::InvalidValue);
    }
    Ok(parts.remove(0))
}
fn tuple_elements(value: Value) -> Result<Vec<AbiTerm>, Error> {
    Ok(match value {
        Value::Tuple(v) => v.into_iter().map(AbiTerm::Data).collect(),
        Value::TypeRep(t) => match t {
            Type::List(t) => vec![AbiTerm::Atom("list"), AbiTerm::Type(*t)],
            Type::Map(k, v) => vec![AbiTerm::Atom("map"), AbiTerm::Type(*k), AbiTerm::Type(*v)],
            Type::Tuple(ts) => vec![AbiTerm::Atom("tuple"), AbiTerm::Types(ts)],
            Type::Variant(ts) => vec![AbiTerm::Atom("variant"), AbiTerm::Types(ts)],
            Type::Variable(n) => vec![
                AbiTerm::Atom("tvar"),
                AbiTerm::Data(Value::Integer(n.into())),
            ],
            Type::Bytes(n) => vec![
                AbiTerm::Atom("bytes"),
                n.map(|n| AbiTerm::Data(Value::Integer(n.into())))
                    .unwrap_or(AbiTerm::Atom("any")),
            ],
            _ => return Err(Error::InvalidValue),
        },
        _ => return Err(Error::InvalidValue),
    })
}
// VM admission checks both tuple tags and a string function identifier.
// It does not require that the identifier is exactly four bytes.
pub fn decode_vm_calldata(bytes: &[u8], limits: Limits) -> Result<(Vec<u8>, Vec<Value>), Error> {
    let Value::Tuple(mut parts) = encoding::decode(bytes, limits)? else {
        return Err(Error::InvalidValue);
    };
    if parts.len() != 2 {
        return Err(Error::InvalidValue);
    }
    let Value::Tuple(args) = parts.pop().expect("two fields") else {
        return Err(Error::InvalidValue);
    };
    let Value::String(id) = parts.pop().expect("one field") else {
        return Err(Error::InvalidValue);
    };
    Ok((id, args))
}
pub fn verify_init_calldata(bytes: &[u8], limits: Limits) -> Result<bool, Error> {
    Ok(decode_vm_calldata(bytes, limits)?.0 == crate::code::INIT_ID)
}
pub fn check_auth_function(bytes: &[u8], hash: [u8; 32], limits: Limits) -> Result<(), AuthError> {
    match function_hash_from_calldata(bytes, limits) {
        Ok(AbiTerm::Data(Value::String(id))) if id == hash[..4] => Ok(()),
        Ok(_) => Err(AuthError::WrongFunction),
        Err(Error::ResourceLimit) => Err(AuthError::ResourceLimit),
        Err(_) => Err(AuthError::BadData),
    }
}
impl Code {
    pub fn function_name(&self, hash: &[u8]) -> Result<Option<&Value>, CodeError> {
        let hash = match hash.len() {
            4 => hash,
            32 => &hash[..4],
            _ => return Err(CodeError::InvalidFunctionId),
        };
        let Value::Map(symbols) = &self.symbols else {
            return Err(CodeError::InvalidContainer);
        };
        Ok(symbols.get(&Value::String(hash.to_vec())))
    }
    pub fn function_types(&self, hash: &[u8]) -> Option<(&[Type], &Type)> {
        let hash = if hash.len() == 32 { &hash[..4] } else { hash };
        self.functions
            .get(hash)
            .map(|f| (f.arguments.as_slice(), &f.returns))
    }
}
