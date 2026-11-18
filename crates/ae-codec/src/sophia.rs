// aeser_contract_code.erl, aeserialization a1204df.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::object::{self, Field, FieldType, ObjectError, ObjectTag, Value};
use ae_crypto::blake2b_256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionInfo {
    pub hash: Vec<u8>,
    pub name: Vec<u8>,
    pub payable: Option<bool>,
    pub arguments: Vec<u8>,
    pub returns: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SophiaCode {
    pub version: u8,
    // The original decoder does not constrain hash or bytecode lengths.
    pub source_hash: Vec<u8>,
    pub type_info: Vec<FunctionInfo>,
    pub byte_code: Vec<u8>,
    pub compiler_version: Option<Vec<u8>>,
    pub payable: bool,
}
impl SophiaCode {
    pub fn from_source(
        version: u8,
        source: &[u8],
        byte_code: Vec<u8>,
        type_info: Vec<FunctionInfo>,
    ) -> Self {
        Self {
            version,
            source_hash: blake2b_256(source).0.to_vec(),
            type_info,
            byte_code,
            compiler_version: None,
            payable: true,
        }
    }
    pub fn encode(&self) -> Result<Vec<u8>, ObjectError> {
        let schema = schema(self.version)?;
        let mut info = Vec::with_capacity(self.type_info.len());
        for f in &self.type_info {
            let mut parts = vec![Value::Binary(f.hash.clone()), Value::Binary(f.name.clone())];
            match (self.version, f.payable) {
                (3, Some(payable)) => parts.push(Value::Boolean(payable)),
                (1 | 2, None) => {}
                _ => return Err(ObjectError::WrongType),
            }
            parts.extend([
                Value::Binary(f.arguments.clone()),
                Value::Binary(f.returns.clone()),
            ]);
            info.push(Value::Tuple(parts));
        }
        let mut fields = vec![
            (
                "source_hash".into(),
                Value::Binary(self.source_hash.clone()),
            ),
            ("type_info".into(), Value::List(info)),
            ("byte_code".into(), Value::Binary(self.byte_code.clone())),
        ];
        if self.version > 1 {
            fields.push((
                "compiler_version".into(),
                Value::Binary(
                    self.compiler_version
                        .clone()
                        .unwrap_or_else(|| b"unknown".to_vec()),
                ),
            ));
        }
        if self.version > 2 {
            fields.push(("payable".into(), Value::Boolean(self.payable)));
        }
        object::serialize(
            ObjectTag::CompilerSophia,
            &u64::from(self.version).into(),
            &schema,
            &fields,
        )
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ObjectError> {
        Self::decode_with_limits(bytes, crate::rlp::DecodeLimits::default())
    }
    pub fn decode_with_limits(
        bytes: &[u8],
        limits: crate::rlp::DecodeLimits,
    ) -> Result<Self, ObjectError> {
        let env = object::decode_envelope_with_limits(bytes, limits)?;
        let version = env
            .version
            .to_u64()
            .ok()
            .and_then(|v| u8::try_from(v).ok())
            .ok_or(ObjectError::WrongVersion)?;
        let fields = object::deserialize_envelope(
            &env,
            ObjectTag::CompilerSophia,
            &env.version,
            &schema(version)?,
        )?;
        let mut vs = fields.into_iter().map(|(_, v)| v);
        let Some(Value::Binary(source_hash)) = vs.next() else {
            return Err(ObjectError::WrongType);
        };
        let Some(Value::List(functions)) = vs.next() else {
            return Err(ObjectError::WrongType);
        };
        let mut type_info = Vec::with_capacity(functions.len());
        for function in functions {
            let Value::Tuple(parts) = function else {
                return Err(ObjectError::WrongType);
            };
            let (hash, name, payable, arguments, returns) = match parts.as_slice() {
                [
                    Value::Binary(h),
                    Value::Binary(n),
                    Value::Binary(a),
                    Value::Binary(r),
                ] if version < 3 => (h, n, None, a, r),
                [
                    Value::Binary(h),
                    Value::Binary(n),
                    Value::Boolean(p),
                    Value::Binary(a),
                    Value::Binary(r),
                ] if version == 3 => (h, n, Some(*p), a, r),
                _ => return Err(ObjectError::WrongType),
            };
            type_info.push(FunctionInfo {
                hash: hash.clone(),
                name: name.clone(),
                payable,
                arguments: arguments.clone(),
                returns: returns.clone(),
            });
        }
        let Some(Value::Binary(byte_code)) = vs.next() else {
            return Err(ObjectError::WrongType);
        };
        let compiler_version = if version > 1 {
            let Some(Value::Binary(v)) = vs.next() else {
                return Err(ObjectError::WrongType);
            };
            Some(v)
        } else {
            None
        };
        let payable = if version > 2 {
            let Some(Value::Boolean(v)) = vs.next() else {
                return Err(ObjectError::WrongType);
            };
            v
        } else {
            true
        };
        Ok(Self {
            version,
            source_hash,
            type_info,
            byte_code,
            compiler_version,
            payable,
        })
    }
}
fn schema(version: u8) -> Result<Vec<Field>, ObjectError> {
    if !(1..=3).contains(&version) {
        return Err(ObjectError::WrongVersion);
    }
    let mut function = vec![FieldType::Binary, FieldType::Binary];
    if version == 3 {
        function.push(FieldType::Boolean);
    }
    function.extend([FieldType::Binary, FieldType::Binary]);
    let mut fields = vec![
        ("source_hash", FieldType::Binary),
        (
            "type_info",
            FieldType::List(Box::new(FieldType::Tuple(function))),
        ),
        ("byte_code", FieldType::Binary),
    ];
    if version > 1 {
        fields.push(("compiler_version", FieldType::Binary));
    }
    if version > 2 {
        fields.push(("payable", FieldType::Boolean));
    }
    Ok(fields
        .into_iter()
        .map(|(name, kind)| Field {
            name: name.into(),
            kind,
        })
        .collect())
}
