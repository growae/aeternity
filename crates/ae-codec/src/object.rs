// Behavioral reference: aeserialization a1204df, aeserialization.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in the repository LICENSE.
use crate::rlp::{self, Rlp};
use ae_types::{Id, TypeError, Unsigned};
use std::{collections::BTreeMap, fmt};

mod tags;
pub use tags::ObjectTag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Integer,
    Boolean,
    Binary,
    Id,
    List(Box<FieldType>),
    Tuple(Vec<FieldType>),
    Record(Vec<Field>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub kind: FieldType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Integer(Unsigned),
    Boolean(bool),
    Binary(Vec<u8>),
    Id(Id),
    List(Vec<Value>),
    Tuple(Vec<Value>),
    Record(BTreeMap<String, Value>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectError {
    Rlp(rlp::DecodeError),
    Type(TypeError),
    ExpectedList,
    WrongType,
    FieldCount,
    FieldName,
    DuplicateField,
    UnknownTag,
    WrongTag,
    WrongVersion,
}
impl fmt::Display for ObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ObjectError {}
impl From<rlp::DecodeError> for ObjectError {
    fn from(e: rlp::DecodeError) -> Self {
        Self::Rlp(e)
    }
}
impl From<TypeError> for ObjectError {
    fn from(e: TypeError) -> Self {
        Self::Type(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<'a> {
    pub tag: ObjectTag,
    pub version: Unsigned,
    pub fields: Vec<Rlp<'a>>,
}

pub fn decode_envelope(bytes: &[u8]) -> Result<Envelope<'_>, ObjectError> {
    decode_envelope_with_limits(bytes, rlp::DecodeLimits::default())
}

pub fn decode_envelope_with_limits(
    bytes: &[u8],
    limits: rlp::DecodeLimits,
) -> Result<Envelope<'_>, ObjectError> {
    let Rlp::List(mut fields) = rlp::decode_with_limits(bytes, limits)? else {
        return Err(ObjectError::ExpectedList);
    };
    if fields.len() < 2 {
        return Err(ObjectError::FieldCount);
    }
    let tag = integer(&fields[0])?
        .to_u64()
        .map_err(|_| ObjectError::UnknownTag)?;
    let tag = ObjectTag::try_from(tag).map_err(|_| ObjectError::UnknownTag)?;
    let version = integer(&fields[1])?;
    fields.drain(..2);
    Ok(Envelope {
        tag,
        version,
        fields,
    })
}

pub fn deserialize(
    bytes: &[u8],
    tag: ObjectTag,
    version: &Unsigned,
    template: &[Field],
) -> Result<Vec<(String, Value)>, ObjectError> {
    let envelope = decode_envelope(bytes)?;
    deserialize_envelope(&envelope, tag, version, template)
}

pub(crate) fn deserialize_envelope(
    envelope: &Envelope<'_>,
    tag: ObjectTag,
    version: &Unsigned,
    template: &[Field],
) -> Result<Vec<(String, Value)>, ObjectError> {
    if envelope.tag != tag {
        return Err(ObjectError::WrongTag);
    }
    if &envelope.version != version {
        return Err(ObjectError::WrongVersion);
    }
    if template.len() != envelope.fields.len() {
        return Err(ObjectError::FieldCount);
    }
    template
        .iter()
        .zip(&envelope.fields)
        .map(|(f, v)| Ok((f.name.clone(), decode_field(&f.kind, v)?)))
        .collect()
}

pub fn serialize(
    tag: ObjectTag,
    version: &Unsigned,
    template: &[Field],
    fields: &[(String, Value)],
) -> Result<Vec<u8>, ObjectError> {
    if template.len() != fields.len() {
        return Err(ObjectError::FieldCount);
    }
    let mut encoded = vec![
        rlp::encode(&Rlp::Bytes(Unsigned::from(tag as u64).as_bytes())),
        rlp::encode(&Rlp::Bytes(version.as_bytes())),
    ];
    for (field, (name, value)) in template.iter().zip(fields) {
        if &field.name != name {
            return Err(ObjectError::FieldName);
        }
        encoded.push(encode_field(&field.kind, value)?);
    }
    encode_list(&encoded)
}

fn encode_list(items: &[Vec<u8>]) -> Result<Vec<u8>, ObjectError> {
    let mut payload = Vec::new();
    for item in items {
        payload.extend_from_slice(item);
    }
    Ok(rlp::with_size(0xc0, &payload))
}

fn integer(value: &Rlp<'_>) -> Result<Unsigned, ObjectError> {
    match value {
        Rlp::Bytes(bytes) => Ok(Unsigned::from_field_bytes(bytes)?),
        _ => Err(ObjectError::WrongType),
    }
}

pub fn decode_field(kind: &FieldType, value: &Rlp<'_>) -> Result<Value, ObjectError> {
    Ok(match (kind, value) {
        (FieldType::Integer, _) => Value::Integer(integer(value)?),
        (FieldType::Binary, Rlp::Bytes(bytes)) => Value::Binary(bytes.to_vec()),
        (FieldType::Id, Rlp::Bytes(bytes)) => Value::Id(Id::from_bytes(bytes)?),
        (FieldType::Boolean, Rlp::Bytes([0])) => Value::Boolean(false),
        (FieldType::Boolean, Rlp::Bytes([1])) => Value::Boolean(true),
        (FieldType::List(kind), Rlp::List(values)) => Value::List(
            values
                .iter()
                .map(|v| decode_field(kind, v))
                .collect::<Result<_, _>>()?,
        ),
        (FieldType::Tuple(kinds), Rlp::List(values)) if kinds.len() == values.len() => {
            Value::Tuple(
                kinds
                    .iter()
                    .zip(values)
                    .map(|(k, v)| decode_field(k, v))
                    .collect::<Result<_, _>>()?,
            )
        }
        (FieldType::Record(fields), Rlp::List(values)) if fields.len() == values.len() => {
            let mut out = BTreeMap::new();
            for (field, value) in fields.iter().zip(values) {
                if out
                    .insert(field.name.clone(), decode_field(&field.kind, value)?)
                    .is_some()
                {
                    return Err(ObjectError::DuplicateField);
                }
            }
            Value::Record(out)
        }
        _ => return Err(ObjectError::WrongType),
    })
}

pub fn encode_field(kind: &FieldType, value: &Value) -> Result<Vec<u8>, ObjectError> {
    match (kind, value) {
        (FieldType::Integer, Value::Integer(n)) => Ok(rlp::encode(&Rlp::Bytes(n.as_bytes()))),
        (FieldType::Binary, Value::Binary(bytes)) => Ok(rlp::encode(&Rlp::Bytes(bytes))),
        (FieldType::Id, Value::Id(id)) => Ok(rlp::encode(&Rlp::Bytes(&id.to_bytes()))),
        (FieldType::Boolean, Value::Boolean(b)) => Ok(vec![u8::from(*b)]),
        (FieldType::List(kind), Value::List(values)) => encode_list(
            &values
                .iter()
                .map(|v| encode_field(kind, v))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (FieldType::Tuple(kinds), Value::Tuple(values)) if kinds.len() == values.len() => {
            encode_list(
                &kinds
                    .iter()
                    .zip(values)
                    .map(|(k, v)| encode_field(k, v))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (FieldType::Record(fields), Value::Record(values)) => encode_list(
            &fields
                .iter()
                .map(|f| encode_field(&f.kind, values.get(&f.name).ok_or(ObjectError::FieldName)?))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => Err(ObjectError::WrongType),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fields_preserve_aeternity_integer_and_bool_semantics() {
        assert_eq!(
            encode_field(&FieldType::Integer, &Value::Integer(0.into())).unwrap(),
            [0]
        );
        assert_eq!(
            decode_field(&FieldType::Integer, &Rlp::Bytes(&[])).unwrap(),
            Value::Integer(0.into())
        );
        assert!(decode_field(&FieldType::Integer, &Rlp::Bytes(&[0, 1])).is_err());
        assert!(decode_field(&FieldType::Boolean, &Rlp::Bytes(&[])).is_err());
        assert!(decode_field(&FieldType::Boolean, &Rlp::Bytes(&[2])).is_err());
    }
    #[test]
    fn object_tag_and_version_are_not_json_or_bincode() {
        let template = vec![Field {
            name: "balance".into(),
            kind: FieldType::Integer,
        }];
        let values = vec![("balance".into(), Value::Integer(42.into()))];
        let raw = serialize(ObjectTag::Account, &1.into(), &template, &values).unwrap();
        assert_eq!(raw, [0xc3, 10, 1, 42]);
        assert_eq!(
            deserialize(&raw, ObjectTag::Account, &1.into(), &template).unwrap(),
            values
        );
        assert_eq!(
            deserialize(&raw, ObjectTag::Oracle, &1.into(), &template),
            Err(ObjectError::WrongTag)
        );
        assert_eq!(
            deserialize(&raw, ObjectTag::Account, &2.into(), &template),
            Err(ObjectError::WrongVersion)
        );
    }
    #[test]
    fn tuple_and_record_order_is_schema_order() {
        let fields = vec![
            Field {
                name: "z".into(),
                kind: FieldType::Integer,
            },
            Field {
                name: "a".into(),
                kind: FieldType::Boolean,
            },
        ];
        let mut values = BTreeMap::new();
        values.insert("a".into(), Value::Boolean(true));
        values.insert("z".into(), Value::Integer(42.into()));
        let kind = FieldType::Record(fields);
        assert_eq!(
            encode_field(&kind, &Value::Record(values)).unwrap(),
            [0xc2, 42, 1]
        );
        let duplicate = FieldType::Record(vec![
            Field {
                name: "x".into(),
                kind: FieldType::Integer
            };
            2
        ]);
        assert_eq!(
            decode_field(
                &duplicate,
                &Rlp::List(vec![Rlp::Bytes(&[1]), Rlp::Bytes(&[2])])
            ),
            Err(ObjectError::DuplicateField)
        );
    }
}
