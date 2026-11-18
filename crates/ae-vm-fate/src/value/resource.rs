// Operational value-graph accounting, independent of consensus gas and codecs.
use super::{ObjectPayload, Value};
use crate::{Budget, Error, Limits, types::Type};

// This also admits runtime-only and non-serializable values. Encoding a value
// merely to bound a copy would incorrectly reject such intermediate states.
pub fn check_limits(value: &Value, limits: Limits) -> Result<Limits, Error> {
    let mut walk = Walk {
        budget: Budget::new(limits),
        bytes: 0,
    };
    walk.value(value, 0)?;
    Ok(Limits {
        max_nodes: limits.max_nodes - walk.budget.nodes,
        max_bytes: limits.max_bytes - walk.bytes,
        ..limits
    })
}
struct Walk {
    budget: Budget,
    bytes: usize,
}
impl Walk {
    fn bytes(&mut self, n: usize) -> Result<(), Error> {
        self.bytes = self.bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.budget.limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    fn number(&mut self, bits: u64) -> Result<(), Error> {
        self.bytes(usize::try_from(bits.div_ceil(8)).map_err(|_| Error::ResourceLimit)?)
    }
    fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)?;
        match value {
            Value::Integer(n) | Value::Bits(n) => self.number(n.magnitude().bits())?,
            Value::Boolean(_) | Value::Tombstone | Value::Void | Value::BitsNil => {}
            Value::String(s) | Value::Bytes(s) | Value::ContractBytearray(s) => {
                self.bytes(s.len())?
            }
            Value::Object(_, p) => self.payload(p, depth + 1)?,
            Value::List(v) | Value::Tuple(v) => self.values(v, depth + 1)?,
            Value::Variant {
                arities,
                tag: _,
                values,
            } => {
                for n in arities {
                    self.budget.visit(depth + 1)?;
                    self.number(n.magnitude().bits())?;
                }
                self.values(values, depth + 1)?;
            }
            Value::Map(m) => self.map(m, depth + 1)?,
            Value::StoreMap { id, cache } => {
                self.number(id.magnitude().bits())?;
                self.map(cache, depth + 1)?;
            }
            Value::TypeRep(t) => self.ty(t, depth + 1)?,
        }
        Ok(())
    }
    fn values(&mut self, values: &[Value], depth: usize) -> Result<(), Error> {
        for value in values {
            self.value(value, depth)?;
        }
        Ok(())
    }
    fn map(&mut self, map: &super::BTreeMap<Value, Value>, depth: usize) -> Result<(), Error> {
        for (k, v) in map {
            self.value(k, depth)?;
            self.value(v, depth)?;
        }
        Ok(())
    }
    fn payload(&mut self, payload: &ObjectPayload, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)?;
        match payload {
            ObjectPayload::Bytes(b) => self.bytes(b.len())?,
            ObjectPayload::List(v) => {
                for p in v {
                    self.payload(p, depth + 1)?;
                }
            }
        }
        Ok(())
    }
    fn ty(&mut self, ty: &Type, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)?;
        match ty {
            Type::Bytes(Some(n)) => self.number(n.bits())?,
            Type::Arity(n) => self.number(n.magnitude().bits())?,
            Type::List(t) => self.ty(t, depth + 1)?,
            Type::Tuple(ts) | Type::Variant(ts) => {
                for t in ts {
                    self.ty(t, depth + 1)?;
                }
            }
            Type::Map(k, v) => {
                self.ty(k, depth + 1)?;
                self.ty(v, depth + 1)?;
            }
            _ => {}
        }
        Ok(())
    }
}
