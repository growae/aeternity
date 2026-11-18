// aefa_engine_state.erl gas/traversal helpers, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    Budget, Error as DataError, Limits,
    maps::{self, MapError, MapOrder, Traversal},
    types::Type,
    value::{ObjectKind, Value},
};
use num_bigint::{BigInt, Sign};
use std::convert::Infallible;
const NODE: i32 = 800;
const DENOMINATOR: i32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostModel {
    Simple,
    Serial,
    Unfold,
    UnfoldCompare,
    UnfoldSerial,
    Final,
}
impl CostModel {
    fn leaf(self) -> u16 {
        match self {
            Self::Serial | Self::UnfoldSerial => 300,
            Self::UnfoldCompare => 4,
            Self::Final => 1000,
            _ => 0,
        }
    }
    fn unfold(self) -> u32 {
        match self {
            Self::Unfold | Self::UnfoldCompare | Self::UnfoldSerial => 20_000_000,
            _ => 0,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E = Infallible> {
    OutOfGas,
    Data(DataError),
    Host(E),
    InvalidOrder,
}
impl<E> From<DataError> for Error<E> {
    fn from(e: DataError) -> Self {
        Self::Data(e)
    }
}
#[derive(Debug, Clone)]
pub struct Charge {
    pub gas: BigInt,
    pub remaining_limits: Limits,
}

// Callers checkpoint any host effects. Repeated references deliberately call
// both helpers again. The host must respect the supplied operational budget.
pub trait UnfoldHost {
    type Error;
    fn map_size(&mut self, id: &BigInt, limits: Limits) -> Result<BigInt, Self::Error>;
    fn unfold(&mut self, id: &BigInt, limits: Limits) -> Result<Value, Self::Error>;
}
pub struct NoUnfold;
impl UnfoldHost for NoUnfold {
    type Error = Infallible;
    fn map_size(&mut self, _: &BigInt, _: Limits) -> Result<BigInt, Infallible> {
        Ok(0.into())
    }
    fn unfold(&mut self, _: &BigInt, _: Limits) -> Result<Value, Infallible> {
        Ok(Value::Tuple(vec![]))
    }
}
pub fn traversal(
    protocol: u32,
    model: CostModel,
    value: &Value,
    gas: BigInt,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Charge, Error> {
    traversal_with_host(protocol, model, value, gas, &mut NoUnfold, order, limits)
}
pub fn traversal_with_host<H: UnfoldHost>(
    protocol: u32,
    model: CostModel,
    value: &Value,
    gas: BigInt,
    host: &mut H,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Charge, Error<H::Error>> {
    if protocol < 5 {
        return Ok(Charge {
            gas,
            remaining_limits: limits,
        });
    }
    let mut result = raw_traversal(model, value, gas * DENOMINATOR, host, order, limits)?;
    if result.gas.sign() == Sign::Minus {
        return Err(Error::OutOfGas);
    }
    result.gas /= DENOMINATOR;
    Ok(result)
}
// The original lower-level helper can return negative milligas. Its caller
// performs the final out-of-gas check and rounds down to whole gas.
pub fn raw_traversal<H: UnfoldHost>(
    model: CostModel,
    value: &Value,
    milligas: BigInt,
    host: &mut H,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Charge, Error<H::Error>> {
    let mut walk = Walk {
        model,
        host,
        order,
        budget: Budget::new(limits),
        bytes: 0,
    };
    let gas = walk.value(milligas, value, 0)?;
    Ok(Charge {
        gas,
        remaining_limits: walk.limits(),
    })
}
struct Walk<'a, H, O> {
    model: CostModel,
    host: &'a mut H,
    order: &'a O,
    budget: Budget,
    bytes: usize,
}
impl<H: UnfoldHost, O: MapOrder> Walk<'_, H, O> {
    fn limits(&self) -> Limits {
        Limits {
            max_nodes: self
                .budget
                .limits
                .max_nodes
                .saturating_sub(self.budget.nodes),
            max_bytes: self.budget.limits.max_bytes.saturating_sub(self.bytes),
            ..self.budget.limits
        }
    }
    fn nonnegative(&self, gas: &BigInt) -> Result<(), Error<H::Error>> {
        if gas.sign() == Sign::Minus {
            Err(Error::OutOfGas)
        } else {
            Ok(())
        }
    }
    fn leaf(&mut self, gas: BigInt, v: &Value) -> Result<BigInt, Error<H::Error>> {
        let rate = self.model.leaf();
        if rate == 0 {
            return Ok(gas - NODE);
        }
        let size = match v {
            Value::Integer(n) | Value::Bits(n) => {
                usize::try_from(n.magnitude().bits().div_ceil(8).max(1))
                    .map_err(|_| DataError::ResourceLimit)?
            }
            Value::String(b) | Value::Bytes(b) => b.len(),
            Value::Object(..) => 32,
            _ => 1,
        };
        self.bytes = self
            .bytes
            .checked_add(size)
            .ok_or(DataError::ResourceLimit)?;
        if self.bytes > self.budget.limits.max_bytes {
            return Err(DataError::ResourceLimit.into());
        }
        Ok(gas - NODE - BigInt::from(size) * rate)
    }
    fn value(&mut self, gas: BigInt, v: &Value, depth: usize) -> Result<BigInt, Error<H::Error>> {
        self.nonnegative(&gas)?;
        self.budget.visit(depth)?;
        match v {
            Value::Tuple(vs) if vs.is_empty() => self.leaf(gas, v),
            Value::Tombstone
            | Value::Boolean(_)
            | Value::Integer(_)
            | Value::Bits(_)
            | Value::Bytes(_)
            | Value::String(_)
            | Value::Object(..) => self.leaf(gas, v),
            Value::TypeRep(ty) => self.ty(gas - NODE, ty, depth + 1),
            Value::Tuple(vs) | Value::Variant { values: vs, .. } => {
                self.list(gas - NODE, vs, depth + 1)
            }
            Value::List(vs) => self.list(gas, vs, depth + 1),
            Value::Map(map) => self.map(gas, map, depth + 1),
            Value::StoreMap { id, cache } => {
                let size = self.host.map_size(id, self.limits()).map_err(Error::Host)?;
                if size.magnitude().bits().div_ceil(8) > self.limits().max_bytes as u64 {
                    return Err(DataError::ResourceLimit.into());
                }
                let gas = gas - size * self.model.unfold() - NODE;
                self.nonnegative(&gas)?;
                let unfolded = self.host.unfold(id, self.limits()).map_err(Error::Host)?;
                let gas = self.value(gas, &unfolded, depth + 1)?;
                self.map(gas, cache, depth + 1)
            }
            Value::ContractBytearray(_) => Err(DataError::InvalidValue.into()),
        }
    }
    fn list(
        &mut self,
        mut gas: BigInt,
        values: &[Value],
        depth: usize,
    ) -> Result<BigInt, Error<H::Error>> {
        for v in values {
            self.budget.visit(depth)?;
            gas = self.value(gas - NODE, v, depth)?;
        }
        self.budget.visit(depth)?;
        Ok(gas - NODE)
    }
    fn map(
        &mut self,
        mut gas: BigInt,
        map: &maps::Map,
        depth: usize,
    ) -> Result<BigInt, Error<H::Error>> {
        self.budget.visit(depth)?;
        gas -= NODE;
        if map.len() > self.limits().max_nodes {
            return Err(DataError::ResourceLimit.into());
        }
        let entries: Vec<_> = map.iter().collect();
        let indices = maps::ordered_indices::<Infallible>(
            &entries.iter().map(|p| p.0).collect::<Vec<_>>(),
            self.order,
            Traversal::Fold,
            &mut self.budget,
        )
        .map_err(|e| match e {
            MapError::Data(e) => Error::Data(e),
            MapError::InvalidOrder => Error::InvalidOrder,
            MapError::Lookup(n) => match n {},
        })?;
        for i in indices {
            gas = self.value(gas - NODE, entries[i].0, depth)?;
            gas = self.value(gas, entries[i].1, depth)?;
        }
        Ok(gas)
    }
    fn ty(&mut self, gas: BigInt, ty: &Type, depth: usize) -> Result<BigInt, Error<H::Error>> {
        self.nonnegative(&gas)?;
        self.budget.visit(depth)?;
        match ty {
            Type::Tuple(vs) | Type::Variant(vs) => self.ty_list(gas - NODE, vs, depth + 1),
            Type::List(t) => self.ty(gas - NODE, t, depth + 1),
            Type::Map(k, v) => {
                let gas = self.ty(gas - NODE, k, depth + 1)?;
                self.ty(gas, v, depth + 1)
            }
            Type::Integer
            | Type::Boolean
            | Type::Bits
            | Type::String
            | Type::ContractBytearray
            | Type::Object(
                ObjectKind::Address
                | ObjectKind::Contract
                | ObjectKind::Oracle
                | ObjectKind::Channel,
            ) => Ok(gas - self.model.leaf()),
            _ => Err(DataError::InvalidType.into()),
        }
    }
    fn ty_list(
        &mut self,
        mut gas: BigInt,
        types: &[Type],
        depth: usize,
    ) -> Result<BigInt, Error<H::Error>> {
        for t in types {
            self.nonnegative(&gas)?;
            self.budget.visit(depth)?;
            gas = self.ty(gas, t, depth + 1)?;
        }
        self.nonnegative(&gas)?;
        self.budget.visit(depth)?;
        Ok(gas - self.model.leaf())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meter {
    pub gas: BigInt,
    pub created_cells: BigInt,
    pub protocol: u32,
    pub metering_floor: Option<u32>,
}
impl Meter {
    pub fn spend(&mut self, cost: BigInt) -> Result<(), Error> {
        let gas = &self.gas - cost;
        if gas.sign() == Sign::Minus {
            return Err(Error::OutOfGas);
        }
        self.gas = gas;
        Ok(())
    }
    pub fn new_cells(&mut self, mut cells: BigInt) -> Result<(), Error> {
        if cells.sign() == Sign::Minus && self.protocol >= 5 {
            cells = -cells + 2;
        }
        let total = &self.created_cells + &cells;
        let cost = if total <= BigInt::from(1024) {
            cells
        } else {
            cells * (BigInt::from(1) + (&total >> 10))
        };
        self.spend(cost)?;
        self.created_cells = total;
        Ok(())
    }
    pub fn store_values(&mut self, count: BigInt) -> Result<(), Error> {
        if self.protocol >= 6 {
            self.spend(count * 5000)?;
        }
        Ok(())
    }
    pub fn store_read_floor(&mut self, charged: BigInt) -> Result<(), Error> {
        let extra = store_read_floor_cost(self.metering_floor, charged);
        if extra > BigInt::from(0) {
            self.spend(extra)?;
        }
        Ok(())
    }
}
pub(crate) fn store_read_floor_cost(floor: Option<u32>, charged: BigInt) -> BigInt {
    if let Some(protocol) = floor
        && protocol < 7
    {
        (BigInt::from(if protocol >= 5 { 2000 } else { 0 }) - charged).max(0.into())
    } else {
        0.into()
    }
}
