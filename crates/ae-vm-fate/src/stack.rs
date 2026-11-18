// aefa_engine_state accumulator helpers and aefa_fate:pop_n, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    Error as DataError, Limits,
    gas::{self, Meter},
    value::{self, Value},
};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accumulator {
    pub top: Value,
    // Front is immediately below top, matching the original Erlang list.
    pub rest: VecDeque<Value>,
}
impl Default for Accumulator {
    fn default() -> Self {
        Self {
            top: Value::Void,
            rest: VecDeque::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Data(DataError),
    OutOfGas,
    EmptyStack,
    // The original dup helper throws add_engine_state, without a state.
    InvalidDuplicateIndex,
    ReferenceException(&'static str),
}
// OutOfGas and EmptyStack must pass through the VM's abort handler, which
// replaces available gas with the caller's reserved gas from its call stack.
// This accumulator layer preserves the pre-abort meter; it does not own that
// call stack. Detached DUP errors and native exceptions do not take this path.
impl From<DataError> for Error {
    fn from(e: DataError) -> Self {
        Self::Data(e)
    }
}
impl Accumulator {
    pub fn pop(&mut self, meter: &mut Meter) -> Result<Value, Error> {
        if self.top == Value::Void && self.rest.is_empty() && meter.protocol >= 5 {
            return Err(Error::EmptyStack);
        }
        let next = self.rest.pop_front().unwrap_or(Value::Void);
        meter.created_cells -= 1;
        Ok(std::mem::replace(&mut self.top, next))
    }
    pub fn push(&mut self, value: Value, meter: &mut Meter, limits: Limits) -> Result<(), Error> {
        let remaining = value::check_limits(&value, limits)?;
        if self.rest.len() >= remaining.max_nodes {
            return Err(DataError::ResourceLimit.into());
        }
        let previous = std::mem::replace(&mut self.top, value);
        if previous != Value::Void || !self.rest.is_empty() {
            self.rest.push_front(previous);
        }
        new_cell(meter)
    }
    pub fn duplicate(
        &mut self,
        index: &Value,
        meter: &mut Meter,
        limits: Limits,
    ) -> Result<(), Error> {
        let n = if let Value::Integer(n) = index {
            n.to_usize()
        } else {
            None
        };
        let valid = n.is_some_and(|n| n <= self.rest.len() && (n > 0 || self.top != Value::Void));
        if !valid && meter.protocol >= 5 {
            return Err(Error::InvalidDuplicateIndex);
        }
        let n = match (index, n) {
            (Value::Integer(_), Some(n)) if n <= self.rest.len() => n,
            (Value::Integer(_), _) => return Err(Error::ReferenceException("function_clause")),
            _ => return Err(Error::ReferenceException("badarith")),
        };
        let selected = if n == 0 { &self.top } else { &self.rest[n - 1] };
        let remaining = value::check_limits(selected, limits)?;
        let remaining = value::check_limits(&self.top, remaining)?;
        if self
            .rest
            .len()
            .checked_add(2)
            .ok_or(DataError::ResourceLimit)?
            > remaining.max_nodes
        {
            return Err(DataError::ResourceLimit.into());
        }
        let selected = selected.clone();
        let previous = std::mem::replace(&mut self.top, selected);
        // get_n returns the complete original [top | rest]; dup then prefixes
        // top again. Preserve both copies despite charging just one new cell.
        self.rest.push_front(previous.clone());
        self.rest.push_front(previous);
        new_cell(meter)
    }
    pub fn pop_n(
        &mut self,
        n: usize,
        meter: &mut Meter,
        limits: Limits,
    ) -> Result<Vec<Value>, Error> {
        if n > limits.max_nodes {
            return Err(DataError::ResourceLimit.into());
        }
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            values.push(self.pop(meter)?);
        }
        values.reverse();
        Ok(values)
    }
    pub fn drop_n(&mut self, n: usize, meter: &mut Meter) {
        if n == 0 {
            return;
        }
        self.top = if n <= self.rest.len() {
            self.rest.drain(..n).next_back().unwrap()
        } else {
            self.rest.clear();
            Value::Void
        };
        meter.created_cells -= BigInt::from(n);
    }
}
fn new_cell(meter: &mut Meter) -> Result<(), Error> {
    // The original helper publishes the new cell count before checking gas.
    // A failed instruction therefore carries changed stack/count diagnostics.
    match meter.new_cells(1.into()) {
        Ok(()) => Ok(()),
        Err(gas::Error::OutOfGas) => {
            meter.created_cells += 1;
            Err(Error::OutOfGas)
        }
        Err(_) => unreachable!("new_cells performs only integer arithmetic and gas charging"),
    }
}
