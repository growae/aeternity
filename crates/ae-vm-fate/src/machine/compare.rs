// aefa_fate_op:bin_comp/comp/comp_iris and aefa_fate:unfold_store_maps, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::{Budget, gas::CostModel, maps, store::vm};

pub(super) fn supported(op: Opcode) -> bool {
    matches!(
        op,
        Opcode::Lt | Opcode::Gt | Opcode::Elt | Opcode::Egt | Opcode::Eq | Opcode::Neq
    )
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: StoreAccess<S::Error>> Step<'_, '_, S, O, H> {
    pub(super) fn compare(
        &mut self,
        op: Opcode,
        operands: &[Operand],
    ) -> Result<(), Fault<S::Error>> {
        let left = self.read(&operands[1])?;
        let left = self.unfold(left, CostModel::UnfoldCompare)?;
        let right = self.read(&operands[2])?;
        let right = self.unfold(right, CostModel::UnfoldCompare)?;
        let result = if self.state.meter.protocol < 5 {
            // Exact Erlang equality does not invoke either ordering helper.
            match op {
                Opcode::Eq => left == right,
                Opcode::Neq => left != right,
                _ => {
                    let (order, remaining) =
                        maps::order::compare_counted(&left, &right, self.remaining)?;
                    self.remaining = remaining;
                    match op {
                        Opcode::Lt => order.is_lt(),
                        Opcode::Gt => order.is_gt(),
                        Opcode::Elt => !order.is_gt(),
                        Opcode::Egt => !order.is_lt(),
                        _ => unreachable!(),
                    }
                }
            }
        } else {
            match op {
                Opcode::Lt => self.less(&left, &right)?,
                Opcode::Gt => self.less(&right, &left)?,
                Opcode::Elt => !self.less(&right, &left)?,
                Opcode::Egt => !self.less(&left, &right)?,
                Opcode::Eq | Opcode::Neq => {
                    // Erlang `and` / `or` evaluate both sides, even if the
                    // first comparison already determines the boolean result.
                    let less = self.less(&left, &right)?;
                    let greater = self.less(&right, &left)?;
                    if op == Opcode::Eq {
                        !less & !greater
                    } else {
                        less | greater
                    }
                }
                _ => unreachable!(),
            }
        };
        self.write(&operands[0], Value::Boolean(result))
    }
    fn less(&mut self, left: &Value, right: &Value) -> Result<bool, Fault<S::Error>> {
        let mut budget = Budget::new(self.remaining);
        let result = value::cmp(left, right, &mut budget, 0);
        self.remaining.max_nodes -= budget.nodes;
        result.map(|order| order.is_lt()).map_err(|e| match e {
            DataError::ResourceLimit | DataError::InvalidOrderProfile => e.into(),
            _ => self.native("function_clause"),
        })
    }
    pub(super) fn unfold(
        &mut self,
        value: Value,
        model: CostModel,
    ) -> Result<Value, Fault<S::Error>> {
        let prepared = vm::prepare_unfold(
            self.state.meter.protocol,
            &value,
            self.state.meter.gas.clone(),
            self.executor.order,
            self.remaining,
            Some(self.native_capacity()),
        )
        .map_err(|e| match e {
            gas::Error::Data(DataError::InvalidValue | DataError::InvalidType) => {
                self.native("function_clause")
            }
            gas::Error::InvalidOrder => DataError::InvalidOrderProfile.into(),
            e => Self::gas_fault(e),
        })?;
        self.state.meter.gas = prepared.charge.gas.clone();
        self.remaining = prepared.charge.remaining_limits;
        if !prepared.has_maps {
            return Ok(value);
        }
        self.ensure_store()?;
        let result = self
            .state
            .stores
            .vm_scope(self.state.meter.protocol, self.context.current_contract)
            .map_err(|_| Fault::Operational(Error::ProtocolMismatch))?
            .unfold_prepared(value, prepared, model, self.executor.order)
            .map_err(|e| self.store_fault(e))?;
        self.state.meter.gas = result.charge.gas;
        self.remaining = result.charge.remaining_limits;
        Ok(result.value)
    }
}
