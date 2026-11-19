// aefa_debug and DEBUG_INFO branches in aefa_fate/aefa_fate_op, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Continue,
    Break,
    StepIn,
    // Original status stores the engine stack but only observes its length.
    // Count gas, return-check and init frames as well as call frames.
    StepOver(usize),
    StepOut(usize),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub file: Value,
    pub line: Value,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub status: Status,
    pub location: Option<Location>,
    pub breakpoints: Vec<Location>,
    pub current_instruction: BigInt,
    pub variables: BTreeMap<Value, Vec<Operand>>,
    // Nearest caller first. A call made before DBG_LOC records None.
    pub call_stack: Vec<Option<Location>>,
    pub contract_names: BTreeMap<[u8; 32], Value>,
}
impl Default for Info {
    fn default() -> Self {
        Self {
            status: Status::Continue,
            location: None,
            breakpoints: vec![],
            current_instruction: 0.into(),
            variables: BTreeMap::new(),
            call_stack: vec![],
            contract_names: BTreeMap::new(),
        }
    }
}
impl Info {
    pub fn variable_register(&self, name: &Value) -> Option<&Operand> {
        self.variables.get(name)?.first()
    }
    pub fn contract_name(&self, key: [u8; 32]) -> Value {
        self.contract_names
            .get(&key)
            .cloned()
            .unwrap_or_else(|| Value::String(key.to_vec()))
    }
    pub(super) fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        limits = integer_budget(&self.current_instruction, limits)?;
        for location in self
            .location
            .iter()
            .chain(self.breakpoints.iter())
            .chain(self.call_stack.iter().flatten())
        {
            limits = value::check_limits(&location.file, limits)?;
            limits = value::check_limits(&location.line, limits)?;
        }
        if self.call_stack.len() > limits.max_depth {
            return Err(DataError::ResourceLimit);
        }
        // Count empty bindings, unnamed frames and contract keys as well.
        limits.max_nodes = limits
            .max_nodes
            .checked_sub(self.call_stack.len())
            .and_then(|n| n.checked_sub(self.variables.len()))
            .and_then(|n| n.checked_sub(self.contract_names.len()))
            .ok_or(DataError::ResourceLimit)?;
        for (name, registers) in &self.variables {
            limits = value::check_limits(name, limits)?;
            for register in registers {
                let value = match register {
                    Operand::Stack => &Value::Void,
                    Operand::Argument(v) | Operand::Variable(v) | Operand::Immediate(v) => v,
                };
                limits = value::check_limits(value, limits)?;
            }
        }
        for name in self.contract_names.values() {
            limits.max_bytes = limits
                .max_bytes
                .checked_sub(32)
                .ok_or(DataError::ResourceLimit)?;
            limits = value::check_limits(name, limits)?;
        }
        Ok(limits)
    }
    fn locate(&mut self, location: Location, frames: usize) {
        let breakpoint = self.breakpoints.contains(&location);
        self.location = Some(location);
        if (breakpoint && self.status == Status::Continue)
            || self.status == Status::StepIn
            || matches!(self.status, Status::StepOver(depth) if frames <= depth)
            || matches!(self.status, Status::StepOut(depth) if frames < depth)
        {
            self.status = Status::Break;
        }
    }
}
pub(crate) fn supported(op: Opcode) -> bool {
    matches!(
        op,
        Opcode::DbgLoc | Opcode::DbgDef | Opcode::DbgUndef | Opcode::DbgContract
    )
}
impl Cursor {
    // Debug mode is explicitly selected by the invocation, never by the Rust
    // build or host environment. Normal program entry leaves it disabled.
    pub fn enable_debug(&mut self, info: Info, limits: Limits) -> Result<(), DataError> {
        info.budget(limits)?;
        self.debug = Some(info);
        self.suspended = false;
        Ok(())
    }
    // execute/1 resumes from the debugger's instruction counter, which can
    // differ from the program PC after protected recovery. Retain that quirk.
    pub fn resume_debug(&mut self, code: &Code) -> Result<(), Exception> {
        let info = self
            .debug
            .as_ref()
            .ok_or(Exception::Native("function_clause"))?;
        let instruction = info
            .current_instruction
            .to_usize()
            .ok_or(Exception::Native("function_clause"))?;
        let instructions = block(code, self).ok_or(Exception::Native("badkey"))?;
        if instruction > instructions.len() {
            return Err(Exception::Native("function_clause"));
        }
        self.instruction = instruction;
        self.suspended = false;
        Ok(())
    }
    pub(super) fn debug_push(&mut self) {
        if let Some(info) = &mut self.debug {
            info.call_stack.insert(0, info.location.clone());
        }
    }
    pub(super) fn debug_pop(&mut self) {
        if let Some(info) = &mut self.debug
            && !info.call_stack.is_empty()
        {
            info.call_stack.remove(0);
        }
    }
    pub(super) fn debug_reset(&mut self) {
        if let Some(info) = &mut self.debug {
            info.current_instruction = 0.into();
        }
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: StoreAccess<S::Error>> Step<'_, '_, S, O, H> {
    pub(super) fn debug_instruction(
        &mut self,
        code: &Code,
        cursor: &mut Cursor,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        let preserve = if ins.opcode == Opcode::DbgUndef {
            let last = block(code, cursor)
                .and_then(|instructions| instructions.last())
                .ok_or_else(|| self.native("function_clause"))?;
            last.operands.len() == 1 && matches!(last.opcode, Opcode::Exit | Opcode::Abort)
        } else {
            false
        };
        let frames = cursor.frames.len();
        let info = cursor
            .debug
            .as_mut()
            .ok_or(Fault::Exception(Exception::UnknownInstruction))?;
        let Operand::Immediate(name) = &ins.operands[0] else {
            return Err(self.native("function_clause"));
        };
        match ins.opcode {
            Opcode::DbgLoc => {
                let Operand::Immediate(line) = &ins.operands[1] else {
                    return Err(self.native("function_clause"));
                };
                info.locate(
                    Location {
                        file: name.clone(),
                        line: line.clone(),
                    },
                    frames,
                );
            }
            Opcode::DbgDef => info
                .variables
                .entry(name.clone())
                .or_default()
                .insert(0, ins.operands[1].clone()),
            Opcode::DbgUndef if preserve => {}
            Opcode::DbgUndef => {
                let regs = info.variables.entry(name.clone()).or_default();
                if let Some(index) = regs.iter().position(|r| r == &ins.operands[1]) {
                    regs.remove(index);
                }
            }
            Opcode::DbgContract => {
                info.contract_names
                    .insert(self.context.current_contract, name.clone());
            }
            _ => unreachable!(),
        }
        info.budget(self.remaining)?;
        cursor.instruction += 1;
        Ok(())
    }
    pub(super) fn debug_after_instruction(
        &mut self,
        cursor: &mut Cursor,
    ) -> Result<(), Fault<S::Error>> {
        if cursor.returned || cursor.instruction == 0 {
            cursor.debug_reset();
        } else if let Some(info) = &mut cursor.debug {
            info.current_instruction = self.native_integer(&info.current_instruction + 1)?;
            info.budget(self.remaining)?;
            cursor.suspended = info.status == Status::Break;
        }
        Ok(())
    }
}
