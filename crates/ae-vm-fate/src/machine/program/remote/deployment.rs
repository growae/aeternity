// aefa_fate_op deployment and init continuations, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
#[derive(Debug, Clone)]
pub struct InitFrame {
    pub contract: [u8; 32],
    pub source: Value,
    pub amount: Value,
    pub protected: bool,
}
fn contract(key: [u8; 32]) -> Value {
    Value::Object(ObjectKind::Contract, ObjectPayload::Bytes(key.to_vec()))
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn deploy_contract<H: Host<S::Error>>(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        if self.context.vm_version < VmVersion::Two {
            return Err(Fault::Abort(Reason::Unavailable(match ins.opcode {
                Opcode::Create => "create",
                Opcode::Clone => "clone",
                _ => "clone_g",
            })));
        }
        let mut args = Vec::with_capacity(ins.operands.len());
        for op in &ins.operands {
            args.push(self.read(op)?);
        }
        let mut args = args.into_iter();
        let source = args.next().unwrap();
        if !matches!(
            (&source, ins.opcode),
            (Value::ContractBytearray(_), Opcode::Create)
                | (
                    Value::Object(ObjectKind::Contract, _),
                    Opcode::Clone | Opcode::CloneG
                )
        ) {
            return Err(self.native("badmatch"));
        }
        let types = args.next().unwrap();
        let amount = args.next().unwrap();
        let cap = if ins.opcode == Opcode::CloneG {
            args.next()
        } else {
            None
        };
        let protected = if ins.opcode == Opcode::Create {
            false
        } else {
            match args.next().unwrap() {
                Value::Boolean(b) => b,
                v => {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "bool",
                    }));
                }
            }
        };
        if let Value::ContractBytearray(bytes) = &source {
            self.spend(BigInt::from(bytes.len()) * 5u64)?;
        }
        let key = session
            .host
            .deploy(
                self.context.current_contract,
                &source,
                &amount,
                self.context.vm_version,
                self.state.meter.protocol,
                self.remaining,
            )
            .map_err(Self::host_fault)?;
        let frame = InitFrame {
            contract: key,
            source,
            amount,
            protected,
        };
        if !self.remote_call_common(
            cursor,
            session,
            contract(key),
            Value::String(INIT_ID.to_vec()),
            types,
            Value::TypeRep(Type::Tuple(vec![])),
            Value::Integer(0.into()),
            cap,
            protected,
            Some(frame.clone()),
        )? {
            self.remove_deployed(session, &frame)?;
            self.write(&Operand::Stack, option(None))?;
            cursor.instruction += 1;
        }
        Ok(())
    }
    fn remove_deployed<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        frame: &InitFrame,
    ) -> Result<(), Fault<S::Error>> {
        session
            .host
            .remove_deployment(
                frame.contract,
                self.context.current_contract,
                &frame.amount,
                self.remaining,
            )
            .map_err(Self::host_fault)?;
        session.code_cache.remove(&frame.contract);
        Ok(())
    }
    pub(super) fn finish_init<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        frame: &InitFrame,
    ) -> Result<(), Fault<S::Error>> {
        if matches!(&self.state.accumulator.top,Value::List(v) if v.is_empty()) {
            return Err(self.native("init_crashed_miserably"));
        }
        let result = self.read(&Operand::Stack)?;
        let next = if !frame.protected {
            if result != Value::Tuple(vec![]) {
                return Err(self.native("badmatch"));
            }
            contract(frame.contract)
        } else if result == option(Some(Value::Tuple(vec![]))) {
            option(Some(contract(frame.contract)))
        } else if result == option(None) {
            self.remove_deployed(session, frame)?;
            option(None)
        } else {
            return Err(self.native("case_clause"));
        };
        self.write(&Operand::Stack, next)
    }
}
