// aefa_fate_op host instructions, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::code::CodeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleOperation {
    Question,
    Answer,
    Fee,
    Expiry,
    Check,
    CheckQuery,
}
impl OracleOperation {
    pub fn name(self) -> &'static str {
        match self {
            Self::Question => "oracle_get_question",
            Self::Answer => "oracle_get_answer",
            Self::Fee => "oracle_query_fee",
            Self::Expiry => "oracle_expiry",
            Self::Check => "oracle_check",
            Self::CheckQuery => "oracle_check_query",
        }
    }
}
#[derive(Clone, Copy)]
pub struct OracleRead<'a> {
    pub operation: OracleOperation,
    pub oracle: &'a [u8],
    pub query: Option<&'a [u8]>,
    pub question_type: Option<&'a Value>,
    pub response_type: Option<&'a Value>,
}
pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        Spend
            | AuthTxHash
            | BytecodeHash
            | OracleGetQuestion
            | OracleGetAnswer
            | OracleQueryFee
            | OracleExpiry
            | OracleCheck
            | OracleCheckQuery
    )
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn host_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let a = &ins.operands;
        match ins.opcode {
            Spend => {
                let recipient = self.read(&a[0])?;
                let Value::Object(ObjectKind::Address, payload) = recipient else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: recipient,
                        expected: "address",
                    }));
                };
                let amount = self.read(&a[1])?;
                let Value::Integer(amount) = amount else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: amount,
                        expected: "integer",
                    }));
                };
                let ObjectPayload::Bytes(to) = payload else {
                    return Err(self.native("function_clause"));
                };
                session
                    .host
                    .spend(self.context.current_contract, &to, &amount)
                    .map_err(Self::host_fault)
            }
            AuthTxHash => {
                let hash = session.host.environment().auth_tx_hash.clone();
                self.cells(if hash.is_some() { 4.into() } else { 3.into() })?;
                let result = option(hash.map(Value::Bytes));
                self.check(&result)?;
                self.write(&a[0], result)
            }
            BytecodeHash => {
                if self.context.vm_version < VmVersion::Two {
                    return Err(Fault::Abort(Reason::Unavailable("bytecode_hash")));
                }
                let v = self.read(&a[1])?;
                let Value::Object(ObjectKind::Contract, payload) = v else {
                    return Err(self.native("badmatch"));
                };
                let ObjectPayload::Bytes(key) = payload else {
                    // The engine charges a cache miss before the chain host
                    // rejects a nonbinary contract payload.
                    self.code_fetch_gas()?;
                    return Err(self.native("function_clause"));
                };
                let result = match self.fetch_code(session, &key)? {
                    None => option(None),
                    Some(code) => {
                        if code.vm_version < VmVersion::Two {
                            return Err(self.native("case_clause"));
                        }
                        let bytes = code.code.encode(self.remaining).map_err(|e| match e {
                            CodeError::Data(DataError::ResourceLimit) => {
                                Fault::Operational(Error::Data(DataError::ResourceLimit))
                            }
                            CodeError::InvalidFunctionId => self.native("illegal_function_id"),
                            CodeError::NoncontiguousBlocks => self.native("not_contiguous_labels"),
                            CodeError::EmptyBlock => self.native("empty_code_block"),
                            CodeError::WrongArity => self.native("wrong_nr_args_opcode"),
                            CodeError::WrongTerminator => self.native("wrong_opcode_in_bb"),
                            e => Fault::Exception(Exception::Host {
                                operation: "serialize_fate_code",
                                detail: format!("{e:?}"),
                            }),
                        })?;
                        self.remaining.max_bytes = self
                            .remaining
                            .max_bytes
                            .checked_sub(bytes.len())
                            .ok_or(DataError::ResourceLimit)?;
                        self.spend(bytes.len().into())?;
                        option(Some(Value::Bytes(
                            ae_crypto::blake2b_256(&bytes).0.to_vec(),
                        )))
                    }
                };
                self.write(&a[0], result)
            }
            _ => self.oracle_instruction(session, ins),
        }
    }
    fn oracle_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let operation = match ins.opcode {
            OracleGetQuestion => OracleOperation::Question,
            OracleGetAnswer => OracleOperation::Answer,
            OracleQueryFee => OracleOperation::Fee,
            OracleExpiry => OracleOperation::Expiry,
            OracleCheck => OracleOperation::Check,
            OracleCheckQuery => OracleOperation::CheckQuery,
            _ => unreachable!(),
        };
        if ins.opcode == OracleExpiry && self.context.vm_version < VmVersion::Two {
            return Err(Fault::Abort(Reason::Unavailable("oracle_expiry")));
        }
        let args = ins.operands[1..]
            .iter()
            .map(|a| self.read(a))
            .collect::<Result<Vec<_>, _>>()?;
        let scalar = matches!(operation, OracleOperation::Fee | OracleOperation::Expiry);
        let oracle = match &args[0] {
            Value::Object(ObjectKind::Oracle, ObjectPayload::Bytes(k)) => k,
            Value::Object(ObjectKind::Oracle, _) if scalar => {
                return Err(self.native("function_clause"));
            }
            v => {
                return Err(Fault::Abort(Reason::ValueType {
                    value: v.clone(),
                    expected: "oracle",
                }));
            }
        };
        let query = if matches!(
            operation,
            OracleOperation::Question | OracleOperation::Answer | OracleOperation::CheckQuery
        ) {
            match &args[1] {
                Value::Object(ObjectKind::OracleQuery, ObjectPayload::Bytes(k)) => {
                    Some(k.as_slice())
                }
                v => {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v.clone(),
                        expected: "oracle_query",
                    }));
                }
            }
        } else {
            None
        };
        let (q, r) = if scalar {
            (None, None)
        } else {
            let i = if query.is_some() { 2 } else { 1 };
            let q = &args[i];
            let r = &args[i + 1];
            // The source deliberately uses orelse here. Do not replace it
            // with stricter admission of both type representations.
            if !matches!(q, Value::TypeRep(_)) && !matches!(r, Value::TypeRep(_)) {
                return Err(Fault::Abort(Reason::Primop {
                    operation: if operation == OracleOperation::CheckQuery {
                        "oracle_check"
                    } else {
                        operation.name()
                    },
                    reason: "bad_type",
                }));
            }
            (Some(q), Some(r))
        };
        let result = session
            .host
            .oracle_read(
                OracleRead {
                    operation,
                    oracle,
                    query,
                    question_type: q,
                    response_type: r,
                },
                self.remaining,
            )
            .map_err(Self::host_fault)?;
        self.check(&result)?;
        self.write(&ins.operands[0], result)
    }
}
