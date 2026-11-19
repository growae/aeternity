// Oracle write instructions and delegation, aefa_fate_op.erl fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::{encoding, gas::CostModel};
use ae_codec::rlp::{self, Rlp};
use ae_types::{Id, IdTag};

#[derive(Debug, Clone)]
pub struct Ttl {
    pub relative: bool,
    pub value: BigInt,
}
#[derive(Debug, Clone)]
pub enum Request {
    Register {
        key: Vec<u8>,
        query_format: Vec<u8>,
        response_format: Vec<u8>,
        fee: BigInt,
        ttl: Ttl,
    },
    Extend {
        key: Vec<u8>,
        ttl: Ttl,
    },
    Query {
        key: Vec<u8>,
        sender: [u8; 32],
        content: Value,
        fee: BigInt,
        query_ttl: Ttl,
        response_ttl: BigInt,
        question_type: Value,
        response_type: Value,
    },
    Respond {
        key: Vec<u8>,
        query: Vec<u8>,
        content: Value,
        question_type: Value,
        response_type: Value,
    },
}
impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Register { .. } => "oracle_register",
            Self::Extend { .. } => "oracle_extend",
            Self::Query { .. } => "oracle_query",
            Self::Respond { .. } => "oracle_respond",
        }
    }
}
#[derive(Debug)]
pub struct ResultValue {
    pub value: Option<Value>,
    pub gas: BigInt,
}
pub(super) fn supported(op: Opcode) -> bool {
    matches!(
        op,
        Opcode::OracleRegister | Opcode::OracleQuery | Opcode::OracleRespond | Opcode::OracleExtend
    )
}
fn ttl(v: &Value, absolute: bool) -> Option<Ttl> {
    let Value::Variant {
        arities,
        tag,
        values,
    } = v
    else {
        return None;
    };
    if arities != &[1.into(), 1.into()] || *tag > usize::from(absolute) {
        return None;
    }
    let [Value::Integer(value)] = values.as_slice() else {
        return None;
    };
    Some(Ttl {
        relative: *tag == 0,
        value: value.clone(),
    })
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: Host<S::Error>> Step<'_, '_, S, O, H> {
    pub(super) fn oracle_error(
        &self,
        operation: &'static str,
        reason: &'static str,
    ) -> Fault<S::Error> {
        Fault::Abort(Reason::Primop { operation, reason })
    }
    pub(super) fn oracle_key(
        &self,
        v: &Value,
        kind: ObjectKind,
        expected: &'static str,
    ) -> Result<Vec<u8>, Fault<S::Error>> {
        match v {
            Value::Object(k, ObjectPayload::Bytes(b)) if *k == kind => Ok(b.clone()),
            _ => Err(Fault::Abort(Reason::ValueType {
                value: v.clone(),
                expected,
            })),
        }
    }
    pub(super) fn oracle_integer(&self, v: &Value) -> Result<BigInt, Fault<S::Error>> {
        match v {
            Value::Integer(n) => Ok(n.clone()),
            _ => Err(Fault::Abort(Reason::ValueType {
                value: v.clone(),
                expected: "integer",
            })),
        }
    }
    pub(super) fn oracle_signature(&self, v: &Value) -> Result<[u8; 64], Fault<S::Error>> {
        if let Value::Bytes(b) = v
            && let Ok(s) = b.as_slice().try_into()
        {
            return Ok(s);
        }
        Err(Fault::Abort(Reason::ValueType {
            value: v.clone(),
            expected: "bytes64",
        }))
    }
    fn oracle_types(
        &self,
        q: &Value,
        r: &Value,
        name: &'static str,
        both: bool,
    ) -> Result<(), Fault<S::Error>> {
        let a = matches!(q, Value::TypeRep(_));
        let b = matches!(r, Value::TypeRep(_));
        if if both { a && b } else { a || b } {
            Ok(())
        } else {
            Err(self.oracle_error(name, if both { "bad_types" } else { "bad_type" }))
        }
    }
    pub(super) fn delegation(
        &mut self,
        _session: &mut Session<S, H>,
        name: &'static str,
        key: &[u8],
        query: Option<&[u8]>,
        signature: [u8; 64],
    ) -> Result<(), Fault<S::Error>> {
        let current = self.context.current_contract;
        if key == current {
            return Ok(());
        }
        self.spend(
            Opcode::VerifySig
                .gas_at_protocol(self.state.meter.protocol)
                .into(),
        )?;
        let network = &self.host.environment().network_id;
        let aens = name.starts_with("aens_");
        let preclaim = name == "aens_preclaim";
        let messages = if self.context.vm_version < VmVersion::Three {
            let parts: Vec<&[u8]> = if aens && !preclaim {
                vec![network, key, query.expect("name hash"), &current]
            } else {
                vec![network, query.unwrap_or(key), &current]
            };
            vec![join_message(&parts, &mut self.remaining)?]
        } else {
            let typed = |tag, raw: &[u8]| -> Result<Vec<u8>, Fault<S::Error>> {
                Ok(Id::new(
                    tag,
                    raw.try_into().map_err(|_| self.native("illegal_field"))?,
                )
                .to_bytes()
                .to_vec())
            };
            let contract = typed(IdTag::Contract, &current)?;
            let account = if aens || query.is_none() {
                typed(IdTag::Account, key)?
            } else {
                vec![]
            };
            let mut forms = Vec::new();
            if aens {
                if preclaim {
                    forms.push((3, vec![account.clone(), contract.clone()]));
                } else {
                    forms.push((
                        2,
                        vec![
                            account.clone(),
                            typed(IdTag::Name, query.expect("name hash"))?,
                            contract.clone(),
                        ],
                    ));
                }
                forms.push((1, vec![account, contract]));
            } else if let Some(query) = query {
                forms.push((5, vec![typed(IdTag::Oracle, query)?, contract]));
            } else {
                forms.push((4, vec![account, contract]));
            }
            let mut messages = Vec::new();
            for (tag, fields) in forms {
                let tag_bytes = [tag];
                let mut values = vec![Rlp::Bytes(&tag_bytes), Rlp::Bytes(&[1])];
                values.extend(fields.iter().map(|b| Rlp::Bytes(b)));
                let encoded = rlp::encode(&Rlp::List(values));
                messages.push(join_message(
                    &[&[0x1a, 0x01], network, &encoded],
                    &mut self.remaining,
                )?);
            }
            messages
        };
        for message in messages {
            if self
                .host
                .verify_delegation(key, &message, &signature, self.context.vm_version)
                .map_err(Self::host_fault)?
            {
                return Ok(());
            }
        }
        Err(self.oracle_error(name, "bad_signature"))
    }

    pub(super) fn oracle_write(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let output = matches!(ins.opcode, OracleRegister | OracleQuery);
        let args = ins.operands[usize::from(output)..]
            .iter()
            .map(|a| self.read(a))
            .collect::<Result<Vec<_>, _>>()?;
        let name = match ins.opcode {
            OracleRegister => "oracle_register",
            OracleQuery => "oracle_query",
            OracleRespond => "oracle_respond",
            OracleExtend => "oracle_extend",
            _ => unreachable!(),
        };
        let req = match ins.opcode {
            OracleRegister => {
                let key = self.oracle_key(&args[1], ObjectKind::Address, "address")?;
                let signature = self.oracle_signature(&args[0])?;
                let fee = self.oracle_integer(&args[2])?;
                if !matches!(args[3], Value::Variant { .. }) {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: args[2].clone(),
                        expected: "ttl",
                    }));
                }
                self.oracle_types(&args[4], &args[5], name, true)?;
                let ttl = ttl(&args[3], true).ok_or_else(|| self.oracle_error(name, "bad_ttl"))?;
                let query_format =
                    encoding::encode_for_vm(&args[4], self.remaining).map_err(|e| {
                        if e.error == DataError::ResourceLimit {
                            Fault::Operational(Error::Data(e.error))
                        } else {
                            self.native(e.native_exception)
                        }
                    })?;
                let response_format =
                    encoding::encode_for_vm(&args[5], self.remaining).map_err(|e| {
                        if e.error == DataError::ResourceLimit {
                            Fault::Operational(Error::Data(e.error))
                        } else {
                            self.native(e.native_exception)
                        }
                    })?;
                self.delegation(session, name, &key, None, signature)?;
                Request::Register {
                    key,
                    query_format,
                    response_format,
                    fee,
                    ttl,
                }
            }
            OracleExtend => {
                let key = self.oracle_key(&args[1], ObjectKind::Oracle, "oracle")?;
                let signature = self.oracle_signature(&args[0])?;
                let Some(ttl) = ttl(&args[2], false) else {
                    self.state = self.entry.clone();
                    self.host = self.entry_host.clone();
                    return Err(self.oracle_error(name, "bad_ttl"));
                };
                self.delegation(session, name, &key, None, signature)?;
                Request::Extend { key, ttl }
            }
            OracleQuery => {
                let key = self.oracle_key(&args[0], ObjectKind::Oracle, "oracle")?;
                let fee = self.oracle_integer(&args[2])?;
                // Original `not Fee >= 0` parses as `(not Fee) >= 0`.
                // For an integer the guard fails, so negative fees reach the
                // later primitive constructor instead of too_low_fee here.
                self.oracle_types(&args[5], &args[6], name, false)?;
                let response_ttl = ttl(&args[4], false)
                    .ok_or_else(|| self.oracle_error(name, "bad_ttl"))?
                    .value;
                let query_ttl =
                    ttl(&args[3], true).ok_or_else(|| self.oracle_error(name, "bad_ttl"))?;
                let content = if self.state.meter.protocol >= 5 {
                    self.unfold(args[1].clone(), CostModel::UnfoldSerial)?
                } else {
                    args[1].clone()
                };
                Request::Query {
                    key,
                    sender: self.context.current_contract,
                    content,
                    fee,
                    query_ttl,
                    response_ttl,
                    question_type: args[5].clone(),
                    response_type: args[6].clone(),
                }
            }
            OracleRespond => {
                let key = self.oracle_key(&args[1], ObjectKind::Oracle, "oracle")?;
                let signature = self.oracle_signature(&args[0])?;
                let query = self.oracle_key(&args[2], ObjectKind::OracleQuery, "oracle_query")?;
                self.oracle_types(&args[4], &args[5], name, false)?;
                let content = if self.state.meter.protocol >= 5 {
                    self.unfold(args[3].clone(), CostModel::UnfoldSerial)?
                } else {
                    args[3].clone()
                };
                self.delegation(session, name, &key, Some(&query), signature)?;
                Request::Respond {
                    key,
                    query,
                    content,
                    question_type: args[4].clone(),
                    response_type: args[5].clone(),
                }
            }
            _ => unreachable!(),
        };
        let mut staged = self.host.clone();
        let result = staged
            .oracle_write(&req, self.state.meter.protocol, self.remaining)
            .map_err(Self::host_fault)?;
        // Register/query publish before dynamic gas; respond/extend publish
        // after it. Protected recovery subsequently restores its own snapshot.
        if output {
            self.host = staged.clone();
        }
        if result.gas <= 0.into() {
            return Err(self.native("function_clause"));
        }
        self.spend(result.gas)?;
        if !output {
            self.host = staged;
        }
        if let Some(value) = result.value {
            self.write(&ins.operands[0], value)?;
        }
        Ok(())
    }
}

fn join_message<E>(parts: &[&[u8]], limits: &mut Limits) -> Result<Vec<u8>, Fault<E>> {
    let len = parts
        .iter()
        .try_fold(0usize, |n, p| n.checked_add(p.len()))
        .ok_or(DataError::ResourceLimit)?;
    limits.max_bytes = limits
        .max_bytes
        .checked_sub(len)
        .ok_or(DataError::ResourceLimit)?;
    Ok(parts.concat())
}
