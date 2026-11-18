// aefa_fate_op chain context and log instructions, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consensus {
    Pow,
    Pos,
}
#[derive(Debug, Clone)]
pub struct Environment {
    pub auth_tx_hash: Option<Vec<u8>>,
    pub origin: [u8; 32],
    pub gas_price: BigInt,
    pub fee: BigInt,
    pub beneficiary: [u8; 32],
    pub timestamp: BigInt,
    pub difficulty: BigInt,
    pub block_gas_limit: BigInt,
    pub network_id: Vec<u8>,
    pub consensus: Consensus,
    pub key_hash: [u8; 32],
}
impl Environment {
    pub(super) fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        if let Some(hash) = &self.auth_tx_hash {
            limits.max_bytes = limits
                .max_bytes
                .checked_sub(hash.len())
                .ok_or(DataError::ResourceLimit)?;
        }
        limits.max_bytes = limits
            .max_bytes
            .checked_sub(self.network_id.len())
            .ok_or(DataError::ResourceLimit)?;
        for n in [
            &self.gas_price,
            &self.fee,
            &self.timestamp,
            &self.difficulty,
            &self.block_gas_limit,
        ] {
            limits = integer_budget(n, limits)?;
        }
        Ok(limits)
    }
}
#[derive(Debug, Clone, Copy)]
pub enum AddressTest {
    Oracle,
    Contract,
    Payable,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log {
    pub contract: [u8; 32],
    // Original object topics retain their payload even if it is malformed.
    pub topics: Vec<ObjectPayload>,
    pub payload: Vec<u8>,
}
impl Log {
    pub(super) fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        limits.max_nodes = limits
            .max_nodes
            .checked_sub(1)
            .ok_or(DataError::ResourceLimit)?;
        limits.max_bytes = limits
            .max_bytes
            .checked_sub(self.payload.len())
            .and_then(|n| n.checked_sub(32))
            .ok_or(DataError::ResourceLimit)?;
        for topic in &self.topics {
            limits = value::check_payload_limits(topic, limits)?;
        }
        Ok(limits)
    }
}
pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        Balance
            | BalanceOther
            | Creator
            | Origin
            | Gasprice
            | Fee
            | Blockhash
            | Beneficiary
            | Timestamp
            | Generation
            | Microblock
            | NetworkId
            | Difficulty
            | Gaslimit
            | Gas
            | Log0
            | Log1
            | Log2
            | Log3
            | Log4
            | IsOracle
            | IsContract
            | IsPayable
            | Deactivate
    )
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn context_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let a = &ins.operands;
        let result = match ins.opcode {
            Microblock | Deactivate => {
                return Err(if self.state.meter.protocol >= 5 {
                    Fault::Abort(Reason::NotImplemented("microblock"))
                } else {
                    Fault::Exception(Exception::Exit("op_not_implemented_yet"))
                });
            }
            Log0 | Log1 | Log2 | Log3 | Log4 => return self.log_instruction(session, a),
            Gas => Value::Integer(self.state.meter.gas.clone()),
            Generation => Value::Integer(session.host.height().into()),
            Origin => address(session.host.environment().origin),
            Gasprice => Value::Integer(session.host.environment().gas_price.clone()),
            Fee => {
                if self.context.vm_version < VmVersion::Two {
                    return Err(Fault::Abort(Reason::Unavailable("fee")));
                }
                Value::Integer(session.host.environment().fee.clone())
            }
            NetworkId => {
                if self.context.vm_version < VmVersion::Three {
                    return Err(Fault::Abort(Reason::Unavailable("network_id")));
                }
                Value::String(session.host.environment().network_id.clone())
            }
            Beneficiary => {
                if session.host.environment().consensus == Consensus::Pos {
                    return Err(Fault::Abort(Reason::DisabledName("Chain.coinbase")));
                }
                address(session.host.environment().beneficiary)
            }
            Timestamp => Value::Integer(session.host.environment().timestamp.clone()),
            Difficulty => {
                Value::Integer(if session.host.environment().consensus == Consensus::Pos {
                    0.into()
                } else {
                    session.host.environment().difficulty.clone()
                })
            }
            Gaslimit => Value::Integer(session.host.environment().block_gas_limit.clone()),
            Creator => {
                let key = self.context.current_contract;
                let creator = match session.creator_cache.get(&key) {
                    Some(k) => *k,
                    None => {
                        let k = session.host.creator(key).map_err(Self::host_fault)?;
                        self.remaining.max_nodes = self
                            .remaining
                            .max_nodes
                            .checked_sub(1)
                            .ok_or(DataError::ResourceLimit)?;
                        self.remaining.max_bytes = self
                            .remaining
                            .max_bytes
                            .checked_sub(64)
                            .ok_or(DataError::ResourceLimit)?;
                        session.creator_cache.insert(key, k);
                        k
                    }
                };
                address(creator)
            }
            Balance => {
                let n = session
                    .host
                    .balance(&self.context.current_contract)
                    .map_err(Self::host_fault)?
                    .ok_or_else(|| self.native("badmatch"))?;
                Value::Integer(n)
            }
            BalanceOther | IsOracle | IsContract | IsPayable => {
                let v = self.read(&a[1])?;
                // BALANCE_OTHER pattern-matches the address tag without the
                // binary guard used by address_is_x. Preserve list payloads.
                let Value::Object(ObjectKind::Address, payload) = &v else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "address",
                    }));
                };
                let ObjectPayload::Bytes(key) = payload else {
                    return Err(if ins.opcode == BalanceOther {
                        self.native("function_clause")
                    } else {
                        Fault::Abort(Reason::ValueType {
                            value: v,
                            expected: "address",
                        })
                    });
                };
                if ins.opcode == BalanceOther {
                    let before = session.host.clone();
                    match session.host.balance(key).map_err(Self::host_fault)? {
                        Some(n) => Value::Integer(n),
                        None => {
                            self.state = self.entry.clone();
                            session.host = before;
                            Value::Integer(0.into())
                        }
                    }
                } else {
                    let what = match ins.opcode {
                        IsOracle => AddressTest::Oracle,
                        IsContract => AddressTest::Contract,
                        _ => AddressTest::Payable,
                    };
                    Value::Boolean(
                        session
                            .host
                            .address_test(what, key)
                            .map_err(Self::host_fault)?,
                    )
                }
            }
            Blockhash => {
                let v = self.read(&a[1])?;
                let Value::Integer(n) = v else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "integer",
                    }));
                };
                let current = BigInt::from(session.host.height());
                if n.sign() == Sign::Minus
                    || n > current
                    || n <= &current - 256
                    || (n == current
                        && (self.context.vm_version < VmVersion::Two
                            || session.host.environment().consensus == Consensus::Pos))
                {
                    option(None)
                } else {
                    let height = n.to_u64().ok_or_else(|| self.native("system_limit"))?;
                    let hash = session
                        .host
                        .block_hash(height, self.context.vm_version, self.remaining)
                        .map_err(Self::host_fault)?;
                    option(Some(Value::Bytes(hash.to_vec())))
                }
            }
            _ => unreachable!(),
        };
        self.check(&result)?;
        self.write(&a[0], result)
    }
    fn log_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        operands: &[Operand],
    ) -> Result<(), Fault<S::Error>> {
        let args = operands
            .iter()
            .map(|a| self.read(a))
            .collect::<Result<Vec<_>, _>>()?;
        let payload = match &args[0] {
            Value::String(b) | Value::Bytes(b) => b.clone(),
            v => {
                return Err(Fault::Exception(Exception::DetachedValueType {
                    value: v.clone(),
                    expected: "string",
                }));
            }
        };
        let mut topics = Vec::new();
        for v in &args[1..] {
            let topic = match v {
                Value::Integer(n) | Value::Bits(n)
                    if n.sign() != Sign::Minus && n.bits() <= 256 =>
                {
                    let bytes = n.to_bytes_be().1;
                    let mut out = vec![0; 32 - bytes.len()];
                    out.extend(bytes);
                    ObjectPayload::Bytes(out)
                }
                Value::Boolean(b) => ObjectPayload::Bytes({
                    let mut out = vec![0; 32];
                    out[31] = u8::from(*b);
                    out
                }),
                Value::Bytes(b) if b.len() <= 32 => {
                    let mut out = vec![0; 32 - b.len()];
                    out.extend(b);
                    ObjectPayload::Bytes(out)
                }
                Value::Object(
                    ObjectKind::Address
                    | ObjectKind::Contract
                    | ObjectKind::Oracle
                    | ObjectKind::OracleQuery,
                    p,
                ) => p.clone(),
                _ => {
                    self.state = self.entry.clone();
                    return Err(Fault::Abort(match v {
                        Value::Integer(n) => Reason::LogIllegalInt(n.clone()),
                        Value::Bits(_) | Value::BitsNil => Reason::LogIllegalBits,
                        _ => Reason::ValueType {
                            value: v.clone(),
                            expected: "word",
                        },
                    }));
                }
            };
            topics.push(topic);
        }
        let size = BigInt::from(topics.len()) * 8 + payload.len();
        let charge = self.native_integer(size * 20)?;
        let log = Log {
            contract: self.context.current_contract,
            topics,
            payload,
        };
        self.remaining = log.budget(self.remaining)?;
        // Logs are outside return-check snapshots. A later protected failure
        // retains earlier log entries; this instruction's OOG retains its log.
        session.logs.insert(0, log);
        if charge == BigInt::from(0) {
            return Err(self.native("function_clause"));
        }
        self.spend(charge)
    }
}
