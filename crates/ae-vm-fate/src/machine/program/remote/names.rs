// aefa_fate_op AENS hosts, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_codec::name_pointer::{Pointer, PointerValue};
use ae_types::IdTag;

#[derive(Debug, Clone)]
pub enum Read {
    Pointer(PointerValue),
    Name {
        owner: [u8; 32],
        expires: BigInt,
        pointers: Vec<Pointer>,
    },
}
#[derive(Debug, Clone)]
pub struct Ttl {
    pub relative: bool,
    pub value: Value,
}
#[derive(Debug, Clone)]
pub enum Pointee {
    Id(IdTag, ObjectPayload),
    Data(Vec<u8>),
}
#[derive(Debug, Clone)]
pub struct RawPointer {
    pub key: Vec<u8>,
    pub value: Pointee,
}
#[derive(Debug, Clone)]
pub enum Request {
    Preclaim {
        owner: Vec<u8>,
        commitment: [u8; 32],
    },
    Claim {
        owner: Vec<u8>,
        name: Vec<u8>,
        salt: BigInt,
        fee: BigInt,
    },
    Update {
        owner: Vec<u8>,
        hash: [u8; 32],
        ttl: Option<Ttl>,
        client_ttl: Option<Value>,
        pointers: Option<Vec<RawPointer>>,
    },
    Transfer {
        owner: Vec<u8>,
        hash: [u8; 32],
        recipient: Vec<u8>,
    },
    Revoke {
        owner: Vec<u8>,
        hash: [u8; 32],
    },
}
impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Preclaim { .. } => "aens_preclaim",
            Self::Claim { .. } => "aens_claim",
            Self::Update { .. } => "aens_update",
            Self::Transfer { .. } => "aens_transfer",
            Self::Revoke { .. } => "aens_revoke",
        }
    }
}
pub(super) fn supported(op: Opcode) -> bool {
    matches!(
        op,
        Opcode::AensResolve
            | Opcode::AensLookup
            | Opcode::AensPreclaim
            | Opcode::AensClaim
            | Opcode::AensUpdate
            | Opcode::AensTransfer
            | Opcode::AensRevoke
    )
}
fn variant(arities: &[u8], tag: usize, values: Vec<Value>) -> Value {
    Value::Variant {
        arities: arities.iter().map(|x| (*x).into()).collect(),
        tag,
        values,
    }
}
fn option(v: Option<Value>) -> Value {
    variant(&[0, 1], usize::from(v.is_some()), v.into_iter().collect())
}
fn address(key: [u8; 32]) -> Value {
    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(key.to_vec()))
}
fn optional<E>(v: &Value) -> Result<Option<&Value>, Fault<E>> {
    match v {
        Value::Variant {
            arities,
            tag: 0,
            values,
        } if arities.len() == 2 && values.is_empty() => Ok(None),
        Value::Variant {
            arities,
            tag: 1,
            values,
        } if arities.len() == 2 && values.len() == 1 => Ok(Some(&values[0])),
        _ => Err(Fault::Exception(Exception::Native("case_clause"))),
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    fn name_string(&self, v: &Value, expected: &'static str) -> Result<Vec<u8>, Fault<S::Error>> {
        if let Value::String(b) = v {
            Ok(b.clone())
        } else {
            Err(Fault::Abort(Reason::ValueType {
                value: v.clone(),
                expected,
            }))
        }
    }
    fn name_guard_variant(&self, v: &Value) -> Result<(), Fault<S::Error>> {
        if matches!(v, Value::Variant { .. }) {
            Ok(())
        } else {
            Err(Fault::Abort(Reason::ValueType {
                value: v.clone(),
                expected: "variant",
            }))
        }
    }
    fn name_pointers(&mut self, value: &Value) -> Result<Vec<RawPointer>, Fault<S::Error>> {
        // OTP26 maps:fold accepts a three-tuple as an iterator. A store-map
        // term reaches the pointer callback with its cache as the pointee;
        // that callback's function_clause is caught as bad_pointer.
        if matches!(value, Value::StoreMap { .. }) {
            return Err(self.oracle_error("aens_update", "bad_pointer"));
        }
        let Value::Map(map) = value else {
            return Err(self.native("badmap"));
        };
        let keys = map.keys().collect::<Vec<_>>();
        let order =
            self.executor
                .order
                .indices(&keys, crate::maps::Traversal::Fold, self.remaining)?;
        let arities = if self.context.vm_version == VmVersion::Two {
            vec![1.into(); 4]
        } else {
            vec![1.into(); 5]
        };
        let mut pointers = Vec::new();
        for index in order {
            let key = keys[index];
            let p = &map[key];
            let Value::Variant {
                arities: a,
                tag,
                values,
            } = p
            else {
                return Err(self.oracle_error("aens_update", "bad_pointer"));
            };
            if *a != arities || values.len() != 1 {
                return Err(self.oracle_error("aens_update", "bad_pointer"));
            }
            // mk_pointer_id runs before aens_pointer:new checks the key.
            let value = match (&values[0], *tag) {
                (Value::Object(ObjectKind::Address, payload), 0..=3) => {
                    let t = [
                        IdTag::Account,
                        IdTag::Oracle,
                        IdTag::Contract,
                        IdTag::Channel,
                    ][*tag];
                    // aeser_id's historical guard accepts any value for the
                    // first tags, but the final channel alternative checks size.
                    if t == IdTag::Channel
                        && !matches!(payload, ObjectPayload::Bytes(b) if b.len()==32)
                    {
                        return Err(self.native("illegal_val"));
                    }
                    Pointee::Id(t, payload.clone())
                }
                (Value::Bytes(b), 4)
                    if self.context.vm_version > VmVersion::Two && b.len() <= 1024 =>
                {
                    Pointee::Data(b.clone())
                }
                _ => return Err(self.oracle_error("aens_update", "bad_pointer")),
            };
            let Value::String(key) = key else {
                return Err(self.oracle_error("aens_update", "bad_pointer"));
            };
            pointers.push(RawPointer {
                key: key.clone(),
                value,
            });
        }
        pointers.reverse();
        Ok(pointers)
    }
    pub(super) fn name_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        if matches!(ins.opcode, AensLookup | AensUpdate) && self.context.vm_version < VmVersion::Two
        {
            return Err(Fault::Abort(Reason::Unavailable(
                if ins.opcode == AensLookup {
                    "aens_lookup"
                } else {
                    "aens_update"
                },
            )));
        }
        let read = matches!(ins.opcode, AensResolve | AensLookup);
        let args = ins.operands[usize::from(read)..]
            .iter()
            .map(|a| self.read(a))
            .collect::<Result<Vec<_>, _>>()?;
        if read {
            return self.name_read_instruction(session, ins, &args);
        }
        let signature = self.oracle_signature(&args[0])?;
        let commitment = if ins.opcode == AensPreclaim {
            match &args[2] {
                Value::Bytes(b) if b.len() == 32 => {
                    Some(b.as_slice().try_into().expect("size checked"))
                }
                v => {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v.clone(),
                        expected: "bytes32",
                    }));
                }
            }
        } else {
            None
        };
        let owner = self.oracle_key(&args[1], ObjectKind::Address, "address")?;
        let req = match ins.opcode {
            AensPreclaim => {
                self.delegation(session, "aens_preclaim", &owner, None, signature)?;
                Request::Preclaim {
                    owner,
                    commitment: commitment.expect("preclaim"),
                }
            }
            AensClaim => {
                let name = self.name_string(&args[2], "string")?;
                let salt = self.oracle_integer(&args[3])?;
                let fee = self.oracle_integer(&args[4])?;
                // The original `not Integer >= 0` guards fail for integers.
                let hash = session
                    .host
                    .name_hash("aens_claim", &name, self.remaining)
                    .map_err(Self::host_fault)?;
                self.delegation(session, "aens_claim", &owner, Some(&hash), signature)?;
                Request::Claim {
                    owner,
                    name,
                    salt,
                    fee,
                }
            }
            AensTransfer | AensRevoke => {
                let transfer = ins.opcode == AensTransfer;
                let recipient = if transfer {
                    Some(self.oracle_key(&args[2], ObjectKind::Address, "address")?)
                } else {
                    None
                };
                let op = if transfer {
                    "aens_transfer"
                } else {
                    "aens_revoke"
                };
                let name = self.name_string(
                    &args[if transfer { 3 } else { 2 }],
                    if transfer { "string" } else { "name" },
                )?;
                let hash = session
                    .host
                    .name_hash(op, &name, self.remaining)
                    .map_err(Self::host_fault)?;
                self.delegation(session, op, &owner, Some(&hash), signature)?;
                if let Some(recipient) = recipient {
                    Request::Transfer {
                        owner,
                        hash,
                        recipient,
                    }
                } else {
                    Request::Revoke { owner, hash }
                }
            }
            AensUpdate => {
                let name = self.name_string(&args[2], "string")?;
                for v in &args[3..] {
                    self.name_guard_variant(v)?;
                }
                let ttl = match optional(&args[3])? {
                    None => None,
                    Some(Value::Variant {
                        arities,
                        tag,
                        values,
                    }) if arities == &[1.into(), 1.into()] && *tag <= 1 && values.len() == 1 => {
                        Some(Ttl {
                            relative: *tag == 0,
                            value: values[0].clone(),
                        })
                    }
                    _ => return Err(self.native("case_clause")),
                };
                let client_ttl = optional(&args[4])?.cloned();
                let pointers = optional(&args[5])?
                    .map(|v| self.name_pointers(v))
                    .transpose()?;
                let hash = session
                    .host
                    .name_hash("aens_update", &name, self.remaining)
                    .map_err(Self::host_fault)?;
                self.delegation(session, "aens_update", &owner, Some(&hash), signature)?;
                Request::Update {
                    owner,
                    hash,
                    ttl,
                    client_ttl,
                    pointers,
                }
            }
            _ => unreachable!(),
        };
        let mut host = session.host.clone();
        let gas = host
            .name_write(
                &req,
                self.state.meter.protocol,
                self.context.vm_version,
                self.remaining,
            )
            .map_err(Self::host_fault)?;
        if matches!(req, Request::Claim { .. }) {
            if gas <= 0.into() {
                return Err(self.native("function_clause"));
            }
            self.spend(gas)?;
        }
        session.host = host;
        Ok(())
    }
    fn name_read_instruction<H: Host<S::Error>>(
        &mut self,
        session: &mut Session<S, H>,
        ins: &Instruction,
        args: &[Value],
    ) -> Result<(), Fault<S::Error>> {
        let name = self.name_string(&args[0], "string")?;
        let pointer = if ins.opcode == Opcode::AensResolve {
            let key = self.name_string(&args[1], "string")?;
            if !matches!(args[2], Value::TypeRep(_)) {
                return Err(Fault::Abort(Reason::ValueType {
                    value: args[2].clone(),
                    expected: "typerep",
                }));
            }
            Some(key)
        } else {
            None
        };
        let result = match session
            .host
            .name_read(&name, pointer.as_deref(), self.remaining)
        {
            Ok(v) => v,
            Err(HostFailure::NameEncoding { .. }) if self.context.vm_version >= VmVersion::Two => {
                None
            }
            Err(HostFailure::NameEncoding { .. }) => return Err(self.native("case_clause")),
            Err(e) => return Err(Self::host_fault(e)),
        };
        let value = match result {
            None => option(None),
            Some(Read::Pointer(p)) => {
                let Value::TypeRep(ty) = &args[2] else {
                    unreachable!()
                };
                let inner = match ty {
                    Type::Variant(a) if a.len() == 2 && a[0] == Type::Tuple(vec![]) => {
                        match &a[1] {
                            Type::Tuple(a) if a.len() == 1 => &a[0],
                            _ => return Err(self.oracle_error("aens_resolve", "bad_type")),
                        }
                    }
                    _ => return Err(self.oracle_error("aens_resolve", "bad_type")),
                };
                let v = if *inner == Type::String {
                    Some(Value::String(match p {
                        PointerValue::Id(id) => id.value.to_vec(),
                        PointerValue::Data(b) => b,
                    }))
                } else {
                    match p {
                        PointerValue::Id(id) => match id.tag {
                            IdTag::Account => Some(address(id.value)),
                            IdTag::Oracle => Some(Value::Object(
                                ObjectKind::Oracle,
                                ObjectPayload::Bytes(id.value.to_vec()),
                            )),
                            IdTag::Contract => Some(Value::Object(
                                ObjectKind::Contract,
                                ObjectPayload::Bytes(id.value.to_vec()),
                            )),
                            _ => None,
                        },
                        PointerValue::Data(_) => None,
                    }
                };
                if let Some(v) = v {
                    let some = option(Some(v));
                    if types::check_type(ty, &some, self.context.vm_version, self.remaining)?
                        .is_some()
                    {
                        some
                    } else {
                        option(None)
                    }
                } else {
                    option(None)
                }
            }
            Some(Read::Name {
                owner,
                expires,
                pointers,
            }) => {
                let mut map = BTreeMap::new();
                for p in pointers {
                    let (tag, v) = match p.value {
                        PointerValue::Id(id) => {
                            let tag = match id.tag {
                                IdTag::Account => 0,
                                IdTag::Oracle => 1,
                                IdTag::Contract => 2,
                                IdTag::Channel => 3,
                                _ => return Err(self.native("case_clause")),
                            };
                            (tag, address(id.value))
                        }
                        PointerValue::Data(b) => {
                            if self.context.vm_version == VmVersion::Two {
                                return Err(self
                                    .oracle_error("aens_lookup", "data_pointer_not_in_fate_vm_2"));
                            }
                            (4, Value::Bytes(b))
                        }
                    };
                    map.insert(
                        Value::String(p.key),
                        variant(
                            if self.context.vm_version == VmVersion::Two {
                                &[1, 1, 1, 1]
                            } else {
                                &[1, 1, 1, 1, 1]
                            },
                            tag,
                            vec![v],
                        ),
                    );
                }
                option(Some(variant(
                    &[3],
                    0,
                    vec![
                        address(owner),
                        variant(&[1, 1], 1, vec![Value::Integer(expires)]),
                        Value::Map(map),
                    ],
                )))
            }
        };
        self.check(&value)?;
        self.write(&ins.operands[0], value)
    }
}
