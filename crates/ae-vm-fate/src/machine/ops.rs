use super::*;
use crate::maps::{Map, MapError};
use num_traits::ToPrimitive;

pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    super::binary::supported(op)
        || super::bls::supported(op)
        || super::crypto::supported(op)
        || super::compare::supported(op)
        || super::numbers::supported(op)
        || matches!(
            op,
            Push | Pop
                | Store
                | Dupa
                | Dup
                | Nop
                | Inca
                | Inc
                | Deca
                | Dec
                | And
                | Or
                | Not
                | Tuple
                | Element
                | Setelement
                | MapEmpty
                | MapLookup
                | MapLookupd
                | MapUpdate
                | MapDelete
                | MapMember
                | MapFromList
                | MapSize
                | MapToList
                | Nil
                | IsNil
                | Cons
                | Hd
                | Tl
                | Length
                | Append
                | Jump
                | Jumpif
                | SwitchV2
                | SwitchV3
                | SwitchVn
                | Abort
                | Exit
        )
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: StoreAccess<S::Error>> Step<'_, '_, S, O, H> {
    pub(super) fn eval(&mut self, instruction: &Instruction) -> Result<Control, Fault<S::Error>> {
        use Opcode::*;
        let a = &instruction.operands;
        let op = instruction.opcode;
        match op {
            Nop => {}
            Push => {
                let v = self.read(&a[0])?;
                self.write(&Operand::Stack, v)?;
            }
            Pop => {
                let v = self.read(&Operand::Stack)?;
                self.write(&a[0], v)?;
            }
            Store => {
                let v = self.read(&a[1])?;
                self.write(&a[0], v)?;
            }
            Dupa | Dup => {
                let n = if op == Dupa {
                    Value::Integer(0.into())
                } else {
                    self.read(&a[0])?
                };
                let result =
                    self.state
                        .accumulator
                        .duplicate(&n, &mut self.state.meter, self.remaining);
                self.native_meter()?;
                result.map_err(Self::stack_fault)?;
            }
            Inca | Inc | Deca | Dec => {
                let to = if matches!(op, Inca | Deca) {
                    &Operand::Stack
                } else {
                    &a[0]
                };
                let v = self.read(to)?;
                let Value::Integer(n) = v else {
                    return Err(if self.state.meter.protocol < 5 {
                        self.native("badarith")
                    } else {
                        Fault::Abort(Reason::ValueType {
                            value: v,
                            expected: "integer",
                        })
                    });
                };
                let n = self.native_integer(n + if matches!(op, Inca | Inc) { 1 } else { -1 })?;
                self.write(to, Value::Integer(n))?;
            }
            And | Or => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                let (Value::Boolean(x), Value::Boolean(y)) = (&x, &y) else {
                    return Err(self.type_error(if op == And { "and" } else { "or" }, vec![x, y]));
                };
                self.write(
                    &a[0],
                    Value::Boolean(if op == And { *x && *y } else { *x || *y }),
                )?;
            }
            Not => {
                let v = self.read(&a[1])?;
                let Value::Boolean(b) = v else {
                    return Err(if self.state.meter.protocol < 5 {
                        self.native("badarg")
                    } else {
                        Fault::Abort(Reason::ValueType {
                            value: v,
                            expected: "boolean",
                        })
                    });
                };
                self.write(&a[0], Value::Boolean(!b))?;
            }
            Tuple => {
                let n = literal(&a[1]);
                let Value::Integer(n) = n else {
                    return Err(Fault::Abort(Reason::InvalidTupleSize(n.clone())));
                };
                if n.sign() == Sign::Minus {
                    return Err(Fault::Abort(Reason::InvalidTupleSize(Value::Integer(
                        n.clone(),
                    ))));
                }
                let size = n.to_usize().ok_or(DataError::ResourceLimit)?;
                let vs = self
                    .state
                    .accumulator
                    .pop_n(size, &mut self.state.meter, self.remaining);
                self.native_meter()?;
                let vs = vs.map_err(Self::stack_fault)?;
                self.cells(n + 2)?;
                self.write(&a[0], Value::Tuple(vs))?;
            }
            Element | Setelement => {
                let index = self.read(&a[1])?;
                let tuple = self.read(&a[2])?;
                let replacement = if op == Setelement {
                    Some(self.read(&a[3])?)
                } else {
                    None
                };
                let valid = matches!(&index,Value::Integer(n)if n.sign()!=Sign::Minus)
                    && matches!(&tuple, Value::Tuple(_));
                if !valid {
                    return Err(if op == Element {
                        self.entry_abort(Reason::Type {
                            operation: "element",
                            arguments: vec![index, tuple],
                        })
                    } else {
                        // Original abort/2 has no bad_arguments_to_setelement clause.
                        self.native("function_clause")
                    });
                }
                let Value::Integer(n) = &index else {
                    unreachable!()
                };
                let Value::Tuple(mut vs) = tuple else {
                    unreachable!()
                };
                let Some(i) = n.to_usize().filter(|&i| i < vs.len()) else {
                    return Err(if op == Element {
                        self.entry_abort(Reason::ElementBounds(index))
                    } else {
                        Fault::Abort(Reason::ElementBounds(index))
                    });
                };
                if let Some(v) = replacement {
                    vs[i] = v;
                    self.cells(BigInt::from(vs.len()) + 2)?;
                    self.write(&a[0], Value::Tuple(vs))?;
                } else {
                    self.write(&a[0], vs.swap_remove(i))?;
                }
            }
            MapEmpty => {
                self.write(&a[0], Value::Map(Map::new()))?;
                self.cells(2.into())?;
            }
            MapLookup | MapLookupd | MapMember | MapSize | MapToList => {
                self.map_instruction(op, a)?
            }
            MapUpdate | MapDelete => {
                let map = self.read(&a[1])?;
                let key = self.read(&a[2])?;
                let value = if op == MapUpdate {
                    Some(self.read(&a[3])?)
                } else {
                    None
                };
                let result = match map {
                    Value::Map(mut map) if !matches!(key, Value::Map(_)) => {
                        if let Some(v) = value {
                            map.insert(key, v);
                        } else {
                            map.remove(&key);
                        }
                        Value::Map(map)
                    }
                    Value::StoreMap { id, mut cache } => {
                        cache.insert(key, value.unwrap_or(Value::Tombstone));
                        Value::StoreMap { id, cache }
                    }
                    map => {
                        let mut args = vec![map, key];
                        if let Some(v) = value {
                            args.push(v);
                        }
                        return Err(self.type_error(
                            if op == MapUpdate {
                                "map_update"
                            } else {
                                "map_delete"
                            },
                            args,
                        ));
                    }
                };
                self.write(&a[0], result)?;
                if op == MapUpdate {
                    self.cells(2.into())?;
                }
            }
            MapFromList => {
                let v = self.read(&a[1])?;
                let Value::List(vs) = v else {
                    return Err(self.type_error("map_from_list", vec![v]));
                };
                let mut map = Map::new();
                for v in vs {
                    if let Value::Tuple(mut pair) = v {
                        if pair.len() != 2 {
                            return Err(self.native("badarg"));
                        }
                        let v = pair.pop().unwrap();
                        map.insert(pair.pop().unwrap(), v);
                    }
                }
                let cells = BigInt::from(map.len()) + 2;
                self.write(&a[0], Value::Map(map))?;
                self.cells(cells)?;
            }
            Nil => self.write(&a[0], Value::List(vec![]))?,
            IsNil | Hd | Tl | Length => {
                let name = match op {
                    IsNil => "is_nil",
                    Hd => "hd",
                    Tl => "tl",
                    _ => "length",
                };
                if op == Length && self.context.vm_version >= VmVersion::Two {
                    return Err(Fault::Abort(Reason::Unavailable("length")));
                }
                let v = self.read(&a[1])?;
                let Value::List(mut vs) = v else {
                    return Err(self.type_error(name, vec![v]));
                };
                let result = match op {
                    IsNil => Value::Boolean(vs.is_empty()),
                    Length => Value::Integer(vs.len().into()),
                    Hd => {
                        if vs.is_empty() {
                            return Err(Fault::Abort(Reason::HeadEmpty));
                        }
                        vs.swap_remove(0)
                    }
                    Tl => {
                        if vs.is_empty() {
                            return Err(Fault::Abort(Reason::TailEmpty));
                        }
                        vs.remove(0);
                        Value::List(vs)
                    }
                    _ => unreachable!(),
                };
                self.write(&a[0], result)?;
            }
            Cons | Append => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                let result = match (op, x, y) {
                    (Cons, x, Value::List(mut ys)) => {
                        ys.insert(0, x);
                        Value::List(ys)
                    }
                    (Append, Value::List(mut xs), Value::List(ys)) => {
                        xs.extend(ys);
                        Value::List(xs)
                    }
                    (_, x, y) => {
                        return Err(
                            self.type_error(if op == Cons { "cons" } else { "append" }, vec![x, y])
                        );
                    }
                };
                self.write(&a[0], result)?;
                if op == Cons {
                    self.cells(2.into())?;
                } else {
                    // APPEND re-reads its first operand from the entry state
                    // solely to obtain its length; that read's effects vanish.
                    let mut probe = Step {
                        host: self.entry_host.clone(),
                        entry_host: self.entry_host.clone(),
                        defer_program_jumps: self.defer_program_jumps,
                        reset_debug_on_jump: self.reset_debug_on_jump,
                        context: self.context.clone(),
                        reserved_gas: self.reserved_gas.clone(),
                        state: self.entry.clone(),
                        entry: self.entry.clone(),
                        executor: self.executor,
                        remaining: self.remaining,
                    };
                    let Value::List(v) = probe.read(&a[1])? else {
                        return Err(self.native("badarg"));
                    };
                    self.remaining = probe.remaining;
                    self.cells(BigInt::from(v.len()) * 2)?;
                }
            }
            Jump => return Ok(Control::Jump(literal(&a[0]).clone())),
            Jumpif => {
                let v = self.read(&a[0])?;
                match v {
                    Value::Boolean(true) => return Ok(Control::Jump(literal(&a[1]).clone())),
                    Value::Boolean(false) => {}
                    _ => {
                        return Err(if self.state.meter.protocol < 5 {
                            self.native("case_clause")
                        } else {
                            Fault::Abort(Reason::ValueType {
                                value: v,
                                expected: "boolean",
                            })
                        });
                    }
                }
            }
            SwitchV2 | SwitchV3 | SwitchVn => {
                let branches = if op == SwitchVn {
                    let Value::List(v) = literal(&a[1]) else {
                        return Err(self.native("badarg"));
                    };
                    v.clone()
                } else {
                    a[1..].iter().map(|a| literal(a).clone()).collect()
                };
                let v = self.read(&a[0])?;
                let Value::Variant { arities, tag, .. } = v else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "variant",
                    }));
                };
                if arities.len() != branches.len() {
                    return Err(Fault::Abort(if op == SwitchVn {
                        Reason::BadVariantTag(tag)
                    } else {
                        Reason::BadVariantSize(arities.len())
                    }));
                }
                match branches.get(tag) {
                    Some(v) => return Ok(Control::Jump(v.clone())),
                    None if op == SwitchVn => return Err(self.native("function_clause")),
                    None if self.state.meter.protocol < 5 => return Err(self.native("case_clause")),
                    None => return Err(Fault::Abort(Reason::BadVariantTag(tag))),
                }
            }
            Abort | Exit => {
                let v = self.read(&a[0])?;
                let Value::String(bytes) = v else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "string",
                    }));
                };
                return Err(if op == Abort {
                    Fault::Revert(bytes)
                } else {
                    Fault::Abort(Reason::UserExit(bytes))
                });
            }
            _ if super::binary::supported(op) => self.binary(op, a)?,
            _ if super::bls::supported(op) => self.bls(op, a)?,
            _ if super::crypto::supported(op) => self.crypto(op, a)?,
            _ if super::compare::supported(op) => self.compare(op, a)?,
            _ if super::numbers::supported(op) => self.numeric(op, a)?,
            _ => return Err(Fault::Operational(Error::Unsupported(op))),
        }
        Ok(Control::Next)
    }
    fn map_instruction(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let map = self.read(&a[1])?;
        let key = if matches!(op, MapLookup | MapLookupd | MapMember) {
            Some(self.read(&a[2])?)
        } else {
            None
        };
        let default = if op == MapLookupd {
            Some(self.read(&a[3])?)
        } else {
            None
        };
        let name = match op {
            MapLookup => "map_lookup",
            MapLookupd => "map_lookupd",
            MapMember => "map_member",
            MapSize => "map_size",
            _ => "map_to_list",
        };
        if !matches!(map, Value::Map(_) | Value::StoreMap { .. }) && self.state.meter.protocol >= 5
        {
            return Err(self.entry_abort(Reason::BadMap {
                operation: name,
                value: map,
            }));
        }
        let mut cells = None;
        let result = match (op, map) {
            (MapLookup | MapLookupd, Value::Map(map)) => map
                .get(key.as_ref().unwrap())
                .filter(|v| **v != Value::Void)
                .cloned()
                .or(default)
                .ok_or(Fault::Abort(Reason::MissingMapKey))?,
            (MapMember, Value::Map(map)) => Value::Boolean(map.contains_key(key.as_ref().unwrap())),
            (MapSize, Value::Map(map)) => Value::Integer(map.len().into()),
            (MapToList, Value::Map(map)) => {
                cells = Some(Some(map.len()));
                self.list(map)?
            }
            (_, Value::StoreMap { id, cache }) => {
                let needs_store = match op {
                    MapLookup | MapLookupd | MapMember => {
                        matches!(cache.get(key.as_ref().unwrap()), None | Some(Value::Void))
                    }
                    _ => true,
                };
                if needs_store {
                    self.ensure_store()?;
                }
                let mut scope = self
                    .state
                    .stores
                    .vm_scope(self.state.meter.protocol, self.context.current_contract)
                    .map_err(|_| Fault::Operational(Error::ProtocolMismatch))?;
                let gas = self.state.meter.gas.clone();
                match op {
                    MapLookup | MapLookupd => {
                        let out = scope
                            .map_lookup(&cache, &id, key.as_ref().unwrap(), gas, self.remaining)
                            .map_err(|e| self.store_fault(e))?;
                        self.state.meter.gas = out.gas;
                        out.value
                            .or(default)
                            .ok_or(Fault::Abort(Reason::MissingMapKey))?
                    }
                    MapMember => {
                        let out = scope
                            .map_member(&cache, &id, key.as_ref().unwrap(), gas, self.remaining)
                            .map_err(|e| self.store_fault(e))?;
                        self.state.meter.gas = out.gas;
                        Value::Boolean(out.value)
                    }
                    MapSize => {
                        let out = scope
                            .map_size(&cache, &id, gas, self.executor.order, self.remaining)
                            .map_err(|e| self.store_fault(e))?;
                        self.state.meter.gas = out.gas;
                        out.value
                    }
                    MapToList => {
                        let out = scope
                            .map_get_clean(&cache, &id, gas, self.executor.order, self.remaining)
                            .map_err(|e| self.store_fault(e))?;
                        self.state.meter.gas = out.gas;
                        let list = self.list(out.value)?;
                        cells = Some(if let Value::List(v) = &list {
                            Some(v.len())
                        } else {
                            None
                        });
                        list
                    }
                    _ => unreachable!(),
                }
            }
            (MapLookup | MapLookupd, _) => return Err(self.native("map_lookup1")),
            _ => return Err(self.native("case_clause")),
        };
        self.write(&a[0], result)?;
        if let Some(size) = cells {
            self.cells(BigInt::from(size.ok_or_else(|| self.native("badarg"))?) * 2)?;
        }
        Ok(())
    }
    fn list(&mut self, map: Map) -> Result<Value, Fault<S::Error>> {
        let result = self
            .executor
            .overrides
            .list(
                map,
                self.state.meter.protocol,
                self.executor.order,
                self.remaining,
            )
            .map_err(|e| match e {
                MapError::Data(DataError::ResourceLimit) => DataError::ResourceLimit.into(),
                MapError::Data(DataError::InvalidOrderProfile) | MapError::InvalidOrder => {
                    Fault::Operational(Error::Data(DataError::InvalidOrderProfile))
                }
                MapError::Data(_) => self.native("function_clause"),
                MapError::Lookup(never) => match never {},
            })?;
        self.remaining = result.remaining_limits;
        Ok(result.value.into_owned())
    }
}
fn literal(operand: &Operand) -> &Value {
    let Operand::Immediate(value) = operand else {
        unreachable!("dispatch checked literal operands")
    };
    value
}
