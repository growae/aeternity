// aefa_fate_op string/bytes/character conversions, pinned fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::value::{ObjectKind, ObjectPayload};
use num_traits::ToPrimitive;

pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        StrJoin
            | StrLength
            | StrReverse
            | StrToBytes
            | StrToList
            | StrFromList
            | StrToUpper
            | StrToLower
            | CharToInt
            | CharFromInt
            | IntToStr
            | IntToBytes
            | AddrToStr
            | AddressToBytes
            | IntToAddr
            | BytesToInt
            | BytesToStr
            | BytesConcat
            | BytesSplit
            | BytesSize
            | BytesSplitAny
            | BytesToFixedSize
            | ContractToAddress
            | AddressToContract
    )
}
fn name(op: Opcode) -> &'static str {
    use Opcode::*;
    match op {
        StrJoin => "str_join",
        StrReverse => "str_reverse",
        StrToBytes => "str_to_bytes",
        IntToStr => "int_to_str",
        IntToBytes => "int_to_bytes",
        IntToAddr => "int_to_addr",
        AddrToStr => "addr_to_str",
        AddressToBytes => "addr_to_bytes",
        BytesToInt => "bytes_to_int",
        BytesToStr => "bytes_to_str",
        BytesConcat => "bytes_concat",
        BytesSplit => "bytes_split",
        BytesSize => "bytes_size",
        BytesSplitAny => "bytes_split_any",
        BytesToFixedSize => "bytes_to_fixed_size",
        ContractToAddress => "contract_to_address",
        AddressToContract => "address_to_contract",
        _ => unreachable!(),
    }
}
fn option(value: Option<Value>) -> Value {
    Value::Variant {
        arities: vec![0.into(), 1.into()],
        tag: usize::from(value.is_some()),
        values: value.into_iter().collect(),
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: StoreAccess<S::Error>> Step<'_, '_, S, O, H> {
    fn binary_bound(&self, bytes: usize) -> Result<(), Fault<S::Error>> {
        if bytes > self.remaining.max_bytes {
            Err(DataError::ResourceLimit.into())
        } else {
            Ok(())
        }
    }
    fn radix_work(&mut self, units: usize) -> Result<(), Fault<S::Error>> {
        // Radix conversion can be quadratic. Preflight a conservative work
        // bound separately from output bytes and consensus gas.
        let work = units.checked_mul(units).ok_or(DataError::ResourceLimit)?;
        self.remaining.max_nodes = self
            .remaining
            .max_nodes
            .checked_sub(work)
            .ok_or(DataError::ResourceLimit)?;
        Ok(())
    }
    fn value_type(&self, value: Value, expected: &'static str) -> Fault<S::Error> {
        Fault::Abort(Reason::ValueType { value, expected })
    }
    fn point(&mut self, points: &mut Vec<u32>, cp: u32) -> Result<(), Fault<S::Error>> {
        self.remaining.max_nodes = self
            .remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(DataError::ResourceLimit)?;
        self.remaining.max_bytes = self
            .remaining
            .max_bytes
            .checked_sub(4)
            .ok_or(DataError::ResourceLimit)?;
        points.push(cp);
        Ok(())
    }
    fn chars(&mut self, bytes: &[u8]) -> Result<Option<Vec<u32>>, Fault<S::Error>> {
        let Ok(s) = std::str::from_utf8(bytes) else {
            return Ok(None);
        };
        let mut points = Vec::new();
        for c in s.chars() {
            self.point(&mut points, c as u32)?;
        }
        Ok(Some(points))
    }
    fn nfc(&mut self, points: &[u32]) -> Result<Vec<u32>, Fault<S::Error>> {
        let mapped = self.context.unicode_profile.nfc(points, self.remaining)?;
        self.remaining = mapped.remaining_limits;
        Ok(mapped.points)
    }
    fn utf8(&self, points: &[u32]) -> Result<Vec<u8>, Fault<S::Error>> {
        let bytes = points.iter().try_fold(0_usize, |n, &cp| {
            n.checked_add(
                char::from_u32(cp)
                    .ok_or(DataError::InvalidValue)?
                    .len_utf8(),
            )
            .ok_or(DataError::ResourceLimit)
        })?;
        self.binary_bound(bytes)?;
        Ok(points
            .iter()
            .map(|&cp| char::from_u32(cp).unwrap())
            .collect::<String>()
            .into_bytes())
    }
    fn chardata(
        &mut self,
        value: &Value,
        points: &mut Vec<u32>,
        depth: usize,
    ) -> Result<bool, Fault<S::Error>> {
        if depth > self.remaining.max_depth {
            return Err(DataError::ResourceLimit.into());
        }
        self.remaining.max_nodes = self
            .remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(DataError::ResourceLimit)?;
        match value {
            Value::Integer(n) => {
                let Some(cp) = n.to_u32().filter(|&c| char::from_u32(c).is_some()) else {
                    return Err(self.native("badarg"));
                };
                self.point(points, cp)?;
            }
            Value::String(bytes) => {
                let Some(chars) = self.chars(bytes)? else {
                    return Ok(false);
                };
                points.extend(chars);
            }
            Value::List(values) => {
                for v in values {
                    if !self.chardata(v, points, depth + 1)? {
                        return Ok(false);
                    }
                }
            }
            _ => return Err(self.native("badarg")),
        }
        Ok(true)
    }
    pub(super) fn simple_traversal(&mut self, value: &Value) -> Result<(), Fault<S::Error>> {
        self.traversal(value, gas::CostModel::Simple)
    }
    pub(super) fn traversal(
        &mut self,
        value: &Value,
        model: gas::CostModel,
    ) -> Result<(), Fault<S::Error>> {
        let charge = gas::TraversalMeter {
            protocol: self.state.meter.protocol,
            model,
            native_capacity: Some(self.native_capacity()),
        }
        .charge(
            value,
            self.state.meter.gas.clone(),
            &mut gas::NoUnfold,
            self.executor.order,
            self.remaining,
        )
        .map_err(|e| match e {
            gas::Error::Data(DataError::InvalidValue | DataError::InvalidType) => {
                self.native("function_clause")
            }
            gas::Error::InvalidOrder => DataError::InvalidOrderProfile.into(),
            e => Self::gas_fault(e),
        })?;
        self.state.meter.gas = charge.gas;
        self.remaining = charge.remaining_limits;
        Ok(())
    }
    pub(super) fn binary(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        if matches!(
            op,
            StrToBytes | IntToBytes | AddressToBytes | BytesSize | BytesSplitAny | BytesToFixedSize
        ) && self.context.vm_version < VmVersion::Three
        {
            return Err(Fault::Abort(Reason::Unavailable(name(op))));
        }
        let x = self.read(&a[1])?;
        if matches!(
            op,
            StrJoin | BytesConcat | IntToBytes | BytesSplit | BytesSplitAny | BytesToFixedSize
        ) {
            let y = self.read(&a[2])?;
            let result = match (op, &x, &y) {
                (StrJoin, Value::String(left), Value::String(right))
                | (BytesConcat, Value::Bytes(left), Value::Bytes(right)) => {
                    let len = left
                        .len()
                        .checked_add(right.len())
                        .ok_or(DataError::ResourceLimit)?;
                    self.binary_bound(len)?;
                    let mut out = Vec::with_capacity(len);
                    out.extend_from_slice(left);
                    out.extend_from_slice(right);
                    self.cells((len / 8 + usize::from(op == StrJoin)).into())?;
                    if op == StrJoin {
                        Value::String(out)
                    } else {
                        Value::Bytes(out)
                    }
                }
                (IntToBytes, Value::Integer(n), Value::Integer(size)) => {
                    let bits = self.product(size, &8.into())?;
                    if bits.sign() == Sign::Minus {
                        return Err(self.native("badarg"));
                    }
                    let size = size.to_usize().ok_or(DataError::ResourceLimit)?;
                    self.binary_bound(size)?;
                    let mut bytes = vec![if n.sign() == Sign::Minus { 255 } else { 0 }; size];
                    let signed = n.to_signed_bytes_be();
                    let len = size.min(signed.len());
                    bytes[size - len..].copy_from_slice(&signed[signed.len() - len..]);
                    self.cells((size / 8 + 1).into())?;
                    Value::Bytes(bytes)
                }
                (
                    BytesSplit | BytesSplitAny | BytesToFixedSize,
                    Value::Bytes(bytes),
                    Value::Integer(n),
                ) => {
                    let size = if op == BytesSplitAny && n <= &BigInt::from(0) {
                        self.native_integer(BigInt::from(bytes.len()) + n)?
                    } else {
                        n.clone()
                    };
                    let size = size.to_usize().filter(|&n| n <= bytes.len());
                    if op == BytesToFixedSize {
                        option((size == Some(bytes.len())).then(|| x.clone()))
                    } else if let Some(size) = size {
                        self.binary_bound(bytes.len())?;
                        let tuple = Value::Tuple(vec![
                            Value::Bytes(bytes[..size].to_vec()),
                            Value::Bytes(bytes[size..].to_vec()),
                        ]);
                        if op == BytesSplit {
                            // This instruction writes its destination before the tuple charge.
                            self.write(&a[0], tuple)?;
                            return self.cells(4.into());
                        }
                        self.cells((size / 8 + (bytes.len() - size) / 8).into())?;
                        self.cells(4.into())?;
                        option(Some(tuple))
                    } else if op == BytesSplitAny {
                        option(None)
                    } else {
                        return Err(self.type_error(name(op), vec![x, y]));
                    }
                }
                _ => return Err(self.type_error(name(op), vec![x, y])),
            };
            return self.write(&a[0], result);
        }
        let result = match op {
            StrLength | StrToList | StrToUpper | StrToLower | StrReverse | StrToBytes => {
                let Value::String(bytes) = &x else {
                    return Err(if matches!(op, StrReverse | StrToBytes) {
                        self.type_error(
                            if op == StrReverse && self.context.vm_version >= VmVersion::Two {
                                "str_reverse_unicode"
                            } else {
                                name(op)
                            },
                            vec![x],
                        )
                    } else {
                        self.value_type(x, "string")
                    });
                };
                if op == StrToBytes {
                    self.cells((bytes.len() / 8 + 1).into())?;
                    Value::Bytes(bytes.clone())
                } else if op == StrLength && self.context.vm_version == VmVersion::One {
                    Value::Integer(bytes.len().into())
                } else if op == StrReverse && self.context.vm_version == VmVersion::One {
                    let mut bytes = bytes.clone();
                    bytes.reverse();
                    self.cells((bytes.len() / 8 + 1).into())?;
                    Value::String(bytes)
                } else {
                    let Some(points) = self.chars(bytes)? else {
                        return Err(if matches!(op, StrLength | StrToList) {
                            self.value_type(x, "string_utf8")
                        } else if op == StrReverse {
                            self.native("function_clause")
                        } else {
                            self.native("badarg")
                        });
                    };
                    if matches!(op, StrToUpper | StrToLower) {
                        let mapped = self.context.unicode_profile.case(
                            &points,
                            op == StrToUpper,
                            self.remaining,
                        )?;
                        self.remaining = mapped.remaining_limits;
                        let result = self.utf8(&mapped.points)?;
                        self.cells(result.len().into())?;
                        Value::String(result)
                    } else {
                        let mut points = self.nfc(&points)?;
                        match op {
                            StrLength => {
                                self.cells(points.len().into())?;
                                Value::Integer(points.len().into())
                            }
                            StrToList => {
                                self.cells(BigInt::from(points.len()) * 2 + 1)?;
                                Value::List(
                                    points
                                        .into_iter()
                                        .map(|c| Value::Integer(c.into()))
                                        .collect(),
                                )
                            }
                            StrReverse => {
                                points.reverse();
                                let points = self.nfc(&points)?;
                                let result = self.utf8(&points)?;
                                self.cells((result.len() / 8 + 1).into())?;
                                Value::String(result)
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
            StrFromList => {
                let Value::List(values) = &x else {
                    return Err(self.value_type(x, "list(char)"));
                };
                self.simple_traversal(&x)?;
                if self.state.meter.protocol>=5 && values.iter().any(|v| !matches!(v,Value::Integer(n) if n>=&BigInt::from(0) && n<=&BigInt::from(0x10ffff))) {
                    return Err(self.value_type(x,"list(char)"));
                }
                let mut points = Vec::new();
                if !self.chardata(&x, &mut points, 0)? {
                    return Err(self.value_type(x, "list(char)"));
                }
                let points = self.nfc(&points)?;
                let bytes = self.utf8(&points)?;
                self.cells((bytes.len() / 8 + 1).into())?;
                Value::String(bytes)
            }
            CharToInt | CharFromInt => {
                let Value::Integer(n) = &x else {
                    return Err(self.value_type(x, if op == CharToInt { "char" } else { "int" }));
                };
                if op == CharToInt {
                    x
                } else {
                    if self.state.meter.protocol >= 5
                        && (n < &BigInt::from(0) || n > &BigInt::from(0x10ffff))
                    {
                        return Err(self.value_type(x, "char"));
                    }
                    let Some(cp) = n.to_u32().filter(|&c| c <= 0x10ffff) else {
                        return Err(self.native("badarg"));
                    };
                    // OTP nfc_list preserves surrogate integers, although its
                    // binary-producing counterpart rejects them.
                    let points = if char::from_u32(cp).is_some() {
                        self.nfc(&[cp])?
                    } else {
                        vec![cp]
                    };
                    self.cells(if points.len() == 1 { 9 } else { 8 }.into())?;
                    option((points.len() == 1).then(|| Value::Integer(points[0].into())))
                }
            }
            IntToStr | IntToAddr => {
                let Value::Integer(n) = &x else {
                    return Err(self.type_error(name(op), vec![x]));
                };
                if op == IntToAddr {
                    return Err(Fault::Abort(Reason::Disabled(IntToAddr)));
                }
                self.binary_bound(
                    usize::try_from(n.magnitude().bits().div_ceil(3).saturating_add(2))
                        .map_err(|_| DataError::ResourceLimit)?,
                )?;
                self.radix_work(
                    usize::try_from(n.magnitude().bits().div_ceil(32))
                        .map_err(|_| DataError::ResourceLimit)?,
                )?;
                let result = n.to_str_radix(10).into_bytes();
                self.cells((result.len() / 8 + 1).into())?;
                Value::String(result)
            }
            AddrToStr | AddressToBytes | ContractToAddress | AddressToContract => {
                let expected = if op == ContractToAddress {
                    ObjectKind::Contract
                } else {
                    ObjectKind::Address
                };
                let Value::Object(kind, ObjectPayload::Bytes(bytes)) = &x else {
                    return Err(self.type_error(name(op), vec![x]));
                };
                if *kind != expected {
                    return Err(self.type_error(name(op), vec![x]));
                }
                match op {
                    AddrToStr => {
                        let checked_bytes =
                            bytes.len().checked_add(4).ok_or(DataError::ResourceLimit)?;
                        self.binary_bound(
                            checked_bytes
                                .checked_mul(2)
                                .and_then(|n| n.checked_add(3))
                                .ok_or(DataError::ResourceLimit)?,
                        )?;
                        self.radix_work(
                            checked_bytes
                                .checked_mul(2)
                                .ok_or(DataError::ResourceLimit)?,
                        )?;
                        let out =
                            ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, bytes)
                                .into_bytes();
                        self.cells((out.len() / 8 + 1).into())?;
                        Value::String(out)
                    }
                    AddressToBytes => {
                        self.cells((bytes.len() / 8 + 1).into())?;
                        Value::Bytes(bytes.clone())
                    }
                    _ => Value::Object(
                        if op == ContractToAddress {
                            ObjectKind::Address
                        } else {
                            ObjectKind::Contract
                        },
                        ObjectPayload::Bytes(bytes.clone()),
                    ),
                }
            }
            BytesToInt | BytesToStr | BytesSize => {
                let Value::Bytes(bytes) = &x else {
                    return Err(self.type_error(name(op), vec![x]));
                };
                match op {
                    BytesToInt => {
                        let n = BigInt::from_bytes_be(Sign::Plus, bytes);
                        // Erlang bit-syntax matching fails instead of raising an
                        // arithmetic system_limit; leading zero bytes are ignored.
                        if n.magnitude().bits() > self.native_capacity() {
                            return Err(self.native("badmatch"));
                        }
                        Value::Integer(n)
                    }
                    BytesSize => {
                        self.cells(1.into())?;
                        Value::Integer(bytes.len().into())
                    }
                    _ => {
                        let len = bytes.len().checked_mul(2).ok_or(DataError::ResourceLimit)?;
                        self.binary_bound(len)?;
                        let mut out = Vec::with_capacity(len);
                        for b in bytes {
                            out.push(b"0123456789ABCDEF"[(b >> 4) as usize]);
                            out.push(b"0123456789ABCDEF"[(b & 15) as usize]);
                        }
                        self.cells((len / 8 + 1).into())?;
                        Value::String(out)
                    }
                }
            }
            _ => unreachable!(),
        };
        self.write(&a[0], result)
    }
}
