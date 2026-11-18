// aefa_fate_op and aeb_fate_data, pinned fa2a6d7 / 009e036.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use num_traits::{Signed, ToPrimitive, Zero};

pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        Add | Sub
            | Mul
            | Div
            | Mod
            | Pow
            | Mulmod
            | Band
            | Bor
            | Bxor
            | Bnot
            | Bsl
            | Bsr
            | BitsNonea
            | BitsNone
            | BitsAlla
            | BitsAll
            | BitsAllN
            | BitsSet
            | BitsClear
            | BitsTest
            | BitsSum
            | BitsOr
            | BitsAnd
            | BitsDiff
            | Variant
            | VariantTest
            | VariantElement
    )
}
fn words(n: &BigInt) -> u64 {
    n.magnitude().bits().div_ceil(64).max(1)
}
fn name(op: Opcode) -> &'static str {
    use Opcode::*;
    match op {
        Add => "add",
        Sub => "sub",
        Mul => "mul",
        Div => "div",
        Mod => "mod",
        Mulmod => "mulmod",
        Band => "band",
        Bor => "bor",
        Bxor => "bxor",
        Bnot => "bnot",
        Bsl => "bsl",
        Bsr => "bsr",
        BitsAllN => "bits_all",
        BitsSet => "bits_set",
        BitsClear => "bits_clear",
        BitsTest => "bits_test",
        BitsSum => "bits_sum",
        BitsOr => "bits_union",
        BitsAnd => "bits_intersection",
        BitsDiff => "bits_difference",
        Variant => "make_variant",
        VariantTest => "variant_test",
        VariantElement => "variant_element",
        _ => unreachable!(),
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn numeric(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let availability = match op {
            Mulmod => Some("mulmod"),
            Band => Some("bin_and"),
            Bor => Some("bin_or"),
            Bxor => Some("bin_xor"),
            Bnot => Some("bin_not"),
            Bsl => Some("bin_sl"),
            Bsr => Some("bin_sr"),
            _ => None,
        };
        if let Some(name) = availability
            && self.executor.context.vm_version < VmVersion::Three
        {
            return Err(Fault::Abort(Reason::Unavailable(name)));
        }
        match op {
            BitsNonea | BitsNone | BitsAlla | BitsAll => {
                self.cells(1.into())?;
                let to = if matches!(op, BitsNonea | BitsAlla) {
                    &Operand::Stack
                } else {
                    &a[0]
                };
                self.write(
                    to,
                    Value::Bits(if matches!(op, BitsAlla | BitsAll) {
                        (-1).into()
                    } else {
                        0.into()
                    }),
                )
            }
            Variant => self.variant(a),
            VariantTest | VariantElement => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                let (Value::Variant { tag, values, .. }, Value::Integer(index)) = (&x, &y) else {
                    return Err(self.type_error(name(op), vec![x, y]));
                };
                if index.sign() == Sign::Minus {
                    return Err(self.type_error(name(op), vec![x, y]));
                }
                let result = if op == VariantTest {
                    Value::Boolean(index == &BigInt::from(*tag))
                } else {
                    let Some(i) = index.to_usize().filter(|&i| i < values.len()) else {
                        return Err(self.type_error(name(op), vec![x, y]));
                    };
                    values[i].clone()
                };
                self.write(&a[0], result)
            }
            Bnot | BitsSum | BitsAllN => {
                let v = self.read(&a[1])?;
                if op == BitsAllN {
                    let Value::Integer(n) = &v else {
                        return Err(self.native("badarith"));
                    };
                    self.cells(n / 64 + 1)?;
                }
                let result = match (op, &v) {
                    (Bnot, Value::Integer(n)) => {
                        self.number_bound(n.magnitude().bits().saturating_add(1))?;
                        Value::Integer(self.native_integer(!n)?)
                    }
                    (BitsAllN, Value::Integer(n)) => {
                        Value::Bits(self.shift(&BigInt::from(1), n, true)? - 1)
                    }
                    (BitsSum, Value::Bits(n)) => {
                        if n.sign() == Sign::Minus {
                            return Err(Fault::Abort(Reason::Arithmetic(
                                "bits_sum_on_infinite_set",
                            )));
                        }
                        Value::Integer(
                            n.magnitude()
                                .iter_u64_digits()
                                .map(|w| u64::from(w.count_ones()))
                                .sum::<u64>()
                                .into(),
                        )
                    }
                    _ => return Err(self.type_error(name(op), vec![v])),
                };
                if let Value::Integer(n) = &result
                    && op == Bnot
                {
                    self.cells(words(n).into())?;
                }
                self.write(&a[0], result)
            }
            Pow => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                let (Value::Integer(mut base), Value::Integer(mut exponent)) = (x, y) else {
                    return Err(self.native("if_clause"));
                };
                if exponent.sign() == Sign::Minus {
                    return Err(Fault::Abort(Reason::Arithmetic("negative_exponent")));
                }
                let mut result = BigInt::from(1);
                while !exponent.is_zero() {
                    self.remaining.max_nodes = self
                        .remaining
                        .max_nodes
                        .checked_sub(1)
                        .ok_or(DataError::ResourceLimit)?;
                    if exponent.bit(0) {
                        self.cells(BigInt::from(words(&result)) + BigInt::from(words(&base)) * 3)?;
                        result = self.product(&result, &base)?;
                    } else {
                        self.cells(BigInt::from(words(&base)) * 2)?;
                    }
                    // The original squares even at the final exponent bit.
                    base = self.product(&base, &base)?;
                    exponent >>= 1;
                }
                self.write(&a[0], Value::Integer(result))
            }
            Mulmod => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                let z = self.read(&a[3])?;
                let (Value::Integer(xn), Value::Integer(yn), Value::Integer(q)) = (&x, &y, &z)
                else {
                    return Err(self.type_error("mulmod", vec![x, y, z]));
                };
                if q.is_zero() {
                    return Err(Fault::Abort(Reason::ModuloByZero));
                }
                let n = self.product(xn, yn)? % q;
                self.cells(words(&n).into())?;
                self.write(&a[0], Value::Integer(n))
            }
            _ => {
                let x = self.read(&a[1])?;
                let y = self.read(&a[2])?;
                if matches!(op, Bsl | BitsSet | BitsClear | BitsTest) {
                    let Value::Integer(index) = &y else {
                        return Err(self.native("badarith"));
                    };
                    self.cells(index / 64 + 1)?;
                }
                let result = match (op, &x, &y) {
                    (BitsSet | BitsClear | BitsTest, Value::Bits(n), Value::Integer(index)) => {
                        if index.sign() == Sign::Minus {
                            return Err(Fault::Abort(Reason::Arithmetic("negative_bit_position")));
                        }
                        let mask = self.shift(&BigInt::from(1), index, true)?;
                        match op {
                            BitsSet => self.bits_result(n | mask),
                            BitsClear => self.bits_result(n & self.native_integer(!mask)?),
                            _ => Value::Boolean(!(n & mask).is_zero()),
                        }
                    }
                    (BitsOr | BitsAnd | BitsDiff, Value::Bits(x), Value::Bits(y)) => {
                        self.number_bound(
                            x.magnitude()
                                .bits()
                                .max(y.magnitude().bits())
                                .saturating_add(1),
                        )?;
                        self.bits_result(match op {
                            BitsOr => x | y,
                            BitsAnd => x & y,
                            _ => {
                                let and = x & y;
                                if and.magnitude().bits() > self.native_capacity() {
                                    return Err(self.native("badarith"));
                                }
                                and ^ x
                            }
                        })
                    }
                    (
                        Add | Sub | Mul | Div | Mod | Band | Bor | Bxor | Bsl | Bsr,
                        Value::Integer(x),
                        Value::Integer(y),
                    ) => {
                        let n = match op {
                            Div | Mod if y.is_zero() => {
                                return Err(Fault::Abort(if op == Div {
                                    Reason::DivisionByZero
                                } else {
                                    Reason::ModuloByZero
                                }));
                            }
                            Mul => self.product(x, y)?,
                            Bsl | Bsr => self.shift(x, y, op == Bsl)?,
                            _ => {
                                self.number_bound(
                                    x.magnitude()
                                        .bits()
                                        .max(y.magnitude().bits())
                                        .saturating_add(1),
                                )?;
                                match op {
                                    Add => x + y,
                                    Sub => x - y,
                                    Div => x / y,
                                    Mod => x % y,
                                    Band => x & y,
                                    Bor => x | y,
                                    Bxor => x ^ y,
                                    _ => unreachable!(),
                                }
                            }
                        };
                        if n.magnitude().bits() > self.native_capacity()
                            && matches!(op, Band | Bor | Bxor)
                        {
                            // OTP returns NIL on this bitwise overflow. The
                            // original words_used([]) then raises function_clause.
                            return Err(self.native("function_clause"));
                        }
                        let n = self.native_integer(n)?;
                        if op != Bsl {
                            self.cells(words(&n).into())?;
                        }
                        Value::Integer(n)
                    }
                    _ => return Err(self.type_error(name(op), vec![x, y])),
                };
                self.write(&a[0], result)
            }
        }
    }
    // Check output allocation before performing potentially expanding math.
    // This is retryable host resource exhaustion, not a VM gas/error result.
    fn number_bound(&self, bits: u64) -> Result<(), Fault<S::Error>> {
        if bits.div_ceil(8) > self.remaining.max_bytes as u64 {
            return Err(DataError::ResourceLimit.into());
        }
        Ok(())
    }
    fn native_capacity(&self) -> u64 {
        // OTP26 erl_term.h BIG_ARITY_MAX and big.h D_EXP. Both are
        // explicit compatibility inputs, independent of the Rust target.
        match self.executor.context.arithmetic_word_size {
            crate::maps::order::WordSize::Bits64 => 65535 * 64,
            crate::maps::order::WordSize::Bits32 => 131071 * 32,
        }
    }
    pub(super) fn native_integer(&self, n: BigInt) -> Result<BigInt, Fault<S::Error>> {
        if n.magnitude().bits() > self.native_capacity() {
            return Err(self.native("system_limit"));
        }
        Ok(n)
    }
    fn bits_result(&self, n: BigInt) -> Value {
        if n.magnitude().bits() > self.native_capacity() {
            Value::BitsNil
        } else {
            Value::Bits(n)
        }
    }
    pub(super) fn product(&self, x: &BigInt, y: &BigInt) -> Result<BigInt, Fault<S::Error>> {
        let bits = if x.is_zero() || y.is_zero() {
            0
        } else {
            x.magnitude()
                .bits()
                .checked_add(y.magnitude().bits())
                .ok_or(DataError::ResourceLimit)?
        };
        if bits.saturating_sub(1) > self.native_capacity() {
            return Err(self.native("system_limit"));
        }
        self.number_bound(bits)?;
        self.native_integer(x * y)
    }
    pub(super) fn native_meter(&self) -> Result<(), Fault<S::Error>> {
        let capacity = self.native_capacity();
        if self.state.meter.gas.magnitude().bits() > capacity
            || self.state.meter.created_cells.magnitude().bits() > capacity
        {
            return Err(self.native("system_limit"));
        }
        Ok(())
    }
    fn shift(&self, n: &BigInt, by: &BigInt, left: bool) -> Result<BigInt, Fault<S::Error>> {
        if n.is_zero() {
            return Ok(0.into());
        }
        let left = left != (by.sign() == Sign::Minus);
        let amount = by.magnitude();
        if !left && amount >= &n.magnitude().bits().into() {
            return Ok(if n.sign() == Sign::Minus {
                (-1).into()
            } else {
                0.into()
            });
        }
        if left
            && (amount > &self.native_capacity().into()
                || amount + num_bigint::BigUint::from(n.magnitude().bits())
                    > self.native_capacity().into())
        {
            return Err(self.native("system_limit"));
        }
        let amount = amount.to_u64().ok_or(DataError::ResourceLimit)?;
        if left {
            self.number_bound(
                n.magnitude()
                    .bits()
                    .checked_add(amount)
                    .ok_or(DataError::ResourceLimit)?,
            )?;
        }
        let amount = usize::try_from(amount).map_err(|_| DataError::ResourceLimit)?;
        Ok(if left { n << amount } else { n >> amount })
    }
    fn variant(&mut self, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        let arities = self.read(&a[1])?;
        let tag = self.read(&a[2])?;
        let count = self.read(&a[3])?;
        let failure = || {
            self.type_error(
                "make_variant",
                vec![arities.clone(), tag.clone(), count.clone()],
            )
        };
        let (Value::List(aa), Value::Integer(t), Value::Integer(n)) = (&arities, &tag, &count)
        else {
            return Err(failure());
        };
        let Some(tag) = t.to_usize().filter(|&t| t < aa.len()) else {
            return Err(failure());
        };
        if n.sign() == Sign::Minus {
            return Err(failure());
        }
        if self.state.meter.protocol >= 5
            && (aa
                .iter()
                .any(|v| !matches!(v,Value::Integer(a) if a.abs()<=BigInt::from(255)))
                || aa[tag] != count)
        {
            return Err(failure());
        }
        let count = n.to_usize().ok_or(DataError::ResourceLimit)?;
        let values = self
            .state
            .accumulator
            .pop_n(count, &mut self.state.meter, self.remaining);
        self.native_meter()?;
        let values = values.map_err(Self::stack_fault)?;
        self.cells(BigInt::from(aa.len()) * 2 + BigInt::from(count) + 4)?;
        let mut sizes = Vec::with_capacity(aa.len());
        for v in aa {
            let Value::Integer(n) = v else {
                return Err(self.native("badmatch"));
            };
            if n >= &BigInt::from(256) {
                return Err(self.native("badmatch"));
            }
            sizes.push(n.clone());
        }
        if sizes[tag] != BigInt::from(count) {
            return Err(self.native("if_clause"));
        }
        self.write(
            &a[0],
            Value::Variant {
                arities: sizes,
                tag,
                values,
            },
        )
    }
}
