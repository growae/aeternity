// aetx gas_limit/inner_gas_limit/fee_gas/min_gas_price/tx_min_gas, fa2a6d7.
use super::{TransactionLeaf, TransactionRef, Wrapper};
use ae_protocol::{Protocol, oracles::state_gas};
use ae_types::Unsigned;
use num_bigint::{BigInt, Sign};
use num_traits::Zero;

// Signed results preserve the original arithmetic even for constructed objects
// with stale nested wire sizes or expired oracle TTLs. Admission is separate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pricing {
    pub gas_limit: BigInt,
    pub inner_gas_limit: BigInt,
    pub fee_gas: BigInt,
    pub minimum_fee: BigInt,
    pub minimum_gas_price: BigInt,
    pub minimum_block_gas: BigInt,
}
fn number(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn size_gas(size: usize) -> BigInt {
    BigInt::from(size) * 20
}
fn price(fee: &Unsigned, gas: &BigInt) -> BigInt {
    if gas.is_zero() {
        BigInt::ZERO
    } else {
        (number(fee) + gas - 1) / gas
    }
}
fn cap(value: BigInt, gas_price: Option<&Unsigned>) -> BigInt {
    gas_price.map_or(value.clone(), |p| value.min(number(p)))
}
impl TransactionRef<'_> {
    pub fn pricing(self, height: &Unsigned, protocol: Protocol) -> Pricing {
        let base = match self.leaf {
            TransactionLeaf::Contract(t) => t.base_gas(),
            TransactionLeaf::GaAttach(_) => 75_000,
            _ => 15_000,
        };
        let size = self.leaf.wire_size();
        let base_and_size = BigInt::from(base) + size_gas(size);
        let (gas_limit, fee_gas, gas_price) = match self.leaf {
            TransactionLeaf::Contract(t) => (
                &base_and_size + number(t.gas()),
                base_and_size.clone(),
                Some(t.gas_price()),
            ),
            TransactionLeaf::GaAttach(t) => (
                &base_and_size + number(t.gas()),
                base_and_size.clone(),
                Some(t.gas_price()),
            ),
            TransactionLeaf::Oracle(t) => {
                let gas = t
                    .operation()
                    .state_ttl()
                    .delta(height)
                    .map_or(BigInt::ZERO, |d| &base_and_size + number(&state_gas(&d)));
                (gas.clone(), gas, None)
            }
            _ => (base_and_size.clone(), base_and_size.clone(), None),
        };
        let mut inner_price = cap(
            price(self.leaf.fee(), &(&fee_gas - size_gas(size))),
            gas_price,
        );
        let mut result = Pricing {
            inner_gas_limit: (&gas_limit - size_gas(size)).max(BigInt::ZERO),
            minimum_fee: &fee_gas * protocol.minimum_gas_price(),
            minimum_gas_price: cap(price(self.leaf.fee(), &fee_gas), gas_price),
            minimum_block_gas: base_and_size,
            gas_limit,
            fee_gas,
        };
        let mut inner_size = size;
        for layer in self.layers {
            let size = layer.wire_size;
            let (base, fee, gas_price, gas_limit) = match &layer.wrapper {
                Wrapper::GaMeta(m) => (
                    75_000,
                    &m.fee,
                    Some(&m.gas_price),
                    BigInt::from(75_000)
                        + size_gas(size)
                        + number(&m.gas)
                        + &result.inner_gas_limit,
                ),
                Wrapper::PayingFor(p) => (
                    3_000,
                    &p.fee,
                    None,
                    BigInt::from(3_000) + size_gas(size) - size_gas(inner_size) + &result.gas_limit,
                ),
            };
            let fee_gas = if protocol >= Protocol::Iris {
                BigInt::from(base) + size_gas(size) - size_gas(inner_size)
            } else if matches!(layer.wrapper, Wrapper::GaMeta(_)) {
                BigInt::from(base) + size_gas(size)
            } else {
                gas_limit.clone()
            };
            let mut outer_price = cap(price(fee, &fee_gas), gas_price);
            let mut next_inner_price = cap(price(fee, &(&fee_gas - size_gas(size))), gas_price);
            if protocol >= Protocol::Iris {
                outer_price = outer_price.min(result.minimum_gas_price);
            } else if matches!(layer.wrapper, Wrapper::GaMeta(_)) {
                outer_price = outer_price.min(inner_price.clone());
                next_inner_price = next_inner_price.min(inner_price);
            }
            inner_price = next_inner_price;
            result = Pricing {
                inner_gas_limit: (&gas_limit - size_gas(size)).max(BigInt::ZERO),
                minimum_fee: &fee_gas * protocol.minimum_gas_price(),
                minimum_gas_price: outer_price,
                minimum_block_gas: BigInt::from(base) + size_gas(size),
                gas_limit,
                fee_gas,
            };
            inner_size = size;
        }
        result
    }
}
