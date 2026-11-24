use super::*;

// Generation policy is captured with each prepared request. It is not a
// historical transaction-validity rule or a guarantee about arbitrary code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Generation {
    Native,
    Bounded(GasPolicy),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GasPolicy {
    pub initial: u64,
    pub per_transfer: u64,
    pub max_transfers: u32,
    pub block_gas_limit: u64,
}
impl Default for GasPolicy {
    fn default() -> Self {
        Self {
            initial: 250,
            per_transfer: 22_000,
            max_transfers: 200,
            block_gas_limit: 6_000_000,
        }
    }
}
impl GasPolicy {
    pub fn validate(self) -> Result<(), Error> {
        if self.initial == 0
            || self.per_transfer == 0
            || self.max_transfers == 0
            || self.max_transfers > 1_000_000
            || self.block_gas_limit == 0
            || self
                .per_transfer
                .checked_mul(u64::from(self.max_transfers))
                .and_then(|n| n.checked_add(self.initial))
                .is_none()
        {
            return Err(Error::GasPolicy);
        }
        Ok(())
    }
    pub fn call(
        self,
        transfers: &Amounts,
        environment: &Environment,
        network: &[u8],
        limits: Limits,
    ) -> Result<Call, Error> {
        self.validate()?;
        if transfers.len() > self.max_transfers as usize {
            return Err(Error::TransferLimit {
                actual: transfers.len(),
                maximum: self.max_transfers,
            });
        }
        for n in transfers.values() {
            if n.sign() == Sign::Minus {
                return Err(Error::NegativeAmount);
            }
            if n.bits() > 256 {
                return Err(Error::AmountWord);
            }
        }
        let mut result = call(transfers, environment, limits)?;
        if result.transaction.amount().as_bytes().len() > 32 {
            return Err(Error::AmountWord);
        }
        let gas = self
            .per_transfer
            .checked_mul(transfers.len() as u64)
            .and_then(|n| n.checked_add(self.initial))
            .ok_or(Error::GasPolicy)?;
        result.run_gas = gas.into();
        result.run_fee = unsigned(&(signed(result.transaction.gas_price()) * BigInt::from(gas)))?;
        result.transaction.set_gas(gas.into());
        // Solve fee = price * fee_gas(final serialized transaction). Starting
        // at zero is monotone; all integer/byte budgets remain enforced.
        result.transaction.set_fee(0.into());
        for _ in 0..16 {
            let fee = unsigned(
                &(signed(result.transaction.gas_price()) * signed(&result.transaction.fee_gas())),
            )?;
            if &fee == result.transaction.fee() {
                if result.transaction.wire_size() > limits.transaction_bytes {
                    return Err(Error::Limit);
                }
                let maximum = if matches!(network, b"ae_mainnet" | b"ae_uat") {
                    self.block_gas_limit.min(6_000_000)
                } else {
                    self.block_gas_limit
                };
                let declared = result.transaction.gas_limit();
                if declared > maximum.into() {
                    return Err(Error::BlockGas { declared, maximum });
                }
                return Ok(result);
            }
            result.transaction.set_fee(fee);
        }
        Err(Error::Limit)
    }
}
impl Generation {
    pub fn validate(self) -> Result<(), Error> {
        match self {
            Self::Native => Ok(()),
            Self::Bounded(p) => p.validate(),
        }
    }
    pub fn call(
        self,
        transfers: &Amounts,
        environment: &Environment,
        network: &[u8],
        limits: Limits,
    ) -> Result<Call, Error> {
        match self {
            Self::Native => call(transfers, environment, limits),
            Self::Bounded(p) => p.call(transfers, environment, network, limits),
        }
    }
}
