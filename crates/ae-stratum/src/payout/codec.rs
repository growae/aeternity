// Versioned prepared payout envelope. Decoding verifies the saved exact call
// against its complete captured inputs; it never reads a new balance or nonce.
use super::*;
use ae_trie::Value as Rlp;
fn bytes(b: impl AsRef<[u8]>) -> Rlp {
    Rlp::Bytes(b.as_ref().to_vec())
}
fn uint(n: u64) -> Rlp {
    bytes(Unsigned::from(n).as_bytes())
}
fn blob(v: &Rlp) -> Result<&[u8], Error> {
    if let Rlp::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Corrupt("payout bytes"))
    }
}
fn uint_value(v: &Rlp) -> Result<Unsigned, Error> {
    let b = blob(v)?;
    if b.len() > crate::MAX_INTEGER_BITS.div_ceil(8) as usize {
        return Err(Error::Limit);
    }
    Unsigned::from_field_bytes(b).map_err(|_| Error::Corrupt("payout unsigned integer"))
}
fn number(v: &Rlp) -> Result<u64, Error> {
    uint_value(v)?
        .to_u64()
        .map_err(|_| Error::Corrupt("payout integer"))
}
fn list<const N: usize>(v: &Rlp) -> Result<&[Rlp; N], Error> {
    let Rlp::List(fields) = v else {
        return Err(Error::Corrupt("payout list"));
    };
    fields
        .as_slice()
        .try_into()
        .map_err(|_| Error::Corrupt("payout arity"))
}
fn accounting(limits: Limits) -> crate::accounting::Limits {
    crate::accounting::Limits {
        maps: limits.transfers,
        record_bytes: limits.record_bytes,
        bytes: limits.record_bytes,
        ..Default::default()
    }
}
impl Prepared {
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        limits.validate()?;
        let request = &self.request;
        if request.network.len() > limits.network_bytes
            || self.call.transaction.wire_size() > limits.transaction_bytes
            || self.estimate.transaction.wire_size() > limits.transaction_bytes
        {
            return Err(Error::Limit);
        }
        let mut payment: Payment = request.payment.clone().into();
        payment.gross = request.gross.clone();
        let record = crate::accounting::Record::Payment(payment);
        let payment = record
            .encode(accounting(limits))
            .map_err(Error::Accounting)?;
        let env = &request.environment;
        let mut fields = vec![
            uint(if self.generation == Generation::Native {
                1
            } else if request.gross.is_some() {
                3
            } else {
                2
            }),
            bytes(record.key()),
            bytes(payment),
            Rlp::List(vec![
                bytes(env.caller.0),
                bytes(env.contract.0),
                bytes(env.account_nonce.as_bytes()),
                bytes(env.minimum_miner_gas_price.as_bytes()),
                uint(env.height),
                uint(env.protocol as u64),
            ]),
            bytes(request.balance.as_bytes()),
            bytes(&request.network),
            bytes(&self.signing_bytes),
            bytes(self.estimate.transaction.fee().as_bytes()),
            bytes(self.estimate.run_fee.as_bytes()),
            bytes(unsigned(&self.net)?.as_bytes()),
            bytes(self.call.run_gas.as_bytes()),
            bytes(self.call.run_fee.as_bytes()),
        ];
        if let Generation::Bounded(p) = self.generation {
            p.validate()?;
            fields.push(Rlp::List(vec![
                uint(p.initial),
                uint(p.per_transfer),
                uint(p.max_transfers.into()),
                uint(p.block_gas_limit),
            ]));
        }
        let encoded = Rlp::List(fields).encode();
        if encoded.len() > limits.record_bytes {
            return Err(Error::Limit);
        }
        Ok(encoded)
    }
    pub fn decode(raw: &[u8], limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        let value = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: limits.record_bytes,
                max_items: 32,
                max_depth: 2,
            },
        )
        .map_err(|e| match e {
            ae_codec::rlp::DecodeError::ResourceLimit => Error::Limit,
            _ => Error::Corrupt("payout RLP"),
        })?;
        let root = Rlp::from_rlp(&value);
        let Rlp::List(mut values) = root else {
            return Err(Error::Corrupt("payout list"));
        };
        let generation = match values.first().map(number).transpose()? {
            Some(1) if values.len() == 12 => Generation::Native,
            Some(2 | 3) if values.len() == 13 => {
                let policy = values.pop().expect("thirteen fields");
                let [initial, per_transfer, count, maximum] = list(&policy)?;
                let p = GasPolicy {
                    initial: number(initial)?,
                    per_transfer: number(per_transfer)?,
                    max_transfers: u32::try_from(number(count)?).map_err(|_| Error::GasPolicy)?,
                    block_gas_limit: number(maximum)?,
                };
                p.validate()?;
                Generation::Bounded(p)
            }
            _ => return Err(Error::Corrupt("payout version")),
        };
        let root = Rlp::List(values);
        let [
            _,
            key,
            payment,
            environment,
            balance,
            network,
            wire,
            _,
            _,
            _,
            _,
            _,
        ] = list(&root)?;
        let crate::accounting::Record::Payment(payment) =
            crate::accounting::Record::decode(blob(key)?, blob(payment)?, accounting(limits))
                .map_err(Error::Accounting)?
        else {
            return Err(Error::Corrupt("payout payment record"));
        };
        let [caller, contract, nonce, price, height, protocol] = list(environment)?;
        let environment = Environment {
            caller: AccountPublicKey(
                blob(caller)?
                    .try_into()
                    .map_err(|_| Error::Corrupt("payout caller"))?,
            ),
            contract: ContractPublicKey(
                blob(contract)?
                    .try_into()
                    .map_err(|_| Error::Corrupt("payout contract"))?,
            ),
            account_nonce: uint_value(nonce)?,
            minimum_miner_gas_price: uint_value(price)?,
            height: number(height)?,
            protocol: Protocol::try_from(
                u32::try_from(number(protocol)?).map_err(|_| Error::Corrupt("payout protocol"))?,
            )
            .map_err(|_| Error::Corrupt("payout protocol"))?,
        };
        if blob(wire)?.len() > limits.transaction_bytes + limits.network_bytes
            || blob(network)?.len() > limits.network_bytes
        {
            return Err(Error::Limit);
        }
        let plan = prepare_with_generation(
            &payment,
            &environment,
            Some(&uint_value(balance)?),
            blob(network)?,
            generation,
            limits,
        )?;
        if plan.encode(limits)? != raw {
            return Err(Error::Corrupt("payout preparation binding"));
        }
        Ok(plan)
    }
}
