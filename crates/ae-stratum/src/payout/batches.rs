use super::*;
use num_traits::ToPrimitive;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct Batching {
    pub input: rewards::Limits,
    pub payout: Limits,
    pub maximum_index: u32,
    pub maximum_batches: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batches {
    pub payments: Vec<Payment>,
    pub first: Prepared,
}

fn gross(payment: &Payment, limits: rewards::Limits) -> Result<Amounts, Error> {
    rewards::check_map(&payment.relative, limits)?;
    let amounts = match &payment.gross {
        Some(m) if !m.is_empty() && m.keys().eq(payment.relative.keys()) => m.clone(),
        Some(_) => return Err(Error::GrossAllocation),
        None => rewards::absolute(&payment.relative, &payment.total, limits)?,
    };
    let mut bytes = 0usize;
    for (key, n) in &amounts {
        if n.sign() != Sign::Plus || n.bits() > 256 {
            return Err(Error::GrossAllocation);
        }
        bytes = bytes
            .checked_add(96 + key.len() + n.bits().div_ceil(8) as usize)
            .ok_or(Error::Limit)?;
        if bytes > limits.bytes {
            return Err(Error::Limit);
        }
    }
    if amounts.is_empty() || amounts.values().sum::<BigInt>() != payment.total {
        return Err(Error::GrossAllocation);
    }
    Ok(amounts)
}

// Split obligations retain integer gross weights. Allocate the net by largest
// remainder, with address-byte order breaking ties. No floating conversion or
// silent loss of a positive beneficiary is allowed in this new recipe.
pub(super) fn allocate(
    payment: &Payment,
    net: &BigInt,
    limits: rewards::Limits,
) -> Result<Amounts, Error> {
    if payment.gross.is_none() {
        return Ok(rewards::absolute(&payment.relative, net, limits)?);
    }
    let gross = gross(payment, limits)?;
    if net.sign() != Sign::Plus || net > &payment.total {
        return Err(Error::GrossAllocation);
    }
    let mut amounts = BTreeMap::new();
    let mut residues = Vec::new();
    let mut used = BigInt::from(0);
    for (key, amount) in &gross {
        let product = amount * net;
        crate::integer(&product).map_err(|_| Error::Limit)?;
        let value = &product / &payment.total;
        used += &value;
        amounts.insert(key.clone(), value);
        residues.push((key, product % &payment.total));
    }
    let left = (net - used).to_usize().ok_or(Error::Limit)?;
    if left >= gross.len() {
        return Err(Error::GrossAllocation);
    }
    residues.sort_by(|(ka, a), (kb, b)| b.cmp(a).then_with(|| ka.cmp(kb)));
    for (key, _) in residues.into_iter().take(left) {
        *amounts.get_mut(key).expect("gross recipient") += 1;
    }
    if amounts.values().any(|n| n.sign() != Sign::Plus) {
        return Err(Error::UnpayableRecipient);
    }
    if amounts.iter().any(|(k, n)| n > &gross[k]) || amounts.values().sum::<BigInt>() != *net {
        return Err(Error::GrossAllocation);
    }
    Ok(amounts)
}

fn clean(p: &Payment) -> bool {
    p.absolute.is_none()
        && p.tx_hash.is_none()
        && p.nonce.is_none()
        && p.fee.is_none()
        && p.gas.is_none()
        && p.run_gas.is_none()
        && p.date.is_none()
}
fn divisible(error: &Error) -> bool {
    matches!(
        error,
        Error::BlockGas { .. } | Error::TransferLimit { .. } | Error::AmountWord | Error::Limit
    )
}

// Plan every replacement before the caller publishes any row. These previews
// use one captured context; later selections recheck their then-current fees,
// nonce, balance and policy. This is generation, not VM execution preflight.
pub fn prepare_batches(
    payment: &Payment,
    environment: &Environment,
    balance: Option<&Unsigned>,
    network: &[u8],
    generation: Generation,
    settings: Batching,
) -> Result<Batches, Error> {
    let limits = settings.payout;
    limits.validate()?;
    generation.validate()?;
    if generation == Generation::Native {
        return Ok(Batches {
            payments: vec![payment.clone()],
            first: prepare_with_generation(
                payment,
                environment,
                balance,
                network,
                generation,
                limits,
            )?,
        });
    }
    if settings.maximum_batches == 0 || settings.maximum_batches > 4096 {
        return Err(Error::BatchLimit);
    }
    rewards::check_map(&payment.relative, settings.input)?;
    if payment.relative.len() <= limits.transfers.entries {
        match prepare_with_generation(payment, environment, balance, network, generation, limits)
            .and_then(|p| {
                p.encode(limits)?;
                Ok(p)
            }) {
            Ok(first) => {
                return Ok(Batches {
                    payments: vec![payment.clone()],
                    first,
                });
            }
            Err(e) if divisible(&e) => {}
            Err(e) => return Err(e),
        }
    }
    if !clean(payment) {
        return Err(Error::DirtyPayment);
    }
    if settings.maximum_index < payment.id.1 {
        return Err(Error::BatchIndex);
    }
    let balance = balance.ok_or(Error::AccountUnavailable)?;
    check_unsigned(balance)?;
    if payment.total >= signed(balance) {
        return Err(Error::InsufficientBalance {
            balance: balance.clone(),
            gross: payment.total.clone(),
        });
    }
    let Generation::Bounded(policy) = generation else {
        unreachable!()
    };
    let cap = (policy.max_transfers as usize).min(limits.transfers.entries);
    if cap == 0 {
        return Err(Error::Limit);
    }
    let source = gross(payment, settings.input)?;
    let groups = source.len().div_ceil(cap);
    if groups > settings.maximum_batches {
        return Err(Error::BatchLimit);
    }
    let size = source.len().div_ceil(groups);
    let entries: Vec<_> = source.into_iter().collect();
    let mut pending: Vec<_> = entries.chunks(size).rev().map(|g| g.to_vec()).collect();
    drop(entries);
    let mut payments = Vec::new();
    let mut first = None;
    while let Some(entries) = pending.pop() {
        if payments.len() + pending.len() + 1 > settings.maximum_batches {
            return Err(Error::BatchLimit);
        }
        let mut p: Payment = rewards::Payment {
            id: payment.id,
            relative: entries
                .iter()
                .map(|(k, _)| (k.clone(), payment.relative[k].clone()))
                .collect(),
            total: entries.iter().map(|(_, n)| n).sum(),
        }
        .into();
        if !payments.is_empty() {
            p.id.1 = settings
                .maximum_index
                .checked_add(u32::try_from(payments.len()).map_err(|_| Error::BatchIndex)?)
                .ok_or(Error::BatchIndex)?;
        }
        p.gross = Some(entries.iter().cloned().collect());
        let prepared =
            prepare_with_generation(&p, environment, Some(balance), network, generation, limits)
                .and_then(|prepared| {
                    prepared.encode(limits)?;
                    Ok(prepared)
                });
        match prepared {
            Ok(prepared) => {
                if first.is_none() {
                    first = Some(prepared);
                }
                payments.push(p);
            }
            Err(e) if divisible(&e) && entries.len() > 1 => {
                let middle = entries.len().div_ceil(2);
                pending.push(entries[middle..].to_vec());
                pending.push(entries[..middle].to_vec());
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Batches {
        payments,
        first: first.ok_or(Error::GrossAllocation)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn payment(values: &[u64]) -> Payment {
        let gross: Amounts = values
            .iter()
            .enumerate()
            .map(|(i, n)| (vec![i as u8], BigInt::from(*n)))
            .collect();
        let mut p: Payment = rewards::Payment {
            id: (1, 0),
            total: gross.values().sum(),
            relative: gross
                .keys()
                .map(|k| (k.clone(), Number::Integer(1.into())))
                .collect(),
        }
        .into();
        p.gross = Some(gross);
        p
    }
    #[test]
    fn integer_fee_allocation_has_exact_totals_stable_ties_and_no_lost_beneficiary() {
        for (weights, net, expected) in [
            (vec![5, 3, 2], 8, vec![4, 2, 2]),
            (vec![2, 2, 2], 5, vec![2, 2, 1]),
            (vec![7, 3, 1], 10, vec![6, 3, 1]),
        ] {
            let got = allocate(&payment(&weights), &BigInt::from(net), Default::default()).unwrap();
            assert_eq!(
                got.into_values().collect::<Vec<_>>(),
                expected.into_iter().map(BigInt::from).collect::<Vec<_>>()
            );
        }
        assert_eq!(
            allocate(&payment(&[1, 1, 1]), &2.into(), Default::default()),
            Err(Error::UnpayableRecipient)
        );
        for a in 1..=8 {
            for b in 1..=8 {
                for c in 1..=8 {
                    let p = payment(&[a, b, c]);
                    for net in 1..=a + b + c {
                        match allocate(&p, &net.into(), Default::default()) {
                            Ok(m) => {
                                assert_eq!(m.values().sum::<BigInt>(), net.into());
                                assert_eq!(m.len(), 3);
                                for (k, n) in m {
                                    assert!(n > 0.into() && n <= p.gross.as_ref().unwrap()[&k]);
                                }
                            }
                            Err(Error::UnpayableRecipient) => {}
                            other => panic!("{other:?}"),
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn exact_allocation_checks_width_sign_sum_keys_and_integer_limits() {
        let p = payment(&[4, 3, 2]);
        for change in 0..5 {
            let mut p = p.clone();
            match change {
                0 => p.total += 1,
                1 => {
                    p.gross.as_mut().unwrap().insert(vec![3], 1.into());
                }
                2 => {
                    p.gross.as_mut().unwrap().insert(vec![0], (-4).into());
                }
                3 => {
                    p.gross
                        .as_mut()
                        .unwrap()
                        .insert(vec![0], BigInt::from(1) << 256u32);
                }
                _ => p.gross.as_mut().unwrap().clear(),
            }
            assert!(allocate(&p, &5.into(), Default::default()).is_err());
        }
        assert!(allocate(&p, &10.into(), Default::default()).is_err());
        assert!(
            allocate(
                &p,
                &5.into(),
                rewards::Limits {
                    entries: 2,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            allocate(
                &p,
                &5.into(),
                rewards::Limits {
                    bytes: 1,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}
