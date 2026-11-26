// aec_trees:apply_txs_on_state_trees and aec_block_micro_candidate, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error as TransactionError, Receipt as TransactionReceipt, *};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    SkipInvalid,
    Strict,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Signatures {
    #[default]
    Verify,
    // Explicit trusted dry-run option; dry_run alone never bypasses checks.
    SkipOuter,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub transaction_hash: Hash256,
    pub transaction_type: TransactionKind,
    // Internal VM events retain their own source context in this value.
    pub value: super::Event,
}
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub mode: Mode,
    pub signatures: Signatures,
    pub gas_limit: Option<BigInt>,
    pub tx_events: bool,
    // aetx_env input order: newest first. Output order is chronological.
    pub initial_events: Vec<Event>,
}
impl Options {
    pub fn strict_block(gas_limit: BigInt) -> Self {
        Self {
            mode: Mode::Strict,
            gas_limit: Some(gas_limit),
            tx_events: true,
            ..Self::default()
        }
    }
}
#[derive(Debug)]
pub enum Failure<A = MemoryStore, C = MemoryStore> {
    Transaction(Box<TransactionError<A, C>>),
    Accounting(accounting::Error),
    GasLimitExceeded,
}
impl<A, C> Failure<A, C> {
    // The shared classifier admits only reference rejections before Lima,
    // and adds modeled native exceptions at Lima. Batch catches native
    // exceptions at every protocol; local failures are never swallowed.
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Transaction(e) => !failures::inner_failure(e, Protocol::Lima),
            Self::Accounting(e) => e.is_operational(),
            Self::GasLimitExceeded => false,
        }
    }
    pub fn is_native(&self) -> bool {
        match self {
            Self::Transaction(e) => {
                failures::inner_failure(e, Protocol::Lima)
                    && !failures::inner_failure(e, Protocol::Roma)
            }
            Self::Accounting(e) => !e.is_operational(),
            Self::GasLimitExceeded => false,
        }
    }
}
#[derive(Debug)]
pub struct Error<A = MemoryStore, C = MemoryStore> {
    pub index: usize,
    pub failure: Failure<A, C>,
}
#[derive(Debug)]
pub enum Rejection<A = MemoryStore, C = MemoryStore> {
    Failure(Failure<A, C>),
    Crash,
}
#[derive(Debug)]
pub struct Receipt<A = MemoryStore, C = MemoryStore> {
    pub accepted: Vec<(usize, TransactionReceipt)>,
    pub rejected: Vec<(usize, Rejection<A, C>)>,
    pub events: Vec<Event>,
    pub remaining_gas: Option<BigInt>,
}
// Publishes all roots once, only after the entire batch succeeds. Invalid
// candidates consume neither state nor gas in SkipInvalid mode. Operational
// failures always abort, even after accepted transactions, and remain retryable.
pub fn apply<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    transactions: &[SignedTransaction<Transaction>],
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    options: &Options,
) -> Result<Receipt<A, C>, Error<A, C>> {
    let mut next = trees.clone();
    let mut result = Receipt {
        accepted: vec![],
        rejected: vec![],
        events: if options.tx_events {
            options.initial_events.iter().rev().cloned().collect()
        } else {
            vec![]
        },
        remaining_gas: options.gas_limit.clone(),
    };
    for (index, signed) in transactions.iter().enumerate() {
        let mut candidate = next.clone();
        let attempt = (|| {
            if options.signatures == Signatures::Verify {
                // The block entry point always uses the outer network domain,
                // regardless of any payer in the caller's environment.
                verify_with_channel_state(
                    signed.transaction().view(),
                    signed.signatures(),
                    config.network,
                    context.protocol,
                    false,
                    config.signature_limits,
                    &candidate.channels,
                )
                .map_err(|e| Failure::Transaction(Box::new(TransactionError::Signature(e))))?;
            }
            // Reuse the original context for every transaction. update_env
            // carries only events forward, never payer or GA authentication.
            let mut receipt = process_with_signed_source(
                signed.transaction(),
                &mut candidate,
                context,
                config,
                Some(signed),
            )
            .map_err(|e| Failure::Transaction(Box::new(e)))?;
            let used = if let Some(remaining) = &result.remaining_gas {
                let used = accounting::used_gas(
                    signed.transaction().view(),
                    &receipt.context.height.into(),
                    receipt.context.protocol,
                    &candidate,
                )
                .map_err(Failure::Accounting)?;
                if &used > remaining {
                    return Err(Failure::GasLimitExceeded);
                }
                Some(used)
            } else {
                None
            };
            receipt.transaction_hash = Some(signed.hash());
            Ok((receipt, used))
        })();
        match attempt {
            Ok((receipt, used)) => {
                if let (Some(remaining), Some(used)) = (&mut result.remaining_gas, used) {
                    *remaining -= used;
                }
                if options.tx_events {
                    let transaction_hash =
                        receipt.transaction_hash.expect("batch transaction hash");
                    let mut source = signed.transaction().view();
                    while matches!(
                        source.layer().map(|l| l.wrapper()),
                        Some(Wrapper::GaMeta(_))
                    ) {
                        source = source.inner().expect("meta inner");
                    }
                    result
                        .events
                        .extend(receipt.events.iter().rev().cloned().map(|value| Event {
                            transaction_hash,
                            transaction_type: source.kind(),
                            value,
                        }));
                }
                result.accepted.push((index, receipt));
                next = candidate;
            }
            Err(failure) if options.mode == Mode::Strict || failure.is_operational() => {
                return Err(Error { index, failure });
            }
            Err(failure) => result.rejected.push((
                index,
                if failure.is_native() {
                    Rejection::Crash
                } else {
                    Rejection::Failure(failure)
                },
            )),
        }
    }
    *trees = next;
    Ok(result)
}

#[derive(Debug)]
pub struct MicroReceipt<A = MemoryStore, C = MemoryStore> {
    pub transactions: Receipt<A, C>,
    pub fees: BigInt,
}
// aec_chain_state:apply_micro_block_transactions. Deep fees are read from the
// final trees for every transaction, after the complete strict batch succeeds.
// Header/body validation and the consensus pre-transform belong to the caller.
pub fn apply_micro<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    transactions: &[SignedTransaction<Transaction>],
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    gas_limit: BigInt,
    previous_fees: BigInt,
) -> Result<MicroReceipt<A, C>, Error<A, C>> {
    apply_micro_observed(
        transactions,
        trees,
        context,
        config,
        gas_limit,
        previous_fees,
        |_| {},
    )
}
// The notification observes the strict batch result before deep-fee reads.
// It cannot supply execution inputs or replace the result. The caller owns
// operational clocks and callback isolation; publication remains atomic.
pub fn apply_micro_observed<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    transactions: &[SignedTransaction<Transaction>],
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    gas_limit: BigInt,
    previous_fees: BigInt,
    completed: impl FnOnce(&Result<Receipt<A, C>, Error<A, C>>),
) -> Result<MicroReceipt<A, C>, Error<A, C>> {
    let mut next = trees.clone();
    let receipt = apply(
        transactions,
        &mut next,
        context,
        config,
        &Options::strict_block(gas_limit),
    );
    completed(&receipt);
    let receipt = receipt?;
    let mut fees = previous_fees;
    for (index, signed) in transactions.iter().enumerate() {
        fees += accounting::deep_fee(signed.transaction().view(), &next).map_err(|e| Error {
            index,
            failure: Failure::Accounting(e),
        })?;
    }
    *trees = next;
    Ok(MicroReceipt {
        transactions: receipt,
        fees,
    })
}
