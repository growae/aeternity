// aec_dry_run prepared sequence execution, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    Context, contracts,
    transactions::{self, batch},
};
use ae_protocol::Protocol;
use ae_state::{
    Trees,
    accounts::{Account, AccountError},
    calls::Call,
};
use ae_trie::{NodeReader, Overlay};
use ae_tx::{SignedTransaction, Transaction, TransactionKind, WireTransaction};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Profile {
    Public,
    Internal,
    Replay,
    Includability,
    #[default]
    Unspecified,
    Other,
}
impl Profile {
    pub fn metering(self, base: Protocol, enabled: bool) -> (Protocol, Option<Protocol>) {
        if enabled && matches!(self, Self::Public | Self::Internal) && base == Protocol::Ceres {
            (Protocol::Arcus, Some(base))
        } else {
            (base, None)
        }
    }
}

// Only the preparation layer constructs these trusted inputs. The HTTP layer
// must first enforce its own transaction-type and authentication input rules.
pub struct Prepared<'a> {
    pub transaction: &'a SignedTransaction<Transaction>,
    pub stateless: bool,
    pub auth_tx: Option<Arc<WireTransaction>>,
    pub auth_tx_hash: Option<Hash256>,
}
impl<'a> Prepared<'a> {
    pub fn transaction(transaction: &'a SignedTransaction<Transaction>) -> Self {
        Self {
            transaction,
            stateless: false,
            auth_tx: None,
            auth_tx_hash: None,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Funding {
    pub account: AccountPublicKey,
    pub amount: Unsigned,
}
#[derive(Debug, Clone)]
pub struct Options {
    pub profile: Profile,
    pub store_read_gas_metering: bool,
    pub tx_events: bool,
    // The native option applies afresh to each transaction, not the sequence.
    pub gas_limit: Option<BigInt>,
    pub max_transactions: usize,
    pub max_accounts: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            profile: Profile::Unspecified,
            store_read_gas_metering: true,
            tx_events: false,
            gas_limit: None,
            max_transactions: 1024,
            max_accounts: 1024,
        }
    }
}
#[derive(Debug)]
pub struct Success {
    pub call: Option<Call>,
    pub events: Vec<batch::Event>,
}
#[derive(Debug)]
pub struct ResultItem<A, C> {
    pub transaction_type: TransactionKind,
    pub result: Result<Success, batch::Failure<Overlay<A>, Overlay<C>>>,
}
#[derive(Debug)]
pub struct Output<A, C> {
    pub results: Vec<ResultItem<A, C>>,
    // prepare_env clears events before every transaction. This is the final
    // transaction's list, not an aggregate of all successful transactions.
    pub events: Vec<batch::Event>,
    pub trees: Trees<Overlay<A>, Overlay<C>>,
    pub context: Context,
}
#[derive(Debug)]
pub enum Error<A, C> {
    ResourceLimit,
    Funding(AccountError),
    Operational {
        index: usize,
        failure: batch::Failure<Overlay<A>, Overlay<C>>,
    },
}
pub const MAGIC_ACCOUNT: AccountPublicKey = {
    let mut bytes = [0; 32];
    bytes[31] = 1;
    AccountPublicKey(bytes)
};

// Requires overlays even when a caller owns a writable backing store: no
// execution or funding write can reach that store. Callers supply a captured
// state and VM environment; chain lookup, parsing and worker lifetime are
// separate concerns. Dropping this result discards every staged change.
pub fn run<A: NodeReader + Clone, C: NodeReader + Clone, O: MapOrder>(
    mut trees: Trees<Overlay<A>, Overlay<C>>,
    context: &Context,
    config: &transactions::Config<'_, O>,
    funding: &[Funding],
    prepared: &[Prepared<'_>],
    options: &Options,
) -> Result<Output<A, C>, Error<A, C>> {
    if prepared.len() > options.max_transactions || funding.len() > options.max_accounts {
        return Err(Error::ResourceLimit);
    }
    let magic = Funding {
        account: MAGIC_ACCOUNT,
        amount: Unsigned::from_decimal("1000000000000000000000").expect("constant amount"),
    };
    for funds in std::iter::once(&magic).chain(funding) {
        let mut account = trees
            .accounts
            .lookup(funds.account)
            .map_err(Error::Funding)?
            .unwrap_or_else(|| Account::new(funds.account, 0.into()));
        account.earn(&funds.amount);
        trees.accounts.enter(&account).map_err(Error::Funding)?;
    }
    let (protocol, floor) = options
        .profile
        .metering(context.protocol, options.store_read_gas_metering);
    let mut context = context.clone();
    context.protocol = protocol;
    context.dry_run = true;
    let fate = config.fate.map(|base| contracts::fate::Config {
        context: base.context,
        order: base.order,
        overrides: base.overrides,
        environment: base.environment,
        history: base.history.clone(),
        auth_tx: base.auth_tx.clone(),
        metering_floor: floor.map(|p| p as u32).or(base.metering_floor),
        diagnostic_profile: base.diagnostic_profile,
    });
    let config = transactions::Config {
        fate: fate.as_ref(),
        ..*config
    };
    let batch_options = batch::Options {
        mode: batch::Mode::Strict,
        signatures: batch::Signatures::SkipOuter,
        gas_limit: options.gas_limit.clone(),
        tx_events: options.tx_events,
        ..Default::default()
    };
    let mut results = Vec::with_capacity(prepared.len());
    let mut events = vec![];
    for (index, request) in prepared.iter().enumerate() {
        if let Some(tx) = &request.auth_tx {
            context.ga_transaction = Some(tx.clone());
        }
        if let Some(hash) = request.auth_tx_hash {
            context.ga_transaction_hash = Some(hash);
        }
        events.clear();
        let mut candidate = trees.clone();
        let result = match batch::apply(
            std::slice::from_ref(request.transaction),
            &mut candidate,
            &context,
            &config,
            &batch_options,
        ) {
            Ok(mut receipt) => {
                let (_, transaction) = receipt.accepted.pop().expect("one strict transaction");
                events = receipt.events;
                if !request.stateless {
                    trees = candidate;
                }
                Ok(Success {
                    call: if matches!(
                        request.transaction.transaction().view().kind(),
                        TransactionKind::ContractCall
                            | TransactionKind::ContractCreate
                            | TransactionKind::GaAttach
                    ) {
                        transaction.call
                    } else {
                        None
                    },
                    events: events.clone(),
                })
            }
            Err(error) if error.failure.is_operational() => {
                return Err(Error::Operational {
                    index,
                    failure: error.failure,
                });
            }
            Err(error) => Err(error.failure),
        };
        results.push(ResultItem {
            transaction_type: request.transaction.transaction().view().kind(),
            result,
        });
    }
    Ok(Output {
        results,
        events,
        trees,
        context,
    })
}
