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
use num_bigint::{BigInt, Sign};
use std::sync::Arc;

pub mod prepare;

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
    pub fn prepare_environment(&self, context: &mut Context, events: &mut Vec<batch::Event>) {
        if let Some(tx) = &self.auth_tx {
            context.ga_transaction = Some(tx.clone());
        }
        if let Some(hash) = self.auth_tx_hash {
            context.ga_transaction_hash = Some(hash);
        }
        events.clear();
    }
}
#[derive(Debug, Clone)]
pub struct Funding {
    pub account: AccountPublicKey,
    pub amount: BigInt,
}
#[derive(Debug, Clone)]
pub struct Options {
    pub control: Option<ae_types::work::Control>,
    pub profile: Profile,
    pub store_read_gas_metering: bool,
    pub tx_events: bool,
    // The native option applies afresh to each transaction, not the sequence.
    pub gas_limit: Option<BigInt>,
    pub max_transactions: usize,
    pub max_accounts: usize,
    pub max_funding_bytes: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            profile: Profile::Unspecified,
            control: None,
            store_read_gas_metering: true,
            tx_events: false,
            gas_limit: None,
            max_transactions: 1024,
            max_accounts: 1024,
            max_funding_bytes: 32 << 20,
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
    Interrupted,
    ResourceLimit,
    Funding(AccountError),
    NegativeBalance {
        index: usize,
        account: AccountPublicKey,
        balance: BigInt,
    },
    Preparation(prepare::Failure),
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

// Setup owns disposable overlays and freezes profile/resource options before
// request preparation. No funding or execution write can reach backing state.
#[derive(Debug)]
pub struct Setup<A, C> {
    trees: Trees<Overlay<A>, Overlay<C>>,
    context: Context,
    floor: Option<Protocol>,
    options: Options,
}
// Funding and metering are separate deterministic stages. An enclosing service
// can resolve its captured metering policy after funding succeeds.
#[derive(Debug)]
pub struct Funded<A, C> {
    trees: Trees<Overlay<A>, Overlay<C>>,
    context: Context,
    options: Options,
}
pub fn setup<A: NodeReader + Clone, C: NodeReader + Clone>(
    trees: Trees<Overlay<A>, Overlay<C>>,
    context: &Context,
    funding: &[Funding],
    options: &Options,
) -> Result<Setup<A, C>, Error<A, C>> {
    fund(trees, context, funding, options)?.metered(options.store_read_gas_metering)
}
pub fn fund<A: NodeReader + Clone, C: NodeReader + Clone>(
    mut trees: Trees<Overlay<A>, Overlay<C>>,
    context: &Context,
    funding: &[Funding],
    options: &Options,
) -> Result<Funded<A, C>, Error<A, C>> {
    check(options.control.as_ref())?;
    if funding.len() > options.max_accounts {
        return Err(Error::ResourceLimit);
    }
    let magic = Funding {
        account: MAGIC_ACCOUNT,
        amount: "1000000000000000000000".parse().expect("constant amount"),
    };
    let mut bytes = options.max_funding_bytes;
    for (index, funds) in std::iter::once(&magic).chain(funding).enumerate() {
        check(options.control.as_ref())?;
        let mut account = trees
            .accounts
            .lookup(funds.account)
            .map_err(Error::Funding)?
            .unwrap_or_else(|| Account::new(funds.account, 0.into()));
        let size =
            usize::try_from(funds.amount.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?;
        let charge = size
            .checked_add(account.balance().as_bytes().len())
            .and_then(|n| n.checked_mul(8))
            .and_then(|n| n.checked_add(256))
            .ok_or(Error::ResourceLimit)?;
        bytes = bytes.checked_sub(charge).ok_or(Error::ResourceLimit)?;
        let (sign, magnitude) = funds.amount.to_bytes_be();
        let amount = Unsigned::from_field_bytes(&magnitude).expect("canonical integer magnitude");
        if sign == Sign::Minus {
            let balance =
                BigInt::from_bytes_be(Sign::Plus, account.balance().as_bytes()) + &funds.amount;
            if balance.sign() == Sign::Minus {
                return Err(Error::NegativeBalance {
                    index: index.saturating_sub(1),
                    account: funds.account,
                    balance,
                });
            }
            account
                .spend_without_nonce_bump(&amount)
                .map_err(Error::Funding)?;
        } else {
            account.earn(&amount);
        }
        trees.accounts.enter(&account).map_err(Error::Funding)?;
    }
    let mut context = context.clone();
    context.dry_run = true;
    Ok(Funded {
        trees,
        context,
        options: options.clone(),
    })
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> Funded<A, C> {
    pub fn metered(mut self, enabled: bool) -> Result<Setup<A, C>, Error<A, C>> {
        let (protocol, floor) = self
            .options
            .profile
            .metering(self.context.protocol, enabled);
        self.context.protocol = protocol;
        self.options.store_read_gas_metering = enabled;
        check(self.options.control.as_ref())?;
        Ok(Setup {
            trees: self.trees,
            context: self.context,
            floor,
            options: self.options,
        })
    }
}
fn check<A, C>(control: Option<&ae_types::work::Control>) -> Result<(), Error<A, C>> {
    if let Some(control) = control {
        control.check().map_err(|_| Error::Interrupted)?;
    }
    Ok(())
}
pub fn run<A: NodeReader + Clone, C: NodeReader + Clone, O: MapOrder>(
    trees: Trees<Overlay<A>, Overlay<C>>,
    context: &Context,
    config: &transactions::Config<'_, O>,
    funding: &[Funding],
    prepared: &[Prepared<'_>],
    options: &Options,
) -> Result<Output<A, C>, Error<A, C>> {
    if prepared.len() > options.max_transactions {
        return Err(Error::ResourceLimit);
    }
    setup(trees, context, funding, options)?.prepared(config, prepared)
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> Setup<A, C> {
    pub fn trees(&self) -> &Trees<Overlay<A>, Overlay<C>> {
        &self.trees
    }
    pub fn context(&self) -> &Context {
        &self.context
    }
    pub fn metering_floor(&self) -> Option<Protocol> {
        self.floor
    }
    pub fn requests<O: MapOrder>(
        self,
        config: &transactions::Config<'_, O>,
        inputs: Vec<prepare::Input<'_>>,
        limits: prepare::Limits,
        state: ae_state::transport::Config<'_>,
    ) -> Result<Output<A, C>, Error<A, C>> {
        if inputs.len() > self.options.max_transactions {
            return Err(Error::ResourceLimit);
        }
        let control =
            ae_types::work::Control::combine(self.options.control.as_ref(), config.control);
        let owned = prepare::all_controlled(inputs, limits, state, control.as_ref())
            .map_err(Error::Preparation)?;
        let prepared: Vec<_> = owned.iter().map(prepare::Owned::as_prepared).collect();
        self.prepared(config, &prepared)
    }
    pub fn prepared<O: MapOrder>(
        self,
        config: &transactions::Config<'_, O>,
        prepared: &[Prepared<'_>],
    ) -> Result<Output<A, C>, Error<A, C>> {
        let Self {
            mut trees,
            mut context,
            floor,
            options,
        } = self;
        let control = ae_types::work::Control::combine(options.control.as_ref(), config.control);
        check(control.as_ref())?;
        if prepared.len() > options.max_transactions {
            return Err(Error::ResourceLimit);
        }
        let fate = config.fate.map(|base| contracts::fate::Config {
            control: base.control.clone(),
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
            control: control.as_ref(),
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
            check(control.as_ref())?;
            request.prepare_environment(&mut context, &mut events);
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
        check(control.as_ref())?;
        Ok(Output {
            results,
            events,
            trees,
            context,
        })
    }
}
