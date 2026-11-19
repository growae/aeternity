use super::*;
use ae_vm_aevm::{
    calls::{ContractVersion, FunctionTypes},
    execution::Budget,
    heap::{Type, Value},
    maps::Store,
    primops::{AddressKind, ChainHost, HostResult, NameWrite, OracleRequest, OracleWrite},
    runtime::StoreHost,
};
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> StoreHost for Host<'_, A, C> {
    type Chain = aevm_chain::State<A, C>;
    type Error = HostError;
    fn set_store(&self, s: Store, c: &Self::Chain) -> Result<Self::Chain, RuntimeFault<HostError>> {
        chain_fault(self.source.set_store(s, c))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> ChainHost for Host<'_, A, C> {
    type Spend = ae_tx::Spend;
    type Oracle = ae_tx::OracleTransaction;
    type Name = ae_tx::NameTransaction;
    fn get_store(&self, c: &Self::Chain) -> Result<Store, RuntimeFault<HostError>> {
        chain_fault(self.source.get_store(c))
    }
    fn prepare_spend(
        &self,
        r: Id,
        v: &BigInt,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Spend, HostError> {
        chain_fault(self.source.prepare_spend(r, v, c, b))
    }
    fn apply_spend(
        &self,
        t: Self::Spend,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Chain, HostError> {
        chain_fault(self.source.apply_spend(t, c, b))
    }
    fn address(
        &self,
        k: AddressKind,
        a: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Word, HostError> {
        chain_fault(self.source.address(k, a, c, b))
    }
    fn oracle(
        &self,
        r: OracleRequest<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Value, HostError> {
        chain_fault(self.source.oracle(r, c, b))
    }
    fn oracle_height(
        &self,
        o: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> Result<BigUint, RuntimeFault<HostError>> {
        chain_fault(self.source.oracle_height(o, c, b))
    }
    fn prepare_oracle(
        &self,
        r: OracleWrite<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Self::Oracle, usize), HostError> {
        chain_fault(self.source.prepare_oracle(r, c, b))
    }
    fn apply_oracle(
        &self,
        o: Word,
        t: Self::Oracle,
        s: Option<[u8; 64]>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Value, Self::Chain), HostError> {
        chain_fault(self.source.apply_oracle(o, t, s, c, b))
    }
    fn resolve_name(
        &self,
        n: &[u8],
        k: &[u8],
        t: &Type,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Value, HostError> {
        chain_fault(self.source.resolve_name(n, k, t, c, b))
    }
    fn name_hash(&self, o: Word, n: &[u8], b: &mut Budget) -> HostResult<Word, HostError> {
        chain_fault(self.source.name_hash(o, n, b))
    }
    fn prepare_name(
        &self,
        r: NameWrite<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Self::Name, usize), HostError> {
        chain_fault(self.source.prepare_name(r, c, b))
    }
    fn apply_name(
        &self,
        o: Word,
        t: Self::Name,
        s: [u8; 64],
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Chain, HostError> {
        chain_fault(self.source.apply_name(o, t, s, c, b))
    }
}
fn rejection(e: &super::super::Error) -> Option<&'static str> {
    use super::super::Error as E;
    use crate::Error as P;
    match e {
        E::Rejected(s) => Some(s),
        E::Primitive(p) => Some(match p {
            P::InsufficientFunds => "insufficient_funds",
            P::AccountNotFound => "account_not_found",
            P::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
            P::NonceTooHigh => "tx_nonce_too_high_for_account",
            P::NotBasicAccount => "not_a_basic_account",
            P::NotGeneralizedAccount => "not_a_generalized_account",
            P::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
            P::BadGaEnvironment => "bad_ga_tx_env",
            P::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
            _ => return None,
        }),
        _ => None,
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> outer_call::Host for Host<'_, A, C> {
    fn function_types(
        &self,
        k: Word,
        v: ContractVersion,
        f: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> FunctionTypes<HostError> {
        chain_fault(c.reads().function_types(k, v, f, b))
    }
    fn balance(&self, k: Word, c: &Self::Chain) -> Result<BigInt, HostError> {
        c.reads()
            .balance(AccountPublicKey(k.0))
            .map(|n| big(&n))
            .map_err(HostError::Chain)
    }
    fn block_hash(
        &self,
        height: Word,
        _chain: &Self::Chain,
        budget: &mut Budget,
    ) -> Result<Word, RuntimeFault<HostError>> {
        use crate::chain_history::{self, Error as E};
        let height = u64::try_from(height.unsigned()).map_err(|_| Stop::InvalidInitialState)?;
        let environment = &self.config.environment;
        chain_history::select(&environment.history, height, environment.key_hash, || {
            budget.work(1).map_err(|_| E::ResourceLimit)
        })
        .map(|selection| Word(selection.branch))
        .map_err(|e| match e {
            E::ResourceLimit => RuntimeFault::Stop(Stop::ResourceLimit),
            E::NoCommonAncestor => RuntimeFault::Native(Native::BlockHashNoCommonAncestor),
            e => RuntimeFault::Host(HostError::History(e)),
        })
    }
    fn call(
        &self,
        r: Request<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> outer_call::CallResult<HostError, Self::Chain> {
        let tx = match prepare_callback(r, c, b)? {
            Ok(tx) => tx,
            Err(response) => return Ok((response, c.clone())),
        };
        let mut context = (*c.context).clone();
        context.origin = Origin::Contract;
        let config = Config {
            network_id: c.network_id.clone(),
            event_context: c.event_context.clone(),
            ..self.config.clone()
        };
        let mut trees = c.trees.clone();
        let receipt = match process_budgeted(&tx, &mut trees, &context, &config, b) {
            Ok(v) => v,
            Err(e) => return reject_callback(e, &BigUint::from_bytes_be(tx.gas().as_bytes()), c),
        };
        finish_callback(c, trees, receipt, b)
    }
}

pub(super) fn reject(gas: &BigUint, reason: String) -> Response {
    Response::Exception {
        reason,
        gas_spent: BigInt::from(gas.clone()),
    }
}
pub(super) fn reject_callback<A: Clone, C: Clone>(
    e: Error<A, C>,
    gas: &BigUint,
    c: &aevm_chain::State<A, C>,
) -> outer_call::CallResult<HostError, aevm_chain::State<A, C>> {
    match e {
        outer_call::Fault::Runtime(RuntimeFault::Host(HostError::Transaction(e))) => {
            if let Some(reason) = rejection(&e) {
                Ok((reject(gas, call_error(reason.as_bytes())), c.clone()))
            } else {
                Err(transaction_fault(e).into())
            }
        }
        e => Err(e),
    }
}
pub(super) fn prepare_callback<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    r: Request<'_>,
    c: &aevm_chain::State<A, C>,
    b: &mut Budget,
) -> Result<Result<ContractTransaction, Response>, Error<A, C>> {
    b.work(1)?;
    let Some(target) = c
        .trees
        .contracts
        .record(r.target.0)
        .map_err(|e| RuntimeFault::Host(HostError::Chain(aevm_chain::Error::Contracts(e))))?
    else {
        return Ok(Err(reject(r.gas, "unknown_contract".into())));
    };
    b.work(1)?;
    let caller = c
        .trees
        .accounts
        .lookup(c.account)
        .map_err(|e| RuntimeFault::Host(HostError::Chain(aevm_chain::Error::Accounts(e))))?
        .ok_or(RuntimeFault::Native(Native::BadMatchNone))?;
    b.bytes(caller.nonce().as_bytes().len())?;
    if r.value.sign() == Sign::Minus || r.call_stack.iter().any(|n| n.sign() == Sign::Minus) {
        return Err(Stop::InvalidInitialState.into());
    }
    b.bytes(r.data.len())?;
    b.work(r.call_stack.len())?;
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Contract, c.account.0),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, r.target.0),
            abi_version: u64::from(target.version.abi).into(),
            call_data: r.data.to_vec(),
        },
    )
    .expect("typed contract callback");
    tx.set_nonce(caller.nonce().add(&1.into()));
    tx.set_amount(magnitude(&r.value.to_biguint().expect("nonnegative")));
    tx.set_gas(magnitude(r.gas));
    tx.set_call_context(
        r.origin.0,
        r.call_stack
            .iter()
            .map(|n| magnitude(&n.to_biguint().expect("nonnegative")))
            .collect(),
    )
    .expect("call transaction");
    Ok(Ok(tx))
}
pub(super) fn finish_callback<A: Clone, C: Clone>(
    c: &aevm_chain::State<A, C>,
    trees: Trees<A, C>,
    receipt: Receipt,
    b: &mut Budget,
) -> outer_call::CallResult<HostError, aevm_chain::State<A, C>> {
    let mut next = c.clone();
    next.trees = trees;
    b.work(
        receipt
            .events
            .len()
            .checked_add(c.events.len())
            .ok_or(Stop::ResourceLimit)?,
    )?;
    for e in c.events.iter() {
        b.bytes(aevm_chain::event_size(&e.event))?;
    }
    let mut events = Vec::new();
    for e in receipt.events {
        events.push(match e {
            Event::Delta {
                account,
                amount,
                debit,
                info,
            } => {
                b.bytes(128usize.saturating_add(amount.as_bytes().len()))?;
                aevm_chain::EnvironmentEvent {
                    event: crate::Event::ContractDelta {
                        account,
                        amount,
                        debit,
                        info,
                    },
                    context: c.event_context.clone(),
                }
            }
            Event::Aevm(e) => {
                b.bytes(aevm_chain::event_size(&e.event))?;
                e
            }
            Event::Internal(_) => unreachable!("AEVM dispatcher does not execute FATE"),
        });
    }
    events.extend(c.events.iter().cloned());
    next.events = Arc::new(events);
    let gas_spent = big(&receipt.call.gas_used);
    let response = match receipt.call.return_type {
        ReturnType::Ok => Response::Complete {
            data: receipt.call.return_value,
            gas_spent,
        },
        ReturnType::Revert => Response::Revert {
            data: receipt.call.return_value,
            gas_spent,
        },
        ReturnType::Error => Response::Exception {
            reason: call_error(&receipt.call.return_value),
            gas_spent,
        },
    };
    Ok((response, next))
}
