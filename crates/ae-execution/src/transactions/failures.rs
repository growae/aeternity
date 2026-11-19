use super::*;
// Only reference transaction rejections and modeled native exceptions can be
// swallowed by a meta. Local budgets, unavailable behavior and storage faults
// must remain retryable errors with no publication of authentication charges.
pub(super) fn inner_failure<A, C>(e: &Error<A, C>, p: Protocol) -> bool {
    match e {
        Error::Primitive(e) => primitive(e, p),
        Error::Contract(e) => contract(e, p),
        Error::Channel(e) => match e.as_ref() {
            crate::channels::Error::Primitive(e) => primitive(e, p),
            crate::channels::Error::Rejected(_) => true,
            crate::channels::Error::Native(_) => p >= Protocol::Lima,
            _ => false,
        },
        Error::Aevm(e) => {
            use ae_vm_aevm::{outer_call::Fault, runtime::Fault as Runtime};
            match e.as_ref() {
                Fault::Runtime(Runtime::Host(contracts::aevm::HostError::Transaction(e))) => {
                    contract(e, p)
                }
                Fault::Runtime(Runtime::Host(contracts::aevm::HostError::Chain(
                    crate::aevm_chain::Error::Primitive(e),
                ))) => primitive(e, p),
                Fault::Runtime(Runtime::Native(_))
                | Fault::ResponseCase { .. }
                | Fault::VersionCase(_) => p >= Protocol::Lima,
                _ => false,
            }
        }
        Error::Signature(e) if e.is_operational() => false,
        Error::Signature(_) | Error::RecursivePayer | Error::NotAllowedToPayFor(_) => true,
        Error::MinimumFeeCaseClause(_) | Error::ContractCallContextCase(_) => p >= Protocol::Lima,
        _ => false,
    }
}
fn primitive(e: &crate::Error, p: Protocol) -> bool {
    use crate::Error::*;
    match e {
        ReferenceException { .. } => p >= Protocol::Lima,
        OracleTransaction(_)
        | Naming(_)
        | InvalidAtProtocol
        | BadTransaction
        | TooLowFee
        | TtlExpired
        | AccountNotFound
        | NotBasicAccount
        | NotGeneralizedAccount
        | GeneralizedAccountsUnavailable
        | GaNonceMustBeZero
        | BadGaEnvironment
        | NonceAlreadyUsed
        | NonceTooHigh
        | InsufficientFunds
        | AccountNotPayable => true,
        Name(
            ae_state::names::Error::NameNotFound
            | ae_state::names::Error::NameRevoked
            | ae_state::names::Error::PointerNotFound,
        ) => true,
        OracleAbi(
            oracles::abi::Error::BadAbiVersion
            | oracles::abi::Error::BadQueryFormat
            | oracles::abi::Error::BadResponseFormat
            | oracles::abi::Error::BadFormat,
        ) => true,
        _ => false,
    }
}
fn contract(e: &contracts::Error, p: Protocol) -> bool {
    use contracts::Error::*;
    match e {
        Primitive(e) => primitive(e, p),
        Rejected(_) => true,
        Exception(_)
        | NativeException(_)
        | IllegalContractVersion(_)
        | AuthenticationResultCase(_) => p >= Protocol::Lima,
        Host(ae_vm_fate::machine::program::remote::HostFailure::Exception(_)) => {
            p >= Protocol::Lima
        }
        _ => false,
    }
}
