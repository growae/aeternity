// Native AEVM response-case diagnostics can own staged trees. Retain the
// failure category and diagnostic roots, never a database snapshot lease.
use super::super::{Error, ReceiptFailure};
use ae_chain::repository::Roots;
use ae_execution::contracts::aevm;
use ae_trie::NodeReader;
use ae_vm_aevm::{outer_call, runtime};

#[derive(Debug)]
pub enum ExecutionFailure {
    Runtime(runtime::Fault<aevm::HostError>),
    ResponseCase {
        response: outer_call::Response,
        roots: Box<Roots>,
    },
    VersionCase(ae_vm_aevm::opcode::Version),
}
#[derive(Debug)]
pub enum EvaluationFailure {
    Input(Box<ae_stratum::payout::Error>),
    Context,
    Allocation,
    BlockGas,
    Execution(ExecutionFailure),
    Accounts(ae_state::accounts::AccountError),
    Contracts(ae_state::contracts::Error),
    Channels(ae_state::channels::Error),
    Names(ae_state::names::Error),
    Oracles(ae_state::oracles::Error),
    Receipt(ReceiptFailure),
    Effects {
        expected: Box<Roots>,
        actual: Box<Roots>,
    },
}
impl<A: NodeReader, C: NodeReader> From<Error<A, C>> for EvaluationFailure {
    fn from(error: Error<A, C>) -> Self {
        match error {
            Error::Input(error) => Self::Input(error),
            Error::Context => Self::Context,
            Error::Allocation => Self::Allocation,
            Error::BlockGas => Self::BlockGas,
            Error::Execution(error) => Self::Execution(match *error {
                outer_call::Fault::Runtime(error) => ExecutionFailure::Runtime(error),
                outer_call::Fault::ResponseCase {
                    response, chain, ..
                } => ExecutionFailure::ResponseCase {
                    response,
                    roots: Box::new(Roots::of(&chain.trees)),
                },
                outer_call::Fault::VersionCase(version) => ExecutionFailure::VersionCase(version),
            }),
            Error::Accounts(error) => Self::Accounts(error),
            Error::Contracts(error) => Self::Contracts(error),
            Error::Channels(error) => Self::Channels(error),
            Error::Names(error) => Self::Names(error),
            Error::Oracles(error) => Self::Oracles(error),
            Error::Receipt(error) => Self::Receipt(error),
            Error::Effects { expected, actual } => Self::Effects { expected, actual },
        }
    }
}
