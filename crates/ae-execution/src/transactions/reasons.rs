// Native rejection identity consumed by aec_tx_pool_failures. Details of a
// tuple are irrelevant to that policy; its tag and tuple shape are not.
use super::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reason {
    pub tag: &'static str,
    pub tuple: bool,
}
fn atom(tag: &'static str) -> Reason {
    Reason { tag, tuple: false }
}
fn tuple(tag: &'static str) -> Reason {
    Reason { tag, tuple: true }
}
// None is an operational/inapplicable error, never an invented native label.
// Batch already separates caught native exceptions into Rejection::Crash.
pub fn rejection<A, C>(e: &batch::Rejection<A, C>) -> Option<Reason> {
    match e {
        batch::Rejection::Crash => Some(atom("crash")),
        batch::Rejection::Failure(batch::Failure::Transaction(e)) => transaction(e),
        batch::Rejection::Failure(batch::Failure::GasLimitExceeded) => {
            Some(atom("gas_limit_exceeded"))
        }
        batch::Rejection::Failure(batch::Failure::Accounting(_)) => None,
    }
}
pub fn transaction<A, C>(e: &Error<A, C>) -> Option<Reason> {
    Some(match e {
        Error::Primitive(e) => return primitive(e),
        Error::Contract(e) => return contract(e),
        Error::Channel(e) => match e.as_ref() {
            crate::channels::Error::Primitive(e) => return primitive(e),
            crate::channels::Error::Rejected(tag) => atom(tag),
            _ => return None,
        },
        Error::Aevm(e) => {
            use ae_vm_aevm::{outer_call::Fault, runtime::Fault as Runtime};
            match e.as_ref() {
                Fault::Runtime(Runtime::Host(contracts::aevm::HostError::Transaction(e))) => {
                    return contract(e);
                }
                Fault::Runtime(Runtime::Host(contracts::aevm::HostError::Chain(
                    crate::aevm_chain::Error::Primitive(e),
                ))) => return primitive(e),
                _ => return None,
            }
        }
        Error::Signature(e) => atom(match e {
            SignatureError::CheckFailed => "signature_check_failed",
            SignatureError::VerificationFailed => "signature_verification_failed",
            SignatureError::InnerIncorrectlySigned => "inner_tx_incorrectly_signed",
            SignatureError::ChannelNotFound => "channel_not_found",
            SignatureError::ChannelDoesNotExist => "channel_does_not_exist",
            _ => return None,
        }),
        Error::RecursivePayer => atom("not_allowed_to_pay_for_recursively"),
        Error::NotAllowedToPayFor(_) => tuple("not_allowed_to_pay_for"),
        Error::HcVoteCannotBeProcessed => atom("hc_vote_tx_cant_be_processed"),
        _ => return None,
    })
}
fn contract(e: &contracts::Error) -> Option<Reason> {
    match e {
        contracts::Error::Primitive(e) => primitive(e),
        contracts::Error::Rejected(tag) => Some(atom(tag)),
        _ => None,
    }
}
pub fn primitive(e: &crate::Error) -> Option<Reason> {
    use crate::Error::*;
    Some(atom(match e {
        InvalidAtProtocol => "invalid_at_protocol",
        BadTransaction => "bad_transaction",
        TooLowFee => "too_low_fee",
        TtlExpired => "ttl_expired",
        AccountNotFound => "account_not_found",
        NotBasicAccount => "not_a_basic_account",
        NotGeneralizedAccount => "not_a_generalized_account",
        GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        BadGaEnvironment => "bad_ga_tx_env",
        NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        NonceTooHigh => "tx_nonce_too_high_for_account",
        InsufficientFunds => "insufficient_funds",
        AccountNotPayable => "account_is_not_payable",
        Name(e) => match e {
            ae_state::names::Error::NameNotFound => "name_does_not_exist",
            ae_state::names::Error::NameRevoked => "name_revoked",
            ae_state::names::Error::PointerNotFound => "pointer_id_not_found",
            _ => return None,
        },
        Naming(e) => match e {
            names::Failure::CommitmentAlreadyPresent => "commitment_already_present",
            names::Failure::CommitmentNotOwned => "commitment_not_owned",
            names::Failure::CommitmentDeltaTooSmall => "commitment_delta_too_small",
            names::Failure::NameNotPreclaimed => "name_not_preclaimed",
            names::Failure::NameAlreadyTaken => "name_already_taken",
            names::Failure::NameAlreadyInAuction => "name_already_in_auction",
            names::Failure::NameNotInAuction => "name_not_in_auction",
            names::Failure::InvalidNameFee => "invalid_name_fee",
            names::Failure::IllegalNameFee => "illegal_name_fee",
            names::Failure::NameFeeIncrementTooLow => "name_fee_increment_too_low",
            names::Failure::InvalidRegistrar => "invalid_registrar",
            names::Failure::NameNotOwned => "name_not_owned",
            names::Failure::TtlTooHigh => "ttl_too_high",
            names::Failure::TtlTooLow => "ttl_too_low",
            names::Failure::InvalidPointers => "invalid_pointers",
            names::Failure::Encoding(e) => match e {
                ae_codec::idna::Error::NoRegistrar => "no_registrar",
                ae_codec::idna::Error::RegistrarUnknown => "registrar_unknown",
                ae_codec::idna::Error::MultipleNamespaces => "multiple_namespaces",
                ae_codec::idna::Error::NoLabelInRegistrar => "no_label_in_registrar",
                ae_codec::idna::Error::InvalidCodepoint(_) => {
                    return Some(tuple("invalid_codepoint"));
                }
                ae_codec::idna::Error::BadLabel(_) => return Some(tuple("bad_label")),
                ae_codec::idna::Error::ReferenceException { .. } => return None,
            },
        },
        OracleTransaction(e) => match e {
            oracles::Failure::TooLowAbsoluteTtl => "too_low_abs_ttl",
            oracles::Failure::TooLowHeight => "too_low_height",
            oracles::Failure::AlreadyOracle => "account_is_already_an_oracle",
            oracles::Failure::NotActiveOracle => "account_is_not_an_active_oracle",
            oracles::Failure::ZeroExtensionTtl => "zero_relative_oracle_extension_ttl",
            oracles::Failure::OracleDoesNotExist => "oracle_does_not_exist",
            oracles::Failure::QueryFeeTooLow => "query_fee_too_low",
            oracles::Failure::TooLongTtl => "too_long_ttl",
            oracles::Failure::NamedQueryUnavailable => {
                "oracle_query_by_name_hash_not_available_at_protocol"
            }
            oracles::Failure::QueryAlreadyPresent => "oracle_query_already_present",
            oracles::Failure::IllegalQuerySpec => "illegal_oracle_query_spec",
            oracles::Failure::NoMatchingQuery => "no_matching_oracle_query",
            oracles::Failure::WrongResponseTtl => "oracle_response_has_wrong_response_ttl",
            oracles::Failure::WrongOracle => "oracle_does_not_match_query_id",
            oracles::Failure::QueryClosed => "oracle_closed_for_response",
        },
        OracleAbi(e) => match e {
            oracles::abi::Error::BadAbiVersion => "bad_abi_version",
            oracles::abi::Error::BadQueryFormat => "bad_query_format",
            oracles::abi::Error::BadResponseFormat => "bad_response_format",
            oracles::abi::Error::BadFormat => "bad_format",
            _ => return None,
        },
        _ => return None,
    }))
}
