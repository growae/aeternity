// Schema order from aeternity fa2a6d7 transaction callbacks.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_codec::object::{Field, FieldType, ObjectTag};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionKind {
    ChannelCloseMutual,
    ChannelCloseSolo,
    ChannelCreate,
    ChannelDeposit,
    ChannelForceProgress,
    ChannelOffchain,
    ChannelSetDelegates,
    ChannelSettle,
    ChannelSlash,
    ChannelSnapshotSolo,
    ChannelWithdraw,
    ContractCall,
    ContractCreate,
    GaAttach,
    GaMeta,
    HcVote,
    NameClaim,
    NamePreclaim,
    NameRevoke,
    NameTransfer,
    NameUpdate,
    OracleExtend,
    OracleQuery,
    OracleRegister,
    OracleResponse,
    PayingFor,
    Spend,
}
impl TransactionKind {
    pub const ALL: [Self; 27] = [
        Self::ChannelCloseMutual,
        Self::ChannelCloseSolo,
        Self::ChannelCreate,
        Self::ChannelDeposit,
        Self::ChannelForceProgress,
        Self::ChannelOffchain,
        Self::ChannelSetDelegates,
        Self::ChannelSettle,
        Self::ChannelSlash,
        Self::ChannelSnapshotSolo,
        Self::ChannelWithdraw,
        Self::ContractCall,
        Self::ContractCreate,
        Self::GaAttach,
        Self::GaMeta,
        Self::HcVote,
        Self::NameClaim,
        Self::NamePreclaim,
        Self::NameRevoke,
        Self::NameTransfer,
        Self::NameUpdate,
        Self::OracleExtend,
        Self::OracleQuery,
        Self::OracleRegister,
        Self::OracleResponse,
        Self::PayingFor,
        Self::Spend,
    ];
    pub const fn tag(self) -> ObjectTag {
        match self {
            Self::ChannelCloseMutual => ObjectTag::ChannelCloseMutualTx,
            Self::ChannelCloseSolo => ObjectTag::ChannelCloseSoloTx,
            Self::ChannelCreate => ObjectTag::ChannelCreateTx,
            Self::ChannelDeposit => ObjectTag::ChannelDepositTx,
            Self::ChannelForceProgress => ObjectTag::ChannelForceProgressTx,
            Self::ChannelOffchain => ObjectTag::ChannelOffchainTx,
            Self::ChannelSetDelegates => ObjectTag::ChannelSetDelegatesTx,
            Self::ChannelSettle => ObjectTag::ChannelSettleTx,
            Self::ChannelSlash => ObjectTag::ChannelSlashTx,
            Self::ChannelSnapshotSolo => ObjectTag::ChannelSnapshotSoloTx,
            Self::ChannelWithdraw => ObjectTag::ChannelWithdrawTx,
            Self::ContractCall => ObjectTag::ContractCallTx,
            Self::ContractCreate => ObjectTag::ContractCreateTx,
            Self::GaAttach => ObjectTag::GaAttachTx,
            Self::GaMeta => ObjectTag::GaMetaTx,
            Self::HcVote => ObjectTag::HcVoteTx,
            Self::NameClaim => ObjectTag::NameClaimTx,
            Self::NamePreclaim => ObjectTag::NamePreclaimTx,
            Self::NameRevoke => ObjectTag::NameRevokeTx,
            Self::NameTransfer => ObjectTag::NameTransferTx,
            Self::NameUpdate => ObjectTag::NameUpdateTx,
            Self::OracleExtend => ObjectTag::OracleExtendTx,
            Self::OracleQuery => ObjectTag::OracleQueryTx,
            Self::OracleRegister => ObjectTag::OracleRegisterTx,
            Self::OracleResponse => ObjectTag::OracleResponseTx,
            Self::PayingFor => ObjectTag::PayingForTx,
            Self::Spend => ObjectTag::SpendTx,
        }
    }
    pub fn from_tag(tag: ObjectTag) -> Option<Self> {
        match tag {
            ObjectTag::ChannelCloseMutualTx => Some(Self::ChannelCloseMutual),
            ObjectTag::ChannelCloseSoloTx => Some(Self::ChannelCloseSolo),
            ObjectTag::ChannelCreateTx => Some(Self::ChannelCreate),
            ObjectTag::ChannelDepositTx => Some(Self::ChannelDeposit),
            ObjectTag::ChannelForceProgressTx => Some(Self::ChannelForceProgress),
            ObjectTag::ChannelOffchainTx => Some(Self::ChannelOffchain),
            ObjectTag::ChannelSetDelegatesTx => Some(Self::ChannelSetDelegates),
            ObjectTag::ChannelSettleTx => Some(Self::ChannelSettle),
            ObjectTag::ChannelSlashTx => Some(Self::ChannelSlash),
            ObjectTag::ChannelSnapshotSoloTx => Some(Self::ChannelSnapshotSolo),
            ObjectTag::ChannelWithdrawTx => Some(Self::ChannelWithdraw),
            ObjectTag::ContractCallTx => Some(Self::ContractCall),
            ObjectTag::ContractCreateTx => Some(Self::ContractCreate),
            ObjectTag::GaAttachTx => Some(Self::GaAttach),
            ObjectTag::GaMetaTx => Some(Self::GaMeta),
            ObjectTag::HcVoteTx => Some(Self::HcVote),
            ObjectTag::NameClaimTx => Some(Self::NameClaim),
            ObjectTag::NamePreclaimTx => Some(Self::NamePreclaim),
            ObjectTag::NameRevokeTx => Some(Self::NameRevoke),
            ObjectTag::NameTransferTx => Some(Self::NameTransfer),
            ObjectTag::NameUpdateTx => Some(Self::NameUpdate),
            ObjectTag::OracleExtendTx => Some(Self::OracleExtend),
            ObjectTag::OracleQueryTx => Some(Self::OracleQuery),
            ObjectTag::OracleRegisterTx => Some(Self::OracleRegister),
            ObjectTag::OracleResponseTx => Some(Self::OracleResponse),
            ObjectTag::PayingForTx => Some(Self::PayingFor),
            ObjectTag::SpendTx => Some(Self::Spend),
            _ => None,
        }
    }
    pub fn schema(self, version: u64) -> Option<Vec<Field>> {
        use FieldType::*;
        let fields = match (self, version) {
            (Self::ChannelCloseMutual, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "initiator_amount_final".into(),
                    kind: Integer,
                },
                Field {
                    name: "responder_amount_final".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelCloseSolo, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
                Field {
                    name: "poi".into(),
                    kind: Binary,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelCreate, 1) => vec![
                Field {
                    name: "initiator_id".into(),
                    kind: Id,
                },
                Field {
                    name: "initiator_amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "responder_id".into(),
                    kind: Id,
                },
                Field {
                    name: "responder_amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "channel_reserve".into(),
                    kind: Integer,
                },
                Field {
                    name: "lock_period".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "delegate_ids".into(),
                    kind: List(Box::new(Id)),
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelCreate, 2) => vec![
                Field {
                    name: "initiator_id".into(),
                    kind: Id,
                },
                Field {
                    name: "initiator_amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "responder_id".into(),
                    kind: Id,
                },
                Field {
                    name: "responder_amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "channel_reserve".into(),
                    kind: Integer,
                },
                Field {
                    name: "lock_period".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "initiator_delegate_ids".into(),
                    kind: List(Box::new(Id)),
                },
                Field {
                    name: "responder_delegate_ids".into(),
                    kind: List(Box::new(Id)),
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelDeposit, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelForceProgress, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "update".into(),
                    kind: Binary,
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "offchain_trees".into(),
                    kind: Binary,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelOffchain, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "updates".into(),
                    kind: List(Box::new(Binary)),
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
            ],
            (Self::ChannelOffchain, 2) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
            ],
            (Self::ChannelSetDelegates, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "initiator_delegate_ids".into(),
                    kind: List(Box::new(Id)),
                },
                Field {
                    name: "responder_delegate_ids".into(),
                    kind: List(Box::new(Id)),
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelSettle, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "initiator_amount_final".into(),
                    kind: Integer,
                },
                Field {
                    name: "responder_amount_final".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelSlash, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
                Field {
                    name: "poi".into(),
                    kind: Binary,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelSnapshotSolo, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "from_id".into(),
                    kind: Id,
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ChannelWithdraw, 1) => vec![
                Field {
                    name: "channel_id".into(),
                    kind: Id,
                },
                Field {
                    name: "to_id".into(),
                    kind: Id,
                },
                Field {
                    name: "amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "state_hash".into(),
                    kind: Binary,
                },
                Field {
                    name: "round".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
            ],
            (Self::ContractCall, 1) => vec![
                Field {
                    name: "caller_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "contract_id".into(),
                    kind: Id,
                },
                Field {
                    name: "abi_version".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: Integer,
                },
                Field {
                    name: "call_data".into(),
                    kind: Binary,
                },
            ],
            (Self::ContractCreate, 1) => vec![
                Field {
                    name: "owner_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "code".into(),
                    kind: Binary,
                },
                Field {
                    name: "ct_version".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "deposit".into(),
                    kind: Integer,
                },
                Field {
                    name: "amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: Integer,
                },
                Field {
                    name: "call_data".into(),
                    kind: Binary,
                },
            ],
            (Self::GaAttach, 1) => vec![
                Field {
                    name: "owner_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "code".into(),
                    kind: Binary,
                },
                Field {
                    name: "auth_fun".into(),
                    kind: Binary,
                },
                Field {
                    name: "ct_version".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: Integer,
                },
                Field {
                    name: "call_data".into(),
                    kind: Binary,
                },
            ],
            (Self::GaMeta, 1) => vec![
                Field {
                    name: "ga_id".into(),
                    kind: Id,
                },
                Field {
                    name: "auth_data".into(),
                    kind: Binary,
                },
                Field {
                    name: "abi_version".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "tx".into(),
                    kind: Binary,
                },
            ],
            (Self::GaMeta, 2) => vec![
                Field {
                    name: "ga_id".into(),
                    kind: Id,
                },
                Field {
                    name: "auth_data".into(),
                    kind: Binary,
                },
                Field {
                    name: "abi_version".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas".into(),
                    kind: Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: Integer,
                },
                Field {
                    name: "tx".into(),
                    kind: Binary,
                },
            ],
            (Self::HcVote, 1) => vec![
                Field {
                    name: "voter_id".into(),
                    kind: Id,
                },
                Field {
                    name: "epoch".into(),
                    kind: Integer,
                },
                Field {
                    name: "type".into(),
                    kind: Integer,
                },
                Field {
                    name: "data".into(),
                    kind: List(Box::new(Tuple(vec![Binary, Binary]))),
                },
            ],
            (Self::NameClaim, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name".into(),
                    kind: Binary,
                },
                Field {
                    name: "name_salt".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NameClaim, 2) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name".into(),
                    kind: Binary,
                },
                Field {
                    name: "name_salt".into(),
                    kind: Integer,
                },
                Field {
                    name: "name_fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NamePreclaim, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "commitment_id".into(),
                    kind: Id,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NameRevoke, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name_id".into(),
                    kind: Id,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NameTransfer, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name_id".into(),
                    kind: Id,
                },
                Field {
                    name: "recipient_id".into(),
                    kind: Id,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NameUpdate, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name_id".into(),
                    kind: Id,
                },
                Field {
                    name: "name_ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "pointers".into(),
                    kind: List(Box::new(Tuple(vec![Binary, Id]))),
                },
                Field {
                    name: "client_ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::NameUpdate, 2) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "name_id".into(),
                    kind: Id,
                },
                Field {
                    name: "name_ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "pointers".into(),
                    kind: List(Box::new(Tuple(vec![Binary, Binary]))),
                },
                Field {
                    name: "client_ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::OracleExtend, 1) => vec![
                Field {
                    name: "oracle_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "oracle_ttl_type".into(),
                    kind: Integer,
                },
                Field {
                    name: "oracle_ttl_value".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::OracleQuery, 1) => vec![
                Field {
                    name: "sender_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "oracle_id".into(),
                    kind: Id,
                },
                Field {
                    name: "query".into(),
                    kind: Binary,
                },
                Field {
                    name: "query_fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "query_ttl_type".into(),
                    kind: Integer,
                },
                Field {
                    name: "query_ttl_value".into(),
                    kind: Integer,
                },
                Field {
                    name: "response_ttl_type".into(),
                    kind: Integer,
                },
                Field {
                    name: "response_ttl_value".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::OracleRegister, 1) => vec![
                Field {
                    name: "account_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "query_format".into(),
                    kind: Binary,
                },
                Field {
                    name: "response_format".into(),
                    kind: Binary,
                },
                Field {
                    name: "query_fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl_type".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl_value".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "abi_version".into(),
                    kind: Integer,
                },
            ],
            (Self::OracleResponse, 1) => vec![
                Field {
                    name: "oracle_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "query_id".into(),
                    kind: Binary,
                },
                Field {
                    name: "response".into(),
                    kind: Binary,
                },
                Field {
                    name: "response_ttl_type".into(),
                    kind: Integer,
                },
                Field {
                    name: "response_ttl_value".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
            ],
            (Self::PayingFor, 1) => vec![
                Field {
                    name: "payer_id".into(),
                    kind: Id,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "tx".into(),
                    kind: Binary,
                },
            ],
            (Self::Spend, 1) => vec![
                Field {
                    name: "sender_id".into(),
                    kind: Id,
                },
                Field {
                    name: "recipient_id".into(),
                    kind: Id,
                },
                Field {
                    name: "amount".into(),
                    kind: Integer,
                },
                Field {
                    name: "fee".into(),
                    kind: Integer,
                },
                Field {
                    name: "ttl".into(),
                    kind: Integer,
                },
                Field {
                    name: "nonce".into(),
                    kind: Integer,
                },
                Field {
                    name: "payload".into(),
                    kind: Binary,
                },
            ],
            _ => return None,
        };
        Some(fields)
    }
}
