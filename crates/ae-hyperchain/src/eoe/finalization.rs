// aec_consensus_hc:create_consensus_call_contract_transaction and
// aec_eoe_gen_vote:convert_to_finalize_transaction, aeternity fa2a6d7.
use ae_crypto::{Ed25519SecretKey, blake2b_256};
use ae_protocol::NetworkProfile;
use ae_state::{Trees, contracts::LoadOptions};
use ae_trie::NodeReader;
use ae_tx::{
    CanonicalTransaction, ContractOperation, ContractTransaction, SignedTransaction, Transaction,
    TransactionLeaf,
};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Contract(ae_state::contracts::Error),
    Account(ae_state::accounts::AccountError),
    MissingContract([u8; 32]),
    MissingAccount(AccountPublicKey),
    Transaction(ae_tx::Error),
}
// Entropy availability is checked by the caller before this function. Its
// value is not substituted into calldata here. The FSM already selected the
// finalize seed when it obtained the voting quorum.
pub fn transaction<A: NodeReader, C: NodeReader>(
    trees: &Trees<A, C>,
    owner: AccountPublicKey,
    election_contract: [u8; 32],
    calldata: Vec<u8>,
    nonce_offset: &Unsigned,
) -> Result<Transaction, Error> {
    // Preserve the reference lookup order, including loading the contract store.
    let contract = trees
        .contracts
        .lookup(election_contract, LoadOptions::default())
        .map_err(Error::Contract)?
        .ok_or(Error::MissingContract(election_contract))?;
    let account = trees
        .accounts
        .lookup(owner)
        .map_err(Error::Account)?
        .ok_or(Error::MissingAccount(owner))?;
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, owner.0),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, election_contract),
            abi_version: u64::from(contract.contract.version.abi).into(),
            call_data: calldata,
        },
    )
    .map_err(Error::Transaction)?;
    tx.set_nonce(account.nonce().add(nonce_offset));
    tx.set_fee(1_859_800_000_000.into());
    tx.set_gas(600_000.into());
    tx.set_gas_price(1_000_000.into());
    Ok(Transaction::new(TransactionLeaf::Contract(tx)))
}
pub fn sign(
    tx: Transaction,
    key: &Ed25519SecretKey,
    network: &NetworkProfile,
) -> SignedTransaction<Transaction> {
    let mut payload = network.network_id().to_vec();
    payload.extend_from_slice(&blake2b_256(&tx.encode()).0);
    SignedTransaction::new(tx, vec![key.sign(&payload)])
}
// The supervisor reserves a second nonce only when fork finalization succeeded.
pub fn length_nonce_offset(fork_succeeded: bool) -> Unsigned {
    if fork_succeeded { 2.into() } else { 1.into() }
}
