// aec_eoe_gen_vote payload callbacks, aeternity fa2a6d7.
use super::*;
use ae_crypto::{Ed25519SecretKey, blake2b_256, verify_ed25519};
use ae_tx::{CanonicalTransaction, HcVote, TransactionLeaf};
use ae_types::Signature;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field {
    Binary(Vec<u8>),
    Integer(BigInt),
}
impl Field {
    pub fn wire(&self) -> Vec<u8> {
        match self {
            Self::Binary(b) => b.clone(),
            Self::Integer(n) => n.to_string().into_bytes(),
        }
    }
}
pub type Payload = BTreeMap<Vec<u8>, Field>;
pub const SIGNATURE: &[u8] = b"signature";
pub const HEIGHT: &[u8] = b"block_height";
pub const FORK_HASH: &[u8] = b"block_hash";
pub const LENGTH_DELTA: &[u8] = b"epoch_length_delta";
pub const LEADER: &[u8] = b"leader";
pub const EPOCH: &[u8] = b"epoch";

pub fn sign_data(payload: &Payload, limits: Limits) -> Result<Vec<u8>, Failure> {
    check_payload(payload, limits)?;
    let mut result = Vec::new();
    for (key, value) in payload {
        if key.as_slice() != SIGNATURE {
            result.extend_from_slice(key);
            result.extend_from_slice(&value.wire());
        }
    }
    Ok(result)
}
pub(super) fn check_payload(payload: &Payload, limits: Limits) -> Result<(), Failure> {
    Budget::new(limits).payload(payload)
}
pub fn create_payload(
    payload: &Payload,
    key: &Ed25519SecretKey,
    limits: Limits,
) -> Result<Payload, Failure> {
    let signature = key.sign(&sign_data(payload, limits)?);
    let mut result: Payload = payload
        .iter()
        .map(|(k, v)| (k.clone(), Field::Binary(v.wire())))
        .collect();
    result.insert(SIGNATURE.to_vec(), Field::Binary(signature.0.to_vec()));
    check_payload(&result, limits)?;
    Ok(result)
}
pub fn create_transaction(
    voter: AccountPublicKey,
    epoch: Unsigned,
    vote_type: Unsigned,
    payload: &Payload,
    key: &Ed25519SecretKey,
    network: &ae_protocol::NetworkProfile,
    limits: Limits,
) -> Result<SignedTransaction<Transaction>, Failure> {
    check_payload(payload, limits)?;
    let data = payload
        .iter()
        .map(|(k, v)| match v {
            Field::Binary(b) => Ok((k.clone(), b.clone())),
            Field::Integer(_) => Err(Failure::Native(Native::FunctionClause)),
        })
        .collect::<Result<_, _>>()?;
    let tx = Transaction::new(TransactionLeaf::HcVote(HcVote::new(
        voter, epoch, vote_type, data,
    )));
    let mut signed_data = network.network_id().to_vec();
    signed_data.extend_from_slice(&blake2b_256(&tx.encode()).0);
    Ok(SignedTransaction::new(tx, vec![key.sign(&signed_data)]))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
    pub kind: VoteType,
    pub epoch: Unsigned,
    pub voter: AccountPublicKey,
    pub fields: Payload,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    NotVote,
    SignatureVerificationFailed,
    BadInteger,
    Failure(Failure),
}
// This checks the detached payload signature. The transport has already
// checked the outer signature; the FSM deliberately does not repeat it.
pub fn convert_transaction(
    signed: &SignedTransaction<Transaction>,
    mode: Mode,
    limits: Limits,
) -> Result<Converted, ConversionError> {
    if !signed.transaction().layers().is_empty() {
        return Err(ConversionError::NotVote);
    }
    let TransactionLeaf::HcVote(vote) = signed.transaction().leaf() else {
        return Err(ConversionError::NotVote);
    };
    let kind = mode
        .vote_type(vote.vote_type())
        .ok_or(ConversionError::NotVote)?;
    Budget::new(limits)
        .signed(signed)
        .map_err(ConversionError::Failure)?;
    if vote.data().len() > limits.maximum_fields {
        return Err(ConversionError::Failure(Failure::ResourceLimit));
    }
    let mut payload: Payload = vote
        .data()
        .iter()
        .map(|(k, v)| (k.clone(), Field::Binary(v.clone())))
        .collect();
    let Some(Field::Binary(signature)) = payload.get(SIGNATURE) else {
        return Err(ConversionError::SignatureVerificationFailed);
    };
    let signature = Signature::try_from(signature.as_slice())
        .map_err(|_| ConversionError::Failure(Failure::Native(Native::Badarg)))?;
    let data = sign_data(&payload, limits).map_err(ConversionError::Failure)?;
    if !verify_ed25519(&vote.voter(), &data, &signature) {
        return Err(ConversionError::SignatureVerificationFailed);
    }
    // Both the fork and length callbacks parse the epoch delta, even though
    // fork voting only consumes the block hash when constructing its vote.
    for name in [HEIGHT, LENGTH_DELTA] {
        if let Some(field) = payload.get_mut(name) {
            let Field::Binary(b) = field else {
                unreachable!()
            };
            *field = Field::Integer(decimal(b).ok_or(ConversionError::BadInteger)?);
        }
    }
    Ok(Converted {
        kind,
        epoch: vote.epoch().clone(),
        voter: vote.voter(),
        fields: payload,
    })
}
pub(super) fn decimal(bytes: &[u8]) -> Option<BigInt> {
    let digits = match bytes.first() {
        Some(b'+' | b'-') => &bytes[1..],
        _ => bytes,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    BigInt::parse_bytes(bytes, 10)
}
