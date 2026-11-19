use super::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureError {
    CheckFailed,
    VerificationFailed,
    InnerIncorrectlySigned,
    ResourceLimit,
}
#[derive(Debug, Clone, Copy)]
pub struct SignatureLimits {
    pub max_layers: usize,
    pub max_encoded_bytes: usize,
}
impl Default for SignatureLimits {
    fn default() -> Self {
        Self {
            max_layers: 4096,
            max_encoded_bytes: 256 * 1024 * 1024,
        }
    }
}
struct Budget {
    layers: usize,
    bytes: usize,
}
impl From<SignatureLimits> for Budget {
    fn from(l: SignatureLimits) -> Self {
        Self {
            layers: l.max_layers,
            bytes: l.max_encoded_bytes,
        }
    }
}
fn check(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    signers: &[AccountPublicKey],
    network: &NetworkProfile,
    protocol: Protocol,
    sponsored: bool,
    b: &mut Budget,
) -> Result<(), SignatureError> {
    if signatures.len() != signers.len() {
        return Err(SignatureError::CheckFailed);
    }
    if signers.is_empty() {
        return Ok(());
    }
    if tx.wire_size() > b.bytes {
        return Err(SignatureError::ResourceLimit);
    }
    let bytes = tx.encode();
    b.bytes = b
        .bytes
        .checked_sub(bytes.len())
        .ok_or(SignatureError::ResourceLimit)?;
    ae_tx::verify_signatures(&bytes, signatures, signers, network, protocol, sponsored)
        .map_err(|_| SignatureError::CheckFailed)
}
fn signers<'a>(
    mut tx: TransactionRef<'a>,
    network: &NetworkProfile,
    b: &mut Budget,
) -> Result<Vec<AccountPublicKey>, SignatureError> {
    let mut pending = Vec::new();
    let mut result = loop {
        match tx.layer().map(|l| l.wrapper()) {
            Some(Wrapper::GaMeta(_)) => break vec![],
            Some(Wrapper::PayingFor(p)) => {
                b.layers = b
                    .layers
                    .checked_sub(1)
                    .ok_or(SignatureError::ResourceLimit)?;
                let layer = tx.layer().expect("payer layer");
                let inner = tx.inner().expect("payer inner");
                pending.push((inner, layer.inner_signatures(), p.payer));
                tx = inner;
            }
            None => break vec![tx.origin()],
        }
    };
    for (inner, sigs, payer) in pending.into_iter().rev() {
        // The payer callback always verifies its inner at Iris and does not
        // remove GA-authenticated identities from that callback check.
        check(inner, sigs, &result, network, Protocol::Iris, true, b).map_err(|e| {
            if e == SignatureError::ResourceLimit {
                e
            } else {
                SignatureError::InnerIncorrectlySigned
            }
        })?;
        result = vec![payer];
    }
    Ok(result)
}
pub fn verify(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    network: &NetworkProfile,
    protocol: Protocol,
    sponsored: bool,
    limits: SignatureLimits,
) -> Result<(), SignatureError> {
    let mut b = limits.into();
    let keys = signers(tx, network, &mut b).map_err(|e| {
        if e == SignatureError::ResourceLimit {
            e
        } else {
            SignatureError::CheckFailed
        }
    })?;
    check(tx, signatures, &keys, network, protocol, sponsored, &mut b)
}
pub fn verify_with_context(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    network: &NetworkProfile,
    context: &Context,
    limits: SignatureLimits,
) -> Result<(), SignatureError> {
    let mut b = limits.into();
    let mut keys = signers(tx, network, &mut b).map_err(|e| {
        if e == SignatureError::ResourceLimit {
            e
        } else {
            SignatureError::VerificationFailed
        }
    })?;
    keys.retain(|k| !context.ga_authenticated.contains(k));
    check(
        tx,
        signatures,
        &keys,
        network,
        context.protocol,
        context.payer.is_some(),
        &mut b,
    )
}
