use super::*;
use ae_trie::NodeReader;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    CheckFailed,
    VerificationFailed,
    InnerIncorrectlySigned,
    ResourceLimit,
    MissingChannelContext,
    ChannelNotFound,
    ChannelDoesNotExist,
    Channel(ae_state::channels::Error),
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
fn remove_authenticated(
    keys: &mut Vec<AccountPublicKey>,
    authenticated: impl IntoIterator<Item = AccountPublicKey>,
) {
    // Erlang list subtraction removes one occurrence for each authenticated
    // entry, including repeated identities from nested GA wrappers.
    for key in authenticated {
        if let Some(index) = keys.iter().position(|k| *k == key) {
            keys.remove(index);
        }
    }
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
    resolve: &impl Fn(&TransactionLeaf) -> Result<Vec<AccountPublicKey>, SignatureError>,
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
            None => {
                break resolve(tx.leaf()).map_err(|e| {
                    if pending.is_empty() || e.is_operational() {
                        e
                    } else {
                        SignatureError::InnerIncorrectlySigned
                    }
                })?;
            }
        }
    };
    for (inner, sigs, payer) in pending.into_iter().rev() {
        // The payer callback always verifies its inner at Iris and does not
        // remove GA-authenticated identities from that callback check.
        check(inner, sigs, &result, network, Protocol::Iris, true, b).map_err(|e| {
            if e.is_operational() {
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
    let keys = signers(tx, network, &mut b, &without_channel_context).map_err(|e| {
        if e.is_operational() {
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
    let mut keys = signers(tx, network, &mut b, &without_channel_context).map_err(|e| {
        if e.is_operational() {
            e
        } else {
            SignatureError::VerificationFailed
        }
    })?;
    remove_authenticated(&mut keys, context.ga_authenticated.iter().copied());
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

// Leading GA wrappers do not make an unrelated signer relevant. The first
// non-meta callback may itself perform the payer's strict inner verification.
pub(super) fn relevant(
    mut tx: TransactionRef<'_>,
    owner: AccountPublicKey,
    network: &NetworkProfile,
    limits: SignatureLimits,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<bool, SignatureError> {
    let mut budget = Budget::from(limits);
    while matches!(tx.layer().map(|l| l.wrapper()), Some(Wrapper::GaMeta(_))) {
        budget.layers = budget
            .layers
            .checked_sub(1)
            .ok_or(SignatureError::ResourceLimit)?;
        tx = tx.inner().expect("meta inner");
    }
    match signers(tx, network, &mut budget, &|leaf| {
        with_channels(leaf, channels)
    }) {
        Ok(keys) => Ok(keys.contains(&owner)),
        Err(e) if e.is_operational() => Err(e),
        Err(_) => Ok(false),
    }
}

impl SignatureError {
    pub fn is_operational(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit | Self::MissingChannelContext | Self::Channel(_)
        )
    }
}
fn requirement(
    leaf: &TransactionLeaf,
) -> Result<ae_tx::channels::SignerRequirement, SignatureError> {
    match leaf {
        TransactionLeaf::Channel(t) => t
            .signer_requirement()
            .map_err(|_| SignatureError::CheckFailed),
        _ => Ok(ae_tx::channels::SignerRequirement::Accounts(vec![
            leaf.origin().map_err(|_| SignatureError::CheckFailed)?,
        ])),
    }
}
fn without_channel_context(
    leaf: &TransactionLeaf,
) -> Result<Vec<AccountPublicKey>, SignatureError> {
    match requirement(leaf)? {
        ae_tx::channels::SignerRequirement::Accounts(keys) => Ok(keys),
        ae_tx::channels::SignerRequirement::Channel(_) => {
            Err(SignatureError::MissingChannelContext)
        }
    }
}
fn with_channels(
    leaf: &TransactionLeaf,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<Vec<AccountPublicKey>, SignatureError> {
    match requirement(leaf)? {
        ae_tx::channels::SignerRequirement::Accounts(keys) => Ok(keys),
        ae_tx::channels::SignerRequirement::Channel(key) => {
            let c = channels
                .lookup(key)
                .map_err(SignatureError::Channel)?
                .ok_or(if leaf.kind() == TransactionKind::ChannelOffchain {
                    SignatureError::ChannelDoesNotExist
                } else {
                    SignatureError::ChannelNotFound
                })?;
            Ok(vec![c.initiator, c.responder])
        }
    }
}
pub fn signer_keys(
    tx: TransactionRef<'_>,
    network: &NetworkProfile,
    limits: SignatureLimits,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<Vec<AccountPublicKey>, SignatureError> {
    signers(tx, network, &mut limits.into(), &|leaf| {
        with_channels(leaf, channels)
    })
}
pub fn verify_with_trees(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    network: &NetworkProfile,
    context: &Context,
    limits: SignatureLimits,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<(), SignatureError> {
    verify_with_authenticated_ids(
        tx,
        signatures,
        network,
        context,
        &context.ga_authenticated.iter().copied().collect::<Vec<_>>(),
        limits,
        channels,
    )
}
// Authentication entries must come from validated wrapper execution. Unlike
// Context's membership set, this list retains repeated nested GA identities.
pub fn verify_with_authenticated_ids(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    network: &NetworkProfile,
    context: &Context,
    authenticated: &[AccountPublicKey],
    limits: SignatureLimits,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<(), SignatureError> {
    let mut b = limits.into();
    let mut keys =
        signers(tx, network, &mut b, &|leaf| with_channels(leaf, channels)).map_err(|e| {
            if e.is_operational() {
                e
            } else {
                SignatureError::VerificationFailed
            }
        })?;
    remove_authenticated(&mut keys, authenticated.iter().copied());
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

pub fn verify_with_channel_state(
    tx: TransactionRef<'_>,
    signatures: &[Signature],
    network: &NetworkProfile,
    protocol: Protocol,
    sponsored: bool,
    limits: SignatureLimits,
    channels: &ae_state::channels::Channels<impl NodeReader>,
) -> Result<(), SignatureError> {
    let mut b = limits.into();
    let keys =
        signers(tx, network, &mut b, &|leaf| with_channels(leaf, channels)).map_err(|e| {
            if e.is_operational() {
                e
            } else {
                SignatureError::CheckFailed
            }
        })?;
    check(tx, signatures, &keys, network, protocol, sponsored, &mut b)
}
