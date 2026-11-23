use super::*;
use ae_codec::headers::Header;
use ae_crypto::Ed25519SecretKey;
use ed25519_dalek::SigningKey;
use rand::RngCore;

pub(super) trait Entropy {
    fn seed(&mut self) -> Result<Zeroizing<[u8; 32]>, Error>;
}
pub(super) struct Os;
impl Entropy for Os {
    fn seed(&mut self) -> Result<Zeroizing<[u8; 32]>, Error> {
        let mut seed = Zeroizing::new([0; 32]);
        rand::rngs::OsRng
            .try_fill_bytes(seed.as_mut())
            .map_err(|_| Error::Entropy)?;
        Ok(seed)
    }
}
fn new_signing(rng: &mut impl Entropy) -> Result<Ed25519SecretKey, Error> {
    let seed = rng.seed()?;
    Ok(Ed25519SecretKey::from_seed(&seed))
}
pub(super) fn peer_secret(seed: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    let key = SigningKey::from_bytes(seed);
    let mut scalar = Zeroizing::new(key.to_scalar_bytes());
    // libsodium's conversion stores the clamped (not reduced) SHA-512 scalar.
    scalar[0] &= 248;
    scalar[31] &= 127;
    scalar[31] |= 64;
    scalar
}
pub(super) struct State {
    _lease: files::Lease,
    signing: Ed25519SecretKey,
    candidate: Ed25519SecretKey,
    identity: Arc<noise::Identity>,
}
impl State {
    pub fn open(config: &Config, rng: &mut impl Entropy) -> Result<Self, Error> {
        let lease = files::lease(&config.directory)?;
        let signing = new_signing(rng)?;
        let candidate = new_signing(rng)?;
        let identity = files::load_or_create(config, rng)?;
        Ok(Self {
            _lease: lease,
            signing,
            candidate,
            identity: Arc::new(identity),
        })
    }
    pub fn public(&self) -> PublicKeys {
        PublicKeys {
            signing: self.signing.public_key(),
            candidate: self.candidate.public_key(),
            peer: self.identity.public_key(),
        }
    }
    pub fn identity(&self) -> Arc<noise::Identity> {
        self.identity.clone()
    }
    pub fn promote(
        &mut self,
        expected: AccountPublicKey,
        rng: &mut impl Entropy,
    ) -> Result<PublicKeys, Error> {
        if self.candidate.public_key() != expected {
            return Err(Error::KeyNotFound);
        }
        // Generate first: entropy failure cannot partially promote a candidate.
        let next = new_signing(rng)?;
        self.signing = std::mem::replace(&mut self.candidate, next);
        Ok(self.public())
    }
    pub fn sign(
        &self,
        mut header: MicroHeader,
        expected: Option<AccountPublicKey>,
    ) -> Result<SignedMicro, Error> {
        let signer = self.signing.public_key();
        if expected.is_some_and(|key| key != signer) {
            return Err(Error::KeyChanged);
        }
        let bytes = Header::Micro(header.clone())
            .signature_bytes()
            .map_err(|_| Error::Header)?;
        header.signature = self.signing.sign(&bytes).0;
        Ok(SignedMicro { header, signer })
    }
}
