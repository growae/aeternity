use snow::params::{CipherChoice, DHChoice, HashChoice};
use snow::resolvers::{CryptoResolver, DefaultResolver};
use snow::types::{Cipher, Dh, Hash, Random};

pub(super) struct Resolver;
impl CryptoResolver for Resolver {
    fn resolve_rng(&self) -> Option<Box<dyn Random>> {
        DefaultResolver.resolve_rng()
    }
    fn resolve_hash(&self, choice: &HashChoice) -> Option<Box<dyn Hash>> {
        DefaultResolver.resolve_hash(choice)
    }
    fn resolve_cipher(&self, choice: &CipherChoice) -> Option<Box<dyn Cipher>> {
        DefaultResolver.resolve_cipher(choice)
    }
    fn resolve_dh(&self, choice: &DHChoice) -> Option<Box<dyn Dh>> {
        DefaultResolver
            .resolve_dh(choice)
            .map(|dh| Box::new(CheckedDh(dh)) as Box<dyn Dh>)
    }
}
struct CheckedDh(Box<dyn Dh>);
impl Dh for CheckedDh {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn pub_len(&self) -> usize {
        self.0.pub_len()
    }
    fn priv_len(&self) -> usize {
        self.0.priv_len()
    }
    fn set(&mut self, private: &[u8]) {
        self.0.set(private);
    }
    fn generate(&mut self, rng: &mut dyn Random) -> Result<(), snow::Error> {
        self.0.generate(rng)
    }
    fn pubkey(&self) -> &[u8] {
        self.0.pubkey()
    }
    fn privkey(&self) -> &[u8] {
        self.0.privkey()
    }
    fn dh(&self, public: &[u8], out: &mut [u8]) -> Result<(), snow::Error> {
        if public.len() < self.pub_len() || out.len() < self.dh_len() {
            return Err(snow::Error::Dh);
        }
        self.0.dh(public, out)?;
        if out[..self.dh_len()]
            .iter()
            .fold(0u8, |acc, byte| acc | byte)
            == 0
        {
            return Err(snow::Error::Dh);
        }
        Ok(())
    }
}
