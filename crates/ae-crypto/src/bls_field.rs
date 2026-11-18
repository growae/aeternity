// emcl e988f69 and MCL 11976be, as used by aefa_fate_op at fa2a6d7.
use num_bigint::BigUint;
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlsField {
    Scalar,
    Base,
}
struct Parameters {
    modulus: BigUint,
    radix: BigUint,
}
impl Parameters {
    fn new(modulus: &[u8], bits: usize) -> Self {
        let modulus = BigUint::parse_bytes(modulus, 16).expect("fixed field modulus");
        let radix = (BigUint::from(1u8) << bits) % &modulus;
        Self { modulus, radix }
    }
}
static SCALAR: LazyLock<Parameters> = LazyLock::new(|| {
    Parameters::new(
        b"73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001",
        256,
    )
});
static BASE: LazyLock<Parameters> = LazyLock::new(|| {
    Parameters::new(b"1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab", 384)
});
impl BlsField {
    fn parameters(self) -> &'static Parameters {
        match self {
            Self::Scalar => &SCALAR,
            Self::Base => &BASE,
        }
    }
    pub fn modulus(self) -> &'static BigUint {
        &self.parameters().modulus
    }
    pub fn encoded_len(self) -> usize {
        match self {
            Self::Scalar => 32,
            Self::Base => 48,
        }
    }
    // The original emcl API exposes little-endian Montgomery limbs, not the
    // usual BLS scalar/point serialization. Inputs here must be canonical.
    pub fn encode_integer(self, value: &BigUint) -> Option<Vec<u8>> {
        let p = self.parameters();
        if value >= &p.modulus {
            return None;
        }
        let mut bytes = (value * &p.radix % &p.modulus).to_bytes_le();
        bytes.resize(self.encoded_len(), 0);
        Some(bytes)
    }
}
