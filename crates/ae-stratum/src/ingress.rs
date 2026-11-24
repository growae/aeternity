// Operational identities are separate from candidate hashes: distinct shares
// may legitimately have the same account, target and candidate hash.
use crate::{Error, integer, nonce::Nonce, share::Effect};
use ae_trie::Value;
use ae_types::{Hash256, Unsigned};
use num_bigint::{BigInt, Sign};

pub const MAX_BYTES: usize = 8192;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id {
    pub epoch: u64,
    pub sequence: u64,
}
impl Id {
    pub fn validate(self) -> Result<(), Error> {
        if self.epoch == 0 || self.sequence == 0 {
            Err(Error::Configuration)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Solution {
    pub nonce: Nonce,
    pub proof: Box<[u64; 42]>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    pub account: String,
    pub target: BigInt,
    pub hash: Hash256,
    pub solution: Option<Solution>,
}
fn num(n: u64) -> Value {
    Value::Bytes(Unsigned::from(n).as_bytes().to_vec())
}
fn blob(v: &Value) -> Result<&[u8], Error> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Configuration)
    }
}
fn uint(v: &Value) -> Result<u64, Error> {
    Unsigned::from_field_bytes(blob(v)?)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Configuration)
}
impl Input {
    pub fn from_effects(effects: &[Effect]) -> Result<Self, Error> {
        let Some(Effect::SubmitShare {
            account,
            target,
            hash,
        }) = effects.first()
        else {
            return Err(Error::Configuration);
        };
        if account.len() > 1024 || !account.starts_with("ak_") {
            return Err(Error::Configuration);
        }
        integer(target)?;
        if target.sign() == Sign::Minus {
            return Err(Error::Integer);
        }
        let solution = match &effects[1..] {
            [] => None,
            [
                Effect::SubmitSolution {
                    hash: h,
                    nonce,
                    proof,
                },
            ] if h == hash => Some(Solution {
                nonce: *nonce,
                proof: proof.clone(),
            }),
            _ => return Err(Error::Configuration),
        };
        let input = Self {
            account: account.clone(),
            target: target.clone(),
            hash: *hash,
            solution,
        };
        input.validate()?;
        Ok(input)
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.account.len() > 1024 {
            return Err(Error::Limit("ingress account"));
        }
        if !self.account.starts_with("ak_") {
            return Err(Error::Configuration);
        }
        if self.target.sign() == Sign::Minus {
            return Err(Error::Integer);
        }
        integer(&self.target)
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let (_, magnitude) = self.target.to_bytes_be();
        let solution = self
            .solution
            .as_ref()
            .map(|s| {
                vec![
                    num(s.nonce.0),
                    Value::List(s.proof.iter().map(|n| num(*n)).collect()),
                ]
            })
            .unwrap_or_default();
        let raw = Value::List(vec![
            num(1),
            Value::Bytes(self.account.as_bytes().to_vec()),
            Value::Bytes(magnitude),
            Value::Bytes(self.hash.0.to_vec()),
            Value::List(solution),
        ])
        .encode();
        if raw.len() > MAX_BYTES {
            return Err(Error::Limit("ingress bytes"));
        }
        Ok(raw)
    }
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        let value = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: MAX_BYTES,
                max_items: 55,
                max_depth: 3,
            },
        )
        .map_err(|_| Error::Configuration)?;
        let Value::List(v) = Value::from_rlp(&value) else {
            return Err(Error::Configuration);
        };
        let [version, account, magnitude, hash, Value::List(solution)] = v.as_slice() else {
            return Err(Error::Configuration);
        };
        if uint(version)? != 1 {
            return Err(Error::Version);
        }
        let solution = match solution.as_slice() {
            [] => None,
            [nonce, Value::List(proof)] if proof.len() == 42 => {
                let mut edges = [0; 42];
                for (i, v) in proof.iter().enumerate() {
                    edges[i] = uint(v)?;
                }
                Some(Solution {
                    nonce: Nonce(uint(nonce)?),
                    proof: Box::new(edges),
                })
            }
            _ => return Err(Error::Configuration),
        };
        let input = Self {
            account: String::from_utf8(blob(account)?.to_vec())
                .map_err(|_| Error::Configuration)?,
            target: BigInt::from_bytes_be(Sign::Plus, blob(magnitude)?),
            hash: Hash256(blob(hash)?.try_into().map_err(|_| Error::Hash)?),
            solution,
        };
        if input.encode()? != raw {
            return Err(Error::Configuration);
        }
        Ok(input)
    }
}
