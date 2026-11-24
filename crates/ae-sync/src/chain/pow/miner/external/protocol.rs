use super::{Error, Outcome, Work, cuckoo};

pub(super) struct Framing {
    pub(super) buffer: Vec<u8>,
    limit: usize,
}
impl Framing {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            buffer: vec![],
            limit,
        }
    }
    // Empty input and a split newline are valid stream fragments. The native
    // string:tokens implementation loses these boundaries; do not copy that bug.
    pub(super) fn feed<T>(
        &mut self,
        bytes: &[u8],
        mut line: impl FnMut(&[u8]) -> Result<Option<T>, Error>,
    ) -> Result<Option<T>, Error> {
        for &byte in bytes {
            if byte == b'\n' {
                let value = line(&self.buffer)?;
                self.buffer.clear();
                if value.is_some() {
                    return Ok(value);
                }
            } else {
                if self.buffer.len() == self.limit {
                    return Err(Error::LineLimit);
                }
                self.buffer.push(byte);
            }
        }
        Ok(None)
    }
}
pub(super) fn parse(line: &[u8], work: Work) -> Result<Option<Outcome>, Error> {
    let Some(rest) = line.strip_prefix(b"Solution") else {
        return Ok(None);
    };
    let mut tokens = rest.split(|&c| c == b' ').filter(|s| !s.is_empty());
    let nonce = tokens.next().ok_or(Error::BadNonce)?;
    let mut proof = [0; 42];
    for edge in &mut proof {
        let token = tokens.next().ok_or(Error::BadSize)?;
        // Miner words are hexadecimal, not decimal; reject signed/range errors
        // rather than reproducing native bit-syntax integer truncation.
        *edge = std::str::from_utf8(token)
            .ok()
            .and_then(|s| u64::from_str_radix(s.trim_matches('\r'), 16).ok())
            .ok_or(Error::BadEdge)?;
    }
    if tokens.next().is_some() {
        return Err(Error::BadSize);
    }
    // Preserve the native target-filter-before-nonce-decoding behavior.
    if !cuckoo::solution_hash(&proof, work.edge_bits).is_some_and(|hash| work.target.accepts(&hash))
    {
        return Ok(None);
    }
    let nonce = std::str::from_utf8(nonce)
        .ok()
        .and_then(|s| u64::from_str_radix(s.trim_matches(['(', ')']), 16).ok())
        .ok_or(Error::BadNonce)?;
    Ok(Some(Outcome::Found {
        nonce,
        proof: Box::new(proof),
    }))
}
