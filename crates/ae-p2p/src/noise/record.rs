use super::{Error, MAX_CIPHERTEXT};

pub fn encode(ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
    if !(16..=MAX_CIPHERTEXT).contains(&ciphertext.len()) {
        return Err(Error::InvalidRecord);
    }
    let mut packet = Vec::with_capacity(ciphertext.len() + 2);
    packet.extend_from_slice(&(ciphertext.len() as u16).to_be_bytes());
    packet.extend_from_slice(ciphertext);
    Ok(packet)
}

pub struct Progress {
    pub consumed: usize,
    pub record: Option<Vec<u8>>,
}
pub struct Decoder {
    prefix: [u8; 2],
    prefix_len: usize,
    body: Vec<u8>,
    expected: usize,
    limit: usize,
    closed: bool,
}
impl Decoder {
    pub fn new(limit: usize) -> Self {
        Self {
            prefix: [0; 2],
            prefix_len: 0,
            body: vec![],
            expected: 0,
            limit: limit.min(MAX_CIPHERTEXT),
            closed: false,
        }
    }
    pub fn is_pending(&self) -> bool {
        self.prefix_len != 0
    }
    pub fn buffered_bytes(&self) -> usize {
        self.prefix_len + self.body.len()
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.prefix_len = 0;
        self.expected = 0;
        self.body = vec![];
    }
    pub fn push(&mut self, input: &[u8]) -> Result<Progress, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        let prefix_take = (2 - self.prefix_len).min(input.len());
        self.prefix[self.prefix_len..self.prefix_len + prefix_take]
            .copy_from_slice(&input[..prefix_take]);
        self.prefix_len += prefix_take;
        if self.prefix_len != 2 {
            return Ok(Progress {
                consumed: prefix_take,
                record: None,
            });
        }
        if self.expected == 0 {
            let length = usize::from(u16::from_be_bytes(self.prefix));
            if length < 16 || length > self.limit {
                self.close();
                return Err(Error::InvalidRecord);
            }
            self.expected = length;
        }
        let body_take = (self.expected - self.body.len()).min(input.len() - prefix_take);
        self.body
            .extend_from_slice(&input[prefix_take..prefix_take + body_take]);
        let record = if self.body.len() == self.expected {
            self.prefix_len = 0;
            self.expected = 0;
            Some(std::mem::take(&mut self.body))
        } else {
            None
        };
        Ok(Progress {
            consumed: prefix_take + body_take,
            record,
        })
    }
}
