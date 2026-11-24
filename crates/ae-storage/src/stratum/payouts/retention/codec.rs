use super::*;
fn bytes(v: impl AsRef<[u8]>) -> Value {
    Value::Bytes(v.as_ref().to_vec())
}
fn blob(v: &Value) -> Result<&[u8], Failure> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::Corrupt("Stratum receipt bytes").into()),
    }
}
fn list(v: &Value) -> Result<&[Value], Failure> {
    match v {
        Value::List(v) => Ok(v),
        _ => Err(Error::Corrupt("Stratum receipt list").into()),
    }
}
fn fields<const N: usize>(v: &Value) -> Result<&[Value; N], Failure> {
    list(v)?
        .try_into()
        .map_err(|_| Error::Corrupt("Stratum receipt arity").into())
}
fn hash(v: &Value) -> Result<Hash256, Failure> {
    Hash256::try_from(blob(v)?).map_err(|_| Error::Corrupt("Stratum receipt hash").into())
}
impl Archive {
    pub(super) fn encode(&self) -> Result<Vec<u8>, Failure> {
        if self.entries.is_empty()
            || self.entries.len() > MAX_ENTRIES
            || self.entries.values().any(|e| e.nodes.len() > MAX_NODES)
        {
            return Err(Error::ResourceLimit.into());
        }
        let entries = self
            .entries
            .values()
            .map(|e| {
                let roots = &e.roots;
                Ok(Value::List(vec![
                    bytes(
                        e.header
                            .encode()
                            .map_err(|_| Error::Corrupt("receipt header"))?,
                    ),
                    Value::List(
                        [
                            roots.accounts,
                            roots.calls,
                            roots.channels,
                            roots.contracts,
                            roots.names,
                            roots.oracles,
                        ]
                        .into_iter()
                        .map(|r| bytes(r.map_or_else(Vec::new, |h| h.0.to_vec())))
                        .collect(),
                    ),
                    Value::List(
                        e.nodes
                            .iter()
                            .map(|(h, raw)| Value::List(vec![bytes(h.0), bytes(raw)]))
                            .collect(),
                    ),
                ]))
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        let raw = Value::List(vec![
            number(1),
            bytes(&self.identity),
            number(self.sequence),
            Value::List(vec![number(self.payment.0), number(self.payment.1.into())]),
            bytes(self.signed.encode()),
            Value::List(entries),
        ])
        .encode();
        if raw.len() > MAX_BYTES {
            return Err(Error::ResourceLimit.into());
        }
        Ok(raw)
    }
    pub(super) fn decode(raw: &[u8], max: usize) -> Result<Self, Failure> {
        let v = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: max.min(MAX_BYTES),
                max_items: 16384,
                max_depth: 6,
            },
        )
        .map_err(|e| match e {
            ae_codec::rlp::DecodeError::ResourceLimit => Error::ResourceLimit,
            _ => Error::Corrupt("Stratum receipt encoding"),
        })?;
        let v = Value::from_rlp(&v);
        let [version, identity, sequence, payment, signed, records] = fields(&v)?;
        if integer(version)? != 1 {
            return Err(Error::Corrupt("Stratum receipt version").into());
        }
        let [height, index] = fields(payment)?;
        let signed = ae_mempool::pool::Signed::decode(blob(signed)?)
            .map_err(|_| Error::Corrupt("Stratum receipt transaction"))?;
        let records = list(records)?;
        if records.len() > MAX_ENTRIES {
            return Err(Error::ResourceLimit.into());
        }
        let mut entries = BTreeMap::new();
        for record in records {
            let [header, roots, proof] = fields(record)?;
            let header = Header::decode(blob(header)?, HeaderMode::ProofOfWork)
                .map_err(|_| Error::Corrupt("receipt header"))?;
            let mut r = [None; 6];
            for (root, field) in r.iter_mut().zip(fields::<6>(roots)?) {
                if !blob(field)?.is_empty() {
                    *root = Some(hash(field)?);
                }
            }
            let [accounts, calls, channels, contracts, names, oracles] = r;
            let proof = list(proof)?;
            if proof.len() > MAX_NODES {
                return Err(Error::ResourceLimit.into());
            }
            let mut nodes = BTreeMap::new();
            for node in proof {
                let [h, raw] = fields(node)?;
                if nodes.insert(hash(h)?, blob(raw)?.to_vec()).is_some() {
                    return Err(Error::Corrupt("Stratum duplicate receipt node").into());
                }
            }
            let key = header
                .hash()
                .map_err(|_| Error::Corrupt("receipt header"))?;
            if entries
                .insert(
                    key,
                    Evidence {
                        header,
                        roots: ae_state::StateRoots {
                            accounts,
                            calls,
                            channels,
                            contracts,
                            names,
                            oracles,
                        },
                        nodes,
                    },
                )
                .is_some()
            {
                return Err(Error::Corrupt("Stratum duplicate receipt block").into());
            }
        }
        let archive = Self {
            identity: blob(identity)?.to_vec(),
            sequence: integer(sequence)?,
            payment: (
                integer(height)?,
                u32::try_from(integer(index)?)
                    .map_err(|_| Error::Corrupt("Stratum receipt payment"))?,
            ),
            signed,
            entries,
        };
        if archive.encode()? != raw {
            return Err(Error::Corrupt("Stratum receipt canonical encoding").into());
        }
        Ok(archive)
    }
}
