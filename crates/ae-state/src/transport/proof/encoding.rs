use super::*;
use ae_codec::object::{FieldType as T, Value as V};

fn template() -> T {
    T::List(Box::new(T::Tuple(vec![
        T::Binary,
        T::List(Box::new(T::Tuple(vec![
            T::Binary,
            T::List(Box::new(T::Binary)),
        ]))),
    ])))
}
impl Proofs {
    pub fn serialize(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let b = Budget::new(limits);
        let mut fields = vec![];
        for (name, part) in [
            ("accounts", &self.accounts),
            ("calls", &self.calls),
            ("channels", &self.channels),
            ("contracts", &self.contracts),
            ("ns", &self.names),
            ("oracles", &self.oracles),
        ] {
            let value = match part {
                None => vec![],
                Some(p) => {
                    let mut nodes = vec![];
                    for (key, values) in &p.nodes {
                        b.entry(key)?;
                        for value in values {
                            b.charge(value.len().saturating_add(1))?;
                        }
                        nodes.push(V::Tuple(vec![
                            V::Binary(key.clone()),
                            V::List(values.iter().cloned().map(V::Binary).collect()),
                        ]));
                    }
                    vec![V::Tuple(vec![V::Binary(p.root.0.to_vec()), V::List(nodes)])]
                }
            };
            fields.push((name, template(), V::List(value)));
        }
        let out = encode(ObjectTag::TreesPoi, fields)?;
        b.charge(out.len())?;
        Ok(out)
    }
    pub fn deserialize(bytes: &[u8], limits: Limits) -> Result<Self, Error> {
        let b = Budget::new(limits);
        let fields = b.decode(
            bytes,
            ObjectTag::TreesPoi,
            &[
                template(),
                template(),
                template(),
                template(),
                template(),
                template(),
            ],
        )?;
        let mut parts = fields.into_iter().map(|v| decode_part(v, &b));
        Ok(Self {
            accounts: parts.next().unwrap()?,
            calls: parts.next().unwrap()?,
            channels: parts.next().unwrap()?,
            contracts: parts.next().unwrap()?,
            names: parts.next().unwrap()?,
            oracles: parts.next().unwrap()?,
        })
    }
}
fn decode_part(v: V, b: &Budget) -> Result<Option<Proof>, Error> {
    let V::List(mut entries) = v else {
        return Err(Error::InvalidValue);
    };
    if entries.is_empty() {
        return Ok(None);
    }
    if entries.len() != 1 {
        return Err(Error::InvalidValue);
    }
    let V::Tuple(fields) = entries.pop().unwrap() else {
        return Err(Error::InvalidValue);
    };
    let mut fields = fields.into_iter();
    let (Some(V::Binary(root)), Some(V::List(nodes))) = (fields.next(), fields.next()) else {
        return Err(Error::InvalidValue);
    };
    let root = Hash256(root.try_into().map_err(|_| Error::InvalidValue)?);
    let mut out = Proof::new(root);
    for node in nodes {
        let V::Tuple(pair) = node else {
            return Err(Error::InvalidValue);
        };
        let mut pair = pair.into_iter();
        let (Some(V::Binary(key)), Some(V::List(values))) = (pair.next(), pair.next()) else {
            return Err(Error::InvalidValue);
        };
        b.entry(&key)?;
        let values = values
            .into_iter()
            .map(|v| match v {
                V::Binary(bytes) => {
                    b.charge(bytes.len())?;
                    Ok(bytes)
                }
                _ => Err(Error::InvalidValue),
            })
            .collect::<Result<Vec<_>, _>>()?;
        out.nodes.insert(key, values);
    }
    Ok(Some(out))
}
