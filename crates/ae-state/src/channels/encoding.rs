use super::*;
use ae_codec::object::{self, Field, FieldType as T, ObjectTag, Value as V};

pub(super) fn encode(tag: ObjectTag, fields: Vec<(&str, T, V)>) -> Result<Vec<u8>, Error> {
    let schema = fields
        .iter()
        .map(|(n, t, _)| Field {
            name: (*n).into(),
            kind: t.clone(),
        })
        .collect::<Vec<_>>();
    let values = fields
        .into_iter()
        .map(|(n, _, v)| (n.into(), v))
        .collect::<Vec<_>>();
    Ok(object::serialize(tag, &1.into(), &schema, &values)?)
}
fn schema(version: u8) -> Result<Vec<Field>, Error> {
    if !(1..=3).contains(&version) {
        return Err(Error::InvalidVersion);
    }
    let mut f = vec![
        ("initiator_id", T::Id),
        ("responder_id", T::Id),
        ("channel_amount", T::Integer),
        ("initiator_amount", T::Integer),
        ("responder_amount", T::Integer),
        ("channel_reserve", T::Integer),
    ];
    if version == 3 {
        f.extend([
            ("initiator_delegate_ids", T::List(Box::new(T::Id))),
            ("responder_delegate_ids", T::List(Box::new(T::Id))),
        ]);
    } else {
        f.push(("delegate_ids", T::List(Box::new(T::Id))));
    }
    f.extend([
        ("state_hash", T::Binary),
        ("round", T::Integer),
        ("solo_round", T::Integer),
        ("lock_period", T::Integer),
        ("locked_until", T::Integer),
    ]);
    if version >= 2 {
        f.extend([("initiator_auth", T::Binary), ("responder_auth", T::Binary)]);
    }
    Ok(f.into_iter()
        .map(|(n, t)| Field {
            name: n.into(),
            kind: t,
        })
        .collect())
}
fn auth_bytes(auth: &Option<Authorization>) -> Result<Vec<u8>, Error> {
    match auth {
        None => Ok(vec![]),
        Some(a) if a.contract.tag == IdTag::Contract => {
            Ok([a.function.as_slice(), &a.contract.value].concat())
        }
        Some(_) => Err(Error::InvalidIdType),
    }
}
fn auth(bytes: Vec<u8>) -> Result<Option<Authorization>, Error> {
    match bytes.len() {
        0 => Ok(None),
        64 => Ok(Some(Authorization {
            function: bytes[..32].to_vec(),
            contract: Id::new(IdTag::Contract, bytes[32..].try_into().unwrap()),
        })),
        _ => Err(Error::InvalidAuthorization),
    }
}
struct Fields(std::vec::IntoIter<(String, V)>);
impl Fields {
    fn next(&mut self) -> Result<V, Error> {
        self.0.next().map(|(_, v)| v).ok_or(Error::InvalidValue)
    }
    fn number(&mut self) -> Result<Unsigned, Error> {
        match self.next()? {
            V::Integer(n) => Ok(n),
            _ => Err(Error::InvalidValue),
        }
    }
    fn binary(&mut self) -> Result<Vec<u8>, Error> {
        match self.next()? {
            V::Binary(b) => Ok(b),
            _ => Err(Error::InvalidValue),
        }
    }
    fn account(&mut self) -> Result<AccountPublicKey, Error> {
        match self.next()? {
            V::Id(id) if id.tag == IdTag::Account => Ok(AccountPublicKey(id.value)),
            V::Id(_) => Err(Error::InvalidIdType),
            _ => Err(Error::InvalidValue),
        }
    }
    fn ids(&mut self) -> Result<Vec<Id>, Error> {
        match self.next()? {
            V::List(v) => v
                .into_iter()
                .map(|v| match v {
                    V::Id(id) => Ok(id),
                    _ => Err(Error::InvalidValue),
                })
                .collect(),
            _ => Err(Error::InvalidValue),
        }
    }
}
impl Channel {
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let schema = schema(self.object_version)?;
        let mut v = vec![
            V::Id(Id::new(IdTag::Account, self.initiator.0)),
            V::Id(Id::new(IdTag::Account, self.responder.0)),
            V::Integer(self.channel_amount.clone()),
            V::Integer(self.initiator_amount.clone()),
            V::Integer(self.responder_amount.clone()),
            V::Integer(self.channel_reserve.clone()),
        ];
        let list = |ids: &[Id]| V::List(ids.iter().copied().map(V::Id).collect());
        match (&self.delegates, self.object_version) {
            (Delegates::Shared(ids), 1 | 2) => v.push(list(ids)),
            (
                Delegates::Separate {
                    initiator,
                    responder,
                },
                3,
            ) => v.extend([list(initiator), list(responder)]),
            _ => return Err(Error::InvalidDelegates),
        }
        v.extend([
            V::Binary(self.state_hash.clone()),
            V::Integer(self.round.clone()),
            V::Integer(self.solo_round.clone()),
            V::Integer(self.lock_period.clone()),
            V::Integer(self.locked_until.clone()),
        ]);
        if self.object_version >= 2 {
            v.extend([
                V::Binary(auth_bytes(&self.initiator_auth)?),
                V::Binary(auth_bytes(&self.responder_auth)?),
            ]);
        }
        let fields = schema
            .iter()
            .zip(v)
            .map(|(f, v)| (f.name.clone(), v))
            .collect::<Vec<_>>();
        Ok(object::serialize(
            ObjectTag::Channel,
            &u64::from(self.object_version).into(),
            &schema,
            &fields,
        )?)
    }
    pub fn deserialize(public_key: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let env = object::decode_envelope(bytes)?;
        let object_version = u8::try_from(env.version.to_u64().map_err(|_| Error::InvalidVersion)?)
            .map_err(|_| Error::InvalidVersion)?;
        let mut f = Fields(
            object::deserialize(
                bytes,
                ObjectTag::Channel,
                &env.version,
                &schema(object_version)?,
            )?
            .into_iter(),
        );
        let initiator = f.account()?;
        let responder = f.account()?;
        let channel_amount = f.number()?;
        let initiator_amount = f.number()?;
        let responder_amount = f.number()?;
        let channel_reserve = f.number()?;
        let first = f.ids()?;
        let delegates = if object_version == 3 {
            // Version 3 retains non-account delegate tags on decoding. They
            // fail only when a caller requests specialized account keys.
            Delegates::Separate {
                initiator: first,
                responder: f.ids()?,
            }
        } else {
            if first.iter().any(|id| id.tag != IdTag::Account) {
                return Err(Error::InvalidIdType);
            }
            Delegates::Shared(first)
        };
        let state_hash = f.binary()?;
        let round = f.number()?;
        let solo_round = f.number()?;
        let lock_period = f.number()?;
        let locked_until = f.number()?;
        let (initiator_auth, responder_auth) = if object_version == 1 {
            (None, None)
        } else {
            (auth(f.binary()?)?, auth(f.binary()?)?)
        };
        Ok(Self {
            public_key,
            initiator,
            responder,
            delegates,
            channel_amount,
            initiator_amount,
            responder_amount,
            channel_reserve,
            state_hash,
            round,
            solo_round,
            lock_period,
            locked_until,
            initiator_auth,
            responder_auth,
            object_version,
        })
    }
    pub fn for_client(&self) -> Result<serde_json::Value, Error> {
        use ae_codec::api::{ApiType, encode, encode_id};
        use serde_json::json;
        let ids = |ids: &[Id]| ids.iter().copied().map(encode_id).collect::<Vec<_>>();
        let delegates = match (&self.delegates, self.object_version) {
            (Delegates::Shared(v), 1 | 2) => json!(ids(v)),
            (
                Delegates::Separate {
                    initiator,
                    responder,
                },
                3,
            ) => json!({"initiator":ids(initiator),"responder":ids(responder)}),
            _ => return Err(Error::InvalidDelegates),
        };
        Ok(
            json!({"id":encode_id(self.id()),"initiator_id":encode_id(Id::new(IdTag::Account,self.initiator.0)),"responder_id":encode_id(Id::new(IdTag::Account,self.responder.0)),
            "channel_amount":number(&self.channel_amount),"initiator_amount":number(&self.initiator_amount),"responder_amount":number(&self.responder_amount),"channel_reserve":number(&self.channel_reserve),
            "delegate_ids":delegates,"state_hash":encode(ApiType::State,&self.state_hash),"round":number(&self.round),"solo_round":number(&self.solo_round),"lock_period":number(&self.lock_period),"locked_until":number(&self.locked_until)}),
        )
    }
}
fn number(n: &Unsigned) -> serde_json::Number {
    let mut n = n.clone();
    let mut digits = vec![];
    loop {
        let (q, r) = n.div_rem_u64(10).unwrap();
        digits.push(char::from(b'0' + r as u8));
        n = q;
        if n == 0.into() {
            break;
        }
    }
    digits
        .into_iter()
        .rev()
        .collect::<String>()
        .parse()
        .expect("decimal unsigned integer")
}
