// Aeternity fa2a6d7, aens_commitments/aens_auctions.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commitment {
    pub hash: [u8; 32],
    pub owner: Id,
    pub created: Unsigned,
    pub expires_by: Unsigned,
}
impl Commitment {
    pub fn new(hash: [u8; 32], owner: AccountPublicKey, delta: Unsigned, height: Unsigned) -> Self {
        Self {
            hash,
            owner: Id::new(IdTag::Account, owner.0),
            expires_by: height.add(&delta),
            created: height,
        }
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        encode(
            ObjectTag::NameCommitment,
            "owner_id",
            self.owner,
            &[("created", &self.created), ("ttl", &self.expires_by)],
        )
    }
    pub fn deserialize(hash: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let (owner, ints) = decode(
            ObjectTag::NameCommitment,
            "owner_id",
            &["created", "ttl"],
            bytes,
        )?;
        Ok(Self {
            hash,
            owner,
            created: ints[0].clone(),
            expires_by: ints[1].clone(),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auction {
    pub name_hash: [u8; 32],
    pub bidder: Id,
    pub bid: Unsigned,
    pub started: Unsigned,
    pub expires_by: Unsigned,
}
impl Auction {
    pub fn new(
        name_hash: [u8; 32],
        bidder: AccountPublicKey,
        bid: Unsigned,
        delta: Unsigned,
        height: Unsigned,
    ) -> Self {
        Self {
            name_hash,
            bidder: Id::new(IdTag::Account, bidder.0),
            bid,
            expires_by: height.add(&delta),
            started: height,
        }
    }
    pub fn extend(
        &self,
        bidder: AccountPublicKey,
        bid: Unsigned,
        delta: Unsigned,
        height: Unsigned,
    ) -> Self {
        let mut new = Self::new(self.name_hash, bidder, bid, delta, height);
        new.expires_by = new.expires_by.max(self.expires_by.clone());
        new
    }
    pub fn hash(&self) -> [u8; 33] {
        super::hashing::auction_hash(self.name_hash)
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        encode(
            ObjectTag::NameAuction,
            "bidder_id",
            self.bidder,
            &[
                ("bid", &self.bid),
                ("started", &self.started),
                ("ttl", &self.expires_by),
            ],
        )
    }
    pub fn deserialize(hash: [u8; 33], bytes: &[u8]) -> Result<Self, Error> {
        if hash[32] != 0 {
            return Err(Error::InvalidValue);
        }
        let (bidder, ints) = decode(
            ObjectTag::NameAuction,
            "bidder_id",
            &["bid", "started", "ttl"],
            bytes,
        )?;
        Ok(Self {
            name_hash: hash[..32].try_into().expect("fixed size"),
            bidder,
            bid: ints[0].clone(),
            started: ints[1].clone(),
            expires_by: ints[2].clone(),
        })
    }
}
fn schema(id_name: &str, ints: &[&str]) -> Vec<Field> {
    std::iter::once(Field {
        name: id_name.into(),
        kind: FieldType::Id,
    })
    .chain(ints.iter().map(|name| Field {
        name: (*name).into(),
        kind: FieldType::Integer,
    }))
    .collect()
}
fn encode(
    tag: ObjectTag,
    id_name: &str,
    id: Id,
    ints: &[(&str, &Unsigned)],
) -> Result<Vec<u8>, Error> {
    let fields = std::iter::once((id_name.into(), Value::Id(id)))
        .chain(
            ints.iter()
                .map(|(name, value)| ((*name).into(), Value::Integer((*value).clone()))),
        )
        .collect::<Vec<_>>();
    Ok(object::serialize(
        tag,
        &1.into(),
        &schema(
            id_name,
            &ints.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        ),
        &fields,
    )?)
}
fn decode(
    tag: ObjectTag,
    id_name: &str,
    ints: &[&str],
    bytes: &[u8],
) -> Result<(Id, Vec<Unsigned>), Error> {
    let fields = object::deserialize(bytes, tag, &1.into(), &schema(id_name, ints))?;
    let Value::Id(id) = fields[0].1 else {
        return Err(Error::InvalidValue);
    };
    let values = fields
        .into_iter()
        .skip(1)
        .map(|(_, value)| match value {
            Value::Integer(n) => Ok(n),
            _ => Err(Error::InvalidValue),
        })
        .collect::<Result<_, _>>()?;
    Ok((id, values))
}
