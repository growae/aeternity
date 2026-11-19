use super::{Channel, Error, encoding::encode};
use ae_codec::object::{self, Field, FieldType as T, ObjectTag, Value as V};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
use ae_types::Hash256;

#[derive(Debug, Clone)]
pub struct Channels<S = MemoryStore> {
    tree: Trie<S>,
}
impl Default for Channels<MemoryStore> {
    fn default() -> Self {
        Self::new(MemoryStore::default())
    }
}
impl<S> From<Trie<S>> for Channels<S> {
    fn from(tree: Trie<S>) -> Self {
        Self { tree }
    }
}
impl<S: NodeReader> Channels<S> {
    pub fn new(store: S) -> Self {
        Self {
            tree: Trie::new(store),
        }
    }
    pub fn from_root(root: Option<Hash256>, store: S) -> Result<Self, Error> {
        Ok(Trie::from_root(root, store)?.into())
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn lookup(&self, key: [u8; 32]) -> Result<Option<Channel>, Error> {
        match self.tree.get(&Nibbles::from_bytes(&key))? {
            None => Ok(None),
            Some(Value::Bytes(b)) => Ok(Some(Channel::deserialize(key, &b)?)),
            _ => Err(Error::InvalidValue),
        }
    }
    pub fn to_binary_without_backend(&self) -> Result<Vec<u8>, Error> {
        let mut values = vec![];
        for item in self.tree.iter() {
            let (key, value) = item?;
            if key.as_slice().len() % 2 != 0 {
                return Err(Error::InvalidValue);
            }
            let key = key
                .as_slice()
                .chunks_exact(2)
                .map(|p| (p[0] << 4) | p[1])
                .collect();
            let Value::Bytes(value) = value else {
                return Err(Error::InvalidValue);
            };
            values.push(V::Binary(encode(
                ObjectTag::MtreeValue,
                vec![
                    ("key", T::Binary, V::Binary(key)),
                    ("val", T::Binary, V::Binary(value)),
                ],
            )?));
        }
        // aeu_mtrees:map prepends each ascending iterator result.
        values.reverse();
        let mtree = encode(
            ObjectTag::Mtree,
            vec![("values", T::List(Box::new(T::Binary)), V::List(values))],
        )?;
        encode(
            ObjectTag::ChannelsMtree,
            vec![("channels", T::Binary, V::Binary(mtree))],
        )
    }
}
impl<S: NodeWriter> Channels<S> {
    pub fn enter(&mut self, channel: &Channel) -> Result<(), Error> {
        self.tree.put(
            &Nibbles::from_bytes(&channel.public_key),
            Value::Bytes(channel.serialize()?),
        )?;
        Ok(())
    }
    pub fn delete(&mut self, key: [u8; 32]) -> Result<(), Error> {
        self.tree.delete(&Nibbles::from_bytes(&key))?;
        Ok(())
    }
    pub fn from_binary_without_backend(bytes: &[u8], store: S) -> Result<Self, Error> {
        let mut outer =
            decode(bytes, ObjectTag::ChannelsMtree, &[("channels", T::Binary)])?.into_iter();
        let Some(V::Binary(mtree)) = outer.next() else {
            return Err(Error::InvalidValue);
        };
        let mut inner = decode(
            &mtree,
            ObjectTag::Mtree,
            &[("values", T::List(Box::new(T::Binary)))],
        )?
        .into_iter();
        let Some(V::List(values)) = inner.next() else {
            return Err(Error::InvalidValue);
        };
        let mut result = Self::new(store);
        for value in values {
            let V::Binary(bytes) = value else {
                return Err(Error::InvalidValue);
            };
            let mut fields = decode(
                &bytes,
                ObjectTag::MtreeValue,
                &[("key", T::Binary), ("val", T::Binary)],
            )?
            .into_iter();
            let (Some(V::Binary(key)), Some(V::Binary(value))) = (fields.next(), fields.next())
            else {
                return Err(Error::InvalidValue);
            };
            let key = Nibbles::from_bytes(&key);
            if result.tree.get(&key)?.is_some() {
                return Err(Error::AlreadyPresent);
            }
            result.tree.put(&key, Value::Bytes(value))?;
        }
        Ok(result)
    }
}
fn decode(bytes: &[u8], tag: ObjectTag, fields: &[(&str, T)]) -> Result<Vec<V>, Error> {
    let schema = fields
        .iter()
        .map(|(n, t)| Field {
            name: (*n).into(),
            kind: t.clone(),
        })
        .collect::<Vec<_>>();
    Ok(object::deserialize(bytes, tag, &1.into(), &schema)?
        .into_iter()
        .map(|(_, v)| v)
        .collect())
}
