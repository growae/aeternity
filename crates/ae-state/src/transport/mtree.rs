use super::*;
use ae_codec::object::FieldType as T;
use ae_codec::object::Value as V;

pub(super) fn serialize<S: NodeReader>(
    tree: &Trie<S>,
    tag: ObjectTag,
    name: &str,
    b: &Budget,
) -> Result<Vec<u8>, Error> {
    let reader = ReadBudget {
        store: tree.store(),
        budget: b,
    };
    let view = Trie::from_root(tree.root_hash(), reader)?;
    let mut values = vec![];
    for row in view.iter() {
        let (key, value) = row?;
        let key = byte_key(&key)?;
        b.entry(&key)?;
        let ae_trie::Value::Bytes(value) = value else {
            return Err(Error::InvalidValue);
        };
        b.charge(value.len())?;
        let row = encode(
            ObjectTag::MtreeValue,
            vec![
                ("key", T::Binary, V::Binary(key)),
                ("val", T::Binary, V::Binary(value)),
            ],
        )?;
        b.charge(row.len())?;
        values.push(V::Binary(row));
    }
    values.reverse();
    let mtree = encode(
        ObjectTag::Mtree,
        vec![("values", T::List(Box::new(T::Binary)), V::List(values))],
    )?;
    b.charge(mtree.len())?;
    let out = encode(tag, vec![(name, T::Binary, V::Binary(mtree))])?;
    b.charge(out.len())?;
    Ok(out)
}
pub(super) fn deserialize<S: NodeWriter>(
    bytes: &[u8],
    tag: ObjectTag,
    store: S,
    b: &Budget,
) -> Result<Trie<S>, Error> {
    let mut wrapper = b.decode(bytes, tag, &[T::Binary])?.into_iter();
    let Some(V::Binary(mtree)) = wrapper.next() else {
        return Err(Error::InvalidValue);
    };
    let mut content = b
        .decode(&mtree, ObjectTag::Mtree, &[T::List(Box::new(T::Binary))])?
        .into_iter();
    let Some(V::List(values)) = content.next() else {
        return Err(Error::InvalidValue);
    };
    let mut tree = Trie::new(WriteBudget { store, budget: b });
    for v in values {
        let V::Binary(bytes) = v else {
            return Err(Error::InvalidValue);
        };
        let mut fields = b
            .decode(&bytes, ObjectTag::MtreeValue, &[T::Binary, T::Binary])?
            .into_iter();
        let (Some(V::Binary(key)), Some(V::Binary(value))) = (fields.next(), fields.next()) else {
            return Err(Error::InvalidValue);
        };
        b.entry(&key)?;
        let key = Nibbles::from_bytes(&key);
        if tree.get(&key)?.is_some() {
            return Err(Error::AlreadyPresent);
        }
        tree.put(&key, ae_trie::Value::Bytes(value))?;
    }
    finish_import(tree)
}
