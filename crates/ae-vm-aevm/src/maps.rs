// aevm_eeevm_maps, fa2a6d7. Copyright (c) 2018, Aeternity Anstalt.
// ISC: repository LICENSE. Immutable map versions and persisted parent overlays.
use crate::{
    execution::{Budget, Limits},
    runtime_heap::{self, Error, TypeRef},
    word::Word,
};
use num_bigint::{BigInt, BigUint};
use num_traits::Zero;
use std::{collections::BTreeMap, sync::Arc};

pub type Bytes = Arc<[u8]>;
pub type Entries = BTreeMap<Bytes, Option<Bytes>>;
pub type Store = BTreeMap<Vec<u8>, Vec<u8>>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Data {
    Stored,
    Local(Entries),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Map {
    pub key_type: TypeRef,
    pub value_type: TypeRef,
    pub parent: Option<BigUint>,
    pub size: BigInt,
    pub data: Data,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Maps {
    pub entries: BTreeMap<BigUint, Map>,
    pub next_id: BigUint,
}

pub(crate) fn reserve_map(map: &Map, b: &mut Budget) -> Result<(), Error> {
    runtime_heap::check_type(&map.key_type, b)?;
    runtime_heap::check_type(&map.value_type, b)?;
    b.bytes(usize::try_from(map.size.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?)?;
    if let Some(id) = &map.parent {
        b.bytes(usize::try_from(id.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?)?;
    }
    if let Data::Local(data) = &map.data {
        reserve_entries(data, b)?;
    }
    Ok(())
}
pub(crate) fn reserve_entries(entries: &Entries, b: &mut Budget) -> Result<(), Error> {
    b.work(entries.len())?;
    for (key, value) in entries {
        b.bytes(key.len())?;
        b.bytes(value.as_ref().map_or(0, |v| v.len()))?;
        b.bytes(64)?;
    }
    Ok(())
}
pub(crate) fn reserve_maps(maps: &Maps, b: &mut Budget) -> Result<(), Error> {
    b.work(maps.entries.len())?;
    b.bytes(usize::try_from(maps.next_id.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?)?;
    for (id, map) in &maps.entries {
        b.bytes(usize::try_from(id.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?)?;
        reserve_map(map, b)?;
    }
    Ok(())
}
pub(crate) fn reserve_store(store: &Store, b: &mut Budget) -> Result<(), Error> {
    b.work(store.len())?;
    for (k, v) in store {
        b.bytes(k.len())?;
        b.bytes(v.len())?;
    }
    Ok(())
}
pub(crate) fn real_id(id: &BigUint, store: &Store) -> Result<Word, Error> {
    let key = Word::from_unsigned(id);
    let bytes = store.get(key.0.as_slice()).ok_or(Error::InvalidValue)?;
    if bytes.len() < 96 {
        return Err(Error::InvalidValue);
    }
    Ok(Word(bytes[..32].try_into().expect("word")))
}
pub(crate) fn stored_get<'a>(
    id: &BigUint,
    key: &[u8],
    store: &'a Store,
    b: &mut Budget,
) -> Result<Option<&'a [u8]>, Error> {
    b.work(1)?;
    b.bytes(key.len().checked_add(32).ok_or(Error::ResourceLimit)?)?;
    let prefix = real_id(id, store)?;
    let mut full = prefix.0.to_vec();
    full.extend_from_slice(key);
    Ok(store
        .get(&full)
        .filter(|v| !v.is_empty())
        .map(Vec::as_slice))
}
pub(crate) fn stored_data(id: &BigUint, store: &Store, b: &mut Budget) -> Result<Entries, Error> {
    let prefix = real_id(id, store)?;
    let mut out = Entries::new();
    for (k, v) in store.range(prefix.0.to_vec()..) {
        if !k.starts_with(&prefix.0) {
            break;
        }
        if k.len() == 32 {
            continue;
        }
        b.work(1)?;
        b.bytes(k.len() - 32)?;
        b.bytes(v.len())?;
        b.bytes(64)?;
        out.insert(Arc::from(&k[32..]), Some(Arc::from(v.as_slice())));
    }
    Ok(out)
}
pub(crate) fn flatten_inner(
    id: &BigUint,
    map: &Map,
    store: &Store,
    b: &mut Budget,
) -> Result<Map, Error> {
    let mut parent = match &map.parent {
        None => Entries::new(),
        Some(p) => stored_data(p, store, b)?,
    };
    let delta = match &map.data {
        Data::Stored => stored_data(id, store, b)?,
        Data::Local(d) => {
            reserve_entries(d, b)?;
            d.clone()
        }
    };
    let data = if map.parent.is_none() {
        delta
    } else {
        for (k, v) in delta {
            b.work(1)?;
            if v.is_none() {
                parent.remove(&k);
            } else {
                parent.insert(k, v);
            }
        }
        parent
    };
    Ok(Map {
        key_type: map.key_type.clone(),
        value_type: map.value_type.clone(),
        size: map.size.clone(),
        parent: None,
        data: Data::Local(data),
    })
}
impl Maps {
    pub fn get_map(&self, id: &BigUint) -> Result<&Map, Error> {
        self.entries
            .get(id)
            .ok_or_else(|| Error::MissingMap(id.clone()))
    }
    pub fn size(&self, id: &BigUint) -> Result<&BigInt, Error> {
        Ok(&self.get_map(id)?.size)
    }
    pub(crate) fn get_inner<'a>(
        &'a self,
        id: &BigUint,
        key: &[u8],
        store: &'a Store,
        b: &mut Budget,
    ) -> Result<Option<&'a [u8]>, Error> {
        b.work(1)?;
        let map = self.get_map(id)?;
        match &map.data {
            Data::Stored => stored_get(id, key, store, b),
            Data::Local(data) => match data.get(key) {
                Some(value) => Ok(value.as_deref()),
                None => match &map.parent {
                    Some(parent) => stored_get(parent, key, store, b),
                    None => Ok(None),
                },
            },
        }
    }
    pub fn get(
        &self,
        id: &BigUint,
        key: &[u8],
        store: &Store,
        limits: Limits,
    ) -> Result<Option<Bytes>, Error> {
        let mut b = Budget::new(limits);
        self.get_inner(id, key, store, &mut b)?
            .map(|v| {
                b.bytes(v.len())?;
                Ok(Arc::from(v))
            })
            .transpose()
    }
    pub fn flatten(&self, id: &BigUint, store: &Store, limits: Limits) -> Result<Map, Error> {
        let mut b = Budget::new(limits);
        flatten_inner(id, self.get_map(id)?, store, &mut b)
    }
    pub fn empty(
        &self,
        key_type: TypeRef,
        value_type: TypeRef,
        limits: Limits,
    ) -> Result<(BigUint, Self), Error> {
        self.empty_inner(key_type, value_type, &mut Budget::new(limits))
    }
    pub(crate) fn empty_inner(
        &self,
        key_type: TypeRef,
        value_type: TypeRef,
        b: &mut Budget,
    ) -> Result<(BigUint, Self), Error> {
        reserve_maps(self, b)?;
        runtime_heap::check_type(&key_type, b)?;
        runtime_heap::check_type(&value_type, b)?;
        let mut next = self.clone();
        let id = next.next_id.clone();
        next.next_id += 1u8;
        next.entries.insert(
            id.clone(),
            Map {
                key_type,
                value_type,
                parent: None,
                size: BigInt::zero(),
                data: Data::Local(Entries::new()),
            },
        );
        Ok((id, next))
    }
    pub fn update(
        &self,
        id: &BigUint,
        key: Bytes,
        value: Option<Bytes>,
        store: &Store,
        limits: Limits,
    ) -> Result<(BigUint, Self), Error> {
        self.update_inner(id, key, value, store, &mut Budget::new(limits))
    }
    pub(crate) fn update_inner(
        &self,
        id: &BigUint,
        key: Bytes,
        value: Option<Bytes>,
        store: &Store,
        b: &mut Budget,
    ) -> Result<(BigUint, Self), Error> {
        reserve_maps(self, b)?;
        b.bytes(key.len())?;
        b.bytes(value.as_ref().map_or(0, |v| v.len()))?;
        let old = self.get_inner(id, &key, store, b)?.is_some();
        let mut map = self.get_map(id)?.clone();
        map.size += i32::from(value.is_some()) - i32::from(old);
        let mut data = match map.data {
            Data::Stored => {
                map.parent = Some(id.clone());
                Entries::new()
            }
            Data::Local(d) => d,
        };
        if map.parent.is_none() && value.is_none() {
            data.remove(&key);
        } else {
            data.insert(key, value);
        }
        map.data = Data::Local(data);
        let mut next = self.clone();
        let new_id = next.next_id.clone();
        next.next_id += 1u8;
        next.entries.insert(new_id.clone(), map);
        Ok((new_id, next))
    }
    pub fn merged(&self, new: &Self, limits: Limits) -> Result<Self, Error> {
        let mut b = Budget::new(limits);
        reserve_maps(self, &mut b)?;
        reserve_maps(new, &mut b)?;
        let mut out = self.clone();
        out.entries.extend(new.entries.clone());
        out.next_id = new.next_id.clone();
        Ok(out)
    }
}
