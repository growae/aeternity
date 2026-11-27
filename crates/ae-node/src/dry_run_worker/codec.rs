use num_bigint::{BigInt, Sign};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Capacity,
    Invalid,
}
pub(super) trait Wire: Sized {
    fn put(&self, e: &mut Encoder) -> Result<(), Error>;
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error>;
}
pub(super) struct Encoder {
    bytes: Vec<u8>,
    maximum: usize,
    depth: usize,
    controls: Vec<ae_types::work::Control>,
}
pub(super) struct Decoder<'a> {
    bytes: &'a [u8],
    remaining: usize,
    depth: usize,
    controls: Option<&'a dyn Fn(u8) -> ae_types::work::Control>,
}
pub(super) fn capture<T: Wire>(
    v: &T,
    maximum: usize,
) -> Result<(Vec<u8>, Vec<ae_types::work::Control>), Error> {
    let mut e = Encoder {
        bytes: vec![],
        maximum,
        depth: 0,
        controls: vec![],
    };
    v.put(&mut e)?;
    Ok((e.bytes, e.controls))
}
pub(super) fn encode<T: Wire>(v: &T, maximum: usize) -> Result<Vec<u8>, Error> {
    let (bytes, controls) = capture(v, maximum)?;
    if !controls.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(bytes)
}
pub(super) fn decode<T: Wire>(bytes: &[u8]) -> Result<T, Error> {
    decode_with(bytes, None)
}
pub(super) fn decode_with<T: Wire>(
    bytes: &[u8],
    controls: Option<&dyn Fn(u8) -> ae_types::work::Control>,
) -> Result<T, Error> {
    if bytes.len() > ae_sync::dry_run::process::wire::MAX_FRAME {
        return Err(Error::Capacity);
    }
    let mut d = Decoder {
        bytes,
        remaining: 2_000_000,
        depth: 0,
        controls,
    };
    let v = T::get(&mut d)?;
    if !d.bytes.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(v)
}
impl Wire for ae_types::work::Control {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        if e.controls.len() >= 16 {
            return Err(Error::Capacity);
        }
        let id = e.controls.len() as u8;
        e.controls.push(self.clone());
        id.put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let id = u8::get(d)?;
        if id >= 16 {
            return Err(Error::Invalid);
        }
        Ok(d.controls.ok_or(Error::Invalid)?(id))
    }
}

impl Encoder {
    pub(super) fn available(&self) -> usize {
        self.maximum - self.bytes.len()
    }
    pub(super) fn raw(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(Error::Capacity)?;
        if next > self.maximum {
            return Err(Error::Capacity);
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| Error::Capacity)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T, Error>) -> Result<T, Error> {
        if self.depth >= 128 {
            return Err(Error::Capacity);
        }
        self.depth += 1;
        let v = f(self);
        self.depth -= 1;
        v
    }
    pub(super) fn blob(&mut self, bytes: &[u8]) -> Result<(), Error> {
        bytes.len().put(self)?;
        self.raw(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_lengths_values_and_duplicate_keys_are_rejected() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"calls":[null,true,{"amount":100000000000000000000000000000001}],"text":"λ"}"#,
        )
        .unwrap();
        let encoded = encode(&value, 4096).unwrap();
        assert_eq!(decode::<serde_json::Value>(&encoded).unwrap(), value);
        for length in 0..encoded.len() {
            assert!(
                decode::<serde_json::Value>(&encoded[..length]).is_err(),
                "prefix {length}"
            );
        }
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(decode::<serde_json::Value>(&trailing), Err(Error::Invalid));
        assert_eq!(encode(&value, encoded.len() - 1), Err(Error::Capacity));
        assert_eq!(decode::<bool>(&[2]), Err(Error::Invalid));
        assert_eq!(
            decode::<String>(&[1_u64.to_be_bytes().as_slice(), &[255]].concat()),
            Err(Error::Invalid)
        );
        assert_eq!(
            decode::<String>(&u64::MAX.to_be_bytes()),
            Err(Error::Invalid)
        );
        for (a, b) in [(1, 1), (2, 1)] {
            let map = [2_u64.to_be_bytes().as_slice(), &[a, 0, b, 1]].concat();
            assert_eq!(decode::<BTreeMap<u8, u8>>(&map), Err(Error::Invalid));
            let set = [2_u64.to_be_bytes().as_slice(), &[a, b]].concat();
            assert_eq!(decode::<BTreeSet<u8>>(&set), Err(Error::Invalid));
        }
        let mut object = vec![5];
        object.extend(2_u64.to_be_bytes());
        for _ in 0..2 {
            object.extend(1_u64.to_be_bytes());
            object.extend(b"k");
            object.push(0);
        }
        assert_eq!(decode::<serde_json::Value>(&object), Err(Error::Invalid));
        // Noncanonical positive zero and a redundant leading zero magnitude.
        for bytes in [vec![0], vec![0, 1]] {
            let raw = [vec![1], (bytes.len() as u64).to_be_bytes().to_vec(), bytes].concat();
            assert_eq!(decode::<BigInt>(&raw), Err(Error::Invalid));
        }
    }

    #[test]
    fn depth_item_and_control_budgets_fail_without_accepting_partial_values() {
        let mut value = serde_json::Value::Null;
        let mut raw = Vec::new();
        for _ in 0..129 {
            value = serde_json::json!({"x":value});
            raw.push(5);
            raw.extend(1_u64.to_be_bytes());
            raw.extend(1_u64.to_be_bytes());
            raw.push(b'x');
        }
        raw.push(0);
        assert_eq!(encode(&value, 65536), Err(Error::Capacity));
        assert_eq!(decode::<serde_json::Value>(&raw), Err(Error::Capacity));
        assert_eq!(
            decode::<BTreeMap<u8, u8>>(&2_000_001_u64.to_be_bytes()),
            Err(Error::Capacity)
        );
        let controls: Vec<_> = (0..17)
            .map(|_| ae_types::work::Control::new(|| false))
            .collect();
        assert!(matches!(capture(&controls, 128), Err(Error::Capacity)));
        assert!(matches!(
            decode::<ae_types::work::Control>(&[0]),
            Err(Error::Invalid)
        ));
        let resolver = |_| ae_types::work::Control::new(|| false);
        assert!(matches!(
            decode_with::<ae_types::work::Control>(&[16], Some(&resolver)),
            Err(Error::Invalid)
        ));
        let (encoded, retained) = capture(&controls[..16].to_vec(), 128).unwrap();
        assert_eq!(retained.len(), 16);
        let resolver = |id| retained[id as usize].clone();
        assert_eq!(
            decode_with::<Vec<ae_types::work::Control>>(&encoded, Some(&resolver))
                .unwrap()
                .len(),
            16
        );
    }

    #[test]
    fn selected_community_fork_decisions_survive_transport() {
        use ae_protocol::{
            Activation, NetworkProfile, Protocol,
            signaling::{Fork, Prior},
        };
        let profile = NetworkProfile::custom(
            b"ae_worker_test".to_vec(),
            vec![
                Activation {
                    height: 0,
                    protocol: Protocol::Roma,
                },
                Activation {
                    height: 1,
                    protocol: Protocol::Ceres,
                },
            ],
        )
        .unwrap()
        .with_community_fork(Some(Fork {
            start: 10,
            end: 20,
            threshold: 5,
            info: 7,
            version: 7,
        }))
        .unwrap();
        for (height, prior, expected) in [
            (9, Prior::Missing, 6),
            (20, Prior::SignalingResult(false), 6),
            (20, Prior::SignalingResult(true), 7),
            (21, Prior::Version(7), 7),
            (21, Prior::Version(6), 6),
        ] {
            let selected = profile.resolve(height, prior).unwrap();
            let restored: NetworkProfile = decode(&encode(&selected, 4096).unwrap()).unwrap();
            assert_eq!(restored.resolution(), Some((height, expected)));
            assert_eq!(restored.version_at(height).unwrap(), expected);
            assert_eq!(restored.network_id(), profile.network_id());
            assert_eq!(restored.community_fork(), profile.community_fork());
        }
        let restored: NetworkProfile = decode(&encode(&profile, 4096).unwrap()).unwrap();
        assert_eq!(restored.resolution(), None);
        assert!(restored.version_at(21).is_err());
        for public in [NetworkProfile::mainnet(), NetworkProfile::uat()] {
            let restored: NetworkProfile = decode(&encode(&public, 4096).unwrap()).unwrap();
            assert_eq!(restored.activations(), public.activations());
            assert_eq!(restored.fixed_block_gas_limit(), Some(6_000_000));
            let mut tampered = encode(&public, 4096).unwrap();
            // Public schedules are validated rather than accepted from a job.
            let offset = 8 + public.network_id().len() + 8;
            tampered[offset] ^= 1;
            assert!(decode::<NetworkProfile>(&tampered).is_err());
        }
    }
}
impl<'a> Decoder<'a> {
    pub(super) fn raw(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if n > self.bytes.len() {
            return Err(Error::Invalid);
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }
    fn nested<T>(
        &mut self,
        n: usize,
        f: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.depth >= 128 {
            return Err(Error::Capacity);
        }
        self.remaining = self.remaining.checked_sub(n).ok_or(Error::Capacity)?;
        self.depth += 1;
        let v = f(self);
        self.depth -= 1;
        v
    }
    pub(super) fn blob(&mut self) -> Result<&'a [u8], Error> {
        let n = usize::get(self)?;
        self.raw(n)
    }
}
macro_rules! number {($($t:ty),*)=>{$(impl Wire for $t {
 fn put(&self,e:&mut Encoder)->Result<(),Error>{e.raw(&self.to_be_bytes())}
 fn get(d:&mut Decoder<'_>)->Result<Self,Error>{Ok(Self::from_be_bytes(d.raw(std::mem::size_of::<Self>())?.try_into().map_err(|_|Error::Invalid)?))}
})*}}
number!(u8, u32, u64);
impl Wire for usize {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        u64::try_from(*self).map_err(|_| Error::Capacity)?.put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        u64::get(d)?.try_into().map_err(|_| Error::Capacity)
    }
}
impl Wire for bool {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        u8::from(*self).put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(d)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Invalid),
        }
    }
}
impl<const N: usize> Wire for [u8; N] {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.raw(self)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        d.raw(N)?.try_into().map_err(|_| Error::Invalid)
    }
}
impl<T: Wire> Wire for Option<T> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.is_some().put(e)?;
        if let Some(v) = self {
            v.put(e)?
        }
        Ok(())
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        if bool::get(d)? {
            Ok(Some(T::get(d)?))
        } else {
            Ok(None)
        }
    }
}
impl<T: Wire> Wire for Vec<T> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.len().put(e)?;
        e.nested(|e| {
            for v in self {
                v.put(e)?
            }
            Ok(())
        })
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let n = usize::get(d)?;
        if n > d.bytes.len() {
            return Err(Error::Invalid);
        }
        d.nested(n, |d| {
            let mut out = Vec::new();
            for _ in 0..n {
                out.push(T::get(d)?)
            }
            Ok(out)
        })
    }
}
impl<T: Wire> Wire for Arc<T> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.as_ref().put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Arc::new(T::get(d)?))
    }
}
impl Wire for Arc<[u8]> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.blob(self)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Arc::from(d.blob()?))
    }
}
impl<K: Wire + Ord, V: Wire> Wire for BTreeMap<K, V> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.len().put(e)?;
        e.nested(|e| {
            for (k, v) in self {
                k.put(e)?;
                v.put(e)?
            }
            Ok(())
        })
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let n = usize::get(d)?;
        d.nested(n, |d| {
            let mut out = BTreeMap::new();
            for _ in 0..n {
                let k = K::get(d)?;
                if out.last_key_value().is_some_and(|(last, _)| last >= &k) {
                    return Err(Error::Invalid);
                }
                out.insert(k, V::get(d)?);
            }
            Ok(out)
        })
    }
}
impl<T: Wire + Ord> Wire for BTreeSet<T> {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.len().put(e)?;
        e.nested(|e| {
            for v in self {
                v.put(e)?
            }
            Ok(())
        })
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let n = usize::get(d)?;
        d.nested(n, |d| {
            let mut out = BTreeSet::new();
            for _ in 0..n {
                let v = T::get(d)?;
                if out.last().is_some_and(|last| last >= &v) {
                    return Err(Error::Invalid);
                }
                out.insert(v);
            }
            Ok(out)
        })
    }
}
impl<A: Wire, B: Wire> Wire for (A, B) {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.0.put(e)?;
        self.1.put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok((A::get(d)?, B::get(d)?))
    }
}
impl Wire for String {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.blob(self.as_bytes())
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        String::from_utf8(d.blob()?.to_vec()).map_err(|_| Error::Invalid)
    }
}
impl Wire for BigInt {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        let (sign, bytes) = self.to_bytes_be();
        (match sign {
            Sign::NoSign => 0_u8,
            Sign::Plus => 1,
            Sign::Minus => 2,
        })
        .put(e)?;
        e.blob(&bytes)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let sign = match u8::get(d)? {
            0 => Sign::NoSign,
            1 => Sign::Plus,
            2 => Sign::Minus,
            _ => return Err(Error::Invalid),
        };
        let bytes = d.blob()?;
        let out = Self::from_bytes_be(sign, bytes);
        let (s, b) = out.to_bytes_be();
        if s != sign || b != bytes {
            return Err(Error::Invalid);
        }
        Ok(out)
    }
}
impl Wire for ae_types::Unsigned {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.blob(self.as_bytes())
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::from_field_bytes(d.blob()?).map_err(|_| Error::Invalid)
    }
}
macro_rules! tuple {($($t:path),*)=>{$(impl Wire for $t{
 fn put(&self,e:&mut Encoder)->Result<(),Error>{self.0.put(e)}
 fn get(d:&mut Decoder<'_>)->Result<Self,Error>{Ok(Self(Wire::get(d)?))}
})*}}
tuple!(ae_types::Hash256, ae_types::AccountPublicKey);
macro_rules! structure {($ty:path {$($field:ident),*$(,)?})=>{impl Wire for $ty{
 fn put(&self,e:&mut Encoder)->Result<(),Error>{$(self.$field.put(e)?;)*Ok(())}
 fn get(d:&mut Decoder<'_>)->Result<Self,Error>{Ok(Self{$($field:Wire::get(d)?,)*})}
}}}
pub(super) use structure;
macro_rules! enumeration {($ty:path {$($variant:ident=$tag:literal),*$(,)?})=>{impl Wire for $ty{
 fn put(&self,e:&mut Encoder)->Result<(),Error>{match self{$(Self::$variant=>($tag as u8).put(e),)*}}
 fn get(d:&mut Decoder<'_>)->Result<Self,Error>{match u8::get(d)?{$($tag=>Ok(Self::$variant),)*_=>Err(Error::Invalid)}}
}}}
pub(super) use enumeration;
impl Wire for serde_json::Value {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.nested(|e| match self {
            Self::Null => 0_u8.put(e),
            Self::Bool(v) => {
                1_u8.put(e)?;
                v.put(e)
            }
            Self::Number(v) => {
                2_u8.put(e)?;
                v.to_string().put(e)
            }
            Self::String(v) => {
                3_u8.put(e)?;
                v.put(e)
            }
            Self::Array(v) => {
                4_u8.put(e)?;
                v.put(e)
            }
            Self::Object(v) => {
                5_u8.put(e)?;
                v.len().put(e)?;
                for (k, v) in v {
                    k.put(e)?;
                    v.put(e)?
                }
                Ok(())
            }
        })
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        d.nested(1, |d| {
            Ok(match u8::get(d)? {
                0 => Self::Null,
                1 => Self::Bool(Wire::get(d)?),
                2 => Self::Number(String::get(d)?.parse().map_err(|_| Error::Invalid)?),
                3 => Self::String(Wire::get(d)?),
                4 => Self::Array(Wire::get(d)?),
                5 => {
                    let n = usize::get(d)?;
                    d.nested(n, |d| {
                        let mut out = serde_json::Map::new();
                        for _ in 0..n {
                            let key = String::get(d)?;
                            if out.contains_key(&key) {
                                return Err(Error::Invalid);
                            }
                            out.insert(key, <Self as Wire>::get(d)?);
                        }
                        Ok(Self::Object(out))
                    })?
                }
                _ => return Err(Error::Invalid),
            })
        })
    }
}
