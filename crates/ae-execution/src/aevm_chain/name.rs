// aec_vm_chain aens_resolve and aens:resolve, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{state::native, *};
use ae_codec::{
    api::{self, ApiType},
    idna,
};
use ae_state::names::{PointerValue, hashing};
use ae_types::IdTag;
use ae_vm_aevm::{
    execution::{Budget, Stop},
    heap::{Type, Value},
    primops::HostResult,
    runtime::{Fault, Native},
};
use num_bigint::{BigInt, Sign};

impl<A: NodeReader, C: NodeReader> Host<A, C> {
    pub fn name_resolution(
        &self,
        name: &[u8],
        key: &[u8],
        ty: &Type,
        chain: &State<A, C>,
        budget: &mut Budget,
    ) -> HostResult<Value, Error> {
        budget.bytes(
            name.len()
                .checked_add(key.len())
                .ok_or(Stop::ResourceLimit)?,
        )?;
        budget.work(name.len().max(1))?;
        // name_parts removes trailing empty labels, including repeated dots.
        let is_name = name
            .iter()
            .rposition(|b| *b != b'.')
            .is_some_and(|i| name[..i].contains(&b'.'));
        let id = if is_name {
            let ascii = match idna::to_ascii(name) {
                Ok(a) => a,
                Err(idna::Error::ReferenceException { class, reason }) => {
                    return Err(Fault::Native(Native::ChainException { class, reason }));
                }
                Err(_) => return Ok(Ok(Value::Option(None))),
            };
            budget.bytes(ascii.len())?;
            budget.work(ascii.len())?;
            let trees = chain.onchain.as_deref().unwrap_or(&chain.trees);
            let Some(n) = trees
                .names
                .name(hashing::name_hash(&ascii).0)
                .map_err(|e| Fault::Host(Error::Names(e)))?
            else {
                return Ok(Ok(Value::Option(None)));
            };
            budget.work(n.pointers.len())?;
            budget.bytes(
                n.serialize()
                    .map_err(|e| Fault::Host(Error::Names(e)))?
                    .len(),
            )?;
            match n.status.as_slice() {
                b"revoked" => return Ok(Ok(Value::Option(None))),
                b"claimed" => {}
                _ => return Err(native("case_clause")),
            }
            if n.owner.tag != IdTag::Account {
                return Err(native("function_clause"));
            }
            let Some(p) = n.pointers.iter().find(|p| p.key == key) else {
                return Ok(Ok(Value::Option(None)));
            };
            match p.value {
                PointerValue::Id(id) => id,
                PointerValue::Data(_) => return Err(native("function_clause")),
            }
        } else {
            let allowed = [
                ApiType::AccountPubkey,
                ApiType::OraclePubkey,
                ApiType::ContractPubkey,
                ApiType::Channel,
            ];
            let Some(id) = std::str::from_utf8(name)
                .ok()
                .and_then(|s| api::decode_id(s, &allowed).ok())
            else {
                return Ok(Ok(Value::Option(None)));
            };
            id
        };
        budget.bytes(32)?;
        let value = match ty {
            Type::Word => Value::Integer(BigInt::from_bytes_be(Sign::Plus, &id.value)),
            Type::String => Value::String(id.value.to_vec()),
            _ => return Ok(Err("out_of_gas".into())),
        };
        Ok(Ok(Value::Option(Some(Box::new(value)))))
    }
}
