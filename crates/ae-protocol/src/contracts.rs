// aect_contracts.erl and aecontract.hrl, fa2a6d7, production (non-TEST) rules.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::Protocol;
use ae_types::Unsigned;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationError {
    UnsupportedAbi,
    UnsupportedVersion,
}
pub fn serialization_available(
    abi: &Unsigned,
    version: &Unsigned,
    protocol: Protocol,
) -> Result<bool, SerializationError> {
    match abi.to_u64() {
        Ok(2) => Ok(true),
        Ok(1 | 3) => match version.to_u64() {
            Ok(1) => Ok(protocol <= Protocol::Fortuna),
            Ok(2) => Ok((Protocol::Minerva..=Protocol::Fortuna).contains(&protocol)),
            Ok(3) => Ok(protocol >= Protocol::Lima),
            _ => Err(SerializationError::UnsupportedVersion),
        },
        _ => Err(SerializationError::UnsupportedAbi),
    }
}
pub const CREATE_BASE_GAS: u64 = 75_000;
pub fn call_base_gas(abi: &Unsigned) -> u64 {
    if *abi == 3.into() { 180_000 } else { 450_000 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub vm: u16,
    pub abi: u16,
}
impl Version {
    pub fn unpack(packed: &Unsigned) -> Self {
        // Erlang's 32-bit segment retains low bits even for oversized inputs.
        let bits = packed
            .as_bytes()
            .iter()
            .fold(0u32, |n, b| n.wrapping_shl(8) | u32::from(*b));
        let abi = bits as u16;
        let vm = (bits >> 16) as u16;
        Self {
            vm: if vm == 0 { abi } else { vm },
            abi,
        }
    }
    pub fn pack(self) -> Unsigned {
        if self.vm == self.abi && self.abi <= 2 {
            u64::from(self.abi).into()
        } else {
            u64::from((u32::from(self.vm) << 16) | u32::from(self.abi)).into()
        }
    }
    pub fn is_aevm(self) -> bool {
        matches!(self.vm, 1 | 3 | 4 | 6)
    }
    pub fn is_fate(self) -> bool {
        matches!(self.vm, 5 | 7 | 8)
    }
    pub fn legal(self) -> bool {
        (self.is_aevm() && self.abi == 1) || (self.is_fate() && self.abi == 3)
    }
    pub fn can_create(self, protocol: Protocol) -> bool {
        if !self.legal() {
            return false;
        }
        match self.vm {
            1 => protocol == Protocol::Roma,
            3 => matches!(protocol, Protocol::Minerva | Protocol::Fortuna),
            4 => protocol == Protocol::Fortuna,
            5 | 6 => protocol == Protocol::Lima,
            7 => protocol == Protocol::Iris,
            8 => protocol >= Protocol::Ceres,
            _ => false,
        }
    }
    // The reference's call admission checks VM activation only, not its ABI.
    pub fn can_call(self, protocol: Protocol) -> bool {
        match self.vm {
            1 => true,
            3 => protocol >= Protocol::Minerva,
            4 => protocol >= Protocol::Fortuna,
            5 | 6 => protocol >= Protocol::Lima,
            7 => protocol >= Protocol::Iris,
            8 => protocol >= Protocol::Ceres,
            _ => false,
        }
    }
    pub fn can_call_from(self, caller: Self) -> bool {
        self.abi == caller.abi && self.is_aevm() && caller.is_aevm() && caller.vm >= self.vm
    }
    pub fn object_version(self) -> u8 {
        if matches!(self.vm, 7 | 8) { 2 } else { 1 }
    }
    pub fn valid_store_key(self, key: &[u8]) -> bool {
        if key.is_empty() {
            return false;
        }
        if self.is_fate() {
            return true;
        }
        if self.is_aevm() {
            return matches!(key, [0] | [1] | [2]) || key.len() >= 32;
        }
        // Kept for decoding legacy/test Solidity objects, despite creation
        // being disabled in the production version matrix.
        self.vm == 2 && (key == [0] || key[0] != 0)
    }
}
