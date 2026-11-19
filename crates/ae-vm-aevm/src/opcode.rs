// aevm_eeevm gates, aevm_opcodes and aec_governance gas, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Sophia1,
    Solidity1,
    Sophia2,
    Sophia3,
    Sophia4,
}
impl Version {
    pub fn from_id(id: u32) -> Option<Self> {
        Some(match id {
            1 => Self::Sophia1,
            2 => Self::Solidity1,
            3 => Self::Sophia2,
            4 => Self::Sophia3,
            6 => Self::Sophia4,
            _ => return None,
        })
    }
    pub fn safe_math(self) -> bool {
        matches!(self, Self::Sophia2 | Self::Sophia3 | Self::Sophia4)
    }
    pub fn permits(self, op: u8) -> bool {
        match op {
            0x1b..=0x1d => self != Self::Sophia1,
            0x2f => matches!(self, Self::Sophia3 | Self::Sophia4),
            0x35..=0x37 | 0x3d..=0x3e | 0x54..=0x55 | 0xf0 | 0xf2 | 0xf4 | 0xff => {
                self == Self::Solidity1
            }
            0x00..=0x0b
            | 0x10..=0x1a
            | 0x20
            | 0x30..=0x34
            | 0x38..=0x3c
            | 0x40..=0x45
            | 0x50..=0x53
            | 0x56..=0x5b
            | 0x60..=0xa4
            | 0xf1
            | 0xf3
            | 0xfd => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GasTable {
    pub zero: u64,
    pub base: u64,
    pub very_low: u64,
    pub low: u64,
    pub mid: u64,
    pub high: u64,
    pub ext_code_size: u64,
    pub ext_code_copy: u64,
    pub balance: u64,
    pub sload: u64,
    pub jump_dest: u64,
    pub sset: u64,
    pub sreset: u64,
    pub self_destruct: u64,
    pub create: u64,
    pub call: u64,
    pub call_value: u64,
    pub call_stipend: u64,
    pub exp: u64,
    pub exp_byte: u64,
    pub memory: u64,
    pub log: u64,
    pub log_data: u64,
    pub log_topic: u64,
    pub sha3: u64,
    pub sha3_word: u64,
    pub copy: u64,
    pub block_hash: u64,
}
impl Default for GasTable {
    fn default() -> Self {
        Self {
            zero: 0,
            base: 2,
            very_low: 3,
            low: 5,
            mid: 8,
            high: 10,
            ext_code_size: 700,
            ext_code_copy: 700,
            balance: 400,
            sload: 200,
            jump_dest: 1,
            sset: 20000,
            sreset: 5000,
            self_destruct: 5000,
            create: 32000,
            call: 700,
            call_value: 9000,
            call_stipend: 2300,
            exp: 10,
            exp_byte: 50,
            memory: 3,
            log: 375,
            log_data: 8,
            log_topic: 375,
            sha3: 30,
            sha3_word: 6,
            copy: 3,
            block_hash: 20,
        }
    }
}
// Retain the original metadata, including its unusual PUSH/DUP/SWAP entries.
// Runtime operand access is implemented by each instruction, not this metadata.
pub fn metadata(op: u8, g: &GasTable) -> Option<(u8, u8, num_bigint::BigUint)> {
    let (popped, pushed, cost) = match op {
        0 => (0, 0, g.zero),
        1 | 3 | 0x10..=0x14 | 0x16..=0x18 | 0x1a..=0x1d => (2, 1, g.very_low),
        2 | 4..=7 | 11 => (2, 1, g.low),
        8 | 9 => (3, 1, g.mid),
        10 => (2, 1, g.exp),
        0x15 | 0x19 => (1, 1, g.very_low),
        0x20 => (2, 1, g.sha3),
        0x2f | 0x30 | 0x32..=0x34 | 0x36 | 0x38 | 0x3a | 0x41..=0x45 | 0x58..=0x5a => {
            (0, 1, g.base)
        }
        0x31 => (1, 1, g.balance),
        0x35 | 0x51 => (1, 1, g.very_low),
        0x37 | 0x39 => (3, 0, g.very_low),
        0x3b => (1, 1, g.ext_code_size),
        0x3c => (4, 0, g.ext_code_copy),
        0x3d => (0, 1, 2),
        0x3e => (3, 0, 3),
        0x40 => (1, 1, g.block_hash),
        0x50 => (1, 0, g.base),
        0x52 | 0x53 => (2, 0, g.very_low),
        0x54 => (1, 1, g.sload),
        0x55 => (2, 0, 0),
        0x56 => (1, 0, g.mid),
        0x57 => (2, 0, g.high),
        0x5b => (0, 0, g.jump_dest),
        0x60..=0x7f => (op - 0x5f, 0, g.very_low),
        0x80..=0x8f => (0, op - 0x7f, g.very_low),
        0x90..=0x9f => (0, op - 0x8e, g.very_low),
        0xa0..=0xa4 => {
            return Some((
                op - 0x9e,
                0,
                num_bigint::BigUint::from(g.log)
                    + num_bigint::BigUint::from(g.log_topic) * (op - 0xa0),
            ));
        }
        0xf0 => (3, 1, g.create),
        0xf1 | 0xf2 => (7, 1, 0),
        0xf3 => (2, 0, g.zero),
        0xf4 => (6, 1, 0),
        0xfa => (6, 1, 40),
        0xfd => (2, 0, 0),
        0xff => (1, 0, g.self_destruct),
        _ => return None,
    };
    Some((popped, pushed, cost.into()))
}
