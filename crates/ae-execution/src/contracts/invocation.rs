// Shared VM/settlement inputs retain the original typed transaction and size.
use super::*;
use ae_tx::GaAttachTransaction;
use std::sync::LazyLock;
static ZERO: LazyLock<Unsigned> = LazyLock::new(|| 0.into());
#[derive(Clone, Copy)]
pub(super) enum Invocation<'a> {
    Contract(&'a ContractTransaction),
    Attach(&'a GaAttachTransaction),
}
impl<'a> Invocation<'a> {
    pub fn signer(self) -> AccountPublicKey {
        match self {
            Self::Contract(t) => t.signer(),
            Self::Attach(t) => t.owner(),
        }
    }
    pub fn actor(self) -> Id {
        match self {
            Self::Contract(t) => t.actor(),
            Self::Attach(t) => Id::new(IdTag::Account, t.owner().0),
        }
    }
    pub fn gas(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.gas(),
            Self::Attach(t) => t.gas(),
        }
    }
    pub fn gas_price(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.gas_price(),
            Self::Attach(t) => t.gas_price(),
        }
    }
    pub fn fee(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.fee(),
            Self::Attach(t) => t.fee(),
        }
    }
    pub fn amount(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.amount(),
            Self::Attach(_) => &ZERO,
        }
    }
    pub fn call_origin(self) -> [u8; 32] {
        match self {
            Self::Contract(t) => t.call_origin(),
            Self::Attach(t) => t.owner().0,
        }
    }
    pub fn call_stack(self) -> &'a [Unsigned] {
        match self {
            Self::Contract(t) => t.call_stack(),
            Self::Attach(_) => &[],
        }
    }
    pub fn call_data(self) -> &'a [u8] {
        match self {
            Self::Contract(t) => match t.operation() {
                ContractOperation::Create { call_data, .. }
                | ContractOperation::Call { call_data, .. } => call_data,
            },
            Self::Attach(t) => t.call_data(),
        }
    }
    pub fn wire_size(self) -> usize {
        match self {
            Self::Contract(t) => t.wire_size(),
            Self::Attach(t) => t.wire_size(),
        }
    }
    pub fn is_create(self) -> bool {
        match self {
            Self::Contract(t) => matches!(t.operation(), ContractOperation::Create { .. }),
            Self::Attach(_) => true,
        }
    }
    pub fn auth_fun(self) -> Option<&'a [u8]> {
        match self {
            Self::Contract(_) => None,
            Self::Attach(t) => Some(t.auth_fun()),
        }
    }
    pub fn used_gas(self, n: &Unsigned) -> Unsigned {
        match self {
            Self::Contract(t) => t.used_gas(n),
            Self::Attach(t) => t.used_gas(n),
        }
    }
}
