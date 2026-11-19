// Shared VM/settlement inputs retain the original typed transaction and size.
use super::*;
use ae_tx::GaAttachTransaction;
use std::sync::LazyLock;
static ZERO: LazyLock<Unsigned> = LazyLock::new(|| 0.into());
#[derive(Clone, Copy)]
pub(super) enum Invocation<'a> {
    Contract(&'a ContractTransaction),
    Attach(&'a GaAttachTransaction),
    Auth(&'a ae_tx::GaMeta, usize),
}
impl<'a> Invocation<'a> {
    pub fn signer(self) -> AccountPublicKey {
        match self {
            Self::Contract(t) => t.signer(),
            Self::Attach(t) => t.owner(),
            Self::Auth(t, _) => t.account,
        }
    }
    pub fn actor(self) -> Id {
        match self {
            Self::Contract(t) => t.actor(),
            Self::Attach(t) => Id::new(IdTag::Account, t.owner().0),
            Self::Auth(t, _) => Id::new(IdTag::Account, t.account.0),
        }
    }
    pub fn gas(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.gas(),
            Self::Attach(t) => t.gas(),
            Self::Auth(t, _) => &t.gas,
        }
    }
    pub fn gas_price(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.gas_price(),
            Self::Attach(t) => t.gas_price(),
            Self::Auth(t, _) => &t.gas_price,
        }
    }
    pub fn fee(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.fee(),
            Self::Attach(t) => t.fee(),
            Self::Auth(t, _) => &t.fee,
        }
    }
    pub fn amount(self) -> &'a Unsigned {
        match self {
            Self::Contract(t) => t.amount(),
            Self::Attach(_) => &ZERO,
            Self::Auth(_, _) => &ZERO,
        }
    }
    pub fn call_origin(self) -> [u8; 32] {
        match self {
            Self::Contract(t) => t.call_origin(),
            Self::Attach(t) => t.owner().0,
            Self::Auth(t, _) => t.account.0,
        }
    }
    pub fn call_stack(self) -> &'a [Unsigned] {
        match self {
            Self::Contract(t) => t.call_stack(),
            Self::Attach(_) => &[],
            Self::Auth(_, _) => &[],
        }
    }
    pub fn call_data(self) -> &'a [u8] {
        match self {
            Self::Contract(t) => match t.operation() {
                ContractOperation::Create { call_data, .. }
                | ContractOperation::Call { call_data, .. } => call_data,
            },
            Self::Attach(t) => t.call_data(),
            Self::Auth(t, _) => &t.auth_data,
        }
    }
    pub fn wire_size(self) -> usize {
        match self {
            Self::Contract(t) => t.wire_size(),
            Self::Attach(t) => t.wire_size(),
            Self::Auth(_, n) => n,
        }
    }
    pub fn is_create(self) -> bool {
        match self {
            Self::Contract(t) => matches!(t.operation(), ContractOperation::Create { .. }),
            Self::Attach(_) => true,
            Self::Auth(_, _) => false,
        }
    }
    pub fn auth_fun(self) -> Option<&'a [u8]> {
        match self {
            Self::Contract(_) => None,
            Self::Attach(t) => Some(t.auth_fun()),
            Self::Auth(_, _) => None,
        }
    }
    pub fn used_gas(self, n: &Unsigned) -> Unsigned {
        match self {
            Self::Contract(t) => t.used_gas(n),
            Self::Attach(t) => t.used_gas(n),
            Self::Auth(_, size) => Unsigned::from(size as u64)
                .mul_u64(20)
                .add(&75_000.into())
                .add(n),
        }
    }
}
