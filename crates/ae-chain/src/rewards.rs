// aec_chain_state, aec_dev_reward and aec_accounts_trees, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_protocol::{NetworkProfile, Protocol, coinbase};
use ae_state::{
    Trees,
    accounts::{Account, AccountError},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Distribution {
    pub first: Unsigned,
    pub second: Unsigned,
    pub locked: BigInt,
}
pub(crate) fn number(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
pub(crate) fn unsigned(n: BigInt) -> Unsigned {
    assert!(n.sign() != Sign::Minus);
    Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("nonnegative")
}
pub fn calculate(
    first_fraud: bool,
    second_fraud: bool,
    fees: &Unsigned,
    mining: &Unsigned,
    fraud: &Unsigned,
    first_is_genesis: bool,
) -> Distribution {
    let fees = number(fees);
    let mining = number(mining);
    let first_part = &fees * 4 / 10;
    let second_full = &fees - &first_part + &mining;
    let (first, second) = match (first_fraud, second_fraud) {
        (true, true) => (BigInt::ZERO, BigInt::ZERO),
        (true, false) => (BigInt::ZERO, second_full + number(fraud)),
        (false, true) => (
            if first_is_genesis {
                BigInt::ZERO
            } else {
                first_part
            },
            BigInt::ZERO,
        ),
        (false, false) => (
            if first_is_genesis {
                BigInt::ZERO
            } else {
                first_part
            },
            second_full,
        ),
    };
    Distribution {
        locked: fees + mining - &first - &second,
        first: unsigned(first),
        second: unsigned(second),
    }
}
#[derive(Debug, Clone)]
pub struct Beneficiary {
    pub key: AccountPublicKey,
    pub shares: u64,
    pub from: Protocol,
    pub through: Protocol,
}
#[derive(Debug, Clone)]
pub struct Policy {
    enabled: bool,
    beneficiaries: Vec<Beneficiary>,
}
#[derive(Debug)]
pub enum Error {
    Allocation,
    Delay,
    ResourceLimit,
    HistoryMissing(Hash256),
    HistoryInvalid,
    FeesMissing(Hash256),
    FraudMissing(Hash256),
    Header(HeaderError),
    Account(AccountError),
    InsufficientLockedBalance,
    Protocol(ae_protocol::ProfileError),
}
impl Policy {
    pub fn custom(enabled: bool, beneficiaries: Vec<Beneficiary>) -> Result<Self, Error> {
        for b in &beneficiaries {
            if b.shares == 0 || b.from > b.through {
                return Err(Error::Allocation);
            }
        }
        for p in 1..=8 {
            let p = Protocol::try_from(p).expect("known protocol");
            let mut sum = 0u64;
            for b in beneficiaries
                .iter()
                .filter(|b| b.from <= p && p <= b.through)
            {
                sum = sum.checked_add(b.shares).ok_or(Error::Allocation)?;
            }
            if sum > 1000 {
                return Err(Error::Allocation);
            }
        }
        Ok(Self {
            enabled,
            beneficiaries,
        })
    }
    pub fn for_network(network: &NetworkProfile, custom: Option<Self>) -> Self {
        let key = if network.network_id() == b"ae_uat" {
            [
                152, 57, 168, 5, 218, 153, 177, 254, 226, 207, 243, 133, 11, 50, 143, 68, 121, 242,
                94, 41, 187, 198, 158, 67, 133, 88, 6, 71, 55, 26, 85, 54,
            ]
        } else {
            if network.network_id() != b"ae_mainnet"
                && let Some(custom) = custom
            {
                return custom;
            }
            [
                172, 241, 128, 85, 116, 104, 119, 143, 197, 105, 4, 192, 224, 207, 200, 138, 230,
                84, 111, 38, 89, 33, 239, 21, 201, 183, 185, 209, 19, 60, 109, 136,
            ]
        };
        Self {
            enabled: true,
            beneficiaries: vec![Beneficiary {
                key: AccountPublicKey(key),
                shares: 109,
                from: Protocol::Fortuna,
                through: Protocol::Iris,
            }],
        }
    }
    pub fn split(
        &self,
        first: &Unsigned,
        second: &Unsigned,
        protocol: Protocol,
    ) -> ((Unsigned, Unsigned), Vec<(AccountPublicKey, Unsigned)>) {
        if !self.enabled || protocol < Protocol::Fortuna {
            return ((first.clone(), second.clone()), vec![]);
        }
        let mut bs: Vec<_> = self
            .beneficiaries
            .iter()
            .filter(|b| b.from <= protocol && protocol <= b.through)
            .collect();
        if bs.is_empty() {
            return ((first.clone(), second.clone()), vec![]);
        }
        bs.sort_by_key(|b| (b.key, b.shares));
        let allocated: u64 = bs.iter().map(|b| b.shares).sum();
        let a = number(first) * allocated / 1000u64;
        let b = number(second) * allocated / 1000u64;
        let total = &a + &b;
        let mut left = total.clone();
        let mut rewards: Vec<_> = bs
            .into_iter()
            .map(|b| {
                let amount = &total * b.shares / allocated;
                left -= &amount;
                (b.key, amount)
            })
            .collect();
        rewards.reverse();
        rewards[0].1 += left;
        (
            (unsigned(number(first) - a), unsigned(number(second) - b)),
            rewards.into_iter().map(|(k, n)| (k, unsigned(n))).collect(),
        )
    }
}
pub fn credit<A: NodeWriter>(
    accounts: &mut ae_state::accounts::Accounts<A>,
    key: AccountPublicKey,
    amount: &Unsigned,
) -> Result<(), Error> {
    let mut a = accounts
        .lookup(key)
        .map_err(Error::Account)?
        .unwrap_or_else(|| Account::new(key, 0.into()));
    a.earn(amount);
    accounts.enter(&a).map_err(Error::Account)
}
pub fn lock<A: NodeWriter>(
    accounts: &mut ae_state::accounts::Accounts<A>,
    amount: &BigInt,
) -> Result<(), Error> {
    if amount == &BigInt::ZERO {
        return Ok(());
    }
    let key = AccountPublicKey::ZERO;
    let mut a = accounts
        .lookup(key)
        .map_err(Error::Account)?
        .unwrap_or_else(|| Account::new(key, 0.into()));
    if amount.sign() == Sign::Minus {
        let debit = unsigned(-amount);
        if a.balance() < &debit {
            return Err(Error::InsufficientLockedBalance);
        }
        a.spend_without_nonce_bump(&debit).map_err(Error::Account)?;
    } else {
        a.earn(&unsigned(amount.clone()));
    }
    accounts.enter(&a).map_err(Error::Account)
}
#[derive(Debug, Clone)]
pub struct Record {
    pub header: KeyHeader,
    pub fees: Option<Unsigned>,
    pub reported_fraud: Option<bool>,
}
pub type History = BTreeMap<Hash256, Record>;
fn predecessor<'a>(
    h: &KeyHeader,
    history: &'a History,
    left: &mut usize,
) -> Result<(Hash256, &'a Record), Error> {
    if *left == 0 {
        return Err(Error::ResourceLimit);
    }
    *left -= 1;
    let hash = Hash256(h.common.previous_key_hash);
    let r = history.get(&hash).ok_or(Error::HistoryMissing(hash))?;
    if r.header.common.height.checked_add(1) != Some(h.common.height)
        || Header::Key(r.header.clone())
            .hash()
            .map_err(Error::Header)?
            != hash
    {
        return Err(Error::HistoryInvalid);
    }
    Ok((hash, r))
}
#[derive(Debug, Clone)]
pub struct Award {
    pub first: Hash256,
    pub second: Hash256,
    pub distribution: Distribution,
    pub beneficiaries: Vec<(AccountPublicKey, Unsigned)>,
}
pub struct Schedule<'a> {
    pub history: &'a History,
    pub delay: u64,
    pub policy: &'a Policy,
    pub max_history: usize,
    pub genesis_hash: Hash256,
}
pub fn grant<A: NodeWriter + Clone, C: NodeReader + Clone>(
    trees: &mut Trees<A, C>,
    current: &KeyHeader,
    previous_fraud: bool,
    schedule: &Schedule<'_>,
) -> Result<Option<Award>, Error> {
    let Schedule {
        history,
        delay,
        policy,
        max_history,
        genesis_hash,
    } = schedule;
    let delay = *delay;
    let max_history = *max_history;
    if delay < coinbase::FRAUD_REPORT_DELAY {
        return Err(Error::Delay);
    }
    if current.common.height <= delay {
        return Ok(None);
    }
    let newest_height = current.common.height - delay + coinbase::FRAUD_REPORT_DELAY;
    let mut left = max_history;
    let mut newest = current;
    let mut newest_fraud = previous_fraud;
    while newest.common.height > newest_height {
        let (hash, r) = predecessor(newest, history, &mut left)?;
        newest = &r.header;
        if newest.common.height == newest_height {
            newest_fraud = r.reported_fraud.ok_or(Error::FraudMissing(hash))?;
        }
    }
    let (third_hash, third) = predecessor(newest, history, &mut left)?;
    let (second_hash, second) = predecessor(&third.header, history, &mut left)?;
    let (first_hash, first) = predecessor(&second.header, history, &mut left)?;
    let first_fraud = third
        .reported_fraud
        .ok_or(Error::FraudMissing(third_hash))?;
    let fees = second
        .fees
        .as_ref()
        .ok_or(Error::FeesMissing(second_hash))?;
    let distribution = calculate(
        first_fraud,
        newest_fraud,
        fees,
        &coinbase::mining_reward(&second.header.common.height.into()),
        &coinbase::fraud_reward(&first.header.common.height.into()),
        first_hash == *genesis_hash,
    );
    let protocol = Protocol::try_from(first.header.common.version).map_err(Error::Protocol)?;
    let ((a, b), dev) = policy.split(&distribution.first, &distribution.second, protocol);
    let beneficiaries = [
        (AccountPublicKey(first.header.beneficiary), a),
        (AccountPublicKey(second.header.beneficiary), b),
    ]
    .into_iter()
    .chain(dev)
    .collect::<Vec<_>>();
    let mut next = trees.clone();
    for (key, amount) in &beneficiaries {
        if amount != &0.into() {
            credit(&mut next.accounts, *key, amount)?;
        }
    }
    lock(&mut next.accounts, &distribution.locked)?;
    *trees = next;
    Ok(Some(Award {
        first: first_hash,
        second: second_hash,
        distribution,
        beneficiaries,
    }))
}
