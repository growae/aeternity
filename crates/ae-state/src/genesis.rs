use crate::{
    StateRoots,
    accounts::{Account, AccountError, Accounts},
};
use ae_codec::{
    api::{self, ApiType},
    headers::{CommonHeader, Header, KeyHeader},
};
use ae_types::{AccountPublicKey, TypeError, Unsigned};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Uat,
}
#[derive(Debug)]
pub enum GenesisError {
    Json(serde_json::Error),
    Shape,
    Address,
    Number(TypeError),
    Account(AccountError),
}

pub const CONTRIBUTORS_MESSAGES_HASH: [u8; 32] = [
    158, 79, 89, 57, 49, 136, 245, 229, 211, 220, 44, 167, 79, 151, 117, 42, 149, 176, 13, 160,
    196, 61, 156, 179, 95, 106, 250, 133, 59, 93, 3, 170,
];

pub fn allocations(network: Network) -> Result<Vec<(AccountPublicKey, Unsigned)>, GenesisError> {
    let json = match network {
        Network::Mainnet => include_str!("../data/mainnet_accounts.json"),
        Network::Uat => include_str!("../data/uat_accounts.json"),
    };
    let value: serde_json::Value = serde_json::from_str(json).map_err(GenesisError::Json)?;
    let object = value.as_object().ok_or(GenesisError::Shape)?;
    let mut accounts = Vec::with_capacity(object.len());
    for (address, amount) in object {
        let bytes =
            api::decode_as(ApiType::AccountPubkey, address).map_err(|_| GenesisError::Address)?;
        let key =
            AccountPublicKey::try_from(bytes.as_slice()).map_err(|_| GenesisError::Address)?;
        let number = amount.as_number().ok_or(GenesisError::Shape)?;
        let balance = Unsigned::from_decimal(&number.to_string()).map_err(GenesisError::Number)?;
        accounts.push((key, balance));
    }
    accounts.sort_by_key(|(key, _)| *key);
    Ok(accounts)
}

pub fn state(network: Network) -> Result<(Accounts, StateRoots), GenesisError> {
    let mut accounts = Accounts::default();
    for (key, balance) in allocations(network)? {
        accounts
            .enter(&Account::new(key, balance))
            .map_err(GenesisError::Account)?;
    }
    let roots = StateRoots {
        accounts: accounts.root_hash(),
        ..StateRoots::default()
    };
    Ok((accounts, roots))
}

pub fn header(network: Network) -> Result<Header, GenesisError> {
    let (_, roots) = state(network)?;
    Ok(Header::Key(KeyHeader {
        common: CommonHeader {
            version: 1,
            height: 0,
            previous_hash: CONTRIBUTORS_MESSAGES_HASH,
            previous_key_hash: [0; 32],
            state_hash: roots.hash().0,
        },
        miner: [0; 32],
        beneficiary: [0; 32],
        target: match network {
            Network::Mainnet => 0x1f1f1f1f,
            Network::Uat => 0x2100ffff,
        },
        seal: [0; 42],
        nonce: 0,
        time: 0,
        info: None,
        hole: false,
        end_of_epoch: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mainnet_allocations_reconstruct_observed_genesis_state_and_header() {
        let allocations = allocations(Network::Mainnet).unwrap();
        assert_eq!(allocations.len(), 716);
        let (accounts, roots) = state(Network::Mainnet).unwrap();
        for (key, balance) in allocations {
            assert_eq!(accounts.lookup(key).unwrap().unwrap().balance(), &balance);
        }
        assert_eq!(
            hex::encode(roots.hash().0),
            "34b7e5f936453358740f7a06b6c608ed5f58984c3573efe54b54781107499931"
        );
        assert_eq!(
            header(Network::Mainnet).unwrap().hash().unwrap(),
            ae_protocol::MAINNET_GENESIS_HASH
        );
    }
    #[test]
    fn public_networks_have_distinct_genesis_state() {
        assert_eq!(allocations(Network::Uat).unwrap().len(), 1);
        assert_ne!(
            header(Network::Uat).unwrap().hash().unwrap(),
            ae_protocol::MAINNET_GENESIS_HASH
        );
    }
}
