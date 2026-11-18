use crate::{Error, Event};
use ae_protocol::Protocol;
use ae_state::{
    accounts::{Account, Accounts},
    names::Names,
};
use ae_trie::NodeWriter;
use ae_types::AccountPublicKey;

// Called once per key-block height, in the pre-block transformation sequence.
// Accounts, naming objects and their expiry index roll back together on error.
pub fn prune<S: NodeWriter, N: NodeWriter, C: NodeWriter>(
    accounts: &mut Accounts<S>,
    names: &mut Names<N, C>,
    next_height: u64,
    protocol: Protocol,
) -> Result<Vec<Event>, Error> {
    accounts.transaction(|accounts| {
        names.transaction(|names| {
            let locks = names.prune(next_height, protocol)?;
            let mut events = Vec::new();
            if !locks.is_empty() {
                let key = AccountPublicKey::ZERO;
                let mut locked = accounts
                    .lookup(key)?
                    .unwrap_or_else(|| Account::new(key, 0.into()));
                for fee in locks {
                    locked.earn(&fee.amount);
                    events.push(Event::NameFeeLocked { amount: fee.amount });
                }
                accounts.enter(&locked)?;
            }
            events.reverse();
            Ok(events)
        })
    })
}
