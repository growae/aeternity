// Aeternity fa2a6d7, aeo_state_tree pre-block expiry and query refunds.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Error, Event};
use ae_state::{accounts::Accounts, oracles::Oracles};
use ae_trie::NodeWriter;

pub fn prune<S: NodeWriter, O: NodeWriter, C: NodeWriter>(
    accounts: &mut Accounts<S>,
    oracles: &mut Oracles<O, C>,
    next_height: u64,
) -> Result<Vec<Event>, Error> {
    accounts.transaction(|accounts| {
        let mut events = Vec::new();
        oracles.prune_with_refunds(next_height, |refund| -> Result<(), Error> {
            let mut account = accounts
                .lookup(refund.account)?
                .ok_or(Error::OracleRefundAccountMissing(refund.account))?;
            // Refunds ignore payability and retain account nonce/GA metadata.
            account.earn(&refund.amount);
            accounts.enter(&account)?;
            if refund.emit_event {
                events.push(Event::OracleRefund {
                    account: refund.account,
                    amount: refund.amount,
                });
            }
            Ok(())
        })?;
        events.reverse();
        Ok(events)
    })
}
