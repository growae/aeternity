// Aeternity fa2a6d7, aeo_state_tree pre-block expiry and query refunds.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Context, Origin};
use crate::{Error, Event};
use ae_protocol::{Protocol, oracles::state_gas};
use ae_state::{accounts::Accounts, oracles::Oracles};
use ae_trie::NodeWriter;
use ae_tx::OracleTransaction;
use ae_types::Unsigned;

pub mod abi;

pub fn gas(tx: &OracleTransaction, context: &Context) -> Unsigned {
    gas_for_size(tx, context, tx.wire_size())
}
pub fn minimum_fee(tx: &OracleTransaction, context: &Context) -> Unsigned {
    let size = if context.origin == Origin::Generalized && context.protocol < Protocol::Iris {
        0
    } else {
        tx.wire_size()
    };
    gas_for_size(tx, context, size).mul_u64(context.protocol.minimum_gas_price())
}
fn gas_for_size(tx: &OracleTransaction, context: &Context, size: usize) -> Unsigned {
    let Ok(delta) = tx.operation().state_ttl().delta(&context.height.into()) else {
        return 0.into();
    };
    crate::simple_gas(size).add(&state_gas(&delta))
}

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
