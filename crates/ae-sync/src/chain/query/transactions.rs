// aec_db:find_tx_with_location / aehttp_dispatch_ext, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::*;
use ae_chain::repository::{Read, View};
use ae_mempool::pool::{Location, Signed};
use ae_storage::pool::Stored;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Hash(Hash256),
    Micro(Hash256),
    // None denotes a positive index larger than usize, never a wrapped value.
    MicroIndex { hash: Hash256, index: Option<usize> },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Transaction {
        transaction: Box<Signed>,
        header: Option<Header>,
    },
    Transactions {
        transactions: Vec<Signed>,
        header: Header,
    },
    MissingTransaction,
    MissingBlock,
    InvalidIndex,
}

pub fn read<S: Read>(view: View<S>, request: Request) -> Result<Reply, repository::Error> {
    let hash = match request {
        Request::Hash(hash) => {
            let stored = Stored::load(view)?;
            return match stored.find_transaction(hash)? {
                None => Ok(Reply::MissingTransaction),
                Some((location, transaction)) => {
                    let header = match location {
                        Location::Pool => None,
                        Location::Block(block) => Some(stored.chain().require_header(block)?),
                        _ => return Err(repository::Error::Corrupt("located transaction")),
                    };
                    Ok(Reply::Transaction {
                        transaction: Box::new(transaction),
                        header,
                    })
                }
            };
        }
        Request::Micro(hash) | Request::MicroIndex { hash, .. } => hash,
    };
    let Some(Block::Micro(micro)) = view.block(hash)? else {
        return Ok(Reply::MissingBlock);
    };
    let header = Header::Micro(micro.header);
    match request {
        Request::Micro(_) => Ok(Reply::Transactions {
            transactions: micro.transactions,
            header,
        }),
        Request::MicroIndex { index, .. } => {
            let Some(index) = index
                .and_then(|i| i.checked_sub(1))
                .filter(|i| *i < micro.transactions.len())
            else {
                return Ok(Reply::InvalidIndex);
            };
            // Consume in stored order, including duplicate transaction entries.
            let transaction = micro
                .transactions
                .into_iter()
                .nth(index)
                .expect("checked index");
            Ok(Reply::Transaction {
                transaction: Box::new(transaction),
                header: Some(header),
            })
        }
        Request::Hash(_) => unreachable!(),
    }
}

pub struct Permit<'a, O> {
    importer: &'a Importer<O>,
    reservation: blocking::Reservation,
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    // Rendering executes on the same joined worker as the database read.
    // Reserve before API hash validation. Rendering credit is supplied by the
    // adapter and added to the repository read admission estimate.
    pub fn transaction_query(&self, rendering_bytes: usize) -> Result<Permit<'_, O>, Error> {
        self.check_open()?;
        let config = &self.context.config;
        let reads = self.context.db.read_config();
        let weight = reads
            .max_block_bytes
            .checked_add(reads.max_transaction_record_bytes)
            .and_then(|n| n.checked_add(config.messages.work_bytes))
            .and_then(|n| n.checked_add(rendering_bytes))
            .ok_or(Error::Busy)?;
        Ok(Permit {
            importer: self,
            reservation: self.jobs.reserve(weight)?,
        })
    }
}
impl<O: MapOrder + Send + Sync + 'static> Permit<'_, O> {
    pub fn submit<T: Send + 'static>(
        self,
        request: Request,
        render: impl FnOnce(Reply) -> T + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.importer.check_open()?;
        let context = self.importer.context.clone();
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let reply = read(context.db.view()?, request)?;
            context.check(&cancel, None)?;
            let result = render(reply);
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.importer.context.config.timeout,
        })
    }
}
