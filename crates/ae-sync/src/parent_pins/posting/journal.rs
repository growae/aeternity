use super::{Chain, Failure, Node, Post, Submission};
use ae_storage::{Database, parent_posts as storage};
use std::sync::Arc;

#[derive(Clone)]
pub struct Config {
    pub database: Arc<Database>,
    pub network: Vec<u8>,
    pub limits: storage::Limits,
}
pub(crate) struct Owner {
    writer: storage::Writer,
    network: Vec<u8>,
}
impl Owner {
    pub(crate) fn start(config: Config) -> Result<Self, storage::Failure> {
        Ok(Self {
            writer: storage::Writer::start(config.database, config.limits)?,
            network: config.network,
        })
    }
    pub(super) fn call<'a>(&'a mut self, node: &Node, post: &Post, payload: &[u8]) -> Call<'a> {
        Call {
            writer: &mut self.writer,
            intent: storage::Intent {
                chain: chain(node.chain),
                network: self.network.clone(),
                account: post.account,
                payload: payload.to_vec(),
            },
            endpoint: node.origin().as_str().as_bytes().to_vec(),
            record: None,
        }
    }
}
fn chain(chain: Chain) -> storage::Chain {
    match chain {
        Chain::Aeternity => storage::Chain::Aeternity,
        Chain::Dogecoin => storage::Chain::Dogecoin,
        Chain::Bitcoin => storage::Chain::Bitcoin,
    }
}
pub(super) fn submission(record: &storage::Record) -> Submission {
    Submission {
        chain: match record.intent.chain {
            storage::Chain::Aeternity => Chain::Aeternity,
            storage::Chain::Dogecoin => Chain::Dogecoin,
            storage::Chain::Bitcoin => Chain::Bitcoin,
        },
        transaction: Arc::from(record.transaction.clone()),
    }
}
pub(super) struct Call<'a> {
    writer: &'a mut storage::Writer,
    intent: storage::Intent,
    endpoint: Vec<u8>,
    record: Option<storage::Record>,
}
impl Call<'_> {
    pub(super) fn existing(&self) -> Result<Option<storage::Record>, Failure> {
        self.writer.get(&self.intent).map_err(Failure::Journal)
    }
    pub(super) fn begin(
        &mut self,
        submission: &Submission,
    ) -> Result<Option<storage::Record>, Failure> {
        if chain(submission.chain) != self.intent.chain {
            return Err(Failure::ParentResponse);
        }
        let record = storage::Record {
            intent: self.intent.clone(),
            endpoint: self.endpoint.clone(),
            transaction: submission.transaction.to_vec(),
            response: None,
        };
        match self.writer.reserve(record).map_err(Failure::Journal)? {
            storage::Reservation::New(record) => {
                self.record = Some(record);
                Ok(None)
            }
            storage::Reservation::Existing(record) => Ok(Some(record)),
        }
    }
    pub(super) fn response(&mut self, hash: Vec<u8>) -> Result<(), Failure> {
        let record = self
            .writer
            .record_response(
                self.record
                    .as_ref()
                    .expect("durable intent before response"),
                hash,
            )
            .map_err(Failure::Journal)?;
        self.record = Some(record);
        Ok(())
    }
}
