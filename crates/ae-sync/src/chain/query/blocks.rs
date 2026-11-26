// aec_chain / aehttp_dispatch_ext block queries, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::*;
use ae_chain::repository::{Read, View, serving};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Top,
    CurrentKey,
    KeyHash(Hash256),
    KeyHeight(u128),
    MicroHeader(Hash256),
    MicroCount(Hash256),
    CurrentGeneration,
    GenerationHash(Hash256),
    GenerationHeight(u128),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Key,
    Micro,
}
impl Kind {
    fn of(header: &Header) -> Self {
        match header {
            Header::Key(_) => Self::Key,
            Header::Micro(_) => Self::Micro,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHeader {
    pub header: Header,
    pub previous: Kind,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Missing {
    Block,
    Previous,
    Key,
    Micro,
    ChainTooShort,
    NotOnChain,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Header(ClientHeader),
    Count(usize),
    Generation {
        key: ClientHeader,
        micros: Vec<Hash256>,
    },
    Missing(Missing),
}

fn header<S: Read>(
    view: &View<S>,
    header: Header,
    genesis: bool,
) -> Result<Reply, repository::Error> {
    let previous = if genesis && header.common().height == 0 {
        Kind::Key
    } else {
        let Some(previous) = view.header(Hash256(header.common().previous_hash))? else {
            return Ok(Reply::Missing(Missing::Previous));
        };
        Kind::of(&previous)
    };
    Ok(Reply::Header(ClientHeader { header, previous }))
}
fn canonical<S: Read>(view: &View<S>, height: u128) -> Result<Option<Hash256>, repository::Error> {
    match u64::try_from(height) {
        Ok(height) => view.canonical_key(height),
        Err(_) => Ok(None),
    }
}
// Pure selection on a caller-owned immutable view. Runtime callers use
// Importer::blocks, which reserves and joins the storage work.
pub fn read<S: Read>(
    view: &View<S>,
    request: Request,
    limits: serving::Limits,
) -> Result<Reply, repository::Error> {
    let missing = |why| Ok(Reply::Missing(why));
    let block = match request {
        Request::Top | Request::CurrentKey => {
            let Some(top) = view.status().tip else {
                return missing(Missing::Block);
            };
            let block = view.require_block(top)?;
            if request == Request::CurrentKey {
                match block {
                    Block::Key(_) => Some(block),
                    Block::Micro(micro) => {
                        view.block(Hash256(micro.header.common.previous_key_hash))?
                    }
                }
            } else {
                Some(block)
            }
        }
        Request::KeyHash(hash) | Request::MicroHeader(hash) | Request::MicroCount(hash) => {
            view.block(hash)?
        }
        Request::KeyHeight(height) => {
            let Some(hash) = canonical(view, height)? else {
                return missing(Missing::ChainTooShort);
            };
            view.block(hash)?
        }
        Request::CurrentGeneration | Request::GenerationHash(_) | Request::GenerationHeight(_) => {
            let hash = match request {
                Request::GenerationHash(hash) => hash,
                Request::GenerationHeight(height) => {
                    let Some(hash) = canonical(view, height)? else {
                        return missing(Missing::ChainTooShort);
                    };
                    hash
                }
                _ => {
                    let top = view.status().tip.ok_or(repository::Error::Uninitialized)?;
                    let height = view.require_header(top)?.common().height;
                    let Some(hash) = view.canonical_key(height)? else {
                        return missing(Missing::Block);
                    };
                    hash
                }
            };
            let Some(generation) = serving::generation_summary(view, hash, true, limits)? else {
                return missing(if matches!(request, Request::GenerationHash(_)) {
                    Missing::NotOnChain
                } else {
                    Missing::Block
                });
            };
            return match header(view, Header::Key(generation.key), true)? {
                Reply::Header(key) => Ok(Reply::Generation {
                    key,
                    micros: generation.micros,
                }),
                reply => Ok(reply),
            };
        }
    };
    let Some(block) = block else {
        return missing(
            if matches!(request, Request::MicroHeader(_) | Request::MicroCount(_)) {
                Missing::Micro
            } else {
                Missing::Block
            },
        );
    };
    match (&block, request) {
        (Block::Micro(_), Request::KeyHash(_)) => return missing(Missing::Key),
        (Block::Key(_), Request::MicroHeader(_) | Request::MicroCount(_)) => {
            return missing(Missing::Micro);
        }
        (Block::Micro(micro), Request::MicroCount(_)) => {
            return Ok(Reply::Count(micro.transactions.len()));
        }
        _ => (),
    }
    let value = block.header();
    drop(block);
    header(view, value, !matches!(request, Request::MicroHeader(_)))
}

pub struct Permit<'a, O> {
    importer: &'a Importer<O>,
    reservation: blocking::Reservation,
    limits: serving::Limits,
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    // Reserve before callback-level parameter decoding, matching the native
    // read queue. Dropping a permit on an invalid hash releases its budget.
    pub fn block_query(&self) -> Result<Permit<'_, O>, Error> {
        let context = &self.context;
        let limits = serving::Limits {
            items: context.config.blocks.min(context.config.messages.items),
            bytes: context
                .config
                .messages
                .body_bytes
                .min(context.config.messages.work_bytes),
        };
        // Hash vector capacity is retained alongside a single full-block read.
        let weight = limits
            .items
            .checked_mul(64)
            .and_then(|n| n.checked_add(context.config.messages.body_bytes))
            .and_then(|n| n.checked_add(context.config.messages.work_bytes))
            .ok_or(Error::Busy)?;
        self.check_open()?;
        Ok(Permit {
            importer: self,
            reservation: self.jobs.reserve(weight)?,
            limits,
        })
    }
    pub fn blocks(&self, request: Request) -> Result<Ticket<Reply>, Error> {
        self.block_query()?.submit(request)
    }
}
impl<O: MapOrder + Send + Sync + 'static> Permit<'_, O> {
    pub fn submit(self, request: Request) -> Result<Ticket<Reply>, Error> {
        self.importer.check_open()?;
        let context = self.importer.context.clone();
        let limits = self.limits;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let reply = read(&view, request, limits)?;
            context.check(&cancel, None)?;
            Ok(reply)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.importer.context.config.timeout,
        })
    }
}
