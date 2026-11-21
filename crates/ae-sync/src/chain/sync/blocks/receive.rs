use super::*;
use ae_chain::{
    block::{Block, Light},
    gossip::{self, Fragment, Precheck, PreparationError, Settings},
};
use ae_codec::headers::Header;
use ae_storage::Database;

fn clock() -> Result<u64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Clock)?
        .as_millis()
        .try_into()
        .map_err(|_| Error::Clock)
}
enum Material {
    Block(Box<Block>),
    Light(Box<Fragment>),
    Ignore(Ignored),
}
fn preliminary(error: PreparationError) -> Error {
    match error {
        PreparationError::Gossip(e) => Error::Gossip(e),
        PreparationError::Repository(e) => Error::Storage(e),
    }
}
fn assembled(
    fragment: Fragment,
    replies: Vec<ae_tx::SignedTransaction<ae_tx::Transaction>>,
    max_bytes: usize,
) -> Result<Block, Error> {
    let block = Block::Micro(fragment.fill(replies).map_err(Error::Gossip)?);
    if block.encode().map_err(Error::Block)?.len() > max_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(block)
}
fn invalid(error: &Error) -> bool {
    match error {
        Error::Block(error) => !error.is_operational(),
        Error::Gossip(_)
        | Error::KeyExpected
        | Error::MicroExpected
        | Error::MalformedResponse
        | Error::Hash => true,
        Error::Import(failure) => matches!(
            failure.error,
            chain::Error::Validation {
                operational: false,
                ..
            } | chain::Error::BlockedByWhitelist
                | chain::Error::Policy { .. }
                | chain::Error::Storage(repository::Error::Missing(_))
        ),
        _ => false,
    }
}
fn prepare(
    db: &Database,
    held: &Held,
    client: &net::Client,
    height: Option<u64>,
    limits: message::Limits,
    settings: Settings,
) -> Result<Material, Error> {
    if held.received.generation != *client.generation() {
        return Err(Error::Stale);
    }
    let message = held.received.message();
    message.encode_packet(limits)?;
    let stored = db.pool_view()?;
    let mut decode = stored.chain().config().block();
    decode.max_block_bytes = decode.max_block_bytes.min(limits.body_bytes);
    let bytes = |field: &str| match message.fields().get(field) {
        Some(Value::Binary(bytes)) => Ok(bytes.as_slice()),
        _ => Err(Error::MalformedResponse),
    };
    if message.kind() == Kind::MicroBlock
        && matches!(message.fields().get("light"), Some(Value::Boolean(true)))
    {
        let light = Light::decode(bytes("micro_block")?, decode).map_err(Error::Block)?;
        if !gossip::allows_height(light.header.common().height, height) {
            return Ok(Material::Ignore(Ignored::Height));
        }
        if gossip::precheck_stored(stored.chain(), &light.header, settings).map_err(preliminary)?
            == Precheck::Known
        {
            return Ok(Material::Ignore(Ignored::Known));
        }
        let mut tx_bytes = 0usize;
        let fragment = Fragment::lookup::<PreparationError>(light, |bytes| {
            let Ok(hash) = bytes.try_into() else {
                return Ok(None);
            };
            let tx = stored.transaction(Hash256(hash))?;
            if let Some(tx) = &tx {
                tx_bytes = tx_bytes
                    .checked_add(tx.encode().len())
                    .filter(|n| *n <= decode.max_block_bytes)
                    .ok_or(repository::Error::ResourceLimit)?;
            }
            Ok(tx)
        })
        .map_err(preliminary)?;
        if fragment.missing().next().is_none() {
            Ok(Material::Block(Box::new(assembled(
                fragment,
                vec![],
                decode.max_block_bytes,
            )?)))
        } else {
            Ok(Material::Light(Box::new(fragment)))
        }
    } else {
        let block = match message.kind() {
            Kind::KeyBlock => {
                let b = Block::decode(bytes("key_block")?, decode).map_err(Error::Block)?;
                if !matches!(b, Block::Key(_)) {
                    return Err(Error::KeyExpected);
                }
                b
            }
            Kind::MicroBlock => {
                let b = Block::decode(bytes("micro_block")?, decode).map_err(Error::Block)?;
                if !matches!(b, Block::Micro(_)) {
                    return Err(Error::MicroExpected);
                }
                b
            }
            _ => return Err(Error::MalformedResponse),
        };
        if !gossip::allows_height(block.header().common().height, height) {
            return Ok(Material::Ignore(Ignored::Height));
        }
        Ok(Material::Block(Box::new(block)))
    }
}
async fn expand<O: MapOrder + Send + Sync + 'static>(
    context: &Arc<Context<O>>,
    client: &net::Client,
    fragment: Fragment,
    hash: Hash256,
    held: Held,
    expansion: Expansion,
) -> Result<(Held, Block, net::Received, Expansion), Error> {
    let limits = context.config.messages;
    // Reserve read/decoded capacity before issuing an expansion request.
    let reserved = context
        .reads
        .reserve_wait(context.config.messages.body_bytes + context.config.messages.work_bytes)
        .await?;
    context.live(client).await?;
    let request = Message::new(
        Kind::GetBlockTransactions,
        1,
        BTreeMap::from([
            ("hash".into(), Value::Binary(hash.0.to_vec())),
            (
                "tx_hashes".into(),
                Value::List(
                    fragment
                        .missing()
                        .map(|h| Value::Binary(h.to_vec()))
                        .collect(),
                ),
            ),
        ]),
        limits,
    )?;
    let net::Reply::Response(received) = client.request(&request, None)?.await? else {
        return Err(Error::MalformedResponse);
    };
    if received.generation != *client.generation() {
        return Err(Error::Stale);
    }
    context.live(client).await?;
    let db = context.importer.context.db.clone();
    reserved
        .submit(move |_| {
            received.message().encode_packet(limits)?;
            let body = wire::response(received.message(), Kind::BlockTransactions)?;
            if !matches!(body.fields().get("hash"), Some(Value::Binary(bytes)) if bytes.as_slice() == hash.0) {
                return Err(Error::Hash);
            }
            let Some(Value::List(txs)) = body.fields().get("txs") else {
                return Err(Error::MalformedResponse);
            };
            let view = db.view()?;
            let decode = view.config().block();
            let mut replies = Vec::with_capacity(txs.len());
            for item in txs {
                let Value::Binary(bytes) = item else {
                    return Err(Error::MalformedResponse);
                };
                replies.push(ae_tx::SignedTransaction::decode_with_state_config(
                    bytes, decode.transaction, decode.state,
                ).map_err(|e| Error::Block(ae_chain::block::Error::Transaction(e)))?);
            }
            // Decode every reply before fill, including any unused tail.
            let block = assembled(fragment, replies, decode.max_block_bytes.min(limits.body_bytes))?;
            // The read worker can outlive its async waiter. Keep the input,
            // aggregate budget and duplicate lease with the reconstruction.
            Ok((held, block, received, expansion))
        })?
        .await?
}
async fn receive<O: MapOrder + Send + Sync + 'static>(
    context: Arc<Context<O>>,
    expansions: Expansions,
    client: net::Client,
    held: Held,
    height: Option<u64>,
) -> Result<Outcome, Error> {
    context.live(&client).await?;
    let worker = context.clone();
    let source = client.clone();
    let now_ms = clock()?;
    let (held, material) = context
        .reads
        .submit_wait(
            context.config.messages.body_bytes + context.config.messages.work_bytes,
            move |cancel| {
                if cancel.cancelled() {
                    return Err(Error::Closed);
                }
                let settings = Settings {
                    now_ms,
                    micro_cycle_ms: worker.importer.context.rules.admission.micro_cycle_ms,
                    gossip_depth: worker.importer.context.config.ledger.policy.gossip_depth,
                };
                let material = prepare(
                    &worker.importer.context.db,
                    &held,
                    &source,
                    height,
                    worker.config.messages,
                    settings,
                )?;
                Ok((held, material))
            },
        )
        .await?
        .await??;
    let (held, block, reply, expansion) = match material {
        Material::Ignore(reason) => return Ok(Outcome::Ignored(reason)),
        Material::Block(block) => (held, *block, None, None),
        Material::Light(fragment) => {
            let hash = Header::Micro(fragment.header().clone())
                .hash()
                .map_err(|_| Error::Header)?;
            let Some(expansion) = Expansion::acquire(&expansions, &client, hash) else {
                return Ok(Outcome::Ignored(Ignored::Duplicate));
            };
            let (held, block, reply, expansion) =
                expand(&context, &client, *fragment, hash, held, expansion).await?;
            (held, block, Some(reply), Some(expansion))
        }
    };
    context.live(&client).await?;
    let now_ms = clock()?;
    let Held {
        received,
        reservation,
        _bytes,
    } = held;
    let import = context.importer.context.clone();
    let pending = reservation.submit(move |cancel| {
        let result = import.apply_received_block(block, now_ms, cancel, &client);
        // Retain input, response, aggregate budget and duplicate lease until
        // the actual worker has completed, even if its async waiter is gone.
        drop((received, reply, _bytes, expansion));
        result
    })?;
    chain::Ticket {
        pending,
        timeout: context.importer.context.config.timeout,
    }
    .wait()
    .await
    .map(Outcome::Accepted)
    .map_err(Error::Import)
}
pub(super) async fn run<O: MapOrder + Send + Sync + 'static>(
    context: Arc<Context<O>>,
    expansions: Expansions,
    client: net::Client,
    held: Held,
    height: Option<u64>,
) -> Completion {
    let monitor = client.monitor();
    let timeout = context.config.timeout;
    let peer = client.peer();
    let generation = client.generation().clone();
    let result = tokio::select! {
        biased;
        _=monitor.wait()=>Err(Error::Stale),
        result=tokio::time::timeout(timeout,receive(context,expansions,client,held,height))=>result.unwrap_or(Err(Error::Import(chain::Failure{progress:None,error:chain::Error::Timeout}))),
    };
    let result = match result {
        Err(error) if invalid(&error) => Ok(Outcome::Ignored(Ignored::Invalid(Box::new(error)))),
        other => other,
    };
    Completion {
        peer,
        generation,
        result,
    }
}
