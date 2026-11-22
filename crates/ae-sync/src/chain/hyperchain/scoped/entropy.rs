use super::*;
use ae_hyperchain::parent_cache;
use num_bigint::BigInt;
use std::time::{Duration, Instant};

pub(super) fn acquire<P: PinInputs>(
    inputs: &Cached<P>,
    host: &mut inputs::Captured<Snapshot<'_>>,
    error: &repository::HyperchainError<inputs::InputError, Snapshot<'_>>,
    check: &mut dyn FnMut() -> Result<(), Error>,
) -> Result<bool, Error> {
    let Some(inputs::InputError::EntropyNotCaptured(height)) = error.host_error() else {
        return Ok(false);
    };
    let mut guard = || {
        check()?;
        inputs.check().map_err(input_error)
    };
    let parent = fetch(&inputs.cache, height, &mut guard)?;
    guard()?;
    host.capture_entropy(height.clone(), parent)
        .map_err(input_error)?;
    Ok(true)
}

fn fetch(
    cache: &cache::Client,
    height: &BigInt,
    check: &mut dyn FnMut() -> Result<(), Error>,
) -> Result<Option<ae_hyperchain::parent_block::ParentBlock>, Error> {
    // aec_consensus_hc:get_entropy_hash uses 1000ms of 10ms sleeps.
    // Each native gen_server call also has a 5s timeout. An additional
    // operational wall limit bounds repeated slow calls; it is never a
    // negative parent fact or a consensus-invalid block.
    let started = Instant::now();
    let wall = Duration::from_secs(6);
    for attempt in 0..=100 {
        check()?;
        if started.elapsed() >= wall {
            return Err(input_error(cache::Error::Timeout));
        }
        let mut call = cache
            .call(parent_cache::Event::Get(height.clone()))
            .map_err(input_error)?;
        let called = Instant::now();
        let reply = loop {
            check()?;
            if called.elapsed() >= Duration::from_secs(5) || started.elapsed() >= wall {
                return Err(input_error(cache::Error::Timeout));
            }
            if let Some(reply) = call.try_reply().map_err(input_error)? {
                break reply.map_err(input_error)?;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let parent_cache::Reply::Block(reply) = reply else {
            unreachable!("cache lookup reply")
        };
        match reply {
            Ok(block) => return Ok(Some(block)),
            Err(_) if attempt == 100 => return Ok(None),
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    unreachable!("final native lookup returns its outcome")
}
