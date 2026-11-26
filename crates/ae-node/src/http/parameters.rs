// aehttp_dispatch_ext:GetProtocolParameters, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_codec::api::{self, ApiType};
use ae_protocol::{Protocol, contracts, gas, names, oracles};
use ae_sync::chain::{self, query::Retained};
use ae_types::Unsigned;
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[cfg(test)]
mod tests;

pub(crate) struct Bound {
    micro_cycle_ms: u64,
    interval: Interval,
    auction: Option<Unsigned>,
}
enum Interval {
    Pow(Value),
    Pos(Unsigned),
}
impl Bound {
    pub(crate) fn capture(prepared: &runtime::Prepared) -> Self {
        let (micro_cycle_ms, interval) = if let Some(pow) = prepared.pow() {
            (
                pow.rules().admission.micro_cycle_ms,
                Interval::Pow(
                    pow.configured()
                        .chain()
                        .settings()
                        .expected_mine_rate()
                        .clone(),
                ),
            )
        } else {
            let rules = &prepared
                .hyperchain()
                .expect("checked family")
                .execution()
                .rules;
            (
                rules.micro_cycle_ms,
                Interval::Pos(rules.consensus.child_block_time.clone()),
            )
        };
        Self {
            micro_cycle_ms,
            interval,
            auction: prepared
                .configured()
                .execution()
                .name_claim_bid_timeout()
                .cloned(),
        }
    }
    fn values(&self) -> Result<Values, Response> {
        // Arbitrary-integer conversion is quadratic. Reserve bounded rendering
        // work before these conversions, not merely after serializing the body.
        let mut work = 64usize << 20;
        for value in self.auction.iter().chain(match &self.interval {
            Interval::Pos(n) => Some(n),
            Interval::Pow(_) => None,
        }) {
            let bytes = value.as_bytes().len();
            work = bytes
                .checked_mul(bytes)
                .and_then(|n| work.checked_sub(n))
                .ok_or_else(|| Response::unavailable(true))?;
        }
        let interval = match &self.interval {
            Interval::Pow(v) => {
                let length = match v {
                    Value::Number(n) => n.as_str().len(),
                    Value::String(s) => s.len(),
                    _ => return Err(Response::invalid_response()),
                };
                if length > 1 << 20 {
                    return Err(Response::unavailable(true));
                }
                ("expected_block_mine_rate", v.clone())
            }
            Interval::Pos(n) => ("child_block_time", super::accounts::number(n)?),
        };
        Ok(Values {
            micro_cycle: self.micro_cycle_ms.into(),
            interval: Some(interval),
            auction: self
                .auction
                .as_ref()
                .map(super::accounts::number)
                .transpose()?,
        })
    }
}

struct Values {
    micro_cycle: Value,
    interval: Option<(&'static str, Value)>,
    auction: Option<Value>,
}
struct Tables {
    network: Vec<u8>,
    schedule: BTreeMap<BigInt, Value>,
    signalled: Option<(BigInt, Value)>,
    genesis: u32,
}
impl Tables {
    fn bound(network: &ae_protocol::NetworkProfile) -> Self {
        Self {
            network: network.network_id().to_vec(),
            schedule: network
                .activations()
                .iter()
                .map(|a| (BigInt::from(a.protocol as u32), json!(a.height)))
                .collect(),
            signalled: network
                .community_fork()
                .map(|f| (f.version.into(), json!(f.end))),
            genesis: network.scheduled_protocol_at(0) as u32,
        }
    }
}
fn body(top: Option<u32>, mut tables: Tables, values: Values) -> Result<Value, Response> {
    let current = top.unwrap_or(tables.genesis);
    if let Some((v, h)) = tables.signalled
        && v == BigInt::from(current)
    {
        tables.schedule.insert(v, h);
    }
    if tables.network.len() > 65536 || tables.schedule.len() > 1024 {
        return Err(Response::unavailable(true));
    }
    let mut rows = Vec::new();
    for (version, height) in tables.schedule {
        if version >= BigInt::from(current) {
            // The native metadata getters have open-ended >= clauses. This
            // preserves their future-version reporting, without admitting an
            // unknown protocol into the execution engine.
            let known = version.clone().min(BigInt::from(Protocol::Salus as u32));
            let protocol = u32::try_from(&known)
                .ok()
                .and_then(|n| Protocol::try_from(n).ok())
                .ok_or_else(Response::internal)?;
            rows.push(protocol_row(
                version,
                height,
                protocol,
                values.auction.as_ref(),
            )?);
        }
    }
    let mut result = json!({"network_id":super::text::binary(&tables.network),
        "current_protocol_version":current,
        "locked_coins_holder_account":api::encode(ApiType::AccountPubkey, &[0;32]),
        "micro_block_cycle":values.micro_cycle,"protocols":rows});
    if let Some((key, value)) = values.interval {
        result[key] = value;
    }
    Ok(result)
}
fn protocol_row(
    version: BigInt,
    height: Value,
    p: Protocol,
    auction: Option<&Value>,
) -> Result<Value, Response> {
    let number = |n: Unsigned| super::accounts::number(&n);
    let timeout = |length| -> Result<Value, Response> {
        if p >= Protocol::Lima
            && let Some(v) = auction
        {
            return Ok(v.clone());
        }
        number(names::bid_timeout_value(length, p, None))
    };
    let plain = [
        "SpendTx",
        "ChannelCloseMutualTx",
        "ChannelCloseSoloTx",
        "ChannelCreateTx",
        "ChannelDepositTx",
        "ChannelForceProgressTx",
        "ChannelSetDelegatesTx",
        "ChannelSettleTx",
        "ChannelSlashTx",
        "ChannelSnapshotSoloTx",
        "ChannelWithdrawTx",
        "NameClaimTx",
        "NamePreclaimTx",
        "NameRevokeTx",
        "NameTransferTx",
        "NameUpdateTx",
        "OracleExtendTx",
        "OracleQueryTx",
        "OracleRegisterTx",
        "OracleRespondTx",
        "PayingForTx",
    ];
    let base: serde_json::Map<_, _> = plain
        .into_iter()
        .map(|name| {
            (
                name.into(),
                json!(match name {
                    "ChannelForceProgressTx" => gas::channel_force_progress(p),
                    "PayingForTx" => gas::PAYING_FOR,
                    _ => gas::TRANSACTION,
                }),
            )
        })
        .collect();
    let mut contract_gas = Vec::new();
    for kind in [
        "ContractCreateTx",
        "ContractCallTx",
        "GAAttachTx",
        "GAMetaTx",
    ] {
        for abi in [1u64, 3] {
            contract_gas.push(json!({"tx_type":kind,"abi_version":abi,
            "tx_base_gas":if kind=="ContractCallTx" {contracts::call_base_gas(&abi.into())} else {contracts::CREATE_BASE_GAS}}));
        }
    }
    let state: serde_json::Map<_, _> = [
        "OracleRegisterTx",
        "OracleExtendTx",
        "OracleQueryTx",
        "OracleRespondTx",
    ]
    .into_iter()
    .map(|n| {
        (
            n.into(),
            json!({"part":oracles::STATE_GAS_PART,"whole":oracles::STATE_GAS_WHOLE}),
        )
    })
    .collect();
    let fees: Vec<_> = (1..=names::MAX_FEE_LENGTH)
        .map(|n| {
            number(names::claim_fee(n.try_into().expect("positive length"), p))
                .map(|v| v.to_string())
        })
        .collect::<Result<_, _>>()?;
    let auctions: Vec<_> = (1..=names::MAX_AUCTION_LENGTH).map(|length| Ok(json!({"length":length,
        "bid_timeout":timeout(length)?,"bid_extension":if p<Protocol::Ceres {timeout(length)?} else {number(names::bid_extension_value(length,p,None))?}})))
        .collect::<Result<_,Response>>()?;
    let mut versions = Vec::new();
    for vm in 1..=8 {
        for abi in [1, 2, 3] {
            if (contracts::Version { vm, abi }).can_create(p) {
                versions.push(json!({"vm_version":vm,"abi_version":abi}));
            }
        }
    }
    let oracle_abis: Vec<_> = [0u64, 1, 2, 3]
        .into_iter()
        .filter(|n| oracles::abi_available(&(*n).into(), p))
        .collect();
    let mut result = json!({"version":serde_json::from_str::<Value>(&version.to_string()).map_err(|_|Response::internal())?,
        "effective_at_height":height,"minimum_gas_price":p.minimum_gas_price().to_string(),
        "gas_per_byte":gas::TRANSACTION_BYTE,"store_byte_gas":gas::STORE_BYTE,"tx_base_gas":base,
        "contract_tx_base_gas":contract_gas,"state_gas_per_block":state,"name_claim_fees":fees,
        "name_auction_timeouts":auctions,"name_claim_bid_increment":names::BID_INCREMENT_PERCENT,
        "name_max_length_starting_auction":names::MAX_AUCTION_LENGTH,"name_claim_max_expiration":names::max_expiration(p),
        "name_registrars":[if p>=Protocol::Lima {"chain"} else {"test"}],
        "name_preclaim_expiration":names::PRECLAIM_EXPIRATION,"name_claim_preclaim_delta":names::PRECLAIM_DELAY,
        "name_protection_period":names::PROTECTION_PERIOD,"name_claim_locked_fee":names::LEGACY_LOCKED_FEE.to_string(),
        "allowed_contract_versions":versions,"allowed_oracle_abi_versions":oracle_abis});
    if p >= Protocol::Iris {
        result["name_pointers_max_count"] = names::POINTERS_MAX_COUNT.into();
        result["name_pointer_max_key_size"] = names::POINTER_MAX_KEY_SIZE.into();
    }
    Ok(result)
}
pub(super) fn finish(mut body: Value, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut body);
    }
    if super::model::valid("GetProtocolParameters", &body, true) {
        Response::json(200, body)
    } else {
        Response::invalid_response()
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let information = parent.information.clone();
    let ticket = importer
        .current_header(48 << 20, move |header| {
            let result = information.protocol_parameters.values().and_then(|values| {
                body(Some(header.version), Tables::bound(&header.network), values)
            });
            match result {
                Ok(value) => finish(value, strings),
                Err(error) => error,
            }
        })
        .map_err(super::transport::failure)?;
    let reply = tokio::time::timeout(config.timeout, ticket.wait_retained())
        .await
        .map_err(|_| Response::internal())?
        .map_err(super::transport::failure)?;
    if !parent.status().ready || parent.importer().is_none_or(|p| !Arc::ptr_eq(&p, importer)) {
        return Err(Response::unavailable(false));
    }
    Ok(reply)
}
