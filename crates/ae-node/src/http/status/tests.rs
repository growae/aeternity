use super::*;
use crate::http::{response::Operation, transport};
use ae_protocol::{ConsensusAlgorithm, signaling::Fork};
use ae_vm_fate::maps::order::{Atom, Mixing, Otp26, WordSize};
use num_bigint::BigInt;

fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_status.json"
    ))
    .unwrap()
}
fn family(v: &Value) -> ConsensusAlgorithm {
    match v.as_str().unwrap() {
        "aec_consensus_bitcoin_ng" => ConsensusAlgorithm::Cuckoo,
        "aec_consensus_on_demand" => ConsensusAlgorithm::OnDemand,
        "aec_consensus_hc" => ConsensusAlgorithm::Hyperchain,
        _ => panic!("native consensus"),
    }
}
fn number(v: &Value) -> BigInt {
    v.to_string().parse().unwrap()
}
fn order() -> Otp26 {
    // Only integer map keys occur here: atom indices do not affect their hash.
    Otp26::new(
        WordSize::Bits64,
        Mixing::Crc32c,
        Atom::ALL
            .into_iter()
            .enumerate()
            .map(|(i, a)| (a, i as u32))
            .collect(),
    )
    .unwrap()
}
#[test]
fn original_status_arithmetic_and_uptime_boundaries_match() {
    let native = corpus();
    let rows = native["math"].as_array().unwrap();
    assert_eq!(rows.len(), 510);
    for row in rows {
        let input = &row["input"];
        let family = family(&input["family"]);
        let target = input["target"].as_u64().and_then(|n| u32::try_from(n).ok());
        if let Some(target) = target {
            let raw = model::raw_difficulty(target, family);
            assert_eq!(raw.is_ok(), row["header_difficulty"].get("ok").is_some());
            if let Ok(raw) = raw {
                assert_eq!(model::integer(&raw), row["header_difficulty"]["ok"]);
                assert_eq!(
                    model::difficulty(&raw, family),
                    row["status_difficulty"]["ok"]
                );
            }
        } else {
            // Six synthetic HC helper inputs exceed the canonical u32 header
            // domain. They test display units, never header admission.
            assert_eq!(family, ConsensusAlgorithm::Hyperchain);
            assert_eq!(
                model::difficulty(&number(&input["target"]), family),
                row["status_difficulty"]["ok"]
            );
        }
        let hash = model::hashrate(target.unwrap_or(0), family, &input["block_rate"]);
        assert_eq!(hash.is_ok(), row["hashrate"].get("ok").is_some(), "{input}");
        if let Ok(hash) = hash {
            assert_eq!(hash, row["hashrate"]["ok"], "{input}");
        }
    }
    let rows = native["uptime"].as_array().unwrap();
    assert_eq!(rows.len(), 25);
    for row in rows {
        assert_eq!(
            model::uptime(row["elapsed_ms"].as_u64().unwrap().into()),
            row["formatted"].as_str().unwrap()
        );
    }
}
#[test]
fn original_status_protocol_fold_fork_selection_and_display_units_match() {
    let native = corpus();
    let mut count = 0;
    for row in native["dispatcher"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["operation"] == "GetStatus" && r["code"] == 200)
    {
        let input = &row["input"];
        let fork = input["fork"].as_object().map(|f| Fork {
            start: 0,
            end: f["signalling_end_height"].as_u64().unwrap(),
            threshold: 1,
            info: 0,
            version: f["version"].as_u64().unwrap() as u32,
        });
        let entries = input["protocols"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(v, h)| (v.parse().unwrap(), h.as_u64().unwrap()))
            .collect();
        assert_eq!(
            model::protocols(
                entries,
                input["version"].as_u64().unwrap() as u32,
                fork,
                &order()
            )
            .unwrap(),
            row["body"]["protocols"]
        );
        // Dispatcher fixtures deliberately supply controlled header difficulty;
        // actual per-family calculations are checked by the arithmetic corpus.
        assert_eq!(
            model::difficulty(&number(&input["difficulty"]), family(&input["consensus"])),
            row["body"]["difficulty"]
        );
        count += 1;
    }
    assert_eq!(count, 144);
}
fn compare(response: Response, row: &Value) {
    let expected = &row["result"]["response"];
    assert_eq!(
        response.status,
        expected["code"].as_u64().unwrap() as u16,
        "{row}"
    );
    if row["result"]["kind"] == "exception" {
        assert!(response.body.is_empty());
        assert!(response.headers.is_empty());
        assert_eq!(expected["headers"], json!({"content-length":"0"}));
    } else {
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            expected["body"]
        );
        // These wrapper fixtures bypass Cowboy REST content negotiation. Its
        // JSON content type is added by the Rust transport and checked on live
        // requests; compare the dispatcher's own headers at this boundary.
        let mut headers = response.headers;
        assert_eq!(
            headers.remove("content-type"),
            Some("application/json".into())
        );
        assert_eq!(serde_json::to_value(headers).unwrap(), expected["headers"]);
    }
}
#[test]
fn original_status_wrapper_replays_match_all_510_saved_dispatcher_replies() {
    let native = corpus();
    let rows = native["responses"].as_array().unwrap();
    assert_eq!(rows.len(), 510);
    for row in rows {
        let input = &native["dispatcher"][row["id"].as_u64().unwrap() as usize];
        let v3 = row["module"] == "oas_endpoints";
        let op = if input["operation"] == "GetStatus" {
            Operation::Status { v3 }
        } else {
            Operation::PeerPublicKey
        };
        let reply = match input["code"].as_u64().unwrap() {
            200 => Ok(input["body"].clone()),
            500 => Err(Response::internal()),
            503 => Err(Response::unavailable(
                input["body"]["reason"] == "Temporary overload",
            )),
            _ => panic!("native response"),
        };
        compare(
            transport::dispatch(Ok((op, row["strings"].as_bool().unwrap())), reply),
            row,
        );
    }
}
#[test]
fn original_status_schema_boundaries_match_all_717_wrapper_results() {
    let native = corpus();
    let rows = native["bounds"].as_array().unwrap();
    assert_eq!(rows.len(), 717);
    for row in rows {
        let body = native["bounds_inputs"][row["id"].as_u64().unwrap() as usize]["body"].clone();
        let op = Operation::Status {
            v3: row["module"] == "oas_endpoints",
        };
        compare(
            transport::dispatch(Ok((op, row["strings"].as_bool().unwrap())), Ok(body)),
            row,
        );
    }
}
