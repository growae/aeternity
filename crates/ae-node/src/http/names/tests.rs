use super::*;
use ae_state::names::{Auction, Name, Pointer, hashing};
use ae_types::{AccountPublicKey, Id, IdTag};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../ae-chain/tests/fixtures/erlang_http_names.json"
    ))
    .unwrap()
}
fn state(row: &Value, input: &[u8], kind: Kind) -> Result<Reply, Error> {
    match row["input"]["scenario"].as_str().unwrap() {
        "no_state" | "no_top" => return Err(Error::NoStateTrees),
        "storage_error" => {
            return Err(Error::Backend(ae_chain::repository::Error::Backend(
                "injected".into(),
            )));
        }
        _ => (),
    }
    let key = match kind {
        Kind::Hash => input.to_vec(),
        Kind::Name | Kind::Auction => {
            let ascii = idna::to_ascii(input).map_err(Error::Normalization)?;
            let hash = hashing::name_hash(&ascii).0;
            if kind == Kind::Auction {
                hashing::auction_hash(hash).to_vec()
            } else {
                hash.to_vec()
            }
        }
    };
    let entries = row["names"].as_array().unwrap();
    let entry = entries
        .iter()
        .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == key)
        .ok_or(Error::NotPresent)?;
    let bytes = hex::decode(entry[1].as_str().unwrap()).unwrap();
    if kind == Kind::Auction {
        return Auction::deserialize(
            key.as_slice()
                .try_into()
                .map_err(|_| Error::InvalidObject)?,
            &bytes,
        )
        .map(Reply::Auction)
        .map_err(Into::into);
    }
    let name = Name::deserialize(
        key.as_slice()
            .try_into()
            .map_err(|_| Error::InvalidObject)?,
        &bytes,
    )?;
    match name.status.as_slice() {
        b"revoked" => Err(Error::Revoked),
        b"claimed" => Ok(Reply::Name(name)),
        _ => Err(Error::InvalidObject),
    }
}
#[test]
fn original_name_dispatcher_matches_raw_pointers_auctions_and_failure_precedence() {
    let f = fixture();
    let rows = f["dispatcher"].as_array().unwrap();
    assert_eq!(rows.len(), 738);
    for row in rows {
        let kind = match row["input"]["operation"].as_str().unwrap() {
            "GetNameEntryByName" => Kind::Name,
            "GetNameEntryByNameHash" => Kind::Hash,
            _ => Kind::Auction,
        };
        let field = if kind == Kind::Hash {
            "name_hash"
        } else {
            "name"
        };
        let op = Operation {
            kind,
            input: hex::decode(row["params_hex"][field].as_str().unwrap()).unwrap(),
        };
        let response = match row["input"]["scenario"].as_str().unwrap() {
            "overload" => Response::unavailable(true),
            "unstable" => Response::unavailable(false),
            _ => op
                .decode()
                .and_then(|input| state(row, &input, kind).map_err(failure))
                .and_then(body)
                .map(|v| Response::json(200, v))
                .unwrap_or_else(|r| r),
        };
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{}",
            row["input"]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{}",
            row["input"]
        );
        assert_eq!(
            response.headers.get("retry-after").map(String::as_str),
            row["headers"]["retry-after"].as_str()
        );
    }
}
#[test]
fn names_bound_aggregate_encoded_keys_data_and_decimal_work() {
    let mut n = Name::new([1; 32], AccountPublicKey([2; 32]), 1.into());
    let p = Pointer {
        key: vec![0; 1024],
        value: PointerValue::Data(vec![255; 1024]),
    };
    n.pointers = vec![p; 1024];
    assert_eq!(body(Reply::Name(n)).unwrap_err().status, 503);
    let mut n = Name::new([1; 32], AccountPublicKey([2; 32]), 1.into());
    n.pointers = vec![
        Pointer {
            key: vec![],
            value: PointerValue::Id(Id::new(IdTag::Account, [3; 32]))
        };
        4097
    ];
    assert_eq!(body(Reply::Name(n)).unwrap_err().status, 503);
    let big = Unsigned::from_field_bytes(&vec![255; 6000]).unwrap();
    let mut a = Auction {
        name_hash: [1; 32],
        bidder: Id::new(IdTag::Account, [2; 32]),
        bid: big.clone(),
        started: 1.into(),
        expires_by: 100.into(),
    };
    assert!(body(Reply::Auction(a.clone())).is_ok());
    a.started = big;
    assert_eq!(body(Reply::Auction(a)).unwrap_err().status, 503);
}
#[test]
fn name_routes_respect_versions_groups_and_preserve_variable_hash_widths() {
    for group in ["name_service", "external"] {
        let cfg = Config {
            groups: [group.into()].into(),
            ..Default::default()
        };
        for v in ["v2", "v3"] {
            assert!(response::route(&format!("/{v}/names/alice.chain"), &cfg).is_some());
            assert!(response::route(&format!("/{v}/auctions/alice.chain"), &cfg).is_some());
            assert_eq!(
                response::route(&format!("/{v}/names/hash/bad"), &cfg).is_some(),
                v == "v3"
            );
        }
    }
    let cfg = Config {
        groups: ["name".into()].into(),
        ..Default::default()
    };
    assert!(response::route("/v3/names/alice.chain", &cfg).is_none());
    for width in [0, 1, 31, 32, 33, 64] {
        let input = vec![1; width];
        let op = Operation {
            kind: Kind::Hash,
            input: api::encode(ApiType::Name, &input).into_bytes(),
        };
        assert_eq!(op.decode().unwrap(), input);
    }
    for kind in [Kind::Name, Kind::Hash, Kind::Auction] {
        assert_eq!(
            Operation {
                kind,
                input: vec![b'a'; 4097]
            }
            .decode()
            .unwrap_err()
            .status,
            503
        );
    }
    assert_eq!(failure(Error::ResourceLimit).status, 503);
    assert_eq!(failure(Error::InvalidObject).status, 500);
}
