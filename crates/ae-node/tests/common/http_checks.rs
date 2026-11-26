use ae_chain::block::Block;
use ae_codec::api::{self, ApiType};
use ae_node::{http, runtime};
use ae_types::Hash256;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn start(running: &mut runtime::Running) -> http::Handle {
    running
        .listen_http(http::Config {
            listen: ([127, 0, 0, 1], 0).into(),
            timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .unwrap()
}
pub async fn verify(http: &http::Handle, database: &ae_storage::Database, root: &runtime::Handle) {
    let bindings = match root.pow() {
        Some(pow) => pow.workers().unwrap().bindings(),
        None => root.hyperchain().unwrap().workers().unwrap().bindings(),
    };
    let peer = bindings
        .keys
        .unwrap()
        .current()
        .unwrap()
        .peer_public_key()
        .unwrap()
        .await
        .unwrap();
    let (key, height) = {
        let view = database.view().unwrap();
        let top = view.status().tip.unwrap();
        let block = view.require_block(top).unwrap();
        let key = match &block {
            Block::Key(_) => top,
            Block::Micro(m) => Hash256(m.header.common.previous_key_hash),
        };
        (key, block.header().common().height)
    };
    for (path, code, body) in [
        (
            "/v2/peers/pubkey",
            200,
            json!({"pubkey":api::encode(ApiType::PeerPubkey,peer.as_ref())}),
        ),
        (
            "/v3/peers/pubkey?int-as-string=true",
            200,
            json!({"pubkey":api::encode(ApiType::PeerPubkey,peer.as_ref())}),
        ),
        (
            "/v2/key-blocks/current/height",
            200,
            json!({"height":height}),
        ),
        (
            "/v3/key-blocks/current/hash",
            200,
            json!({"hash":api::encode(ApiType::KeyBlockHash,key.as_ref())}),
        ),
        (
            "/v2/sync-status",
            404,
            json!({"reason":"Node is not syncing"}),
        ),
        (
            "/v3/sync-status?int-as-string=true",
            404,
            json!({"reason":"Node is not syncing"}),
        ),
    ] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut socket = TcpStream::connect(http.status().address).await.unwrap();
            socket
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut bytes = vec![];
            socket.read_to_end(&mut bytes).await.unwrap();
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut response = httparse::Response::new(&mut headers);
            let end = response.parse(&bytes).unwrap().unwrap();
            assert_eq!(response.code, Some(code));
            assert_eq!(
                response
                    .headers
                    .iter()
                    .find(|h| h.name == "x-ae-height")
                    .unwrap()
                    .value,
                height.to_string().as_bytes()
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes[end..]).unwrap(),
                body
            );
        })
        .await
        .unwrap();
    }
    let selected = root
        .importer()
        .unwrap()
        .top_key()
        .unwrap()
        .wait()
        .await
        .unwrap();
    let network_name = match selected.network.network_id() {
        b"ae_mainnet" => "Mainnet",
        b"ae_uat" => "Testnet",
        other => std::str::from_utf8(other).unwrap(),
    };
    for (path, units) in [
        ("/v3/currency", json!(1_000_000_000_000_000_000u64)),
        (
            "/v3/currency?int-as-string=true",
            json!("1000000000000000000"),
        ),
    ] {
        assert_eq!(
            block_get(http, path).await,
            (
                200,
                json!({
                    "name":"aeternity", "symbol":"AE", "subunit":"aetto", "subunits_per_unit":units,
                    "primary_colour":"#f7296e", "secondary_colour":"#1e1e1e", "network_name":network_name
                })
            )
        );
    }
    let bindings = match root.pow() {
        Some(pow) => pow.workers().unwrap().bindings(),
        None => root.hyperchain().unwrap().workers().unwrap().bindings(),
    };
    let pool = bindings
        .admission
        .current()
        .unwrap()
        .client
        .inspection()
        .unwrap()
        .await
        .unwrap();
    let build = ae_sync::peer::info::Metadata::current();
    for (path, strings) in [
        ("/v2/status", false),
        ("/v3/status", false),
        ("/v3/status?int-as-string=true", true),
    ] {
        let mut socket = TcpStream::connect(http.status().address).await.unwrap();
        socket
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut bytes = vec![];
        socket.read_to_end(&mut bytes).await.unwrap();
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut headers);
        let end = response.parse(&bytes).unwrap().unwrap();
        assert_eq!(
            response.code,
            Some(200),
            "{path}: {}",
            String::from_utf8_lossy(&bytes)
        );
        let body: Value = serde_json::from_slice(&bytes[end..]).unwrap();
        assert_eq!(
            response
                .headers
                .iter()
                .find(|h| h.name == "content-type")
                .unwrap()
                .value,
            b"application/json"
        );
        let num = |n: u64| {
            if strings {
                json!(n.to_string())
            } else {
                json!(n)
            }
        };
        assert_eq!(body["top_block_height"], num(height));
        assert_eq!(
            body["top_key_block_hash"],
            api::encode(ApiType::KeyBlockHash, key.as_ref())
        );
        assert_eq!(
            body["genesis_key_block_hash"],
            api::encode(ApiType::KeyBlockHash, selected.genesis.as_ref())
        );
        assert_eq!(
            body["network_id"],
            std::str::from_utf8(selected.network.network_id()).unwrap()
        );
        assert_eq!(
            body["node_version"],
            std::str::from_utf8(&build.version).unwrap()
        );
        assert_eq!(
            body["node_revision"],
            std::str::from_utf8(&build.revision).unwrap()
        );
        assert_eq!(
            body["peer_pubkey"],
            api::encode(ApiType::PeerPubkey, peer.as_ref())
        );
        assert_eq!(
            body["pending_transactions_count"],
            num((pool.active + pool.visited) as u64)
        );
        assert_eq!(body["syncing"], false);
        assert_eq!(body["sync_progress"], json!(100.0));
        assert_eq!(body["solutions"], num(0));
        assert_eq!(body["listening"], true);
        match selected.algorithm {
            ae_protocol::ConsensusAlgorithm::OnDemand => {
                assert_eq!(body["difficulty"], num(1338));
                assert_eq!(body["hashrate"], num(0));
            }
            ae_protocol::ConsensusAlgorithm::Hyperchain => {
                assert_eq!(body["difficulty"], num(0));
                assert_eq!(body["hashrate"], num(0));
            }
            ae_protocol::ConsensusAlgorithm::Cuckoo => {
                let expected = ae_consensus::target::Target(selected.header.target)
                    .difficulty()
                    .unwrap();
                let expected = num_bigint::BigUint::from_bytes_be(expected.as_bytes()).to_string();
                assert_eq!(
                    body["difficulty"],
                    if strings {
                        json!(expected)
                    } else {
                        serde_json::from_str::<Value>(&expected).unwrap()
                    }
                );
            }
        }
        assert_eq!(
            body["protocols"].as_array().unwrap().len(),
            selected.network.activations().len()
        );
        let _ = uptime_ms(body["uptime"].as_str().unwrap());
    }
    verify_blocks(http, database).await;
}

pub async fn verify_blocks(http: &http::Handle, database: &ae_storage::Database) {
    let (top_hash, top, key_hash, key, generation) = {
        let view = database.view().unwrap();
        let hash = view.status().tip.unwrap();
        let top = view.require_block(hash).unwrap();
        let key_hash = match &top {
            Block::Key(_) => hash,
            Block::Micro(m) => Hash256(m.header.common.previous_key_hash),
        };
        let key = view.require_block(key_hash).unwrap().header();
        let generation = if view
            .canonical_key(top.header().common().height)
            .unwrap()
            .is_some()
        {
            let mut hashes = vec![];
            let mut cursor = hash;
            while cursor != key_hash {
                let Block::Micro(m) = view.require_block(cursor).unwrap() else {
                    panic!()
                };
                hashes.push(api::encode(ApiType::MicroBlockHash, cursor.as_ref()));
                cursor = Hash256(m.header.common.previous_hash);
            }
            hashes.reverse();
            Some(hashes)
        } else {
            None
        };
        (hash, top, key_hash, key, generation)
    };
    let key_encoded = api::encode(ApiType::KeyBlockHash, key_hash.as_ref());
    let top_encoded = api::encode(
        if matches!(top, Block::Key(_)) {
            ApiType::KeyBlockHash
        } else {
            ApiType::MicroBlockHash
        },
        top_hash.as_ref(),
    );
    let key_height = key.common().height;
    for v3 in [false, true] {
        let version = if v3 { "v3" } else { "v2" };
        for suffix in [
            "/key-blocks/current".to_owned(),
            format!("/key-blocks/hash/{key_encoded}"),
            format!("/key-blocks/height/{key_height}"),
        ] {
            let path = format!("/{version}{suffix}?int-as-string=true");
            let (code, body) = block_get(http, &path).await;
            assert_eq!(code, 200, "{path}: {body}");
            assert_eq!(body["hash"], key_encoded);
            assert_eq!(
                body["height"],
                if v3 {
                    json!(key_height.to_string())
                } else {
                    json!(key_height)
                }
            );
            assert_eq!(
                api::decode_as(
                    ApiType::BlockStateHash,
                    body["state_hash"].as_str().unwrap()
                )
                .unwrap(),
                key.common().state_hash
            );
        }
        let path = if v3 {
            "/v3/headers/top"
        } else {
            "/v2/blocks/top"
        };
        let (code, body) = block_get(http, path).await;
        assert_eq!(code, 200, "{body}");
        let header = if v3 {
            &body
        } else if matches!(top, Block::Key(_)) || top.header().common().height == 0 {
            &body["key_block"]
        } else {
            &body["micro_block"]
        };
        assert_eq!(header["hash"], top_encoded);
        for suffix in [
            "/generations/current".to_owned(),
            format!("/generations/hash/{key_encoded}"),
            format!("/generations/height/{}", top.header().common().height),
        ] {
            let path = format!("/{version}{suffix}");
            let (code, body) = block_get(http, &path).await;
            if let Some(expected) = &generation {
                assert_eq!(code, 200, "{path}: {body}");
                assert_eq!(body["key_block"]["hash"], key_encoded);
                assert_eq!(body["micro_blocks"], json!(expected));
            } else {
                assert_eq!(
                    code,
                    if suffix.contains("/hash/") { 400 } else { 404 },
                    "{path}: {body}"
                );
            }
        }
        if let Block::Micro(m) = &top {
            let (code, body) = block_get(
                http,
                &format!("/{version}/micro-blocks/hash/{top_encoded}/header"),
            )
            .await;
            assert_eq!(code, 200, "{body}");
            assert_eq!(body["hash"], top_encoded);
            let (code, body) = block_get(
                http,
                &format!("/{version}/micro-blocks/hash/{top_encoded}/transactions/count"),
            )
            .await;
            assert_eq!((code, body), (200, json!({"count":m.transactions.len()})));
        }
    }
}
async fn block_get(http: &http::Handle, path: &str) -> (u16, Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut socket = TcpStream::connect(http.status().address).await.unwrap();
        socket
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut bytes = vec![];
        socket.read_to_end(&mut bytes).await.unwrap();
        let mut fields = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut fields);
        let end = response.parse(&bytes).unwrap().unwrap();
        (
            response.code.unwrap(),
            serde_json::from_slice(&bytes[end..]).unwrap(),
        )
    })
    .await
    .unwrap()
}
pub fn uptime_ms(value: &str) -> u128 {
    value
        .split(':')
        .map(|part| {
            if let Some((seconds, ms)) = part.split_once("s.") {
                return seconds.parse::<u128>().unwrap() * 1000 + ms.parse::<u128>().unwrap();
            }
            let (number, unit) = part.split_at(part.len() - 1);
            number.parse::<u128>().unwrap()
                * match unit {
                    "d" => 86_400_000,
                    "h" => 3_600_000,
                    "m" => 60_000,
                    "s" => 1000,
                    _ => panic!("uptime unit"),
                }
        })
        .sum()
}
pub async fn joined(http: &http::Handle) {
    let status = http.status();
    assert!(status.joined && !status.running && status.failure.is_none());
    assert_eq!(status.active, 0);
    assert!(TcpStream::connect(status.address).await.is_err());
}
