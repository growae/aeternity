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
