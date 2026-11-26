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
}
pub async fn joined(http: &http::Handle) {
    let status = http.status();
    assert!(status.joined && !status.running && status.failure.is_none());
    assert_eq!(status.active, 0);
    assert!(TcpStream::connect(status.address).await.is_err());
}
