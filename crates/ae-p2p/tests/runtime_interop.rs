use ae_p2p::{
    message::{Kind, Limits, Message},
    noise::{Identity, Network, Role},
    pool,
    runtime::{self, Config, Reply},
};
use ae_types::Hash256;
use std::{collections::BTreeMap, time::Duration};
use tokio::net::{TcpListener, TcpStream};

#[test]
#[ignore = "requires the isolated pinned Erlang peer runtime harness"]
fn native_runtime_worker() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let role = std::env::var("AE_NOISE_ROLE").unwrap();
            let network = Network::new(1, Hash256([0; 32]), b"ae_dev").unwrap();
            let initiator = Identity::from_secret([1; 32]);
            let responder = Identity::from_secret([2; 32]);
            let (socket, identity, role) = if role == "initiator" {
                let socket = tokio::time::timeout(
                    Duration::from_secs(10),
                    TcpStream::connect(std::env::var("AE_NOISE_ADDRESS").unwrap()),
                )
                .await
                .unwrap()
                .unwrap();
                (socket, &initiator, Role::Initiator(responder.public_key()))
            } else {
                assert_eq!(role, "responder");
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                std::fs::write(
                    std::env::var("AE_NOISE_READY").unwrap(),
                    listener.local_addr().unwrap().port().to_string(),
                )
                .unwrap();
                let (socket, _) = tokio::time::timeout(Duration::from_secs(30), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                (socket, &responder, Role::Responder)
            };
            let (client, _events, driver) =
                runtime::establish(socket, identity, &network, role, Config::default())
                    .await
                    .unwrap();
            let expected_peer = if matches!(role, Role::Initiator(_)) {
                responder.public_key()
            } else {
                initiator.public_key()
            };
            assert_eq!(client.peer(), expected_peer);
            let task = tokio::spawn(driver.run());
            for (request, expected) in [
                (pool::Request::Init, pool::Reply::Init),
                (
                    pool::Request::Unfold(vec![vec![1, 2, 3]]),
                    pool::Reply::Unfold(vec![vec![4, 5, 6]]),
                ),
                (
                    pool::Request::Get(vec![vec![9; 32]]),
                    pool::Reply::Transactions(vec![vec![7; 200_000]]),
                ),
                (pool::Request::Finish(true), pool::Reply::Finished(false)),
            ] {
                let message = request.clone().into_message(Limits::default()).unwrap();
                let received = tokio::time::timeout(
                    Duration::from_secs(10),
                    client.request(&message, None).unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
                let Reply::Response(received) = received else {
                    panic!("expected response")
                };
                assert_eq!(&received.generation, client.generation());
                assert_eq!(request.response(received.message()).unwrap(), expected);
            }
            let close = Message::new(Kind::Close, 1, BTreeMap::new(), Limits::default()).unwrap();
            assert!(matches!(
                client.send(&close).unwrap().await,
                Ok(Reply::Sent)
            ));
            client.shutdown();
            let exit = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&exit.generation, client.generation());
            assert!(matches!(
                exit.reason,
                runtime::Error::Shutdown | runtime::Error::Io(std::io::ErrorKind::UnexpectedEof)
            ));
        });
}
