use super::*;
use ae_state::channels::{Nonce, channel_id};
use ae_tx::{
    CanonicalTransaction, ChannelTransaction, SignedTransaction, Transaction, TransactionLeaf,
    channels::{CreateDelegates, Operation},
};
use ae_types::{Id, IdTag};
fn responder() -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&[44; 32])
}
fn signed(
    open: &configured::Open,
    nonce: u64,
    operation: Operation,
) -> Arc<SignedTransaction<Transaction>> {
    let tx = ChannelTransaction::new(
        operation,
        nonce.into(),
        100_000_000_000_000u64.into(),
        0.into(),
        Default::default(),
    )
    .unwrap();
    let mut bytes = open.chain().settings().profile().signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Channel(Box::new(tx))),
        vec![payer().sign(&bytes), responder().sign(&bytes)],
    ))
}
async fn admit(h: &node::Handle, tx: Arc<SignedTransaction<Transaction>>) {
    h.pow()
        .unwrap()
        .workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx, ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
}
#[tokio::test]
async fn channel_http_tracks_cosigned_create_deposit_withdraw_close_replacement_pressure_and_rollback()
 {
    let lab = Lab::new();
    let initiator = payer().public_key();
    let responder = responder().public_key();
    lab.write("alloc.json",serde_json::to_vec(&json!({api::encode(ApiType::AccountPubkey,initiator.as_ref()):1_000_000_000_000_000_000u64,api::encode(ApiType::AccountPubkey,responder.as_ref()):1_000_000_000_000_000_000u64})).unwrap());
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let channel = channel_id(initiator, Nonce::Number(&1.into()), responder).0;
    let key = api::encode(ApiType::Channel, &channel);
    let i = api::encode(ApiType::AccountPubkey, initiator.as_ref());
    let r = api::encode(ApiType::AccountPubkey, responder.as_ref());
    let create = signed(
        prepared.configured(),
        1,
        Operation::Create {
            initiator,
            initiator_amount: 1_000_000_000_000_000u64.into(),
            responder,
            responder_amount: 2_000_000_000_000_000u64.into(),
            reserve: 10.into(),
            lock_period: 20.into(),
            delegates: CreateDelegates::Separate {
                initiator: vec![initiator],
                responder: vec![responder],
            },
            state_hash: [1; 32],
        },
    );
    let deposit = signed(
        prepared.configured(),
        2,
        Operation::Deposit {
            channel,
            from: initiator,
            amount: 7.into(),
            state_hash: [2; 32],
            round: 2.into(),
        },
    );
    let withdraw = signed(
        prepared.configured(),
        3,
        Operation::Withdraw {
            channel,
            to: initiator,
            amount: 3.into(),
            state_hash: [3; 32],
            round: 3.into(),
        },
    );
    let close = signed(
        prepared.configured(),
        4,
        Operation::CloseMutual {
            channel,
            from: Id::new(IdTag::Account, initiator.0),
            initiator_amount: 1_000_000_000_000_000u64.into(),
            responder_amount: 1_900_000_000_000_004u64.into(),
        },
    );
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups = ["channel".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let path = format!("/v3/channels/{key}");
    assert_eq!(get(address, &path).await.0, 404);
    info::mine(&h, 1, Kind::Key).await;
    admit(&h, create.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    assert!(
        db.view()
            .unwrap()
            .location(create.hash())
            .unwrap()
            .is_some()
    );
    let initial = get(address, &path).await;
    assert_eq!(initial.0, 200, "{}", initial.2);
    assert_eq!(
        initial.2,
        json!({"id":key,"initiator_id":i,"responder_id":r,"channel_amount":3_000_000_000_000_000u64,"initiator_amount":1_000_000_000_000_000u64,"responder_amount":2_000_000_000_000_000u64,"channel_reserve":10,"delegate_ids":{"initiator":[i],"responder":[r]},"state_hash":api::encode(ApiType::State,&[1;32]),"round":1,"solo_round":0,"lock_period":20,"locked_until":0})
    );
    assert_eq!(
        initial.1["access-control-allow-origin"],
        "https://client.example"
    );
    assert_eq!(get(address, &path.replace("v3", "v2")).await.2, initial.2);
    admit(&h, deposit.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    let deposit_top = {
        let view = db.view().unwrap();
        assert!(view.location(deposit.hash()).unwrap().is_some());
        view.status().tip.unwrap()
    };
    let deposited = get(address, &path).await;
    assert_eq!(deposited.0, 200, "{}", deposited.2);
    assert_eq!(deposited.2["channel_amount"], 3_000_000_000_000_007u64);
    assert_eq!(deposited.2["round"], 2);
    assert_eq!(
        deposited.2["state_hash"],
        api::encode(ApiType::State, &[2; 32])
    );
    admit(&h, withdraw.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    assert!(
        db.view()
            .unwrap()
            .location(withdraw.hash())
            .unwrap()
            .is_some()
    );
    let withdrawn = get(address, &path).await;
    assert_eq!(withdrawn.0, 200, "{}", withdrawn.2);
    assert_eq!(withdrawn.2["channel_amount"], 3_000_000_000_000_004u64);
    assert_eq!(withdrawn.2["round"], 3);
    assert_eq!(
        withdrawn.2["state_hash"],
        api::encode(ApiType::State, &[3; 32])
    );
    let strings = get(address, &format!("{path}?int-as-string=true")).await;
    assert_eq!(strings.0, 200, "{}", strings.2);
    assert_eq!(strings.2["channel_amount"], "3000000000000004");
    assert_eq!(strings.2["round"], "3");
    assert_eq!(strings.2["delegate_ids"], withdrawn.2["delegate_ids"]);
    assert_eq!(
        get(
            address,
            &format!("{}?int-as-string=true", path.replace("v3", "v2"))
        )
        .await
        .2,
        withdrawn.2
    );
    let workers = h.pow().unwrap().workers().unwrap();
    let generation = workers.status().admission_generation;
    let command_generation = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || h.commands().is_none_or(|c| c.number <= command_generation) {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(get(address, &path).await.2, withdrawn.2);
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let permit = importer.channel_query(48 << 20).unwrap();
    assert!(importer.available().0 < capacity.0);
    drop(permit);
    assert_eq!(importer.available(), capacity);
    let held = importer
        .channel_query(48 << 20)
        .unwrap()
        .submit(channel, |r| r)
        .unwrap()
        .wait_retained()
        .await
        .unwrap();
    assert_eq!(held.value.unwrap().round, 3.into());
    assert!(importer.available().0 < capacity.0);
    drop(held.credits);
    assert_eq!(importer.available(), capacity);
    let mut held = vec![];
    while let Ok(ticket) = importer.current() {
        held.push(ticket);
        assert!(held.len() < 64);
    }
    let busy = get(address, "/v3/channels/bad").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    let invalid = request(
        address,
        "GET /v3/channels/bad?ignored=%GG HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(invalid.0, 400);
    assert!(invalid.2.is_empty());
    assert!(!invalid.1.contains_key("x-ae-height"));
    drop(held);
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(get(address, "/v3/channels/bad").await.0, 400);
    assert_eq!(
        get(
            address,
            &format!("/v3/channels/{}", api::encode(ApiType::Channel, &[1; 31]))
        )
        .await
        .0,
        400
    );
    assert_eq!(
        get(
            address,
            &format!("/v3/channels/{}", api::encode(ApiType::Channel, &[1; 32]))
        )
        .await
        .0,
        404
    );
    admit(&h, close.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    assert!(db.view().unwrap().location(close.hash()).unwrap().is_some());
    assert_eq!(get(address, &path).await.0, 404);
    db.rollback(
        ae_chain::repository::rollback::Target::Micro(deposit_top),
        Default::default(),
    )
    .unwrap();
    assert_eq!(get(address, &path).await.2, deposited.2);
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    assert_eq!(get(address, &path).await.0, 404);
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(http.status().joined);
}
