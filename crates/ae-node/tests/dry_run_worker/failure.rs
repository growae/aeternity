use super::*;
use ae_execution::dry_run as core;
use std::sync::Arc;

fn unpack(value: &J, pool: &[J]) -> J {
    match value {
        J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
            unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
        }
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), unpack(v, pool)))
                .collect(),
        ),
        J::Array(a) => J::Array(a.iter().map(|v| unpack(v, pool)).collect()),
        v => v.clone(),
    }
}
fn view() -> Arc<View> {
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "bad_call")
        .unwrap();
    Backend::new(row).view(hash(&row["seed"]["hash"])).unwrap()
}
async fn run(
    client: &admission::Client,
    view: Arc<View>,
    calls: &[J],
    funding: Vec<core::Funding>,
    diagnostic_bytes: usize,
) -> worker::Failure {
    let original = view.trees.roots();
    let t = configured(&FIXTURE["profile"], |base| {
        let mut params = parameters(base, None);
        params.rendering.diagnostic.max_bytes = diagnostic_bytes;
        worker::submit(
            client,
            process(),
            view.clone(),
            worker::Request {
                inputs: calls
                    .iter()
                    .map(core::prepare::Input::CallRequest)
                    .collect(),
                funding,
                options: Default::default(),
            },
            params,
            Timeout::After(Duration::from_secs(10)),
        )
    })
    .unwrap();
    let done = t.wait_retained().await.unwrap();
    let worker::Reply::Failed(f) =
        worker::decode_reply(done.value.result.as_ref().unwrap()).unwrap()
    else {
        panic!()
    };
    assert_eq!(view.trees.roots(), original);
    assert_eq!(client.available().0, 0);
    drop(done);
    assert_eq!(client.available().0, 1);
    f
}
#[tokio::test]
async fn malformed_call_transport_matches_native_rendering_and_preserves_failure_phase() {
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let data: J =
        serde_json::from_str(include_str!("../fixtures/erlang_dry_run_failures.json")).unwrap();
    let data = unpack(&data["root"], data["values"].as_array().unwrap());
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2084);
    let view = view();
    for index in (0..20).chain([507, 1000, 2050]).chain(2070..2084) {
        let row = &rows[index];
        let call =
            serde_json::from_slice(&hex::decode(row["raw"].as_str().unwrap()).unwrap()).unwrap();
        let failure = run(&client, view.clone(), &[call], vec![], 1 << 20).await;
        assert_eq!(failure.stage, worker::Stage::Execution);
        assert_eq!(failure.kind, worker::Kind::BadCallRequest);
        assert_eq!(failure.index, Some(0));
        assert_eq!(
            dry_run::failure::body(dry_run::failure::worker(&failure).unwrap(), 1 << 20).unwrap(),
            (400, row["body"].clone())
        );
    }
    let valid = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "call_success" && r["input"]["protocol"] == 6)
        .unwrap()["input"]["body"]["txs"][0]["call_req"]
        .clone();
    let bad: J =
        serde_json::from_slice(&hex::decode(rows[0]["raw"].as_str().unwrap()).unwrap()).unwrap();
    let failure = run(
        &client,
        view.clone(),
        &[valid, bad.clone()],
        vec![],
        1 << 20,
    )
    .await;
    assert_eq!(failure.kind, worker::Kind::BadCallRequest);
    assert_eq!(failure.index, Some(1));
    // The v3 native pipeline reaches negative funding before malformed calls.
    // Its implementation-specific Erlang stack is retained in the fixture;
    // this check covers phase/data, not a fabricated byte-identical stack.
    let mut key = [0; 32];
    key[31] = 9;
    let failure = run(
        &client,
        view.clone(),
        std::slice::from_ref(&bad),
        vec![core::Funding {
            account: ae_types::AccountPublicKey(key),
            amount: (-1).into(),
        }],
        1 << 20,
    )
    .await;
    assert_eq!(failure.kind, worker::Kind::NegativeBalance);
    assert_eq!(failure.index, Some(0));
    assert_eq!(failure.account, Some(ae_types::AccountPublicKey(key)));
    assert_eq!(failure.balance, Some((-1).into()));
    assert!(dry_run::failure::worker(&failure).is_none());
    let failure = run(&client, view, &[bad], vec![], 0).await;
    assert_eq!(failure.kind, worker::Kind::Capacity);
    assert_eq!(failure.stage, worker::Stage::Rendering);
    assert!(failure.native_reason.is_none());
    owner.shutdown().await;
}
