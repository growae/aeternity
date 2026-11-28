use super::*;
use ae_codec::headers::{Header, HeaderMode};
use query::blocks::{ClientHeader, Kind};

#[test]
fn original_pending_dispatcher_headers_and_failure_codes_match() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_blocks.json"
    ))
    .unwrap();
    let Header::Key(mut key) = Header::decode(
        &hex::decode(corpus["headers"]["key"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap() else {
        panic!()
    };
    // set_key_seal(no_value) changes the proof bytes, retaining the nonce in
    // canonical hashing even though both fields are omitted from client JSON.
    key.seal = [0; 42];
    let mut checked = 0;
    for row in corpus["rows"].as_array().unwrap() {
        let input = &row["input"];
        if input["operation"] != "GetPendingKeyBlock" {
            continue;
        }
        let mut key = key.clone();
        let response = match input["scenario"].as_str().unwrap() {
            "beneficiary" => failure(&service::Error::BeneficiaryNotConfigured),
            "not_mining" => missing("not_mining"),
            "missing" => failure(&service::Error::NotFound),
            "missing_prev" => body(None).unwrap_err(),
            "storage" => Response::internal(),
            "overload" => failure(&service::Error::Busy),
            "unstable" => failure(&service::Error::Closed),
            scenario => {
                if scenario == "genesis" {
                    key.common.height = 0;
                }
                let value = body(Some(ClientHeader {
                    seal_present: None,
                    header: Header::Key(key),
                    previous: if input["previous_type"] == "key" {
                        Kind::Key
                    } else {
                        Kind::Micro
                    },
                }))
                .unwrap();
                Response::json(200, value)
            }
        };
        assert_eq!(
            response.status,
            row["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{row}"
        );
        checked += 1;
    }
    assert_eq!(checked, 15);
    assert_eq!(
        failure(&service::Error::BeneficiaryNotApplicable).status,
        500
    );
    assert_eq!(
        failure(&service::Error::Preparation("Time".into())).status,
        500
    );
}
