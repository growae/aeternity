use ae_execution::oracles::abi::{self, Error, Limits};
use ae_protocol::Protocol;
use ae_types::Unsigned;
use serde_json::Value as Json;
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn verify(result: Result<(), Error>, row: &Json) {
    let expected = &row["result"];
    match result {
        Ok(()) => assert_eq!(expected, "ok", "{row}"),
        Err(Error::StoredAevmFormat(_)) => {
            assert_eq!(expected["exception"], "error", "{row}");
            assert_eq!(expected["reason"], "badmatch", "{row}");
        }
        Err(Error::StoredFateFormat(_)) => assert_eq!(expected["exception"], "error", "{row}"),
        Err(Error::UnknownStoredAbi) => {
            assert_eq!(expected["exception"], "error", "{row}");
            assert_eq!(expected["reason"], "case_clause", "{row}");
        }
        Err(error) => {
            let reason = match error {
                Error::BadAbiVersion => "bad_abi_version",
                Error::BadQueryFormat => "bad_query_format",
                Error::BadResponseFormat => "bad_response_format",
                Error::BadFormat => "bad_format",
                _ => panic!("unexpected {error:?}: {row}"),
            };
            assert_eq!(expected, reason, "{row}");
        }
    }
}
#[test]
fn historical_oracle_registration_and_content_rules_match_original_primitives() {
    let fixture: Json =
        serde_json::from_str(include_str!("fixtures/erlang_oracle_abi.json")).unwrap();
    for row in fixture["registration"].as_array().unwrap() {
        let abi = Unsigned::from_decimal(row["abi"].as_str().unwrap()).unwrap();
        let protocol = (row["protocol"].as_u64().unwrap() as u32)
            .try_into()
            .unwrap();
        verify(
            abi::validate_registration(
                &abi,
                &bytes(&row["query_format"]),
                &bytes(&row["response_format"]),
                protocol,
                Limits::default(),
            ),
            row,
        );
    }
    for row in fixture["content"].as_array().unwrap() {
        let abi = Unsigned::from_decimal(row["abi"].as_str().unwrap()).unwrap();
        let protocol = (row["protocol"].as_u64().unwrap() as u32)
            .try_into()
            .unwrap();
        verify(
            abi::validate_content(
                &abi,
                &bytes(&row["format"]),
                &bytes(&row["content"]),
                protocol,
                Limits::default(),
            ),
            row,
        );
    }
}
#[test]
fn abi_resource_exhaustion_is_not_format_rejection() {
    let mut limits = Limits::default();
    limits.aevm.max_nodes = 0;
    limits.fate.max_nodes = 0;
    let word = ae_vm_aevm::heap::encode(
        &ae_vm_aevm::heap::Value::Type(ae_vm_aevm::heap::Type::Word),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        abi::validate_registration(&1.into(), &word, &word, Protocol::Minerva, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::validate_registration(&3.into(), &[7], &[7], Protocol::Lima, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::validate_content(&1.into(), &word, &[0; 32], Protocol::Ceres, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::validate_content(&3.into(), &[7], &[0], Protocol::Ceres, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::validate_registration(&1.into(), b"ignored", b"ignored", Protocol::Roma, limits),
        Ok(())
    );
    assert_eq!(
        abi::validate_registration(&0.into(), b"ignored", b"ignored", Protocol::Ceres, limits),
        Ok(())
    );
    assert_eq!(
        abi::validate_content(&0.into(), b"ignored", b"ignored", Protocol::Ceres, limits),
        Ok(())
    );
    let mut content_limits = Limits::default();
    content_limits.fate.max_bytes = 1;
    assert_eq!(
        abi::validate_content(&3.into(), &[7], &[0, 0], Protocol::Ceres, content_limits),
        Err(Error::ResourceLimit)
    );
}
