use super::*;
use crate::http::{
    response::{self, Operation},
    transport,
};
use std::cell::Cell;
struct Native<'a> {
    row: &'a Value,
    reads: [Cell<u64>; 3],
}
impl Source for Native<'_> {
    async fn progress(&self) -> Result<Progress, sync::Error> {
        self.reads[0].set(self.reads[0].get() + 1);
        if self.row["failure"] == "sync_failure" {
            return Err(sync::Error::Closed);
        }
        Ok(Progress {
            syncing: self.row["active"].as_bool().unwrap(),
            percent: self.row["progress"].as_f64().unwrap(),
            target: self.row["target"].as_u64().unwrap(),
            task: Some(7),
        })
    }
    fn estimate(&self, task: u64) -> Result<Option<Value>, sync::Error> {
        assert_eq!(task, 7);
        self.reads[1].set(self.reads[1].get() + 1);
        if self.row["failure"] == "stats_failure" {
            return Err(sync::Error::Closed);
        }
        Ok(self.row["stats"].as_array().map(|v| v[1].clone()))
    }
    async fn height(&self) -> Result<u64, sync::Error> {
        self.reads[2].set(self.reads[2].get() + 1);
        if self.row["failure"] == "chain_failure" {
            return Err(sync::Error::Closed);
        }
        Ok(self.row["top"].as_u64().unwrap())
    }
}
#[tokio::test]
async fn original_sync_status_dispatch_and_dependency_order_match_both_versions() {
    let native: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_sync_status.json"
    ))
    .unwrap();
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 35);
    for row in rows {
        let source = Native {
            row,
            reads: Default::default(),
        };
        let result = match row["failure"].as_str().unwrap() {
            "overload" => Err(Response::unavailable(true)),
            "unstable" => Err(Response::unavailable(false)),
            _ => body(&source).await,
        };
        let response = result.map_or_else(|r| r, |b| Response::json(200, b));
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
        let mut headers = response.headers;
        headers.remove("content-type");
        assert_eq!(json!(headers), row["headers"]);
        assert_eq!(
            source.reads.map(|c| c.get()),
            [
                row["sync_reads"].as_u64().unwrap(),
                row["stats_reads"].as_u64().unwrap(),
                row["chain_reads"].as_u64().unwrap()
            ]
        );
        for v3 in [false, true] {
            assert_eq!(
                response::route(
                    if v3 {
                        "/v3/sync-status"
                    } else {
                        "/v2/sync-status"
                    },
                    &Config::default()
                ),
                Some((v3, Operation::SyncStatus { v3 }))
            );
        }
    }
}
#[test]
fn original_sync_status_response_validation_and_stream_failures_match() {
    let dispatch: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_sync_status.json"
    ))
    .unwrap();
    let native: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_sync_validation.json"
    ))
    .unwrap();
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 126);
    for row in rows {
        let result = if let Some(input) = dispatch["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == row["id"])
        {
            if input["code"] == 200 {
                Ok(input["body"].clone())
            } else {
                let mut response = Response::json(
                    input["code"].as_u64().unwrap() as u16,
                    input["body"].clone(),
                );
                for (k, v) in input["headers"].as_object().unwrap() {
                    response
                        .headers
                        .insert(k.clone(), v.as_str().unwrap().into());
                }
                Err(response)
            }
        } else {
            let estimate: Value = serde_json::from_str(row["id"].as_str().unwrap()).unwrap();
            Ok(json!({"estimate":estimate,"target":1,"progress":1.0,"speed":1.0}))
        };
        let v3 = row["module"] == "oas_endpoints";
        let mut actual = transport::dispatch(
            Ok((
                Operation::SyncStatus { v3 },
                row["strings"].as_bool().unwrap(),
            )),
            result,
        );
        let expected = &row["result"]["response"];
        assert_eq!(
            actual.status,
            expected["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        if row["result"]["kind"] == "exception" {
            assert_eq!(row["result"]["class"], "throw");
            assert_eq!(expected["body"], "");
            actual.stamp(
                &Config::default(),
                &[("origin".into(), "https://client.example".into())].into(),
                Some(99),
            );
            assert!(actual.body.is_empty());
            assert!(actual.headers.is_empty());
            assert_eq!(expected["headers"], json!({"content-length":"0"}));
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&actual.body).unwrap(),
                expected["body"],
                "{row}"
            );
            actual.headers.remove("content-type");
            assert_eq!(json!(actual.headers), expected["headers"], "{row}");
        }
    }
}

#[tokio::test]
async fn sync_status_keeps_arbitrary_size_estimates_and_rejects_non_finite_results() {
    for (speed, success) in [(1e-100, true), (1e-320, false)] {
        let row = json!({"active":true,"progress":0.0,"target":u64::MAX,"top":0,"failure":"none","stats":["speed",speed]});
        let source = Native {
            row: &row,
            reads: Default::default(),
        };
        let result = body(&source).await;
        assert_eq!(result.is_ok(), success);
        if let Ok(mut value) = result {
            assert!(value["estimate"].to_string().len() > 100);
            assert!(!valid_response(&value, false));
            assert!(!valid_response(&value, true));
            response::integers_as_strings(&mut value);
            assert!(value["estimate"].as_str().unwrap().len() > 100);
        }
    }
}
