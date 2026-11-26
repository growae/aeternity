use super::*;
use ae_codec::headers::{Header, HeaderMode};
use serde_json::{Value, json};

fn hash(n: u8) -> Hash256 {
    let mut value = [0; 32];
    value[31] = n;
    Hash256(value)
}
fn encode(header: &KeyHeader) -> Value {
    json!({
        "prev": hex::encode_upper(header.common.previous_hash),
        "header": hex::encode_upper(Header::Key(header.clone()).encode().unwrap())
    })
}
struct Providers {
    row: Value,
    selected: Hash256,
    template: KeyHeader,
    calls: Vec<Value>,
}
impl Providers {
    fn call(&mut self, name: &str) {
        self.calls.push(json!({"method":name,"args":[]}));
    }
}
impl Host for Providers {
    type Error = String;
    fn top(&mut self) -> Result<Option<Hash256>, Self::Error> {
        self.call("top_block_hash");
        Ok((self.row["top"] == "present").then_some(self.selected))
    }
    fn beneficiary(&mut self) -> Result<AccountPublicKey, Self::Error> {
        if self.row["mode"] == "stratum" {
            return Ok(AccountPublicKey(hash(6).0));
        }
        self.call("top_header");
        if self.row["family"] == "hyperchain" {
            return Err("not_applicable".into());
        }
        self.call("beneficiary_config");
        match self.row["fault"].as_str().unwrap() {
            "missing_env" => Err("beneficiary_not_configured".into()),
            "invalid_prefix" => Err("{beneficiary_error,invalid_prefix}".into()),
            "invalid_encoding" => Err("{beneficiary_error,invalid_encoding}".into()),
            _ => Ok(AccountPublicKey(hash(7).0)),
        }
    }
    fn candidate_key(&mut self) -> Result<AccountPublicKey, Self::Error> {
        self.call("aec_keys");
        if self.row["fault"] == "key_unavailable" {
            Err("key_not_ready".into())
        } else {
            Ok(AccountPublicKey(hash(8).0))
        }
    }
    fn create(
        &mut self,
        parent: Hash256,
        beneficiary: AccountPublicKey,
        miner: AccountPublicKey,
    ) -> Result<Option<KeyHeader>, Self::Error> {
        self.calls.push(json!({"method":"create","args":[
            hex::encode_upper(parent.0),hex::encode_upper(beneficiary.0),hex::encode_upper(miner.0)
        ]}));
        if self.row["fault"] == "create_error" {
            return Ok(None);
        }
        // The native capture uses this same controlled constructor. Actual
        // historical state calculation has separate preparation fixtures.
        let mut header = self.template.clone();
        header.common.previous_hash = parent.0;
        Ok(Some(header))
    }
}
fn fixture() -> (Value, KeyHeader) {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_pending_key_cache.json"
    ))
    .unwrap();
    let Header::Key(template) = Header::decode(
        &hex::decode(fixture["template"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap() else {
        panic!("native key template");
    };
    (fixture, template)
}

#[test]
fn native_pending_cache_and_provider_order_match_576_original_callback_outcomes() {
    let (fixture, template) = fixture();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 576);
    for row in rows {
        let mut cache = Cache::default();
        if row["cache"] != "absent" {
            let mut header = template.clone();
            header.common.previous_hash = hash(if row["cache"] == "matching" { 1 } else { 2 }).0;
            cache.candidate = Some(Arc::new(header));
        }
        let before = cache.candidate.clone();
        let mut host = Providers {
            row: row.clone(),
            selected: hash(1),
            template: template.clone(),
            calls: vec![],
        };
        let result = cache.get(&mut host, row["beneficiary"].as_bool().unwrap());
        let mut actual = match result {
            Ok(header) => json!({"reply":{"ok":encode(&header)}}),
            Err(Error::NotFound) => json!({"reply":{"error":"not_found"}}),
            Err(Error::BeneficiaryNotConfigured) => {
                json!({"reply":{"error":"beneficiary_not_configured"}})
            }
            Err(Error::Host(reason)) => json!({"exception":{
                "class":"error","reason":format!("{{badmatch,{{error,{reason}}}}}")
            }}),
        };
        actual["cached"] = cache.cached().map_or(Value::Null, |h| encode(h));
        if actual.get("reply").is_some() {
            actual["unchanged_state"] = json!(match (&before, cache.cached()) {
                (None, None) => true,
                (Some(old), Some(new)) => Arc::ptr_eq(old, new),
                _ => false,
            });
        }
        assert_eq!(actual, row["result"], "{row}");
        assert_eq!(
            host.calls,
            row["calls"].as_array().unwrap().as_slice(),
            "{row}"
        );
    }
}

#[test]
fn failed_rebuild_preserves_the_cache_for_return_to_its_parent() {
    let (_, template) = fixture();
    let mut host = Providers {
        row: json!({"top":"present","family":"cuckoo","mode":"local_pow","fault":"configured"}),
        selected: hash(1),
        template,
        calls: vec![],
    };
    let mut cache = Cache::default();
    let first = cache.get(&mut host, true).unwrap();
    host.selected = hash(2);
    for fault in ["create_error", "key_unavailable", "missing_env"] {
        host.row["fault"] = json!(fault);
        assert!(cache.get(&mut host, true).is_err());
        assert!(Arc::ptr_eq(cache.cached().unwrap(), &first));
    }
    host.row["top"] = json!("missing");
    host.calls.clear();
    assert_eq!(cache.get(&mut host, false), Err(Error::NotFound));
    assert_eq!(
        host.calls,
        vec![json!({"method":"top_block_hash","args":[]})]
    );
    assert!(Arc::ptr_eq(cache.cached().unwrap(), &first));
    host.selected = hash(1);
    host.row["top"] = json!("present");
    host.calls.clear();
    let reused = cache.get(&mut host, false).unwrap();
    assert!(Arc::ptr_eq(&first, &reused));
    assert_eq!(
        host.calls,
        vec![json!({"method":"top_block_hash","args":[]})]
    );
}
