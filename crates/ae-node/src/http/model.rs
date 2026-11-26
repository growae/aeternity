// The reachable v2/v3 response schemas come from the original aehttp YAML,
// aeternity fa2a6d7. Copyright (c) Aeternity developers. ISC: LICENSE.
use jsonschema::{Keyword, PatternOptions, ValidationError, Validator};
use num_bigint::BigInt;
use serde_json::Value;
use std::{collections::BTreeMap, sync::OnceLock};

struct Integer {
    minimum: Option<BigInt>,
    maximum: Option<BigInt>,
}
impl Keyword for Integer {
    fn is_valid(&self, value: &Value) -> bool {
        value
            .as_number()
            .and_then(|n| n.as_str().parse::<BigInt>().ok())
            .is_some_and(|n| {
                self.minimum.as_ref().is_none_or(|m| &n >= m)
                    && self.maximum.as_ref().is_none_or(|m| &n <= m)
            })
    }
    fn validate<'i>(&self, value: &'i Value) -> Result<(), ValidationError<'i>> {
        if self.is_valid(value) {
            Ok(())
        } else {
            Err(ValidationError::custom("native integer range"))
        }
    }
}
fn adapt(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            if fields.get("type").and_then(Value::as_str) == Some("integer") {
                fields.remove("type");
                let mut bounds = serde_json::Map::new();
                for key in ["minimum", "maximum"] {
                    if let Some(bound) = fields.remove(key) {
                        bounds.insert(key.into(), bound);
                    }
                }
                fields.insert("nativeInteger".into(), Value::Object(bounds));
            }
            for child in fields.values_mut() {
                adapt(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                adapt(child);
            }
        }
        _ => (),
    }
}
fn validators(v3: bool) -> &'static BTreeMap<String, Validator> {
    static V2: OnceLock<BTreeMap<String, Validator>> = OnceLock::new();
    static V3: OnceLock<BTreeMap<String, Validator>> = OnceLock::new();
    (if v3 { &V3 } else { &V2 }).get_or_init(|| {
        let mut document: Value = serde_json::from_str(if v3 {
            include_str!("v3-models.json")
        } else {
            include_str!("v2-models.json")
        })
        .expect("bundled HTTP models");
        adapt(&mut document);
        document["operations"]
            .as_object()
            .expect("operations")
            .iter()
            .map(|(name, schema)| {
                let mut root = schema.clone();
                root["definitions"] = document["definitions"].clone();
                let validator = jsonschema::draft4::options()
                    .with_pattern_options(PatternOptions::regex().size_limit(1 << 20))
                    .with_keyword("nativeInteger", |_, bounds, _| {
                        let get = |key| {
                            bounds.get(key).map(|n| {
                                n.to_string()
                                    .parse::<BigInt>()
                                    .expect("integer schema bound")
                            })
                        };
                        Ok(Box::new(Integer {
                            minimum: get("minimum"),
                            maximum: get("maximum"),
                        }))
                    })
                    .build(&root)
                    .expect("local HTTP schema compiles");
                (name.clone(), validator)
            })
            .collect()
    })
}
pub(super) fn valid(operation: &str, body: &Value, v3: bool) -> bool {
    validators(v3)
        .get(operation)
        .is_some_and(|v| v.is_valid(body))
}
