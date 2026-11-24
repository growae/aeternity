use super::Error;
use jsonschema::{Keyword, PatternOptions, ValidationError, Validator};
use serde_json::Value;
use std::{collections::BTreeMap, sync::OnceLock};

pub(super) fn original() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(include_str!("schema.json")).expect("bundled native schema")
    })
}
struct Integer;
impl Keyword for Integer {
    fn is_valid(&self, value: &Value) -> bool {
        value
            .as_number()
            .is_some_and(|n| !n.as_str().contains(['.', 'e', 'E']))
    }
    fn validate<'i>(&self, value: &'i Value) -> Result<(), ValidationError<'i>> {
        if self.is_valid(value) {
            Ok(())
        } else {
            Err(ValidationError::custom("expected native integer"))
        }
    }
}
fn adapt(node: &mut Value) {
    if let Some(m) = node.as_object_mut() {
        if m.get("type").and_then(Value::as_str) == Some("integer") {
            m.insert("nativeInteger".into(), Value::Bool(true));
        }
        if let Some(Value::String(pattern)) = m.get_mut("pattern") {
            *pattern = pcre(pattern);
        }
        if let Some(Value::Object(patterns)) = m.get_mut("patternProperties") {
            *patterns = std::mem::take(patterns)
                .into_iter()
                .map(|(p, v)| (pcre(&p), v))
                .collect();
        }
        for value in m.values_mut() {
            adapt(value);
        }
    } else if let Some(a) = node.as_array_mut() {
        for value in a {
            adapt(value);
        }
    }
}
// The pinned schema uses only these PCRE extensions. Rust regex is linear;
// no user-supplied patterns or external schema references are accepted.
fn pcre(pattern: &str) -> String {
    let p = pattern.replace(
        r"\h",
        r"[\t \u{00a0}\u{1680}\u{180e}\u{2000}-\u{200a}\u{202f}\u{205f}\u{3000}]",
    );
    match p.strip_suffix('$') {
        Some(prefix) => format!("{prefix}(?:\\n)?\\z"),
        None => p,
    }
}
pub(super) fn validate(value: &Value) -> Result<(), Error> {
    static VALIDATOR: OnceLock<Validator> = OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let mut schema = original().clone();
        adapt(&mut schema);
        jsonschema::draft4::options()
            .with_pattern_options(PatternOptions::regex().size_limit(1 << 20))
            .with_keyword("nativeInteger", |_, _, _| Ok(Box::new(Integer)))
            .build(&schema)
            .expect("bundled native schema compiles without external retrieval")
    });
    validator.validate(value).map_err(|e| Error::Schema {
        path: e.instance_path().to_string(),
    })
}
pub(super) fn names(
    node: &Value,
    prefix: &str,
    path: &[String],
    out: &mut BTreeMap<String, (Vec<String>, String)>,
) {
    if let Some(props) = node.get("properties").and_then(Value::as_object) {
        for (key, value) in props {
            let name = format!("{prefix}__{}", key.to_uppercase()).replace('-', "_");
            let mut path = path.to_vec();
            path.push(key.clone());
            out.insert(
                name.clone(),
                (
                    path.clone(),
                    value
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                ),
            );
            names(value, &name, &path, out);
        }
    }
}
