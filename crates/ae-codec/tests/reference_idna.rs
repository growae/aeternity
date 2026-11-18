use ae_codec::idna::{self, BadLabel, Error};
use serde_json::Value as Json;

fn category(error: &Error) -> &str {
    match error {
        Error::NoRegistrar => "no_registrar",
        Error::RegistrarUnknown => "registrar_unknown",
        Error::MultipleNamespaces => "multiple_namespaces",
        Error::NoLabelInRegistrar => "no_label_in_registrar",
        Error::InvalidCodepoint(_) => "invalid_codepoint",
        Error::ReferenceException { reason, .. } => reason,
        Error::BadLabel(b) => match b {
            BadLabel::Nfc => "nfc",
            BadLabel::Hyphen => "hyphen",
            BadLabel::InitialCombiner => "initial_combiner",
            BadLabel::Context => "context",
            BadLabel::ContextJ => "contextj",
            BadLabel::ContextO => "contexto",
            BadLabel::Bidi => "bidi",
            BadLabel::Alabel => "alabel",
            BadLabel::TooLong => "too_long",
        },
    }
}
#[test]
fn pinned_idna_acceptance_ascii_bytes_and_failure_categories_match_erlang() {
    let cases: Json = serde_json::from_str(include_str!("fixtures/erlang_idna.json")).unwrap();
    let rows = cases.as_array().unwrap();
    assert_eq!(rows.len(), 27896);
    let (mut accepted, mut failures) = (0, Vec::new());
    for row in rows {
        let input = hex::decode(row["input"].as_str().unwrap()).unwrap();
        let result = idna::to_ascii(&input);
        let matches = match &result {
            Ok(ascii) => {
                accepted += 1;
                Some(hex::encode(ascii).as_str()) == row["ascii"].as_str()
            }
            Err(e) => {
                let class = match e {
                    Error::ReferenceException { class, .. } => Some(*class),
                    _ => None,
                };
                let cp = match e {
                    Error::InvalidCodepoint(cp) => Some(u64::from(*cp)),
                    _ => None,
                };
                Some(category(e)) == row["error"].as_str()
                    && class == row["exception"].as_str()
                    && cp == row["codepoint"].as_u64()
            }
        };
        if !matches && failures.len() < 20 {
            failures.push(format!(
                "{} => {result:?}; expected {row}",
                String::from_utf8_lossy(&input)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert!(accepted > 5000);
}
