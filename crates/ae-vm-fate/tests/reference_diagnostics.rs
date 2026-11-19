use ae_vm_fate::{
    Limits,
    diagnostic::{self, Profile, Style, Term},
};
use num_bigint::BigInt;
use serde_json::Value as J;
use std::borrow::Cow;

fn term(v: &J) -> Term<'static> {
    let rows = || v[1].as_array().unwrap();
    match v[0].as_str().unwrap() {
        "integer" => Term::Integer(Cow::Owned(
            v[1].as_str().unwrap().parse::<BigInt>().unwrap(),
        )),
        "atom" => Term::Atom(Cow::Owned(v[1].as_str().unwrap().to_owned())),
        "binary" => Term::Binary(Cow::Owned(hex::decode(v[1].as_str().unwrap()).unwrap())),
        "latin1" => Term::List(
            hex::decode(v[1].as_str().unwrap())
                .unwrap()
                .into_iter()
                .map(|c| Term::Integer(Cow::Owned(c.into())))
                .collect(),
        ),
        "tuple" => Term::Tuple(rows().iter().map(term).collect()),
        "list" => Term::List(rows().iter().map(term).collect()),
        "map" => {
            let pairs: Vec<_> = rows().iter().map(|r| (term(&r[0]), term(&r[1]))).collect();
            Term::Map(pairs)
        }
        tag => panic!("{tag}"),
    }
}
#[test]
fn native_term_printing_and_fate_abort_messages_match_original_profiles() {
    let data: J =
        serde_json::from_str(include_str!("fixtures/erlang_fate_diagnostics.json")).unwrap();
    let data = expand(
        &data["root"],
        data["values"].as_array().unwrap(),
        data["values"].as_array().unwrap().len(),
    );
    assert_eq!(data["profiles"].as_array().unwrap().len(), 2);
    for profile in data["profiles"].as_array().unwrap() {
        assert_eq!(profile["formats"].as_array().unwrap().len(), 10272);
        assert_eq!(profile["cases"].as_array().unwrap().len(), 4521);
        let p = if profile["profile"] == "release" {
            Profile::Release
        } else {
            Profile::Test
        };
        for (i, row) in profile["formats"].as_array().unwrap().iter().enumerate() {
            let style = if row["kind"] == "pretty" {
                Style::Pretty
            } else {
                Style::Write
            };
            let actual = diagnostic::format(
                &term(&row["term"]),
                style,
                row["depth"].as_i64().unwrap() as i32,
                row["column"].as_u64().unwrap() as usize,
                Limits::default(),
            )
            .unwrap()
            .0;
            let expected = hex::decode(row["output"].as_str().unwrap()).unwrap();
            assert_eq!(actual, expected, "{p:?} format {i} {row}");
        }
        for (i, row) in profile["cases"].as_array().unwrap().iter().enumerate() {
            let actual = diagnostic::native_abort(&term(&row["reason"]), p, Limits::default())
                .unwrap()
                .0;
            let expected = hex::decode(row["result"]["message"].as_str().unwrap()).unwrap();
            assert_eq!(actual, expected, "{p:?} abort {i} {row}");
        }
    }
}

fn expand(v: &J, pool: &[J], ceiling: usize) -> J {
    match v {
        J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
            let i = m["$ref"].as_u64().unwrap() as usize;
            assert!(i < ceiling);
            expand(&pool[i], pool, i)
        }
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, pool, ceiling)))
                .collect(),
        ),
        J::Array(v) => J::Array(v.iter().map(|v| expand(v, pool, ceiling)).collect()),
        _ => v.clone(),
    }
}

#[test]
fn diagnostic_work_limits_are_retryable_and_runtime_map_order_is_validated() {
    use ae_vm_fate::{
        Error,
        machine::Reason,
        maps::{MapOrder, Traversal},
        value::Value,
    };
    assert!(matches!(
        diagnostic::format(
            &Term::List(vec![
                Term::Integer(Cow::Owned(1.into())),
                Term::Integer(Cow::Owned(2.into()))
            ]),
            Style::Pretty,
            6,
            usize::MAX,
            Limits::default()
        ),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(
        diagnostic::format(
            &Term::Binary(Cow::Borrowed(&[0, 1])),
            Style::Pretty,
            i32::MAX,
            1,
            Limits::default()
        )
        .unwrap()
        .0,
        b"<<0,1>>"
    );
    let reason = Term::Tuple(vec![
        Term::Atom("type_error".into()),
        Term::Atom("MAP_LOOKUP".into()),
        Term::List(vec![
            Term::Map(vec![(
                Term::Integer(Cow::Owned(1.into())),
                Term::Binary(Cow::Borrowed(&[42; 100])),
            )]),
            Term::Atom("false".into()),
        ]),
    ]);
    for profile in [Profile::Release, Profile::Test] {
        let expected = diagnostic::native_abort(&reason, profile, Limits::default()).unwrap();
        for bytes in [false, true] {
            let mut succeeded = false;
            for allowance in 0..10000 {
                let limits = if bytes {
                    Limits {
                        max_bytes: allowance,
                        ..Default::default()
                    }
                } else {
                    Limits {
                        max_nodes: allowance,
                        ..Default::default()
                    }
                };
                match diagnostic::native_abort(&reason, profile, limits) {
                    Ok((out, _)) => {
                        assert_eq!(out, expected.0);
                        succeeded = true;
                        break;
                    }
                    Err(e) => assert_eq!(e, Error::ResourceLimit),
                }
            }
            assert!(succeeded);
        }
        assert!(matches!(
            diagnostic::native_abort(
                &reason,
                profile,
                Limits {
                    max_depth: 0,
                    ..Default::default()
                }
            ),
            Err(Error::ResourceLimit)
        ));
        assert_eq!(
            diagnostic::native_abort(&reason, profile, Limits::default())
                .unwrap()
                .0,
            expected.0
        );
    }
    struct Invalid;
    impl MapOrder for Invalid {
        fn indices(&self, _: &[&Value], _: Traversal, _: Limits) -> Result<Vec<usize>, Error> {
            Ok(vec![0, 0])
        }
    }
    let reason = Reason::Type {
        operation: "add",
        arguments: vec![Value::Map(
            [
                (Value::Integer(1.into()), Value::Boolean(true)),
                (Value::Integer(2.into()), Value::Boolean(false)),
            ]
            .into(),
        )],
    };
    assert!(matches!(
        diagnostic::reason(&reason, &Invalid, Profile::Release, Limits::default()),
        Err(Error::InvalidOrderProfile)
    ));
}

#[test]
fn full_idna_native_error_terms_match_original_normalization() {
    let data: J =
        serde_json::from_str(include_str!("fixtures/erlang_idna_diagnostics.json")).unwrap();
    check_idna(data, [10267, 17621, 8]);
    let edges: J =
        serde_json::from_str(include_str!("fixtures/erlang_idna_diagnostic_edges.json")).unwrap();
    check_idna(edges, [2, 40, 0]);
}
fn check_idna(data: J, expected_counts: [usize; 3]) {
    use ae_codec::idna;
    let pool = data["values"].as_array().unwrap();
    let data = expand(&data["root"], pool, pool.len());
    let rows = data["cases"].as_array().unwrap();
    assert_eq!(rows.len(), expected_counts.iter().sum::<usize>());
    let mut counts = [0; 3];
    let mut failures = Vec::new();
    for row in rows {
        let input = hex::decode(row["input"].as_str().unwrap()).unwrap();
        match idna::to_ascii_detailed(&input) {
            Ok(ascii) => {
                counts[0] += 1;
                assert_eq!(row["kind"], "ok");
                assert_eq!(hex::encode(ascii), row["ascii"]);
            }
            Err(error) if row["kind"] == "exception" => {
                counts[2] += 1;
                let idna::Error::ReferenceException { class, .. } = error.category() else {
                    panic!("{error:?}");
                };
                assert_eq!(class, row["class"]);
            }
            Err(error) => {
                counts[1] += 1;
                assert_eq!(row["kind"], "error");
                let actual = diagnostic::idna_error(&error, Limits::default())
                    .unwrap()
                    .unwrap()
                    .0;
                let expected = term(&row["reason"]);
                if actual != expected && failures.len() < 10 {
                    failures.push(format!(
                        "{}: {error:?}\nactual {actual:?}\nexpected {expected:?}",
                        String::from_utf8_lossy(&input)
                    ));
                }
            }
        }
    }
    assert_eq!(counts, expected_counts);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn nested_idna_error_formatting_is_bounded_and_retryable() {
    let error = ae_codec::idna::to_ascii_detailed(b"xn--abc--.chain").unwrap_err();
    let expected = diagnostic::idna_error(&error, Limits::default())
        .unwrap()
        .unwrap()
        .0;
    for bytes in [false, true] {
        let mut success = false;
        for allowance in 0..50000 {
            let limits = if bytes {
                Limits {
                    max_bytes: allowance,
                    ..Limits::default()
                }
            } else {
                Limits {
                    max_nodes: allowance,
                    ..Limits::default()
                }
            };
            match diagnostic::idna_error(&error, limits) {
                Ok(Some((actual, _))) => {
                    assert_eq!(actual, expected);
                    success = true;
                    break;
                }
                Err(e) => assert_eq!(e, ae_vm_fate::Error::ResourceLimit),
                _ => panic!("missing retained IDNA error"),
            }
        }
        assert!(success);
    }
    assert_eq!(
        diagnostic::idna_error(
            &error,
            Limits {
                max_depth: 0,
                ..Limits::default()
            }
        )
        .unwrap_err(),
        ae_vm_fate::Error::ResourceLimit
    );
}
