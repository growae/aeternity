use super::*;
use ae_types::work::Control;
use std::sync::atomic::{AtomicUsize, Ordering};

fn counted(limit: usize) -> (Control, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    (
        Control::new(move || c.fetch_add(1, Ordering::Relaxed) >= limit),
        count,
    )
}
fn row() -> &'static J {
    FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "call_success" && r["protocol"] == 6)
        .unwrap()
}
#[test]
fn interruption_at_each_funding_sequence_and_fate_checkpoint_preserves_backing_state() {
    let row = row();
    let initial = trees(row);
    let before = initial.serialize_to_binary(Default::default()).unwrap();
    let signed = signed(row);
    let prepared = prepared(row, &signed);
    let run = |control: Control| {
        configured(&FIXTURE["profile"], |cfg| {
            dry::run(
                initial.clone(),
                &Context::transaction(50, Protocol::Ceres),
                cfg,
                &funding(),
                &prepared,
                &dry::Options {
                    control: Some(control),
                    ..Default::default()
                },
            )
        })
    };
    let (control, count) = counted(usize::MAX);
    let expected = run(control).unwrap();
    let total = count.load(Ordering::Relaxed);
    assert!(total > 20);
    let mut in_vm = false;
    let mut after_first = false;
    for limit in 0..total {
        let (control, _) = counted(limit);
        match run(control) {
            Err(dry::Error::Interrupted) => {}
            Err(dry::Error::Operational {
                index,
                failure: batch::Failure::Transaction(e),
            }) => {
                after_first |= index > 0;
                match *e {
                    transactions::Error::Interrupted => {}
                    transactions::Error::Contract(e) => {
                        assert!(matches!(*e, contracts::Error::Interrupted));
                        in_vm = true;
                    }
                    e => panic!("unexpected {e:?}"),
                }
            }
            other => panic!("checkpoint {limit}/{total}: {other:?}"),
        }
        assert_eq!(
            initial.serialize_to_binary(Default::default()).unwrap(),
            before
        );
    }
    assert!(in_vm && after_first);
    let (control, _) = counted(usize::MAX);
    let retry = run(control).unwrap();
    assert_eq!(retry.trees.roots(), expected.trees.roots());
    assert_eq!(retry.results.len(), expected.results.len());
}

#[test]
fn cancellation_precedes_invalid_preparation_and_combines_existing_controls() {
    let stop = Control::new(|| false);
    stop.cancel();
    let invalid = json!({});
    let result = dry::prepare::all_controlled(
        vec![dry::prepare::Input::CallRequest(&invalid)],
        Default::default(),
        Default::default(),
        Some(&stop),
    );
    assert!(matches!(
        result,
        Err(dry::prepare::Failure {
            error: dry::prepare::Error::Interrupted,
            ..
        })
    ));
    for options_stopped in [false, true] {
        let open = Control::new(|| false);
        let setup = dry::setup(
            trees(row()),
            &Context::transaction(50, Protocol::Ceres),
            &[],
            &dry::Options {
                control: Some(open.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        if options_stopped {
            open.cancel();
        }
        let independent = Control::new(|| false);
        let result = configured(&FIXTURE["profile"], |cfg| {
            let cfg = transactions::Config {
                control: Some(if options_stopped { &independent } else { &stop }),
                ..*cfg
            };
            setup.prepared(&cfg, &[])
        });
        assert!(matches!(result, Err(dry::Error::Interrupted)));
    }
    let signed = signed(row());
    let (control, count) = counted(usize::MAX);
    let inputs = || {
        signed
            .iter()
            .map(|s| dry::prepare::Input::transaction(s.transaction().clone()))
            .collect()
    };
    dry::prepare::all_controlled(
        inputs(),
        Default::default(),
        Default::default(),
        Some(&control),
    )
    .unwrap();
    for limit in 0..count.load(Ordering::Relaxed) {
        let (control, _) = counted(limit);
        assert!(matches!(
            dry::prepare::all_controlled(
                inputs(),
                Default::default(),
                Default::default(),
                Some(&control)
            ),
            Err(dry::prepare::Failure {
                error: dry::prepare::Error::Interrupted,
                ..
            })
        ));
    }
}
