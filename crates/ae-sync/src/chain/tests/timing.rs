use super::*;
use ae_chain::instrumentation::{self, Event, Publisher, Sink, Span, Stage};
use ae_chain::publication::Kind as BlockKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Trace {
    Started(Stage),
    Finished(Stage, bool),
    Dropped(Stage),
}
#[derive(Default)]
pub(super) struct Timings(Arc<Mutex<Vec<Trace>>>);
struct Sample {
    stage: Stage,
    trace: Arc<Mutex<Vec<Trace>>>,
    finished: bool,
}
impl Sink for Timings {
    fn event(&self, _: Event) {}
    fn start(&self, stage: Stage) -> Option<Box<dyn Span>> {
        self.0.lock().unwrap().push(Trace::Started(stage));
        Some(Box::new(Sample {
            stage,
            trace: self.0.clone(),
            finished: false,
        }))
    }
}
impl Span for Sample {
    fn finish(mut self: Box<Self>, success: bool) {
        self.finished = true;
        self.trace
            .lock()
            .unwrap()
            .push(Trace::Finished(self.stage, success));
    }
}
impl Drop for Sample {
    fn drop(&mut self) {
        if !self.finished {
            self.trace.lock().unwrap().push(Trace::Dropped(self.stage));
        }
    }
}
impl Timings {
    pub(super) fn bind(publisher: &Publisher) -> (Arc<Self>, instrumentation::Binding) {
        let sink = Arc::new(Self::default());
        let binding = publisher.bind(sink.clone()).unwrap();
        (sink, binding)
    }
    pub(super) fn take(&self) -> Vec<Trace> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}
const KEY: Stage = Stage::Insert(BlockKind::Key);
const MICRO: Stage = Stage::Insert(BlockKind::Micro);
const TXS: Stage = Stage::MicroTransactions;

async fn created(importer: &Importer, block: &Block) -> Result<Report, Failure> {
    within(
        importer
            .import_created_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
}

#[tokio::test]
async fn actual_import_timers_exclude_duplicates_and_prevalidation_rejections() {
    let (_temp, importer) = setup(Default::default());
    let (timings, _binding) = Timings::bind(importer.context.db.instrumentation());
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    created(&importer, &all[0]).await.unwrap();
    assert_eq!(
        timings.take(),
        [Trace::Started(KEY), Trace::Finished(KEY, true)]
    );
    created(&importer, &all[1]).await.unwrap();
    assert_eq!(
        timings.take(),
        [
            Trace::Started(MICRO),
            Trace::Started(TXS),
            Trace::Finished(TXS, true),
            Trace::Finished(MICRO, true),
        ]
    );
    created(&importer, &all[1]).await.unwrap();
    assert!(timings.take().is_empty());
    let Block::Key(mut bad) = all[0].clone() else {
        panic!()
    };
    bad.seal = [0; 42];
    assert!(created(&importer, &Block::Key(bad)).await.is_err());
    assert!(timings.take().is_empty());
    assert_eq!(
        importer.context.db.view().unwrap().status().tip,
        Some(all[1].hash().unwrap())
    );
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn missing_parent_is_a_returned_context_error_without_transaction_timing() {
    let (_temp, importer) = setup(Default::default());
    let (timings, _binding) = Timings::bind(importer.context.db.instrumentation());
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let failure = created(&importer, &all[1]).await.unwrap_err();
    assert!(
        matches!(failure.error, Error::Storage(repository::Error::Missing(_))),
        "{failure:?}"
    );
    assert_eq!(
        timings.take(),
        [Trace::Started(MICRO), Trace::Finished(MICRO, false)]
    );
    assert_eq!(importer.context.db.view().unwrap().status().revision, 1);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn conflict_retries_have_one_insertion_sample_and_each_strict_execution_sample() {
    let (_temp, mut importer) = setup(Default::default());
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    created(&importer, &all[0]).await.unwrap();
    let (timings, _binding) = Timings::bind(importer.context.db.instrumentation());
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(repository::Error::Conflict)
        } else {
            db.commit(update)
        }
    }));
    created(&importer, &all[1]).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        timings.take(),
        [
            Trace::Started(MICRO),
            Trace::Started(TXS),
            Trace::Finished(TXS, true),
            Trace::Started(TXS),
            Trace::Finished(TXS, true),
            Trace::Finished(MICRO, true),
        ]
    );
    assert_eq!(importer.context.db.view().unwrap().status().revision, 3);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn uncertain_writes_and_panics_keep_strict_success_and_drop_outer_samples() {
    for mode in 0..3 {
        let (_temp, mut importer) = setup(Default::default());
        let all = fixtures::blocks(&NetworkProfile::mainnet());
        created(&importer, &all[0]).await.unwrap();
        let (timings, _binding) = Timings::bind(importer.context.db.instrumentation());
        Arc::get_mut(&mut importer.context).unwrap().commit_hook =
            Some(Arc::new(move |db, update| {
                if mode > 0 {
                    db.commit(update)?;
                }
                if mode == 2 {
                    panic!("timing: escaped post-commit exception");
                }
                Err(repository::Error::Backend(
                    "timing: uncertain commit".into(),
                ))
            }));
        let failure = created(&importer, &all[1]).await.unwrap_err();
        assert!(matches!(
            failure.error,
            Error::WorkerPanicked | Error::CommitUncertain { .. }
        ));
        assert!(importer.recovery_required());
        assert_eq!(
            timings.take(),
            [
                Trace::Started(MICRO),
                Trace::Started(TXS),
                Trace::Finished(TXS, true),
                Trace::Dropped(MICRO),
            ]
        );
        assert_eq!(
            importer
                .context
                .db
                .view()
                .unwrap()
                .block(all[1].hash().unwrap())
                .unwrap()
                .is_some(),
            mode > 0
        );
        importer.stop();
        importer.join().await;
    }
}
