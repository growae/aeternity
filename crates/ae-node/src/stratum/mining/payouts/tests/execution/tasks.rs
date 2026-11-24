use super::*;
use crate::stratum::mining::payouts::preflight::{self, task};
use ae_chain::repository::{self as repo, Roots};
use ae_storage::snapshots;
use ae_sync::chain::{self, pow::stratum::execution as bound};

pub(super) fn setup(
    temp: &Temp,
    row: &J,
) -> (Arc<Database>, NetworkProfile, Arc<payout::Prepared>) {
    let input = from_snapshot(&row["before"]);
    let network = NetworkProfile::custom(
        b"ae_payout_preflight_local".to_vec(),
        vec![Activation {
            height: 0,
            protocol: protocol(row),
        }],
    )
    .unwrap();
    let Header::Key(mut header) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    header.common.version = row["protocol"].as_u64().unwrap() as u32;
    header.common.state_hash = input.roots().hash().0;
    header.info = Some([0; 4]);
    let identity = Identity::new(
        &network,
        Header::Key(header.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(&temp.0, identity).unwrap());
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &header,
            &input,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let prepared = payout::prepare_with_generation(
        &prepared_payment(row),
        &environment(row),
        Some(&unsigned("1000000000000000000000000000")),
        network.network_id(),
        payout::Generation::Bounded(Default::default()),
        Default::default(),
    )
    .unwrap();
    (db, network, Arc::new(prepared))
}
pub(super) fn importer(
    db: &Arc<Database>,
    network: &NetworkProfile,
    vm: Limits,
    gas: u64,
) -> chain::Importer {
    let owned = fixture::execution(network).with_config(|c| {
        ae_execution::transactions::owned::Owned::capture(&ae_execution::transactions::Config {
            aevm_limits: vm,
            ..*c
        })
        .unwrap()
    });
    let mut rules = fixture::rules(db.view().unwrap().identity().genesis);
    rules.admission.configured_gas_limit = gas.into();
    chain::Importer::new(db.clone(), owned, rules, Default::default()).unwrap()
}
fn stop(importer: chain::Importer) {
    importer.stop();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(importer.join());
}
fn source(importer: &chain::Importer, owner: &snapshots::Owner) -> bound::Source {
    importer
        .stratum_admitter()
        .execution(owner.clone(), Default::default())
        .unwrap()
}
#[test]
fn owned_preflight_uses_importer_context_and_original_contract_without_writing_chain_state() {
    for version in [3, 6, 8] {
        let temp = Temp::new();
        let row = gas::gas_find(version, "prepared_two");
        let (db, network, prepared) = setup(&temp, row);
        let importer = importer(&db, &network, Default::default(), 6_000_000);
        let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
        let source = source(&importer, &owner);
        let frame = source.capture().unwrap();
        let view = frame.view();
        assert_eq!(view.context.height, 0);
        assert_eq!(view.context.protocol, protocol(row));
        assert_eq!(view.context.origin, ae_execution::Origin::Transaction);
        assert!(!view.context.dry_run);
        assert!(view.context.payer.is_none() && view.context.ga_authenticated.is_empty());
        assert_eq!(
            view.aevm.as_ref().unwrap().network_id.as_ref(),
            network.network_id()
        );
        assert_eq!(
            view.aevm.as_ref().unwrap().environment.key_hash,
            frame.metadata().status.tip.unwrap().0
        );
        assert_eq!(
            view.aevm.as_ref().unwrap().environment.block_gas_limit,
            BigInt::from(6_000_000)
        );
        assert_eq!(
            view.aevm.as_ref().unwrap().float_profile,
            FloatProfile::Otp26Word64
        );
        assert_eq!(
            view.aevm.as_ref().unwrap().store_order,
            OrderProfile::Otp26Word64Crc32c
        );
        let before = Roots::of(&view.trees);
        let metadata = frame.metadata().clone();
        drop(frame);
        assert_eq!(owner.live_snapshots(), 0);
        let builder = task::Builder::new(source, Default::default());
        assert!(builder.uses_database(&db));
        let task = builder.start(prepared.clone()).unwrap();
        let id = task.id();
        let completion = task.wait().unwrap();
        assert_eq!(completion.id, id);
        assert!(Arc::ptr_eq(&completion.prepared, &prepared));
        let checked = completion.result.unwrap();
        assert_eq!(checked.metadata().status, metadata.status);
        assert_eq!(checked.metadata().source_id, metadata.source_id);
        assert_eq!(checked.evaluation().before(), &before);
        assert_eq!(
            checked.evaluation().preparation_hash(),
            ae_crypto::blake2b_256(&prepared.encode(Default::default()).unwrap())
        );
        assert_eq!(checked.evaluation().call().height, 0.into());
        let native = from_snapshot(&row["result"][1][1]);
        assert_eq!(
            checked.evaluation().after().state.accounts,
            native.accounts.root_hash()
        );
        assert_eq!(
            checked.evaluation().after().state.contracts,
            native.contracts.root_hash()
        );
        assert!(builder.is_current(&checked).unwrap());
        assert_eq!(db.view().unwrap().status(), metadata.status);
        assert_eq!(
            db.view()
                .unwrap()
                .require_record(metadata.status.tip.unwrap())
                .unwrap()
                .roots,
            before
        );
        assert_eq!(owner.live_snapshots(), 0);
        assert!(!builder.busy());
        stop(importer);
    }
    println!(
        "Owned payout preflight: three original-contract simulations preserve canonical state"
    );
}
#[test]
fn owned_preflight_preserves_importer_vm_and_block_gas_limits() {
    for zero_vm in [false, true] {
        let temp = Temp::new();
        let row = gas::gas_find(8, "prepared_two");
        let (db, network, prepared) = setup(&temp, row);
        let mut vm = Limits::default();
        if zero_vm {
            vm.max_work = 0;
        }
        let importer = importer(&db, &network, vm, if zero_vm { 6_000_000 } else { 1 });
        let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
        let source = source(&importer, &owner);
        let frame = source.capture().unwrap();
        assert_eq!(frame.vm_limits().max_work, vm.max_work);
        assert_eq!(frame.vm_limits().max_bytes, vm.max_bytes);
        assert_eq!(frame.vm_limits().max_depth, vm.max_depth);
        assert_eq!(frame.vm_limits().max_memory_words, vm.max_memory_words);
        let metadata = frame.metadata().clone();
        drop(frame);
        let builder = task::Builder::new(source, Default::default());
        let error = builder
            .start(prepared)
            .unwrap()
            .wait()
            .unwrap()
            .result
            .unwrap_err();
        match error {
            task::Error::Evaluation(error) if zero_vm => {
                assert!(matches!(*error, task::EvaluationFailure::Execution(_)))
            }
            task::Error::Evaluation(error) => {
                assert!(matches!(*error, task::EvaluationFailure::BlockGas))
            }
            error => panic!("unexpected failure {error:?}"),
        }
        assert_eq!(owner.live_snapshots(), 0);
        assert_eq!(db.view().unwrap().status(), metadata.status);
        stop(importer);
    }
}
#[test]
fn completed_unjoined_preflight_holds_clone_capacity_and_releases_snapshots() {
    let temp = Temp::new();
    let row = gas::gas_find(8, "prepared_two");
    let (db, network, prepared) = setup(&temp, row);
    let importer = importer(&db, &network, Default::default(), 6_000_000);
    let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let source = source(&importer, &owner);
    let builder = task::Builder::new(source.clone(), Default::default());
    let other = task::Builder::new(source, Default::default());
    let cloned = builder.clone();
    let mut task = builder.start(prepared.clone()).unwrap();
    until(|| task.is_finished());
    assert_eq!(owner.live_snapshots(), 0);
    assert!(builder.busy() && cloned.busy());
    assert!(matches!(
        cloned.start(prepared.clone()),
        Err(task::Error::Busy)
    ));
    task.request_cancel();
    let completion = task.try_result().unwrap();
    assert!(Arc::ptr_eq(&completion.prepared, &prepared));
    let checked = completion.result.unwrap();
    assert!(cloned.is_current(&checked).unwrap());
    assert!(!other.is_current(&checked).unwrap());
    assert!(task.try_result().is_none());
    assert!(!builder.busy() && !cloned.busy());
    let next = cloned.start(prepared.clone()).unwrap();
    assert_ne!(next.id(), completion.id);
    assert!(Arc::ptr_eq(next.prepared(), &prepared));
    drop(next);
    assert_eq!(owner.live_snapshots(), 0);
    assert!(!builder.busy());
    assert!(
        builder
            .start(prepared)
            .unwrap()
            .wait()
            .unwrap()
            .result
            .is_ok()
    );
    stop(importer);
}
#[test]
fn snapshot_exhaustion_and_foreign_owner_fail_without_replacing_the_request() {
    let temp = Temp::new();
    let row = gas::gas_find(8, "prepared_two");
    let (db, network, prepared) = setup(&temp, row);
    let importer = importer(&db, &network, Default::default(), 6_000_000);
    let owner = snapshots::Owner::start(
        db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let source = source(&importer, &owner);
    let held = source.capture().unwrap();
    let builder = task::Builder::new(source, Default::default());
    let completion = builder.start(prepared.clone()).unwrap().wait().unwrap();
    assert!(Arc::ptr_eq(&completion.prepared, &prepared));
    assert!(matches!(
        completion.result,
        Err(task::Error::Source(bound::Error::Snapshot(
            snapshots::Error::ResourceLimit
        )))
    ));
    assert_eq!(owner.live_snapshots(), 1);
    assert!(!builder.busy());
    drop(held);
    assert!(
        builder
            .start(prepared)
            .unwrap()
            .wait()
            .unwrap()
            .result
            .is_ok()
    );
    let foreign_temp = Temp::new();
    let (foreign, _, _) = setup(&foreign_temp, row);
    assert!(!builder.uses_database(&foreign));
    let foreign_owner = snapshots::Owner::start(foreign, Default::default()).unwrap();
    assert!(matches!(
        importer
            .stratum_admitter()
            .execution(foreign_owner, Default::default()),
        Err(bound::Error::Binding)
    ));
    assert!(matches!(
        importer.stratum_admitter().execution(
            owner.clone(),
            repo::pow::key::Limits {
                history: 0,
                ..Default::default()
            }
        ),
        Err(bound::Error::Binding)
    ));
    assert_eq!(owner.live_snapshots(), 0);
    stop(importer);
}
#[test]
fn same_root_chain_revision_invalidates_check_and_rechecks_identical_captured_bytes() {
    let temp = Temp::new();
    let row = gas::gas_find(8, "prepared_two");
    let (db, network, prepared) = setup(&temp, row);
    let importer = importer(&db, &network, Default::default(), 6_000_000);
    let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let source = source(&importer, &owner);
    let frame = source.capture().unwrap();
    let builder = task::Builder::new(source.clone(), Default::default());
    let checked = builder
        .start(prepared.clone())
        .unwrap()
        .wait()
        .unwrap()
        .result
        .unwrap();
    assert!(builder.is_current(&checked).unwrap());
    let before = checked.metadata().status;
    let encoded = prepared.encode(Default::default()).unwrap();
    // A real repository rollback advances revision even when the target is
    // the same selected genesis. Equal roots do not authorize an old check.
    db.rollback(repo::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(db.view().unwrap().status().tip, before.tip);
    assert_ne!(db.view().unwrap().status().revision, before.revision);
    assert!(!source.is_current(frame.metadata()).unwrap());
    assert!(!builder.is_current(&checked).unwrap());
    assert_eq!(
        Roots::of(&frame.view().trees),
        *checked.evaluation().before()
    );
    assert!(
        frame
            .view()
            .trees
            .accounts
            .lookup(prepared.call().transaction().signer())
            .unwrap()
            .is_some()
    );
    drop(frame);
    let retry = builder.start(prepared.clone()).unwrap().wait().unwrap();
    assert!(Arc::ptr_eq(&retry.prepared, &prepared));
    let new = retry.result.unwrap();
    assert!(builder.is_current(&new).unwrap());
    assert_eq!(new.evaluation(), checked.evaluation());
    assert_eq!(prepared.encode(Default::default()).unwrap(), encoded);
    assert_eq!(owner.live_snapshots(), 0);
    stop(importer);
}

#[test]
fn native_response_case_diagnostics_release_their_staged_snapshot_trees() {
    let temp = Temp::new();
    let row = gas::gas_find(8, "prepared_two");
    let (db, network, prepared) = setup(&temp, row);
    let importer = importer(&db, &network, Default::default(), 6_000_000);
    let owner = snapshots::Owner::start(db, Default::default()).unwrap();
    let frame = source(&importer, &owner).capture().unwrap();
    let roots = Roots::of(&frame.view().trees);
    let chain = aevm_chain::State {
        trees: (*frame.view().trees).clone(),
        onchain: None,
        account: prepared.call().transaction().signer(),
        vm_version: ae_vm_aevm::opcode::Version::Sophia4,
        network_id: network.network_id().into(),
        context: Arc::new(frame.view().context.clone()),
        event_context: aevm_chain::EventContext::None,
        events: Arc::default(),
    };
    let response = ae_vm_aevm::outer_call::Response::Exception {
        reason: "diagnostic test".into(),
        gas_spent: 1.into(),
    };
    // An explicit diagnostic specimen exercises lease ownership; this is not
    // claimed as an independently captured bytecode execution failure.
    let error = preflight::Error::<snapshots::Reader, snapshots::Reader>::Execution(Box::new(
        ae_vm_aevm::outer_call::Fault::ResponseCase {
            response: response.clone(),
            state: Box::new(ae_vm_aevm::execution::State::new(
                ae_vm_aevm::opcode::Version::Sophia4,
                vec![],
                1u64.into(),
                FloatProfile::Otp26Word64,
            )),
            chain: Box::new(chain),
        },
    ));
    drop(frame);
    assert_eq!(owner.live_snapshots(), 1);
    let task::EvaluationFailure::Execution(task::ExecutionFailure::ResponseCase {
        response: actual,
        roots: actual_roots,
    }) = task::EvaluationFailure::from(error)
    else {
        panic!()
    };
    assert_eq!(actual, response);
    assert_eq!(*actual_roots, roots);
    assert_eq!(owner.live_snapshots(), 0);
    stop(importer);
}

#[test]
fn selected_key_and_micro_contexts_keep_public_gas_policy_and_bounded_history() {
    let temp = Temp::new();
    let network = NetworkProfile::mainnet();
    let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
        panic!()
    };
    let trees = Trees {
        accounts: genesis::state(Network::Mainnet).unwrap().0,
        contracts: Default::default(),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let identity = Identity::new(
        &network,
        Header::Key(header.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(&temp.0, identity).unwrap());
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &header,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut rules = fixture::rules(db.view().unwrap().identity().genesis);
    // Public-network consensus fixes this ceiling even if local config is
    // negative. Custom networks must reject that invalid configuration.
    rules.admission.configured_gas_limit = (-1).into();
    let importer = chain::Importer::new(
        db.clone(),
        fixture::execution(&network),
        rules,
        Default::default(),
    )
    .unwrap();
    let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let source = source(&importer, &owner);
    let initial = source.capture().unwrap();
    let mut key_count = 0;
    let mut micro_count = 0;
    for block in fixture::blocks(&network) {
        let mut ticket = importer
            .import_created_block(block.encode().unwrap(), u64::MAX - 600_000)
            .unwrap();
        let mut result = None;
        until(|| {
            result = ticket.try_result();
            result.is_some()
        });
        result.unwrap().unwrap();
        let frame = source.capture().unwrap();
        let view = db.view().unwrap();
        assert_eq!(frame.metadata().status, view.status());
        let (leader, timestamp) = match &block {
            ae_chain::block::Block::Key(key) => {
                key_count += 1;
                (key.clone(), BigInt::from(key.time) + 1)
            }
            ae_chain::block::Block::Micro(micro) => {
                micro_count += 1;
                let ae_chain::block::Block::Key(key) = view
                    .require_block(Hash256(micro.header.common.previous_key_hash))
                    .unwrap()
                else {
                    panic!()
                };
                (key, BigInt::from(micro.header.time))
            }
        };
        assert_eq!(frame.view().context.height, leader.common.height);
        assert_eq!(
            frame.view().context.protocol,
            Protocol::try_from(leader.common.version).unwrap()
        );
        let config = frame.view().aevm.as_ref().unwrap();
        assert_eq!(config.environment.timestamp_ms, timestamp);
        assert_eq!(
            config.environment.beneficiary,
            AccountPublicKey(leader.beneficiary)
        );
        assert_eq!(
            config.environment.key_hash,
            Header::Key(leader).hash().unwrap().0
        );
        assert_eq!(config.environment.block_gas_limit, BigInt::from(6_000_000));
        assert_eq!(
            Roots::of(&frame.view().trees),
            view.require_record(view.status().tip.unwrap())
                .unwrap()
                .roots
        );
    }
    assert_eq!(key_count, 6);
    assert!(micro_count > 0);
    assert!(!source.is_current(initial.metadata()).unwrap());
    assert_eq!(Roots::of(&initial.view().trees), Roots::of(&trees));
    drop(initial);
    let limited = importer
        .stratum_admitter()
        .execution(
            owner.clone(),
            repo::pow::key::Limits {
                history: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        limited.capture(),
        Err(bound::Error::Lookup(
            ae_hyperchain::chain::LookupError::ResourceLimit
        ))
    ));
    assert_eq!(owner.live_snapshots(), 0);
    stop(importer);
    println!(
        "Owned payout context: six saved mainnet keys and {micro_count} saved microblocks use their selected snapshot"
    );
}
