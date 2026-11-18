use super::*;
use ae_execution::fate_chain::{
    deployment::{self, Source},
    remote::{State as Chain, View},
};
use ae_state::{StateRoots, accounts::Accounts, contracts::Code as ContractCode};
use ae_trie::NodeWriter;
use ae_types::AccountPublicKey;
use ae_vm_fate::machine::program::remote::{Host, HostFailure};
use remote_tests::{event_json, key, small, tree, tree_entries};
use serde_json::json;

static DEPLOY: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_deployment.json")).unwrap();
    assert_eq!(p["format"], "interned-json-v1");
    let pool = rows(&p["values"]);
    name_tests::expand(&p["root"], pool, pool.len())
});
fn chain(row: &J) -> Chain {
    let mut s = remote_tests::chain(
        &json!({"initial":{"chain":{"accounts":row["initial"]["accounts"]}},"contracts":row["initial"]["base_contracts"],"height":50}),
    );
    s.authentication.dry_run = row["dry_run"].as_bool().unwrap();
    if row["ga_auth"] == true {
        s.authentication
            .ga_authenticated
            .insert(AccountPublicKey(small(1)));
    }
    if row["payer"] == true {
        s.authentication.payer = Some(AccountPublicKey(small(9)));
    }
    if row["offchain"] == true {
        let t = tree(&row["onchain_accounts"]);
        s.onchain = Some(Box::new(View {
            accounts: Accounts::from_root(t.root_hash(), t.into_store()).unwrap(),
            contracts: Default::default(),
            names: Default::default(),
            name_cache: Default::default(),
            oracles: Default::default(),
            oracle_reads: Default::default(),
        }));
    }
    s
}
fn source<'a>(row: &J, data: &'a [u8]) -> Source<'a> {
    if row["code"][0] == "code" {
        Source::Inline(data)
    } else {
        Source::Reference(data)
    }
}
fn exception(e: HostFailure<ae_execution::fate_chain::Error>) -> J {
    match e {
        HostFailure::Exception(Exception::Native(reason)) => {
            json!({"kind":"exception","class":"error","reason":reason})
        }
        other => panic!("unexpected failure {other:?}"),
    }
}
fn snapshot<A: NodeWriter + Clone, C: NodeWriter + Clone>(s: &Chain<A, C>) -> J {
    let cache:Vec<_>=s.contract_cache.pending.values().map(|p|{
 let c=&p.contract;
 json!({"key":hex::encode(c.public_key),"owner":hex::encode(c.owner.0),"vm":c.version.vm,"abi":c.version.abi,
 "code":match &c.code {ContractCode::Inline(b)=>json!(["code",hex::encode(b)]),ContractCode::Reference(k)=>json!(["ref",hex::encode(k.value)])},
 "encoded":match c.serialize() {Ok(b)=>json!(hex::encode(b)),Err(_)=>json!({"kind":"exception","class":"error","reason":"function_clause"})},
 "writes":p.writes.iter().map(|(k,v)|json!([hex::encode(k),hex::encode(v)])).collect::<Vec<_>>()})
 }).collect();
    let publication = match s
        .contract_cache
        .materialize(&s.contracts, Limits::default())
    {
        Ok(c) => {
            json!({"contracts":tree_entries(c.tree()),"contract_root":c.root_hash().map(|h|hex::encode(h.0)),"tree_root":hex::encode(StateRoots{accounts:s.accounts.root_hash(),contracts:c.root_hash(),..Default::default()}.hash().0)})
        }
        Err(e) => exception(e),
    };
    json!({"accounts":tree_entries(s.accounts.tree()),"account_root":s.accounts.root_hash().map(|h|hex::encode(h.0)),"base_contracts":tree_entries(s.contracts.tree()),"contract_cache":cache,"publication":publication,"events":s.events.iter().map(event_json).collect::<Vec<_>>()})
}
fn action<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut Chain<A, C>,
    row: &J,
    step: &J,
    limits: Limits,
) -> Result<Option<[u8; 32]>, HostFailure<ae_execution::fate_chain::Error>> {
    let code = bytes(&row["code"][1]);
    let src = source(row, &code);
    let amount = value(&row["amount"]);
    match string(&step["action"]) {
        "put" => deployment::put(
            state,
            small(1),
            src,
            &amount,
            row["vm"].as_u64().unwrap() as u16,
            row["protocol"].as_u64().unwrap() as u32,
            limits,
        )
        .map(Some),
        "clone_pending" => deployment::put(
            state,
            small(1),
            Source::Reference(&key(&row["key"])),
            &amount,
            row["vm"].as_u64().unwrap() as u16,
            row["protocol"].as_u64().unwrap() as u32,
            limits,
        )
        .map(Some),
        "remove_self" => {
            deployment::remove(state, key(&row["key"]), key(&row["key"]), &amount, limits)
                .map(|()| None)
        }
        "remove" => {
            deployment::remove(state, key(&row["key"]), small(1), &amount, limits).map(|()| None)
        }
        "event" => {
            state.events.insert(0, src.event());
            Ok(None)
        }
        _ => panic!(),
    }
}
#[test]
fn deployment_cache_nonce_accounts_transfer_and_cleanup_match_original_chain_api() {
    assert_eq!(rows(&DEPLOY["cases"]).len(), 1316);
    let mut count = 0;
    for row in rows(&DEPLOY["cases"]) {
        let mut state = chain(row);
        assert_eq!(snapshot(&state), row["initial"]);
        for (i, step) in rows(&row["steps"]).iter().enumerate() {
            let before = format!("{state:?}");
            let result = match action(&mut state, row, step, Limits::default()) {
                Ok(k) => {
                    assert_eq!(json!(k.map(hex::encode)), step["key"]);
                    json!({"kind":"ok","state":snapshot(&state)})
                }
                Err(e) => {
                    assert_eq!(format!("{state:?}"), before);
                    exception(e)
                }
            };
            assert_eq!(
                result, step["result"],
                "case {} protocol {} VM {} step {i}",
                row["case_id"], row["protocol"], row["vm"]
            );
            count += 1;
        }
    }
    assert_eq!(count, 5243);
}
fn selected(id: u64) -> &'static J {
    rows(&DEPLOY["cases"])
        .iter()
        .find(|r| r["case_id"] == id && r["protocol"] == 6 && r["vm"] == 8)
        .unwrap()
}
#[test]
fn deployment_operational_failures_preserve_complete_host_and_retry() {
    use oracle_write_tests::{Faults, fault_host};
    for id in [0, 32, 64, 122, 123] {
        let row = selected(id);
        let step = &row["steps"][0];
        let faults = Faults::default();
        let base = fault_host(chain(row), &faults);
        for limits in [
            Limits {
                max_bytes: 0,
                ..Default::default()
            },
            Limits {
                max_nodes: 0,
                ..Default::default()
            },
        ] {
            let mut s = base.clone();
            let before = format!("{s:?}");
            assert!(matches!(
                action(&mut s, row, step, limits),
                Err(HostFailure::ResourceLimit)
            ));
            assert_eq!(before, format!("{s:?}"));
        }
        let mut failures = 0;
        for n in 0..128 {
            let mut s = base.clone();
            faults.writes_left.set(Some(n));
            let before = format!("{s:?}");
            match action(&mut s, row, step, Limits::default()) {
                Err(HostFailure::Backend(_)) => {
                    faults.writes_left.set(Some(n));
                    assert_eq!(before, format!("{s:?}"));
                    failures += 1;
                    faults.writes_left.set(None);
                    action(&mut s, row, step, Limits::default()).unwrap();
                    assert_eq!(snapshot(&s), step["result"]["state"]);
                }
                Ok(_) => {
                    faults.writes_left.set(None);
                    assert_eq!(snapshot(&s), step["result"]["state"]);
                    break;
                }
                r => panic!("{r:?}"),
            }
        }
        assert!(failures > 1);
        faults.fail_reads.set(true);
        let mut s = base.clone();
        let before = format!("{s:?}");
        assert!(matches!(
            action(&mut s, row, step, Limits::default()),
            Err(HostFailure::Backend(_))
        ));
        assert_eq!(before, format!("{s:?}"));
        faults.fail_reads.set(false);
        action(&mut s, row, step, Limits::default()).unwrap();
        assert_eq!(snapshot(&s), step["result"]["state"]);
    }
}
#[test]
fn pending_contracts_are_readable_before_publication_and_removed_without_store_residue() {
    let row = selected(0);
    let mut state = chain(row);
    let step = &row["steps"][0];
    let k = action(&mut state, row, step, Limits::default())
        .unwrap()
        .unwrap();
    assert_eq!(state.creator(k).unwrap(), small(1));
    assert!(
        state
            .address_test(
                ae_vm_fate::machine::program::remote::context::AddressTest::Contract,
                &k
            )
            .unwrap()
    );
    assert!(!state.contracts.is_contract(k).unwrap());
    assert!(state.code(&k, Limits::default()).unwrap().is_none()); // Empty bytecode is accepted by put; loading rejects it.
    deployment::remove(
        &mut state,
        k,
        small(1),
        &value(&row["amount"]),
        Limits::default(),
    )
    .unwrap();
    assert!(
        state
            .accounts
            .lookup(AccountPublicKey(k))
            .unwrap()
            .is_some()
    );
    assert!(state.contract_cache.pending.is_empty());
    assert_eq!(
        tree_entries(state.contracts.tree()),
        row["initial"]["base_contracts"]
    );
}

#[test]
fn deployment_publication_and_cleanup_faults_are_atomic_and_retryable() {
    use oracle_write_tests::{Faults, fault_host};
    for id in [0, 122, 127] {
        let row = selected(id);
        let faults = Faults::default();
        let mut base = fault_host(chain(row), &faults);
        let removal = if id == 127 {
            &row["steps"][0]
        } else {
            action(&mut base, row, &row["steps"][0], Limits::default()).unwrap();
            action(&mut base, row, &row["steps"][1], Limits::default()).unwrap();
            &row["steps"][2]
        };
        let mut succeeded = false;
        let mut failures = 0;
        for n in 0..128 {
            let mut s = base.clone();
            faults.writes_left.set(Some(n));
            let before = format!("{s:?}");
            match action(&mut s, row, removal, Limits::default()) {
                Err(HostFailure::Backend(_)) => {
                    faults.writes_left.set(Some(n));
                    assert_eq!(before, format!("{s:?}"));
                    faults.writes_left.set(None);
                    action(&mut s, row, removal, Limits::default()).unwrap();
                    assert_eq!(snapshot(&s), removal["result"]["state"]);
                    failures += 1;
                }
                Ok(_) => {
                    faults.writes_left.set(None);
                    assert_eq!(snapshot(&s), removal["result"]["state"]);
                    succeeded = true;
                    break;
                }
                r => panic!("{r:?}"),
            }
        }
        assert!(succeeded && failures > 1);
    }
    let row = selected(126);
    let faults = Faults::default();
    let mut state = fault_host(chain(row), &faults);
    for step in rows(&row["steps"]) {
        action(&mut state, row, step, Limits::default()).unwrap();
    }
    let expected = &rows(&row["steps"]).last().unwrap()["result"]["state"]["publication"];
    let mut succeeded = false;
    let mut failures = 0;
    for n in 0..128 {
        faults.writes_left.set(Some(n));
        let before = format!("{state:?}");
        match state
            .contract_cache
            .materialize(&state.contracts, Limits::default())
        {
            Err(HostFailure::Backend(_)) => {
                faults.writes_left.set(Some(n));
                assert_eq!(before, format!("{state:?}"));
                faults.writes_left.set(None);
                let retry = state
                    .contract_cache
                    .materialize(&state.contracts, Limits::default())
                    .unwrap();
                assert_eq!(tree_entries(retry.tree()), expected["contracts"]);
                failures += 1;
            }
            Ok(c) => {
                faults.writes_left.set(None);
                assert_eq!(tree_entries(c.tree()), expected["contracts"]);
                succeeded = true;
                break;
            }
            r => panic!("{r:?}"),
        }
    }
    assert!(succeeded && failures > 2);
    for id in [143, 144] {
        let row = selected(id);
        let mut s = chain(row);
        let k = action(&mut s, row, &row["steps"][0], Limits::default())
            .unwrap()
            .unwrap();
        let loaded = s.code(&k, Limits::default()).unwrap().unwrap();
        assert_eq!(loaded.vm_version, VmVersion::Three);
        assert_eq!(loaded.code.functions.len(), 1);
        assert!(s.contracts.record(k).unwrap().is_none());
    }
}
