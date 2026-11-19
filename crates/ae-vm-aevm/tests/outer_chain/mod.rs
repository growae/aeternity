use super::*;
use ae_types::{Id, IdTag};
use ae_vm_aevm::{
    calls,
    primops::{self, AddressKind, ChainHost, HostResult},
};

static CHAIN: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("../fixtures/erlang_chain_primops.json")));
static EDGES: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("../fixtures/erlang_chain_call_edges.json")));
impl ChainHost for Source<'_> {
    type Spend = J;
    fn get_store(&self, _: &J) -> Result<Store, runtime::Fault<Self::Error>> {
        self.event(atom("store"));
        self.check("store").map_err(runtime::Fault::Host)?;
        Ok(Store::new())
    }
    fn prepare_spend(
        &self,
        recipient: Id,
        value: &BigInt,
        _: &J,
        _: &mut Budget,
    ) -> HostResult<J, Self::Error> {
        assert_eq!(recipient.tag, IdTag::Account);
        let recipient = tuple(vec![atom("id"), atom("account"), binary(recipient.value)]);
        self.event(tuple(vec![atom("spend_tx"), recipient.clone(), int(value)]));
        self.check("spend_tx").map_err(runtime::Fault::Host)?;
        match self.mode {
            "prepare_error" => Ok(Err("not_allowed_off_chain".into())),
            "throw" => Err(runtime::Fault::Native(runtime::Native::BadCallData)),
            _ => Ok(Ok(map(vec![
                (atom("recipient"), recipient),
                (atom("amount"), int(value)),
            ]))),
        }
    }
    fn apply_spend(&self, spend: J, chain: &J, _: &mut Budget) -> HostResult<J, Self::Error> {
        self.event(tuple(vec![atom("spend"), spend.clone()]));
        self.check("spend").map_err(runtime::Fault::Host)?;
        Ok(match self.mode {
            "apply_error" => Err("not_allowed_off_chain".into()),
            "not_payable" => Err("account_is_not_payable".into()),
            _ => Ok(put(chain, "spent", spend)),
        })
    }
    fn address(
        &self,
        kind: AddressKind,
        address: Word,
        _: &J,
        _: &mut Budget,
    ) -> HostResult<Word, Self::Error> {
        let name = match kind {
            AddressKind::Oracle => "oracle",
            AddressKind::Contract => "contract",
            AddressKind::Payable => "payable",
        };
        self.event(tuple(vec![atom(name), binary(address.0)]));
        self.check("address").map_err(runtime::Fault::Host)?;
        match self.mode {
            "prepare_error" => Ok(Err("not_allowed_off_chain".into())),
            "apply_error" => Ok(Err("query_failed".into())),
            "not_payable" => Ok(Err("account_is_not_payable".into())),
            "throw" | "boolean" => Err(runtime::Fault::Native(runtime::Native::BadCallData)),
            mode => Ok(Ok(Word::from(u64::from(mode != "false")))),
        }
    }
}
fn host(row: &J) -> Source<'_> {
    Source {
        mode: row["mode"].as_str().unwrap_or("normal"),
        events: RefCell::new(vec![]),
        fail: Cell::new(None),
    }
}
pub(super) fn expected(row: &J) -> (primops::Outcome, J) {
    let r = &row["result"];
    assert_eq!(r["kind"], "ok");
    let v = &r["value"][1];
    if v[0] == atom("ok") {
        return (
            primops::Outcome::Complete {
                data: bytes(&v[1]),
                gas_spent: uint(&v[2]),
                state: Box::new(state(row, &v[3])),
            },
            logical(field(&v[3], "chain_state").clone()),
        );
    }
    assert_eq!(v[0], atom("primop_error"));
    let error = if v[1][0] == "tuple" {
        let p = &v[1][1];
        if p[0] == atom("illegal_oracle_primop_call") {
            return (
                primops::Outcome::Error(primops::Error::IllegalOracle(Word::from_unsigned(&uint(
                    &p[1],
                )))),
                logical(field(&row["before"], "chain_state").clone()),
            );
        }
        assert_eq!(p[0], atom("out_of_gas"));
        let charge = &p[1][1];
        let resource = &charge[0][1];
        assert_eq!(resource[0], atom("primop"));
        let base = &resource[1][1];
        assert_eq!(base[0], atom("base"));
        assert_eq!(
            logical(field(&p[2], "chain_state").clone()),
            logical(field(&row["before"], "chain_state").clone())
        );
        primops::Error::BaseGas {
            op: Word::from_unsigned(&uint(&base[1])),
            required: uint(&charge[1]),
            state: Box::new(state(row, &row["before"])),
        }
    } else {
        match v[1][1].as_str().unwrap() {
            "out_of_gas" => primops::Error::OutOfGas,
            "illegal_primop_call" => primops::Error::IllegalCall,
            reason => primops::Error::Chain(reason.into()),
        }
    };
    (
        primops::Outcome::Error(error),
        logical(field(&row["before"], "chain_state").clone()),
    )
}
#[test]
fn spend_and_address_primitives_match_original_gas_auth_callbacks_and_errors() {
    let rows = CHAIN["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 3360);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let h = host(row);
        let actual = primops::call(
            &s,
            &row["call_gas"].as_u64().unwrap().into(),
            &row["value"].as_str().unwrap().parse().unwrap(),
            &bytes(&row["data"]),
            &chain,
            &h,
            Limits::default(),
        );
        assert_eq!(actual, Ok(expected(row)), "{row}");
        assert_eq!(
            logical(list(h.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
}
#[test]
fn spend_and_address_input_types_skip_dynamic_hashes_and_preserve_gas() {
    let rows = CHAIN["inputs"].as_array().unwrap();
    assert_eq!(rows.len(), 192);
    struct Types;
    impl calls::InputHost for Types {
        type Error = &'static str;
        fn function_types(
            &self,
            _: Word,
            _: Version,
            _: Word,
        ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
            panic!("static primitive types")
        }
    }
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let actual = calls::prepare_input(
            &s,
            calls::Input {
                target: Word::ZERO,
                offset: Word::from_unsigned(&row["pointer"].as_str().unwrap().parse().unwrap()),
                size: Word::from(777),
            },
            &Types,
            Limits::default(),
        );
        let r = &row["result"];
        let expected = if r["kind"] == "native" {
            assert!(
                r["reason"] == atom("out_of_gas")
                    || r["reason"]
                        == tuple(vec![atom("aevm_eval_error"), atom("out_of_gas"), int(0)])
            );
            Err(runtime::Fault::Native(runtime::Native::OutOfGas))
        } else {
            let v = &r["value"][1];
            Ok(calls::Prepared {
                data: bytes(&v[0]),
                output_type: Some(ty(&v[1])),
                state: Box::new(state(row, &v[2])),
            })
        };
        assert_eq!(actual, expected, "{row}");
        assert_eq!(s, before);
        assert_eq!(row["host_calls"], list(vec![]));
    }
}
#[test]
fn outer_spend_and_address_calls_preserve_chain_updates_and_special_errors() {
    let rows = CHAIN["outer"].as_array().unwrap();
    assert_eq!(rows.len(), 1344);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let h = host(row);
        check(row, outer_call::evaluate(&s, &chain, &h, Limits::default()));
        assert_eq!(
            logical(list(h.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
}
#[test]
fn chain_primitives_do_not_publish_staged_state_on_limits_or_host_failure() {
    let mut count = 0;
    for row in CHAIN["runs"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && r["mode"] == "normal"
            && r["value"] == "7"
            && r["result"]["value"][1][0] == atom("ok")
    }) {
        count += 1;
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let h = host(row);
        let data = bytes(&row["data"]);
        let gas = row["call_gas"].as_u64().unwrap().into();
        let value = 7.into();
        let expected = Ok(expected(row));
        for dim in 0..2 {
            let mut stopped = false;
            let mut reached = false;
            for n in [
                0, 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384,
            ] {
                let mut limits = Limits::default();
                if dim == 0 {
                    limits.max_work = n;
                } else {
                    limits.max_bytes = n;
                }
                match primops::call(&s, &gas, &value, &data, &chain, &h, limits) {
                    Err(runtime::Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    actual => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                }
            }
            assert!(stopped && reached);
        }
        let failures = if row["op"] == 1 {
            vec!["spend_tx", "spend"]
        } else {
            vec!["address"]
        };
        for failure in failures {
            h.fail.set(Some(failure));
            assert_eq!(
                primops::call(&s, &gas, &value, &data, &chain, &h, Limits::default()),
                Err(runtime::Fault::Host(failure))
            );
            h.fail.set(None);
            assert_eq!(
                primops::call(&s, &gas, &value, &data, &chain, &h, Limits::default()),
                expected
            );
        }
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(count, 8);
}

#[test]
fn malformed_chain_calls_preserve_base_check_order_and_native_catch() {
    let rows = EDGES["malformed"].as_array().unwrap();
    assert_eq!(rows.len(), 600);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let h = host(row);
        assert_eq!(
            primops::call(
                &s,
                &row["call_gas"].as_u64().unwrap().into(),
                &7.into(),
                &bytes(&row["data"]),
                &chain,
                &h,
                Limits::default()
            ),
            Ok(expected(row)),
            "{row}"
        );
        assert_eq!(
            logical(list(h.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
    }
}
#[test]
fn full_chain_invocations_preserve_finalization_and_parent_rollback() {
    let rows = EDGES["invocations"].as_array().unwrap();
    assert_eq!(rows.len(), 600);
    let mut spent_then_failed = 0;
    let mut stored_then_failed = 0;
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let h = host(row);
        let input = ty(&row["type"]);
        assert_eq!(
            runtime::initialize(&s, Some(&input), Limits::default()).unwrap(),
            state(row, &row["ready"])
        );
        let actual = outer_call::invoke(
            &s,
            Some(&input),
            &chain,
            &h,
            OrderProfile::Otp26Word64Crc32c,
            Limits::default(),
        );
        if !matches!(&actual, Ok(Outcome::Complete { .. })) {
            let events = h.events.borrow();
            if events
                .iter()
                .any(|e| e[0] == "tuple" && e[1][0] == atom("spend"))
            {
                spent_then_failed += 1;
            }
            if events
                .iter()
                .any(|e| e[0] == "tuple" && e[1][0] == atom("set_store"))
            {
                stored_then_failed += 1;
            }
        }
        check(row, actual);
        assert_eq!(
            logical(list(h.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert!(spent_then_failed > 0);
    assert!(stored_then_failed > 0);
}

#[test]
fn full_chain_invocations_retry_after_limits_and_each_host_failure() {
    let mut count = 0;
    for row in EDGES["invocations"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && r["mode"] == "normal"
            && field(&r["before"], "protocol_version") == &int(4)
            && field(&r["before"], "gas") == &int(30000)
    }) {
        count += 1;
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let h = host(row);
        let input = ty(&row["type"]);
        let run = |limits| {
            outer_call::invoke(
                &s,
                Some(&input),
                &chain,
                &h,
                OrderProfile::Otp26Word64Crc32c,
                limits,
            )
        };
        let expected = run(Limits::default());
        check(row, expected.clone());
        for dim in 0..4 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dim {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    2 => limits.max_depth = n,
                    _ => limits.max_memory_words = n,
                }
                match run(limits) {
                    Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    actual => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                }
            }
            assert!(stopped && reached);
        }
        let failures = if row["op"] == 1 {
            vec!["balance", "spend_tx", "spend", "set_store"]
        } else {
            vec!["balance", "address", "set_store"]
        };
        for failure in failures {
            h.fail.set(Some(failure));
            assert_eq!(
                run(Limits::default()),
                Err(Fault::Runtime(runtime::Fault::Host(failure)))
            );
            h.fail.set(None);
            assert_eq!(run(Limits::default()), expected);
        }
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(count, 4);
}
