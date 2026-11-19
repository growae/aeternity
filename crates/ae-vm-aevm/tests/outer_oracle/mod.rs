use super::*;
use ae_types::Id;
use ae_vm_aevm::{
    calls::{self, InputHost},
    heap::{Type as T, Value as V},
    primops::{self, AddressKind, ChainHost, HostResult, OracleRequest},
    runtime::Native,
};
static ORACLE: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("../fixtures/erlang_oracle_primops.json")));
struct OracleSource<'a>(Source<'a>);
fn host(row: &J) -> OracleSource<'_> {
    OracleSource(Source {
        mode: row["mode"].as_str().unwrap(),
        events: RefCell::new(vec![]),
        fail: Cell::new(None),
    })
}
impl OracleSource<'_> {
    fn format(&self) -> T {
        match self.0.mode {
            "string" => T::String,
            "tuple" => T::Tuple(vec![T::Word, T::String]),
            _ => T::Word,
        }
    }
    fn value(&self) -> V {
        match self.0.mode {
            "string" => V::String(b"abc".to_vec()),
            "tuple" => V::Tuple(vec![V::Integer(7.into()), V::String(b"abc".to_vec())]),
            _ => V::Integer(7.into()),
        }
    }
}
fn type_json(t: &T) -> J {
    match t {
        T::Word => atom("word"),
        T::String => atom("string"),
        T::Tuple(ts) => tuple(vec![
            atom("tuple"),
            list(ts.iter().map(type_json).collect()),
        ]),
        _ => panic!("{t:?}"),
    }
}
impl StoreHost for OracleSource<'_> {
    type Chain = J;
    type Error = &'static str;
    fn set_store(&self, s: Store, c: &J) -> Result<J, runtime::Fault<Self::Error>> {
        self.0.set_store(s, c)
    }
}
impl ChainHost for OracleSource<'_> {
    type Spend = J;
    type Oracle = ();
    type Name = ();
    fn get_store(&self, _: &J) -> Result<Store, runtime::Fault<Self::Error>> {
        panic!("no map reads")
    }
    fn prepare_spend(
        &self,
        _: Id,
        _: &BigInt,
        _: &J,
        _: &mut Budget,
    ) -> HostResult<J, Self::Error> {
        panic!("no spend")
    }
    fn apply_spend(&self, _: J, _: &J, _: &mut Budget) -> HostResult<J, Self::Error> {
        panic!("no spend")
    }
    fn address(
        &self,
        _: AddressKind,
        _: Word,
        _: &J,
        _: &mut Budget,
    ) -> HostResult<Word, Self::Error> {
        panic!("no address")
    }
    fn oracle(&self, r: OracleRequest<'_>, _: &J, _: &mut Budget) -> HostResult<V, Self::Error> {
        let (event, value, format) = match r {
            OracleRequest::QueryFormat(k) => (
                vec![atom("query_format"), binary(k.0)],
                V::Type(self.format()),
                true,
            ),
            OracleRequest::ResponseFormat(k) => (
                vec![atom("response_format"), binary(k.0)],
                V::Type(self.format()),
                true,
            ),
            OracleRequest::Question(k, q) => (
                vec![atom("question"), binary(k.0), binary(q.0)],
                self.value(),
                false,
            ),
            OracleRequest::Answer(k, q) => (
                vec![atom("answer"), binary(k.0), binary(q.0)],
                V::Option((self.0.mode != "none").then(|| Box::new(self.value()))),
                false,
            ),
            OracleRequest::QueryFee(k) => (
                vec![atom("fee"), binary(k.0)],
                if self.0.mode == "none" {
                    V::Option(None)
                } else {
                    V::Integer(7.into())
                },
                false,
            ),
            OracleRequest::Check(k, q, r) => (
                vec![atom("check"), binary(k.0), type_json(q), type_json(r)],
                V::Integer(1.into()),
                false,
            ),
            OracleRequest::CheckQuery(k, i, q, r) => (
                vec![
                    atom("check_query"),
                    binary(k.0),
                    binary(i.0),
                    type_json(q),
                    type_json(r),
                ],
                V::Integer(1.into()),
                false,
            ),
            _ => panic!("no TTL lookup"),
        };
        self.0.event(tuple(event));
        self.0.check("oracle").map_err(runtime::Fault::Host)?;
        match self.0.mode {
            "error" => Ok(Err("no_such_oracle".into())),
            "throw" => Err(runtime::Fault::Native(Native::ChainException {
                class: "error",
                reason: "host_failure",
            })),
            "bad_value" if !format => Ok(Ok(V::Boolean(true))),
            _ => Ok(Ok(value)),
        }
    }
}
impl Host for OracleSource<'_> {
    fn function_types(
        &self,
        _: Word,
        _: Version,
        _: Word,
        _: &J,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
        panic!("no remote types")
    }
    fn balance(&self, k: Word, _: &J) -> Result<BigInt, Self::Error> {
        self.0.event(tuple(vec![atom("balance"), binary(k.0)]));
        self.0.check("balance")?;
        Ok(100.into())
    }
    fn call(
        &self,
        _: Request<'_>,
        _: &J,
        _: &mut Budget,
    ) -> Result<(Response, J), Fault<Self::Error, J>> {
        panic!("no remote call")
    }
}
impl InputHost for OracleSource<'_> {
    type Error = &'static str;
    fn function_types(
        &self,
        _: Word,
        _: Version,
        _: Word,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
        panic!("no remote types")
    }
    fn oracle_format(
        &self,
        k: Word,
        response: bool,
        b: &mut Budget,
    ) -> HostResult<TypeRef, Self::Error> {
        let r = if response {
            OracleRequest::ResponseFormat(k)
        } else {
            OracleRequest::QueryFormat(k)
        };
        Ok(self.oracle(r, &J::Null, b)?.map(|v| {
            let V::Type(t) = v else { panic!("type") };
            ty(&type_json(&t))
        }))
    }
}
fn events(h: &OracleSource<'_>, row: &J) {
    assert_eq!(
        logical(list(h.0.events.borrow().clone())),
        logical(row["host_calls"].clone()),
        "{} {}",
        row["op"],
        row["mode"]
    );
}
#[test]
fn oracle_read_primitives_match_original_gas_gates_auth_values_and_callback_order() {
    let rows = ORACLE["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1400);
    for row in rows {
        let s = state(row, &row["before"]);
        let original = s.clone();
        let c = logical(field(&row["before"], "chain_state").clone());
        let h = host(row);
        let actual = primops::call(
            &s,
            &row["call_gas"].as_u64().unwrap().into(),
            &0.into(),
            &bytes(&row["data"]),
            &c,
            &h,
            Limits::default(),
        );
        assert_eq!(
            actual,
            Ok(outer_chain::expected(row)),
            "{} {} {}",
            row["vm"],
            row["op"],
            row["mode"]
        );
        events(&h, row);
        assert_eq!(s, original);
    }
}
fn input_expected(row: &J) -> Result<calls::Prepared, runtime::Fault<&'static str>> {
    let r = &row["result"];
    if r["kind"] == "native" {
        let n = if r["reason"] == atom("out_of_gas")
            || r["reason"] == tuple(vec![atom("aevm_eval_error"), atom("out_of_gas"), int(0)])
        {
            Native::OutOfGas
        } else {
            assert_eq!(r["reason"], atom("host_failure"));
            Native::ChainException {
                class: "error",
                reason: "host_failure",
            }
        };
        return Err(runtime::Fault::Native(n));
    }
    let v = &r["value"][1];
    Ok(calls::Prepared {
        data: bytes(&v[0]),
        output_type: Some(ty(&v[1])),
        state: Box::new(state(row, &v[2])),
    })
}
#[test]
fn oracle_and_name_input_types_hashes_and_serialization_match_original() {
    let rows = ORACLE["inputs"].as_array().unwrap();
    assert_eq!(rows.len(), 4800);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let h = host(row);
        let input = calls::Input {
            target: Word::ZERO,
            offset: Word::from_unsigned(&row["pointer"].as_str().unwrap().parse().unwrap()),
            size: Word::from_unsigned(&row["size"].as_str().unwrap().parse().unwrap()),
        };
        assert_eq!(
            calls::prepare_input(&s, input, &h, Limits::default()),
            input_expected(row),
            "{} {} {}",
            row["vm"],
            row["op"],
            row["mode"]
        );
        events(&h, row);
        assert_eq!(s, before);
    }
}
#[test]
fn oracle_outer_calls_match_original_type_lookup_gas_and_return_imports() {
    let rows = ORACLE["outer"].as_array().unwrap();
    assert_eq!(rows.len(), 960);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let c = logical(field(&row["before"], "chain_state").clone());
        let h = host(row);
        let actual = outer_call::evaluate(&s, &c, &h, Limits::default());
        if row["result"]["kind"] == "native" && row["result"]["reason"] == atom("host_failure") {
            assert_eq!(
                actual,
                Err(Fault::Runtime(runtime::Fault::Native(
                    Native::ChainException {
                        class: "error",
                        reason: "host_failure"
                    }
                )))
            );
        } else {
            check(row, actual);
        }
        events(&h, row);
        assert_eq!(s, before);
    }
}

#[test]
fn oracle_call_limits_and_backend_failures_remain_retryable() {
    for op in [104, 106] {
        let row = ORACLE["outer"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["vm"] == 6
                    && r["op"] == op
                    && r["mode"] == "string"
                    && field(&r["before"], "gas") == &int(30000)
                    && r["result"]["kind"] == "ok"
                    && r["result"]["value"][1][0] == atom("ok")
            })
            .unwrap();
        let s = state(row, &row["before"]);
        let before = s.clone();
        let c = logical(field(&row["before"], "chain_state").clone());
        let chain = c.clone();
        let h = host(row);
        let expected = outer_call::evaluate(&s, &c, &h, Limits::default());
        check(row, expected.clone());
        events(&h, row);
        for dim in 0..4 {
            let mut stopped = false;
            let mut done = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut l = Limits::default();
                match dim {
                    0 => l.max_work = n,
                    1 => l.max_bytes = n,
                    2 => l.max_depth = n,
                    _ => l.max_memory_words = n,
                };
                match outer_call::evaluate(&s, &c, &h, l) {
                    Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    r => {
                        assert_eq!(r, expected);
                        done = true;
                        break;
                    }
                }
            }
            assert!(stopped && done);
            assert_eq!(s, before);
            assert_eq!(c, chain);
        }
        h.0.fail.set(Some("oracle"));
        assert_eq!(
            outer_call::evaluate(&s, &c, &h, Limits::default()),
            Err(Fault::Runtime(runtime::Fault::Host("oracle")))
        );
        h.0.fail.set(None);
        assert_eq!(
            outer_call::evaluate(&s, &c, &h, Limits::default()),
            expected
        );
        assert_eq!(s, before);
        assert_eq!(c, chain);
    }
}
