use super::*;
use remote_tests::small;

static HOSTS: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_names.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    expand(&packed["root"], pool, pool.len())
});
pub(super) fn expand(value: &J, pool: &[J], ceiling: usize) -> J {
    match value {
        J::Object(fields) => {
            if let Some(index) = fields.get("$ref") {
                assert_eq!(fields.len(), 1);
                let index = index.as_u64().unwrap() as usize;
                assert!(index < ceiling, "fixture references must be acyclic");
                expand(&pool[index], pool, index)
            } else {
                J::Object(
                    fields
                        .iter()
                        .map(|(key, value)| (key.clone(), expand(value, pool, ceiling)))
                        .collect(),
                )
            }
        }
        J::Array(values) => J::Array(
            values
                .iter()
                .map(|value| expand(value, pool, ceiling))
                .collect(),
        ),
        value => value.clone(),
    }
}

fn unpack(row: &J) -> J {
    let mut row = row.clone();
    for field in rows(&HOSTS["input_fields"]) {
        let field = string(field);
        let index = row[field].as_u64().unwrap() as usize;
        row[field] = HOSTS["inputs"][index].clone();
    }
    row
}
#[test]
fn names_reads_writes_delegations_and_rollback_match_original_instruction_and_whole_engine() {
    assert_eq!(HOSTS["base"], FIXTURE["base"]);
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for packed in rows(&HOSTS["cases"]) {
        let row = unpack(packed);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let host = context_tests::chain_base(&row);
        for run in rows(&row["runs"]) {
            let (mut state, mut limits) =
                context_tests::start_with_host(&executor, &row, &run["gas"], host.clone());
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let expected = &HOSTS["results"][step.as_u64().unwrap() as usize];
                let label = format!(
                    "case {} {} protocol {} vm {} gas {} step {}",
                    row["case_id"], row["name"], row["protocol"], row["vm"], run["gas"], index
                );
                let out = executor
                    .advance_remote_program(&state, limits)
                    .unwrap_or_else(|e| panic!("{label}: {e:?}"));
                let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    remote_tests::check(out, expected)
                }))
                .unwrap_or_else(|e| {
                    eprintln!("{label}");
                    std::panic::resume_unwind(e)
                });
                if let Some((next, budget)) = checked {
                    state = next;
                    limits = budget;
                } else {
                    assert_eq!(index + 1, rows(&run["steps"]).len());
                    break;
                }
            }
        }
    }
}

#[test]
fn auction_overrides_match_original_fate_traces_under_both_observed_atom_profiles() {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_auction_vm_override.json")).unwrap();
    let pool = rows(&packed["values"]);
    let data = expand(&packed["root"], pool, pool.len());
    let cases = rows(&data["fate"]["cases"]);
    assert_eq!(cases.len(), 128);
    assert_eq!(data["fate"]["base"], FIXTURE["base"]);
    for profile in [&data["fate"]["profile"], &data["repeat_profile"]] {
        let order = otp_order::profile(profile);
        let overrides = Overrides::default();
        let sources = remote_tests::sources();
        for row in cases {
            let mut context = context(row);
            context.current_contract = small(1);
            context.reserved_gas = 18.into();
            let executor = Executor {
                context: &context,
                order: &order,
                overrides: &overrides,
                contracts: &sources,
            };
            let mut host = context_tests::chain_base(row);
            host.name_claim_bid_timeout = row["override"]
                .as_str()
                .map(|s| ae_types::Unsigned::from_decimal(s).unwrap());
            for run in rows(&row["runs"]) {
                let (mut state, mut limits) =
                    context_tests::start_with_host(&executor, row, &run["gas"], host.clone());
                for (index, step) in rows(&run["steps"]).iter().enumerate() {
                    let expected = &data["fate"]["results"][step.as_u64().unwrap() as usize];
                    let out = executor.advance_remote_program(&state, limits).unwrap();
                    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        remote_tests::check(out, expected)
                    }))
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "auction {} protocol {} vm {} override {} gas {} step {}",
                            row["name"],
                            row["protocol"],
                            row["vm"],
                            row["override"],
                            run["gas"],
                            index
                        );
                        std::panic::resume_unwind(e)
                    });
                    if let Some((next, budget)) = checked {
                        state = next;
                        limits = budget;
                    } else {
                        assert_eq!(index + 1, rows(&run["steps"]).len());
                        break;
                    }
                }
            }
        }
    }
}

use ae_state::names::hashing;
use ae_types::{IdTag, Unsigned};
use ae_vm_fate::machine::program::remote::{
    self, Host, HostFailure,
    names::{Pointee, RawPointer, Request, Ttl},
};
use ae_vm_fate::value::ObjectPayload;
use oracle_write_tests::{Faults, fault_host};
fn selected(id: u64) -> J {
    unpack(
        rows(&HOSTS["cases"])
            .iter()
            .find(|r| r["case_id"] == id && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap(),
    )
}
fn terminal(row: &J) -> &J {
    let run = rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "200000")
        .unwrap();
    &HOSTS["results"][run["terminal"].as_u64().unwrap() as usize]
}
fn request(id: u64) -> Request {
    let name = b"abcdefghijklmn.chain";
    let hash = hashing::name_hash(name).0;
    let owner = small(1).to_vec();
    match id {
        24 => Request::Preclaim {
            owner,
            commitment: hashing::commitment_hash(name, &Unsigned::from(1))
                .unwrap()
                .0,
        },
        36 => Request::Claim {
            owner,
            name: name.to_vec(),
            salt: 1.into(),
            fee: BigInt::from_bytes_be(
                num_bigint::Sign::Plus,
                ae_protocol::names::claim_fee(
                    std::num::NonZeroUsize::new(14).unwrap(),
                    ae_protocol::Protocol::Ceres,
                )
                .as_bytes(),
            ),
        },
        48 => Request::Update {
            owner,
            hash,
            ttl: Some(Ttl {
                relative: true,
                value: Value::Integer(10.into()),
            }),
            client_ttl: Some(Value::Integer(300.into())),
            pointers: Some(
                [
                    ("oracle_pubkey", IdTag::Oracle, 3),
                    ("contract_pubkey", IdTag::Contract, 4),
                    ("channel", IdTag::Channel, 5),
                    ("account_pubkey", IdTag::Account, 2),
                ]
                .into_iter()
                .map(|(key, tag, id)| RawPointer {
                    key: key.as_bytes().to_vec(),
                    value: Pointee::Id(tag, ObjectPayload::Bytes(small(id).to_vec())),
                })
                .collect(),
            ),
        },
        60 => Request::Transfer {
            owner,
            hash,
            recipient: small(2).to_vec(),
        },
        72 => Request::Revoke { owner, hash },
        _ => unreachable!(),
    }
}
#[test]
fn name_write_failures_preserve_account_name_expiry_and_primitive_cache_checkpoints() {
    for id in [24, 36, 48, 60, 72] {
        let row = selected(id);
        let base = context_tests::chain_base(&row);
        let faults = Faults::default();
        let req = request(id);
        let mut failed = 0;
        let mut success = false;
        for allowed in 0..256 {
            faults.writes_left.set(None);
            let mut host = fault_host(base.clone(), &faults);
            faults.writes_left.set(Some(allowed));
            let before = format!("{host:?}");
            match host.name_write(&req, 6, VmVersion::Three, Limits::default()) {
                Err(HostFailure::Backend(_)) => {
                    failed += 1;
                    faults.writes_left.set(Some(allowed));
                    assert_eq!(format!("{host:?}"), before, "case {id} write {allowed}");
                    faults.writes_left.set(None);
                    host.name_write(&req, 6, VmVersion::Three, Limits::default())
                        .unwrap();
                    remote_tests::assert_chain(&host, &terminal(&row)["state"]["chain"]);
                }
                Ok(_) => {
                    success = true;
                    remote_tests::assert_chain(&host, &terminal(&row)["state"]["chain"]);
                    break;
                }
                other => panic!("case {id} write {allowed}: {other:?}"),
            }
        }
        assert!(success && failed > 1, "case {id}, failed={failed}");
    }
}
#[test]
fn name_host_read_failures_and_overlay_limits_are_retryable() {
    let faults = Faults::default();
    let mut host = fault_host(context_tests::chain_base(&selected(0)), &faults);
    let name = b"abcdefghijklmn.chain";
    faults.fail_reads.set(true);
    let before = format!("{host:?}");
    assert!(matches!(
        host.name_read(name, Some(b"account_pubkey"), Limits::default()),
        Err(HostFailure::Backend(_))
    ));
    assert_eq!(format!("{host:?}"), before);
    faults.fail_reads.set(false);
    for limits in [
        Limits {
            max_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_nodes: 0,
            ..Limits::default()
        },
    ] {
        let before = format!("{host:?}");
        assert!(matches!(
            host.name_read(name, None, limits),
            Err(HostFailure::ResourceLimit)
        ));
        assert_eq!(format!("{host:?}"), before);
    }
    host.name_read(name, Some(b"account_pubkey"), Limits::default())
        .unwrap();
    faults.fail_reads.set(true);
    assert!(
        host.name_read(name, Some(b"account_pubkey"), Limits::default())
            .unwrap()
            .is_some()
    );
    faults.fail_reads.set(false);
    for id in [24, 36, 48, 60, 72] {
        let mut host = context_tests::chain_base(&selected(id));
        let before = format!("{host:?}");
        assert!(
            matches!(
                host.name_write(
                    &request(id),
                    6,
                    VmVersion::Three,
                    Limits {
                        max_bytes: 1,
                        ..Limits::default()
                    }
                ),
                Err(HostFailure::ResourceLimit)
            ),
            "case {id}"
        );
        assert_eq!(format!("{host:?}"), before);
        host.name_write(&request(id), 6, VmVersion::Three, Limits::default())
            .unwrap();
        remote_tests::assert_chain(&host, &terminal(&selected(id))["state"]["chain"]);
        let before = format!("{host:?}");
        assert!(matches!(
            host.name_write(
                &request(48),
                6,
                VmVersion::Three,
                Limits {
                    max_nodes: 0,
                    ..Limits::default()
                }
            ),
            Err(HostFailure::ResourceLimit)
        ));
        assert_eq!(format!("{host:?}"), before);
    }
}
#[test]
fn protected_name_host_faults_escape_contract_catches_and_retry_original_outcomes() {
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    let cases = rows(&HOSTS["cases"])
        .iter()
        .filter(|r| r["name"] == "protected_name" && r["protocol"] == 6 && r["vm"] == 8);
    for packed in cases {
        let row = unpack(packed);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let faults = Faults::default();
        let host = fault_host(context_tests::chain_base(&row), &faults);
        let (mut state, mut limits) =
            remote_tests::start(&ex, &row, &J::String("200000".into()), host);
        while state.session.current_contract == small(1) {
            let remote::Outcome::Continue {
                state: next,
                remaining_limits,
            } = ex.advance_remote_program(&state, limits).unwrap()
            else {
                panic!()
            };
            state = next;
            limits = remaining_limits;
        }
        let loaded = &state.session.code_cache[&small(2)].code;
        let op = loaded
            .functions
            .values()
            .next()
            .unwrap()
            .blocks
            .values()
            .next()
            .unwrap()[0]
            .opcode;
        let read = matches!(op, Opcode::AensResolve | Opcode::AensLookup);
        if read {
            faults.fail_reads.set(true)
        } else {
            faults.writes_left.set(Some(0))
        }
        let before = format!("{state:?}");
        assert!(
            matches!(
                ex.advance_remote_program(&state, limits),
                Err(Error::Backend(_))
            ),
            "case {}",
            row["case_id"]
        );
        assert_eq!(format!("{state:?}"), before);
        faults.fail_reads.set(false);
        faults.writes_left.set(None);
        assert!(
            remote_tests::check(
                ex.run_remote_program(&state, limits, 100).unwrap(),
                terminal(&row)
            )
            .is_none()
        );
    }
}
