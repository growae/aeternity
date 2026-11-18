use ae_vm_fate::{
    Error, Limits, abi,
    code::{Attribute, Code, CodeError, Function, Instruction, Operand, symbol_identifier},
    encoding,
    opcodes::Opcode,
    types::Type,
    value::Value,
};
use serde_json::{Value as J, json};
use std::{collections::BTreeMap, sync::LazyLock};
static FIXTURE: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_fate_code.json")).unwrap());
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn limits() -> Limits {
    Limits {
        max_depth: 512,
        ..Limits::default()
    }
}
fn encoded(v: &Value) -> J {
    json!(encoding::encode(v, limits()).ok().map(hex::encode))
}
fn ty(v: &J) -> Type {
    encoding::decode_type(&bytes(v), limits()).unwrap()
}
fn parse(j: &J) -> Code {
    let functions = j["functions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            let Type::Tuple(arguments) = ty(&f["arguments"]) else {
                panic!()
            };
            let function = Function {
                attributes: f["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|a| match a.as_str().unwrap() {
                        "private" => Attribute::Private,
                        "payable" => Attribute::Payable,
                        _ => panic!(),
                    })
                    .collect(),
                arguments,
                returns: ty(&f["returns"]),
                blocks: f["blocks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|b| {
                        (
                            b[0].as_u64().unwrap() as usize,
                            b[1].as_array()
                                .unwrap()
                                .iter()
                                .map(|i| Instruction {
                                    opcode: Opcode::from_byte(i["opcode"].as_u64().unwrap() as u8)
                                        .unwrap(),
                                    operands: i["args"]
                                        .as_array()
                                        .unwrap()
                                        .iter()
                                        .map(|a| {
                                            if a[0] == "stack" {
                                                return Operand::Stack;
                                            }
                                            let v =
                                                encoding::decode(&bytes(&a[1]), limits()).unwrap();
                                            match a[0].as_str().unwrap() {
                                                "arg" => Operand::Argument(v),
                                                "var" => Operand::Variable(v),
                                                "immediate" => Operand::Immediate(v),
                                                _ => panic!(),
                                            }
                                        })
                                        .collect(),
                                })
                                .collect(),
                        )
                    })
                    .collect(),
            };
            (bytes(&f["id"]), function)
        })
        .collect();
    let symbols = encoding::decode(&bytes(&j["symbols"]), limits()).unwrap();
    let annotations = encoding::decode(&bytes(&j["annotations"]), limits()).unwrap();
    Code {
        functions,
        symbols,
        annotations,
    }
}
fn metadata(code: &Code) -> J {
    let functions: Vec<J> = code.functions.iter().map(|(id, f)| {
        let attributes: Vec<_> = f.attributes.iter().map(|a| match a {
            Attribute::Private => "private",
            Attribute::Payable => "payable",
        }).collect();
        let blocks: Vec<_> = f.blocks.iter().map(|(n, block)| {
            let instructions: Vec<_> = block.iter().map(|instruction| {
                let arguments: Vec<_> = instruction.operands.iter().map(|arg| match arg {
                    Operand::Stack => json!(["stack", null]),
                    Operand::Argument(v) => json!(["arg", encoded(v)]),
                    Operand::Variable(v) => json!(["var", encoded(v)]),
                    Operand::Immediate(v) => json!(["immediate", encoded(v)]),
                }).collect();
                json!({"opcode": instruction.opcode as u8, "args": arguments})
            }).collect();
            json!([n, instructions])
        }).collect();
        json!({
            "id": hex::encode(id),
            "attributes": attributes,
            "arguments": hex::encode(encoding::encode_type(&Type::Tuple(f.arguments.clone()), limits()).unwrap()),
            "returns": hex::encode(encoding::encode_type(&f.returns, limits()).unwrap()),
            "blocks": blocks,
        })
    }).collect();
    json!({
        "functions": functions,
        "symbols": encoded(&code.symbols),
        "annotations": encoded(&code.annotations),
        "canonical": code.encode(limits()).ok().map(hex::encode),
    })
}
#[test]
fn all_opcode_metadata_and_historical_base_gas_match_erlang() {
    let rows: J = serde_json::from_str(include_str!("fixtures/erlang_fate_opcodes.json")).unwrap();
    assert_eq!(Opcode::ALL.len(), rows.as_array().unwrap().len());
    for b in 0..=255u8 {
        assert_eq!(
            Opcode::from_byte(b).is_some(),
            rows.as_array().unwrap().iter().any(|r| r["opcode"] == b)
        );
    }
    for row in rows.as_array().unwrap() {
        let op = Opcode::from_byte(row["opcode"].as_u64().unwrap() as u8).unwrap();
        let i = op.info();
        assert_eq!(i.name, row["name"]);
        assert_eq!(i.arity, row["arity"].as_u64().unwrap() as usize);
        assert_eq!(i.end_block, row["end_bb"]);
        assert_eq!(i.in_auth, row["in_auth"]);
        assert_eq!(i.offchain, row["offchain"]);
        assert_eq!(
            i.literal_operands,
            row["format"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .filter_map(|(i, f)| (f != "a").then_some(i))
                .collect::<Vec<_>>()
        );
        for p in 1..=8 {
            assert_eq!(
                op.gas_at_protocol(p),
                row["prices"][(p - 1) as usize].as_u64().unwrap()
            );
        }
    }
}
#[test]
fn every_opcode_operand_form_and_structural_encoding_match_erlang() {
    for (i, row) in FIXTURE["encode"].as_array().unwrap().iter().enumerate() {
        let c = parse(&row["code"]);
        assert_eq!(
            c.encode(limits()).ok().map(hex::encode),
            row["encoded"].as_str().map(str::to_string),
            "encode {i}"
        );
    }
}
#[test]
fn fate_loader_admission_normalization_and_function_replacement_match_erlang() {
    for (i, row) in FIXTURE["decode"].as_array().unwrap().iter().enumerate() {
        let r = Code::decode(&bytes(&row["bytes"]), limits());
        let actual = r.as_ref().map(metadata).unwrap_or(J::Null);
        assert_eq!(actual, row["result"], "decode {i}: {r:?}");
    }
}
#[test]
fn fate_symbols_init_stripping_and_call_abi_match_erlang() {
    for row in FIXTURE["symbols"].as_array().unwrap() {
        assert_eq!(
            symbol_identifier(&bytes(&row["name"])).as_slice(),
            bytes(&row["id"])
        );
    }
    let mut code = Code::default();
    code.insert_function(
        b"init",
        Function {
            attributes: vec![],
            arguments: vec![],
            returns: Type::Integer,
            blocks: BTreeMap::from([(
                0,
                vec![Instruction {
                    opcode: Opcode::Returnr,
                    operands: vec![Operand::Immediate(Value::Integer(0.into()))],
                }],
            )]),
        },
    )
    .unwrap();
    code.insert_function(
        b"foo",
        Function {
            attributes: vec![Attribute::Payable],
            arguments: vec![Type::Integer],
            returns: Type::Integer,
            blocks: BTreeMap::from([(
                0,
                vec![Instruction {
                    opcode: Opcode::Returnr,
                    operands: vec![Operand::Argument(Value::Integer(0.into()))],
                }],
            )]),
        },
    )
    .unwrap();
    code.insert_comment(Value::Integer(7.into()), b"hello")
        .unwrap();
    assert_eq!(metadata(&code), FIXTURE["init"]);
    for row in FIXTURE["lookups"].as_array().unwrap() {
        let h = bytes(&row["hash"]);
        let name = match code.function_name(&h) {
            Err(_) => J::Null,
            Ok(None) => json!(false),
            Ok(Some(v)) => encoded(v),
        };
        assert_eq!(name, row["name"]);
        let types = code
            .function_types(&h)
            .map(|(a, r)| {
                json!([
                    hex::encode(encoding::encode_type(&Type::Tuple(a.to_vec()), limits()).unwrap()),
                    hex::encode(encoding::encode_type(r, limits()).unwrap())
                ])
            })
            .unwrap_or(json!(false));
        assert_eq!(types, row["types"]);
    }
    let before = code.clone();
    code.insert_symbol(b"foo").unwrap();
    assert_eq!(code, before);
    let Value::Map(symbols) = &mut code.symbols else {
        panic!()
    };
    symbols.insert(
        Value::String(symbol_identifier(b"foo").to_vec()),
        Value::String(b"collision".to_vec()),
    );
    let before = code.clone();
    assert_eq!(code.insert_symbol(b"foo"), Err(CodeError::SymbolCollision));
    assert_eq!(code, before);
    let mut code = parse(&FIXTURE["init"]);
    code.strip_init().unwrap();
    assert_eq!(metadata(&code), FIXTURE["stripped"]);
    for row in FIXTURE["calldata"].as_array().unwrap() {
        let Value::Tuple(args) = encoding::decode(&bytes(&row["args"]), limits()).unwrap() else {
            panic!()
        };
        assert_eq!(
            abi::create_calldata(&bytes(&row["name"]), args, limits()).unwrap(),
            bytes(&row["bytes"])
        );
    }
    for row in FIXTURE["calldata"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| &r["decode"])
        .chain(FIXTURE["bad_calldata"].as_array().unwrap())
    {
        let b = bytes(&row["bytes"]);
        let n = bytes(&row["name"]);
        let hash = abi::function_hash_from_calldata(&b, limits())
            .as_ref()
            .map(term_json)
            .unwrap_or(J::Null);
        assert_eq!(hash, row["hash"]);
        let arguments = abi::decode_calldata(&n, &b, limits())
            .map(|a| json!(a.iter().map(term_json).collect::<Vec<_>>()))
            .unwrap_or(J::Null);
        assert_eq!(arguments, row["arguments"]);
        assert_eq!(
            abi::decode_vm_calldata(&b, limits()).is_ok(),
            row["vm_valid"]
        );
        assert_eq!(
            abi::verify_init_calldata(&b, limits()).unwrap_or(false),
            row["init_valid"]
        );
        let mut auth_hash = [0; 32];
        auth_hash[..4].copy_from_slice(&symbol_identifier(b"foo"));
        let auth = match abi::check_auth_function(&b, auth_hash, limits()) {
            Ok(()) => "ok",
            Err(abi::AuthError::WrongFunction) => "wrong_auth_function",
            Err(abi::AuthError::BadData) => "bad_auth_data",
            Err(e) => panic!("{e:?}"),
        };
        assert_eq!(auth, row["auth"]);
    }
}
#[test]
fn code_and_abi_budgets_abort_distinctly_and_allow_a_larger_retry() {
    let function = Function {
        attributes: vec![],
        arguments: vec![],
        returns: Type::Any,
        blocks: BTreeMap::from([(
            0,
            vec![Instruction {
                opcode: Opcode::Return,
                operands: vec![],
            }],
        )]),
    };
    let mut code = Code::default();
    for i in 0u32..100 {
        code.functions
            .insert(i.to_be_bytes().to_vec(), function.clone());
    }
    let bytes = code.encode(limits()).unwrap();
    let constrained = Limits {
        max_nodes: 50,
        ..limits()
    };
    assert_eq!(
        code.encode(constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    assert_eq!(
        Code::decode(&bytes, constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    assert_eq!(Code::decode(&bytes, limits()).unwrap(), code);
    let constrained = Limits {
        max_bytes: bytes.len() - 1,
        ..limits()
    };
    assert_eq!(
        code.encode(constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    assert_eq!(
        Code::decode(&bytes, constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    let mut nested = Value::Integer(1.into());
    for _ in 0..10 {
        nested = Value::Tuple(vec![nested]);
    }
    let args = vec![nested];
    let call = abi::create_calldata(b"foo", args.clone(), limits()).unwrap();
    let constrained = Limits {
        max_depth: 3,
        ..limits()
    };
    assert_eq!(
        abi::create_calldata(b"foo", args.clone(), constrained),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::decode_calldata(b"foo", &call, constrained),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        abi::decode_calldata(b"foo", &call, limits()).unwrap(),
        args.into_iter().map(abi::AbiTerm::Data).collect::<Vec<_>>()
    );
    let mut nested = Type::Integer;
    for _ in 0..64 {
        nested = Type::List(Box::new(nested));
    }
    code.functions
        .get_mut(&0u32.to_be_bytes()[..])
        .unwrap()
        .arguments = vec![nested];
    assert_eq!(
        code.encode(constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    let bytes = code.encode(limits()).unwrap();
    assert_eq!(
        Code::decode(&bytes, constrained),
        Err(CodeError::Data(Error::ResourceLimit))
    );
    assert_eq!(Code::decode(&bytes, limits()).unwrap(), code);
}

fn term_json(t: &abi::AbiTerm) -> J {
    match t {
        abi::AbiTerm::Data(v) => json!({"data":encoded(v)}),
        abi::AbiTerm::Atom(a) => json!({"atom":a}),
        abi::AbiTerm::Types(ts) if ts.is_empty() => json!({"data":encoded(&Value::List(vec![]))}),
        abi::AbiTerm::Types(ts) => {
            json!({"types":ts.iter().map(|t|hex::encode(encoding::encode_type(t,limits()).unwrap())).collect::<Vec<_>>()})
        }
        abi::AbiTerm::Type(t) => {
            let atom = match t {
                Type::Integer => Some("integer"),
                Type::Boolean => Some("boolean"),
                Type::Bits => Some("bits"),
                Type::String => Some("string"),
                Type::Any => Some("any"),
                Type::ContractBytearray => Some("contract_bytearray"),
                Type::Object(k) => Some(match k {
                    ae_vm_fate::value::ObjectKind::Address => "address",
                    ae_vm_fate::value::ObjectKind::Contract => "contract",
                    ae_vm_fate::value::ObjectKind::Oracle => "oracle",
                    ae_vm_fate::value::ObjectKind::OracleQuery => "oracle_query",
                    ae_vm_fate::value::ObjectKind::Channel => "channel",
                }),
                _ => None,
            };
            if let Some(a) = atom {
                json!({"atom":a})
            } else {
                json!({"type":hex::encode(encoding::encode_type(t,limits()).unwrap())})
            }
        }
    }
}
