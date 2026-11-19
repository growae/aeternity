// aefa_fate:abort/2, Aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;

enum Part<'t, 'a> {
    Text(&'static [u8]),
    Term(&'t Term<'a>, Style, i32),
}
pub fn native_abort(
    reason: &Term<'_>,
    profile: Profile,
    limits: Limits,
) -> Result<(Vec<u8>, Limits), Error> {
    use Part::{Term as T, Text as S};
    let depth = profile.depth();
    let p = |t| T(t, Style::Pretty, depth);
    let w = |t| T(t, Style::Write, depth);
    let (tag, args) = match reason {
        Term::Atom(a) => (a.as_ref(), &[][..]),
        Term::Tuple(v) => match v.split_first() {
            Some((Term::Atom(a), rest)) => (a.as_ref(), rest),
            _ => return Err(Error::InvalidValue),
        },
        _ => return Err(Error::InvalidValue),
    };
    let parts = match (tag, args) {
        ("invalid_tuple_size", [a]) => vec![S(b"Invalid tuple size: "), p(a)],
        ("element_index_out_of_bounds", [a]) => {
            vec![S(b"Bad index argument to element, Index: "), p(a)]
        }
        ("bad_variant_tag", [a]) => vec![
            S(b"Type error in switch: tag "),
            p(a),
            S(b" is larger than switch op"),
        ],
        ("bad_variant_size", [a]) => vec![S(b"Type error in switch: wrong size "), p(a)],
        ("type_error", [a, b]) => vec![S(b"Bad arguments to "), p(a), S(b": "), p(b)],
        ("hd_on_empty_list", []) => vec![S(b"Head on empty list")],
        ("tl_on_empty_list", []) => vec![S(b"Tail on empty list")],
        ("arithmetic_error", [a]) => vec![S(b"Arithmetic error: "), p(a)],
        ("division_by_zero", []) => vec![S(b"Arithmetic error: division by zero")],
        ("mod_by_zero", []) => vec![S(b"Arithmetic error: mod by zero")],
        ("pow_too_large_exp", []) => vec![S(b"Arithmetic error: pow with too large exponent")],
        ("missing_map_key", []) => vec![S(b"Maps: Key does not exist")],
        ("bad_store_map_id", []) => vec![S(b"Maps: Map does not exist")],
        ("undefined_var", [a]) => vec![S(b"Undefined var: "), p(a)],
        ("bad_return_type", [a, b]) => vec![
            S(b"Type error on return: "),
            p(a),
            S(b" is not of type "),
            p(b),
        ],
        ("remote_type_mismatch", []) => {
            vec![S(b"Type of remote function does not match expected type")]
        }
        ("function_arity_mismatch", [given, expected]) => vec![
            S(b"Expected "),
            p(expected),
            S(b" arguments, got "),
            p(given),
        ],
        ("value_does_not_match_type", [a, b]) => vec![
            S(b"Type error on call: "),
            p(a),
            S(b" is not of type "),
            p(b),
        ],
        ("trying_to_reach_bb", [a]) => vec![S(b"Trying to jump to non existing bb: "), p(a)],
        ("trying_to_call_function", [a]) => vec![S(b"Trying to call undefined function: "), w(a)],
        ("invalid_init_call", []) => vec![S(b"Calling init is not allowed in this context")],
        ("trying_to_call_contract", [a]) => vec![
            S(b"Trying to call invalid contract: "),
            T(a, Style::Write, -1),
        ],
        ("not_allowed_in_auth_context", [a]) => vec![
            S(b"Operation "),
            p(a),
            S(b" not allowed in GA Authentication context"),
        ],
        ("not_allowed_offchain", [a]) => vec![S(b"Operation "), p(a), S(b" not allowed off chain")],
        ("negative_value_in_call", []) => vec![S(b"Trying to transfer negative value in call")],
        ("call_error", [a]) => vec![S(b"Error in call: "), w(a)],
        ("function_is_not_payable", [a]) => {
            vec![S(b"Function with hash "), w(a), S(b" is not payable")]
        }
        ("function_is_private", [a]) => vec![S(b"Function with hash "), w(a), S(b" is private")],
        ("primop_error", [a, b]) => vec![S(b"Error in "), w(a), S(b": "), w(b)],
        ("reentrant_call", []) => vec![S(b"Reentrant call")],
        ("log_illegal_int", [a]) => vec![S(b"Illegal integer in log: "), w(a)],
        ("log_illegal_bits", []) => vec![S(b"Illegal bits in log")],
        ("out_of_gas", []) => vec![S(b"Out of gas")],
        ("bad_bytecode", []) => vec![S(b"Bad byte code")],
        ("disabled_operation", [a]) => vec![S(b"Error: operation "), p(a), S(b" is disabled")],
        ("pop_empty_stack", [a]) => vec![S(b"Stack overflow when pop:ing "), p(a), S(b" elements")],
        ("bad_map", [a, b]) => vec![
            S(b"Operation "),
            p(a),
            S(b" expected a map as argument, got "),
            p(b),
        ],
        ("op_not_implemented", [a]) => vec![S(b"Operation "), p(a), S(b" is not implemented")],
        ("auth_tx_type_not_handled", [a]) => {
            vec![S(b"Tx type "), p(a), S(b" is not handled by Auth.tx")]
        }
        _ => return Err(Error::InvalidValue),
    };
    let mut work = Work::new(limits);
    work.input(reason, 0)?;
    let mut out = Vec::new();
    let mut indent = 0;
    for part in parts {
        let start = out.len();
        match part {
            S(s) => work.push(&mut out, s)?,
            T(t, style, d) => render(t, style, d, indent + 1, &mut work, &mut out)?,
        }
        for &c in &out[start..] {
            indent = match c {
                b'\n' => 0,
                b'\t' => (indent / 8 + 1) * 8,
                _ => indent + 1,
            };
        }
    }
    Ok((out, work.remaining()))
}
