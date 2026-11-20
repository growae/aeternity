use ae_trie::Value;
use ae_tx::{
    ContractOperation, ContractTransaction, GaMeta, SignedTransaction, Transaction,
    TransactionLeaf, Wrapper, storage,
};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

fn call() -> SignedTransaction<Transaction> {
    let mut call = ContractTransaction::new(
        Id::new(IdTag::Account, [1; 32]),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, [2; 32]),
            abi_version: 3.into(),
            call_data: vec![0, 255],
        },
    )
    .unwrap();
    call.set_call_context(
        [9; 32],
        vec![
            1.into(),
            Unsigned::from_decimal("340282366920938463463374607431768211456").unwrap(),
        ],
    )
    .unwrap();
    SignedTransaction::new(Transaction::new(TransactionLeaf::Contract(call)), vec![])
}
fn modify(encoded: &[u8], action: impl FnOnce(&mut Vec<Value>)) -> Vec<u8> {
    let Value::List(mut fields) = Value::decode(&encoded[storage::MAGIC.len()..]).unwrap() else {
        panic!()
    };
    action(&mut fields);
    let mut out = storage::MAGIC.to_vec();
    out.extend(Value::List(fields).encode());
    out
}
#[test]
fn database_records_preserve_internal_call_context_and_zero_metered_size() {
    let original = call();
    let encoded = storage::encode(&original, Default::default()).unwrap();
    assert_eq!(
        storage::decode(&encoded, Default::default()).unwrap(),
        original
    );
    assert_ne!(
        SignedTransaction::<Transaction>::decode(&original.encode()).unwrap(),
        original
    );
    let zero = modify(&encoded, |f| f[1] = Value::Bytes(vec![0]));
    let restored = storage::decode(&zero, Default::default()).unwrap();
    assert_eq!(restored.transaction().view().wire_size(), 0);
    assert_eq!(restored.encode(), original.encode());
    assert_eq!(
        storage::encode(&restored, Default::default()).unwrap(),
        zero
    );
}
#[test]
fn database_metadata_rejects_corruption_and_applies_explicit_read_limits() {
    let original = call();
    let encoded = storage::encode(&original, Default::default()).unwrap();
    for mode in 0..9 {
        let bad = match mode {
            0 => {
                let mut b = encoded.clone();
                b[5] = 99;
                b
            }
            1 => encoded[..encoded.len() - 1].to_vec(),
            2 => {
                let mut b = encoded.clone();
                b.push(0);
                b
            }
            _ => modify(&encoded, |f| match mode {
                3 => f.push(Value::Bytes(vec![])),
                4 => f[2] = Value::List(vec![Value::List(vec![])]),
                5 => f[3] = Value::List(vec![Value::Bytes(vec![1; 64])]),
                6 => f[4] = Value::List(vec![]),
                7 => f[1] = Value::Bytes(vec![0, 1]),
                _ => f[0] = Value::Bytes(vec![0]),
            }),
        };
        assert!(
            storage::decode(&bad, Default::default()).is_err(),
            "mode {mode}"
        );
    }
    let mut config = storage::Config {
        max_record_bytes: encoded.len() - 1,
        ..Default::default()
    };
    assert_eq!(
        storage::encode(&original, config),
        Err(storage::Error::ResourceLimit)
    );
    assert_eq!(
        storage::decode(&encoded, config),
        Err(storage::Error::ResourceLimit)
    );
    config.max_record_bytes = encoded.len();
    assert_eq!(storage::decode(&encoded, config).unwrap(), original);
    let huge = modify(&encoded, |f| f[1] = Value::Bytes(vec![255; 16]));
    assert!(
        storage::decode(&huge, Default::default())
            .unwrap_err()
            .is_operational()
    );
}
#[test]
fn deeply_nested_database_records_restore_without_recursive_rust_frames() {
    let leaf =
        ae_tx::Spend::new(AccountPublicKey([1; 32]), Id::new(IdTag::Account, [2; 32])).unwrap();
    let mut tx = Transaction::new(TransactionLeaf::Spend(leaf));
    for _ in 0..512 {
        tx = tx.wrap(
            Wrapper::GaMeta(GaMeta {
                account: AccountPublicKey([1; 32]),
                auth_data: vec![],
                abi_version: 3.into(),
                fee: 1.into(),
                gas: 100.into(),
                gas_price: 1.into(),
                ttl: None,
            }),
            vec![],
        );
    }
    let signed = SignedTransaction::new(tx, vec![]);
    let encoded = storage::encode(&signed, Default::default()).unwrap();
    assert_eq!(
        storage::decode(&encoded, Default::default()).unwrap(),
        signed
    );
    let mut limited = storage::Config::default();
    limited.transaction.max_layers = 511;
    assert_eq!(
        storage::encode(&signed, limited),
        Err(storage::Error::ResourceLimit)
    );
    assert!(
        storage::decode(&encoded, limited)
            .unwrap_err()
            .is_operational()
    );
    let payer = ae_tx::PayingFor {
        payer: AccountPublicKey([3; 32]),
        nonce: 1.into(),
        fee: 1.into(),
    };
    let nested = SignedTransaction::new(
        signed
            .transaction()
            .clone()
            .wrap(Wrapper::PayingFor(payer.clone()), vec![])
            .wrap(Wrapper::PayingFor(payer), vec![]),
        vec![],
    );
    assert_eq!(
        SignedTransaction::<Transaction>::decode(&nested.encode()),
        Err(ae_tx::Error::RecursivePayer)
    );
    let stored = storage::encode(&nested, Default::default()).unwrap();
    assert_eq!(
        storage::decode(&stored, Default::default()).unwrap(),
        nested
    );
}
