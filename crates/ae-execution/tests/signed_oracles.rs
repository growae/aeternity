use ae_crypto::{Ed25519SecretKey, blake2b_256};
use ae_execution::{Context, Error, Event, oracles};
use ae_protocol::{NetworkProfile, Protocol, oracles::Ttl};
use ae_state::{
    accounts::{Account, Accounts},
    names::{Name, Names, Pointer, PointerValue},
    oracles::{Oracles, query_id},
};
use ae_tx::{CanonicalTransaction, OracleOperation, OracleTransaction, SignedTransaction};
use ae_types::{Id, IdTag, Unsigned};

#[test]
fn signed_oracle_lifecycles_preserve_escrow_rewards_and_expiry_across_protocols() {
    let oracle = Ed25519SecretKey::from_seed(&[31; 32]);
    let sender = Ed25519SecretKey::from_seed(&[32; 32]);
    let network = NetworkProfile::mainnet();
    let balance = Unsigned::from_decimal("1000000000000000000000000000000").unwrap();
    let fee = Unsigned::from(100_000_000_000u64);
    for version in 1..=8 {
        let protocol = Protocol::try_from(version).unwrap();
        for abi in [0u64, 1, 3] {
            if !ae_protocol::oracles::abi_available(&abi.into(), protocol) {
                continue;
            }
            let (mut accounts, mut state, mut names) =
                (Accounts::default(), Oracles::default(), Names::default());
            for signer in [&oracle, &sender] {
                accounts
                    .enter(&Account::new(signer.public_key(), balance.clone()))
                    .unwrap();
            }
            let (format, content) = match abi {
                1 => (
                    ae_vm_aevm::heap::encode(
                        &ae_vm_aevm::heap::Value::Type(ae_vm_aevm::heap::Type::Word),
                        Default::default(),
                    )
                    .unwrap(),
                    ae_vm_aevm::heap::encode(
                        &ae_vm_aevm::heap::Value::Integer(42.into()),
                        Default::default(),
                    )
                    .unwrap(),
                ),
                3 => (vec![7], vec![84]),
                _ => (b"text".to_vec(), b"hello".to_vec()),
            };
            let name_hash = [17; 32];
            let mut name = Name::new(name_hash, oracle.public_key(), 100.into());
            name.pointers = vec![Pointer {
                key: b"oracle_pubkey".to_vec(),
                value: PointerValue::Id(Id::new(IdTag::Oracle, oracle.public_key().0)),
            }];
            names.enter_name(&name).unwrap();
            let ident = if protocol >= Protocol::Iris {
                Id::new(IdTag::Name, name_hash)
            } else {
                Id::new(IdTag::Oracle, oracle.public_key().0)
            };
            let answered = query_id(sender.public_key(), &1.into(), ident.value).0;
            let open = query_id(sender.public_key(), &2.into(), ident.value).0;
            let operations = [
                (
                    0,
                    1,
                    50,
                    OracleOperation::Register {
                        query_format: format.clone(),
                        response_format: format,
                        query_fee: 7.into(),
                        oracle_ttl: Ttl::Delta(20.into()),
                        abi_version: abi.into(),
                    },
                ),
                (0, 2, 50, OracleOperation::Extend { delta: 10.into() }),
                (
                    1,
                    1,
                    50,
                    OracleOperation::Query {
                        oracle: ident,
                        query: content.clone(),
                        query_fee: 7.into(),
                        query_ttl: Ttl::Delta(10.into()),
                        response_ttl: 5.into(),
                    },
                ),
                (
                    0,
                    3,
                    51,
                    OracleOperation::Response {
                        query_id: answered.to_vec(),
                        response: content.clone(),
                        response_ttl: 5.into(),
                    },
                ),
                (
                    1,
                    2,
                    52,
                    OracleOperation::Query {
                        oracle: ident,
                        query: content,
                        query_fee: 7.into(),
                        query_ttl: Ttl::Delta(10.into()),
                        response_ttl: 5.into(),
                    },
                ),
            ];
            for (who, nonce, height, operation) in operations {
                let signer = if who == 0 { &oracle } else { &sender };
                let mut tx = OracleTransaction::new(signer.public_key(), operation).unwrap();
                tx.set_nonce(Unsigned::from(nonce as u64));
                tx.set_fee(fee.clone());
                let mut payload = network.signing_prefix(false);
                if protocol >= Protocol::Lima {
                    payload.extend_from_slice(&blake2b_256(&tx.encode()).0)
                } else {
                    payload.extend_from_slice(&tx.encode())
                }
                let signed = SignedTransaction::new(tx, vec![signer.sign(&payload)]);
                let decoded =
                    SignedTransaction::<OracleTransaction>::decode(&signed.encode()).unwrap();
                let ctx = Context::transaction(height, protocol);
                let before = (
                    accounts.root_hash(),
                    state.root_hash(),
                    state.cache_root_hash(),
                );
                assert!(matches!(
                    oracles::execute_signed(
                        &decoded,
                        &mut accounts,
                        &mut state,
                        &names,
                        &NetworkProfile::uat(),
                        &ctx
                    ),
                    Err(Error::Signature(_))
                ));
                assert_eq!(
                    (
                        accounts.root_hash(),
                        state.root_hash(),
                        state.cache_root_hash()
                    ),
                    before
                );
                let receipt = oracles::execute_signed(
                    &decoded,
                    &mut accounts,
                    &mut state,
                    &names,
                    &network,
                    &ctx,
                )
                .unwrap();
                assert_eq!(receipt.transaction_hash, Some(signed.hash()));
                assert_eq!(
                    accounts
                        .lookup(signer.public_key())
                        .unwrap()
                        .unwrap()
                        .nonce(),
                    &Unsigned::from(nonce as u64)
                );
            }
            let record = state.oracle(oracle.public_key().0).unwrap().unwrap();
            assert_eq!(record.expires_by, 80.into());
            let q = state
                .query(oracle.public_key().0, answered)
                .unwrap()
                .unwrap();
            assert!(!q.is_open());
            assert_eq!(q.expires_by, 56.into());
            assert_eq!(q.oracle, Id::new(IdTag::Oracle, oracle.public_key().0));
            assert!(
                state
                    .query(oracle.public_key().0, open)
                    .unwrap()
                    .unwrap()
                    .is_open()
            );
            assert_eq!(
                accounts
                    .lookup(oracle.public_key())
                    .unwrap()
                    .unwrap()
                    .balance(),
                &balance.checked_sub(&fee.mul_u64(3)).unwrap().add(&7.into())
            );
            assert_eq!(
                accounts
                    .lookup(sender.public_key())
                    .unwrap()
                    .unwrap()
                    .balance(),
                &balance
                    .checked_sub(&fee.mul_u64(2))
                    .unwrap()
                    .checked_sub(&14.into())
                    .unwrap()
            );
            let mut refunds = Vec::new();
            for height in 53..=81 {
                refunds.extend(oracles::prune(&mut accounts, &mut state, height).unwrap());
            }
            assert_eq!(
                refunds,
                vec![Event::OracleRefund {
                    account: sender.public_key(),
                    amount: 7.into()
                }]
            );
            assert!(state.oracle(oracle.public_key().0).unwrap().is_none());
            assert!(state.query_ids(oracle.public_key().0).unwrap().is_empty());
            assert_eq!(
                accounts
                    .lookup(sender.public_key())
                    .unwrap()
                    .unwrap()
                    .balance(),
                &balance
                    .checked_sub(&fee.mul_u64(2))
                    .unwrap()
                    .checked_sub(&7.into())
                    .unwrap()
            );
        }
    }
}
