use ae_crypto::{Ed25519SecretKey, blake2b_256};
use ae_execution::{Context, Error, names};
use ae_protocol::{NetworkProfile, Protocol, names as rules};
use ae_state::{
    accounts::{Account, Accounts},
    names::{NameResolver, Names, Pointer, PointerValue, hashing},
};
use ae_tx::{CanonicalTransaction, NameOperation, NameTransaction, SignedTransaction};
use ae_types::{Id, IdTag, Unsigned};

#[test]
fn signed_lifecycle_checks_network_domain_ownership_nonces_and_expiry_at_every_protocol() {
    let owner = Ed25519SecretKey::from_seed(&[17; 32]);
    let recipient = Ed25519SecretKey::from_seed(&[18; 32]);
    let network = NetworkProfile::mainnet();
    let balance = Unsigned::from_decimal("1000000000000000000000000000000").unwrap();
    let fee = Unsigned::from(100_000_000_000u64);
    for version in 1..=8 {
        let protocol = Protocol::try_from(version).unwrap();
        let mut accounts = Accounts::default();
        for key in [owner.public_key(), recipient.public_key()] {
            accounts.enter(&Account::new(key, balance.clone())).unwrap();
        }
        let mut state = Names::default();
        let raw = if protocol < Protocol::Lima {
            b"abcdefghijklmn.test".as_slice()
        } else {
            b"abcdefghijklmn.chain"
        };
        let hash = hashing::name_hash(raw).0;
        let commitment = hashing::commitment_hash(raw, &1.into()).unwrap().0;
        let name_fee = if protocol < Protocol::Lima {
            3.into()
        } else {
            rules::claim_fee(std::num::NonZeroUsize::new(14).unwrap(), protocol)
        };
        let mut pointers = vec![Pointer {
            key: b"account_pubkey".to_vec(),
            value: PointerValue::Id(Id::new(IdTag::Account, recipient.public_key().0)),
        }];
        if protocol >= Protocol::Ceres {
            pointers.push(Pointer {
                key: b"description".to_vec(),
                value: PointerValue::Data(b"value".to_vec()),
            });
        }
        let operations = [
            NameOperation::Preclaim { commitment },
            NameOperation::Claim {
                name: raw.to_vec(),
                salt: 1.into(),
                name_fee: (protocol >= Protocol::Lima).then(|| name_fee.clone()),
            },
            NameOperation::Update {
                name: hash,
                name_ttl: 20.into(),
                client_ttl: 300.into(),
                pointers: pointers.clone(),
            },
            NameOperation::Transfer {
                name: hash,
                recipient: Id::new(IdTag::Account, recipient.public_key().0),
            },
            NameOperation::Revoke { name: hash },
        ];
        for (i, op) in operations.into_iter().enumerate() {
            let signer = if i == 4 { &recipient } else { &owner };
            let nonce = if i == 4 { 1 } else { i as u64 + 1 };
            let height = if i == 0 { 50 } else { 51 };
            let ctx = Context::transaction(height, protocol);
            let mut tx = NameTransaction::new(signer.public_key(), op).unwrap();
            tx.set_nonce(nonce.into());
            tx.set_fee(fee.clone());
            let mut payload = network.signing_prefix(false);
            if protocol >= Protocol::Lima {
                payload.extend_from_slice(&blake2b_256(&tx.encode()).0);
            } else {
                payload.extend_from_slice(&tx.encode());
            }
            let signed = SignedTransaction::new(tx, vec![signer.sign(&payload)]);
            let decoded = SignedTransaction::<NameTransaction>::decode(&signed.encode()).unwrap();
            let before = (
                accounts.root_hash(),
                state.root_hash(),
                state.cache_root_hash(),
            );
            assert!(matches!(
                names::execute_signed(
                    &decoded,
                    &mut accounts,
                    &mut state,
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
            let receipt =
                names::execute_signed(&decoded, &mut accounts, &mut state, &network, &ctx).unwrap();
            assert_eq!(receipt.transaction_hash, Some(signed.hash()));
            assert_eq!(
                accounts
                    .lookup(signer.public_key())
                    .unwrap()
                    .unwrap()
                    .nonce(),
                &nonce.into()
            );
            if i == 2 {
                let record = state.name(hash).unwrap().unwrap();
                assert_eq!(
                    (record.expires_by, record.client_ttl, record.pointers),
                    (71.into(), 300.into(), pointers.clone())
                );
                assert_eq!(state.resolve_account(hash).unwrap(), recipient.public_key());
            }
            if i == 3 {
                assert_eq!(
                    state.name(hash).unwrap().unwrap().owner.value,
                    recipient.public_key().0
                );
            }
        }
        assert_eq!(
            accounts
                .lookup(owner.public_key())
                .unwrap()
                .unwrap()
                .balance(),
            &balance
                .checked_sub(&fee.mul_u64(4))
                .unwrap()
                .checked_sub(&name_fee)
                .unwrap()
        );
        assert_eq!(
            accounts
                .lookup(recipient.public_key())
                .unwrap()
                .unwrap()
                .balance(),
            &balance.checked_sub(&fee).unwrap()
        );
        assert_eq!(
            accounts
                .lookup(ae_types::AccountPublicKey::ZERO)
                .unwrap()
                .unwrap()
                .balance(),
            &name_fee
        );
        assert!(state.commitment(commitment).unwrap().is_none());
        assert_eq!(state.name(hash).unwrap().unwrap().status, b"revoked");
        names::prune(&mut accounts, &mut state, 2067, protocol).unwrap();
        assert!(state.name(hash).unwrap().is_some());
        names::prune(&mut accounts, &mut state, 2068, protocol).unwrap();
        assert!(state.name(hash).unwrap().is_none());
    }
}
