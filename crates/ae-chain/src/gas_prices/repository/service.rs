use super::*;

// These limits apply to observations, never block admission. Keep the caller's
// network/header mode, atom profile and any stricter source decoding limits.
fn config(source: &repo::ReadConfig) -> repo::ReadConfig {
    let mut c = source.clone();
    c.max_block_bytes = c.max_block_bytes.min(16 << 20);
    c.max_transactions = c.max_transactions.min(100_000);
    c.max_transaction_record_bytes = c.max_transaction_record_bytes.min(1 << 20);
    c.body.max_input_bytes = c.body.max_input_bytes.min(16 << 20);
    c.body.max_items = c.body.max_items.min(100_006);
    c.body.max_depth = c.body.max_depth.min(64);
    c.transaction.rlp.max_input_bytes = c.transaction.rlp.max_input_bytes.min(1 << 20);
    c.transaction.rlp.max_items = c.transaction.rlp.max_items.min(16_384);
    c.transaction.rlp.max_depth = c.transaction.rlp.max_depth.min(64);
    c.transaction.max_layers = c.transaction.max_layers.min(256);
    c.transaction.max_total_bytes = c.transaction.max_total_bytes.min(8 << 20);
    let s = &mut c.state_limits;
    s.max_input_bytes = s.max_input_bytes.min(1 << 20);
    s.max_items = s.max_items.min(16_384);
    s.max_depth = s.max_depth.min(64);
    s.max_entries = s.max_entries.min(4096);
    s.max_key_bytes = s.max_key_bytes.min(256);
    s.max_work_bytes = s.max_work_bytes.min(4 << 20);
    c
}
fn limits() -> ReadLimits {
    ReadLimits {
        reads: point::Limits {
            node_bytes: 1 << 20,
            value_bytes: 512 << 10,
            rlp: rlp::DecodeLimits {
                max_input_bytes: 1 << 20,
                max_items: 4096,
                max_depth: 64,
            },
            ..ReadLimits::default().reads
        },
        proof_bytes: 1 << 20,
        ..Default::default()
    }
}
impl<S: Read> Reader<S> {
    pub fn service(view: &View<S>, block_gas_limit: BigInt) -> Result<Self, Error> {
        Self::with_config(view, block_gas_limit, limits(), config(view.config()))
    }
}
// Reservation for query-owned allocations, separate from aggregate I/O and
// the runtime-owned cache/database. All factors include overlapping temporary
// copies and geometric Vec capacity; the service decoder caps above are part
// of this contract. Rendering is added by the caller and kept through transport.
pub fn service_read_weight(source: &repo::ReadConfig) -> Result<usize, Error> {
    let c = config(source);
    let l = limits();
    let sum = |parts: &[(usize, usize)]| -> Result<usize, Error> {
        parts.iter().try_fold(0usize, |n, &(count, bytes)| {
            n.checked_add(count.checked_mul(bytes).ok_or(Error::ResourceLimit)?)
                .ok_or(Error::ResourceLimit)
        })
    };
    // Stored RLP/value/typed transaction copies and encoding temporaries;
    // item storage across borrowed/owned RLP, integer/signature vectors and
    // typed fields; wrapper metadata; imported nodes and state serialization.
    let transaction = sum(&[
        (c.max_transaction_record_bytes, 24),
        (c.transaction.rlp.max_items, 512),
        (c.transaction.max_layers, 4096),
        (c.state_limits.max_work_bytes, 4),
        (c.state_limits.max_items, 512),
    ])?;
    // Proof decoding follows validation, after the last transaction drops.
    let proof = sum(&[(l.proof_bytes, 16)])?;
    sum(&[
        (1, c.header_read_weight()?),
        (1, transaction.max(proof)),
        (l.reads.node_bytes, 8),
        (l.reads.value_bytes, 8),
        (l.reads.rlp.max_items, 512),
        (l.observations.integer_bytes, 48),
        (1, 1 << 20),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, sync::Arc};

    #[test]
    fn service_profile_preserves_source_identity_and_fits_default_pool() {
        let source = repo::ReadConfig::default();
        let weight = service_read_weight(&source).unwrap();
        assert!(weight + (1 << 20) < 128 << 20, "weight={weight}");
        assert!(weight < limits().reads.read_bytes);
        let atoms = Arc::new(BTreeSet::from([b"custom_native_atom".to_vec()]));
        let source = repo::ReadConfig {
            mode: ae_codec::headers::HeaderMode::Hyperchain,
            max_block_bytes: 4096,
            max_transactions: 3,
            max_transaction_record_bytes: 2048,
            existing_name_status_atoms: Some(atoms.clone()),
            transaction: ae_tx::TransactionLimits {
                max_layers: 2,
                max_total_bytes: 4096,
                ..Default::default()
            },
            state_limits: ae_state::transport::Limits {
                max_entries: 2,
                max_work_bytes: 2048,
                ..Default::default()
            },
            ..Default::default()
        };
        let c = config(&source);
        assert_eq!(c.mode, source.mode);
        assert!(Arc::ptr_eq(
            c.existing_name_status_atoms.as_ref().unwrap(),
            &atoms
        ));
        assert_eq!(c.max_block_bytes, 4096);
        assert_eq!(c.max_transactions, 3);
        assert_eq!(c.max_transaction_record_bytes, 2048);
        assert_eq!(c.transaction.max_layers, 2);
        assert_eq!(c.transaction.max_total_bytes, 4096);
        assert_eq!(c.state_limits.max_entries, 2);
        assert_eq!(c.state_limits.max_work_bytes, 2048);
        assert!(service_read_weight(&source).unwrap() < weight);
    }

    #[test]
    fn missing_atom_profiles_are_not_reclassified_as_capacity_failures() {
        use ae_tx::storage::Error as Storage;
        let missing = ae_tx::Error::StateTransport(ae_state::transport::Error::MissingAtomProfile(
            b"native_atom".to_vec(),
        ));
        assert!(matches!(
            Error::from(repo::Error::TransactionStorage(Storage::Transaction(
                missing
            ))),
            Error::Backend(repo::Error::TransactionStorage(Storage::Transaction(
                ae_tx::Error::StateTransport(ae_state::transport::Error::MissingAtomProfile(_))
            )))
        ));
        for error in [
            ae_tx::Error::ResourceLimit,
            ae_tx::Error::Object(ae_codec::object::ObjectError::Rlp(
                rlp::DecodeError::ResourceLimit,
            )),
        ] {
            assert!(matches!(
                Error::from(repo::Error::TransactionStorage(Storage::Transaction(error))),
                Error::ResourceLimit
            ));
        }
    }
}
