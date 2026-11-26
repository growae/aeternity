use super::*;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone)]
struct Probe {
    values: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    reads: Arc<Mutex<Vec<u8>>>,
    fault: Option<u8>,
}
impl NodeReader for Probe {
    fn node(&self, _: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        panic!("no trie lookup")
    }
}
impl Read for Probe {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.get_bounded(key, usize::MAX)
    }
    fn get_bounded(&self, key: &[u8], limit: usize) -> Result<Option<Vec<u8>>, Error> {
        if key[0] != 0 {
            self.reads.lock().unwrap().push(key[0]);
        }
        if self.fault == Some(key[0]) {
            return Err(Error::Backend("injected read".into()));
        }
        match self.values.get(key) {
            Some(value) if value.len() > limit => Err(Error::ResourceLimit),
            value => Ok(value.cloned()),
        }
    }
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, Error> {
        panic!("no prefix scan")
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, Error> {
        panic!("no range scan")
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), Error> {
        panic!("no range scan")
    }
}
fn probe(
    values: BTreeMap<Vec<u8>, Vec<u8>>,
    fault: Option<u8>,
    config: ReadConfig,
) -> Stored<Probe> {
    let mut values = values;
    values.insert(repo::IDENTITY.to_vec(), identity().encode());
    values.insert(repo::STATUS.to_vec(), Status::default().encode());
    let source = Probe {
        values: Arc::new(values),
        reads: Default::default(),
        fault,
    };
    Stored::load(View::load_with_config(source, config).unwrap()).unwrap()
}

#[test]
fn client_transaction_visibility_and_read_order_match_native_location_cases() {
    let fixture: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_transaction_locations.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    let tx = tx();
    let hash = tx.hash();
    let mut block = [0; 32];
    block[31] = 9;
    let encoded = ae_tx::storage::encode(&tx, Default::default()).unwrap();
    for prefix in [repo::TRANSACTIONS, RECORDS] {
        for row in rows {
            let input = &row["input"];
            let mut values = BTreeMap::new();
            if input["record"] == true {
                values.insert(repo::key(prefix, hash.0), encoded.clone());
            }
            if input["location"] == true {
                values.insert(repo::key(repo::LOCATIONS, hash.0), block.to_vec());
            }
            if input["member"] == true {
                values.insert(repo::key(MEMBERS, hash.0), MEMBER.to_vec());
            }
            let fault = match input["fault"].as_str().unwrap() {
                "aec_signed_tx" => Some(repo::TRANSACTIONS),
                "aec_tx_location" => Some(repo::LOCATIONS),
                "aec_tx_pool" => Some(MEMBERS),
                "none" => None,
                _ => unreachable!(),
            };
            let stored = probe(values, fault, Default::default());
            let result = stored.find_transaction(hash);
            match row["result"]["kind"].as_str().unwrap() {
                "none" => assert_eq!(result, Ok(None), "{row}"),
                "error" => assert_eq!(result, Err(Error::Backend("injected read".into())), "{row}"),
                "pending" => assert_eq!(result, Ok(Some((Location::Pool, (*tx).clone()))), "{row}"),
                "mined" => assert_eq!(
                    result,
                    Ok(Some((Location::Block(Hash256(block)), (*tx).clone()))),
                    "{row}"
                ),
                _ => unreachable!(),
            }
            // Two physical Rust record namespaces implement one native shared
            // record lookup. The fallback is not another logical table read.
            let logical: Vec<_> = stored
                .chain()
                .source()
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter_map(|p| match *p {
                    repo::TRANSACTIONS => Some("aec_signed_tx"),
                    repo::LOCATIONS => Some("aec_tx_location"),
                    MEMBERS => Some("aec_tx_pool"),
                    RECORDS => None,
                    _ => panic!("unexpected lookup"),
                })
                .collect();
            assert_eq!(
                serde_json::to_value(logical).unwrap(),
                row["reads"],
                "{row}"
            );
        }
    }
}

#[test]
fn client_record_visibility_precedes_decode_and_pool_copies_are_bounded() {
    let hash = tx().hash();
    for prefix in [repo::TRANSACTIONS, RECORDS] {
        let mut values = BTreeMap::from([(repo::key(prefix, hash.0), vec![0, 99])]);
        assert_eq!(
            probe(values.clone(), None, Default::default()).find_transaction(hash),
            Ok(None)
        );
        values.insert(repo::key(MEMBERS, hash.0), MEMBER.to_vec());
        assert!(
            probe(values.clone(), None, Default::default())
                .find_transaction(hash)
                .is_err()
        );
        values.insert(repo::key(prefix, hash.0), vec![0; 257]);
        let limits = ReadConfig {
            max_transaction_record_bytes: 256,
            ..Default::default()
        };
        let stored = probe(values, None, limits);
        assert_eq!(
            stored.transaction(hash),
            Err(Error::TransactionStorage(
                ae_tx::storage::Error::ResourceLimit
            ))
        );
        assert_eq!(
            stored.find_transaction(hash),
            Err(Error::TransactionStorage(
                ae_tx::storage::Error::ResourceLimit
            ))
        );
    }
}

#[test]
fn client_lookup_keeps_one_snapshot_across_pool_delete_mining_and_rollback() {
    let temp = Temp::new();
    let db = database(&temp);
    let tx = tx();
    let hash = tx.hash();
    let unknown = db.pool_view().unwrap();
    let mut cfg = config();
    cfg.policy.allow_reentry = true;
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), cfg).unwrap();
    service.push(tx.clone(), Event::Created, false).unwrap();
    let pending = db.pool_view().unwrap();
    assert_eq!(unknown.find_transaction(hash).unwrap(), None);
    assert_eq!(
        pending.find_transaction(hash).unwrap(),
        Some((Location::Pool, (*tx).clone()))
    );
    service.delete(hash).unwrap();
    assert_eq!(
        db.pool_view().unwrap().find_transaction(hash).unwrap(),
        None
    );
    assert!(pending.find_transaction(hash).unwrap().is_some());
    service.push(tx.clone(), Event::Created, false).unwrap();
    let blocks = blocks();
    for b in &blocks[..2] {
        db.commit(prepare(&db, b)).unwrap();
    }
    let mined = db.pool_view().unwrap();
    assert_eq!(
        mined.find_transaction(hash).unwrap(),
        Some((Location::Block(blocks[1].hash().unwrap()), (*tx).clone()))
    );
    assert_eq!(
        pending.find_transaction(hash).unwrap(),
        Some((Location::Pool, (*tx).clone()))
    );
    db.rollback(repo::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(
        db.pool_view().unwrap().find_transaction(hash).unwrap(),
        None
    );
    assert!(matches!(
        mined.find_transaction(hash).unwrap(),
        Some((Location::Block(_), _))
    ));
}
