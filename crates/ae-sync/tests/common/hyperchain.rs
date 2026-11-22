pub mod parents;
pub mod pinned;
use super::ledger_fixtures::otp_order;
use ae_chain::{block::Block, repository as repo};
use ae_codec::headers::{Header, HeaderMode};
use ae_hyperchain::{
    block::{self as hc, Host},
    chain,
    consensus_contract::Contracts,
};
use ae_protocol::{Activation, ConsensusType, NetworkProfile, Protocol};
use ae_state::{
    Trees,
    contracts::{Nonce, contract_id},
};
use ae_storage::{Database, hyperchain::Config};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::{
    machine,
    maps::order::{Otp26, WordSize},
};
use num_bigint::BigInt;
use serde_json::Value as J;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicBool, Ordering},
};
pub fn order(profile: &J) -> Otp26 {
    otp_order::profile(profile)
}
pub static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!(
        "../../../ae-hyperchain/tests/fixtures/erlang_hc_finalized.json"
    ))
    .unwrap()
});
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Parent {
    pub reads: Vec<BigInt>,
    pub unavailable: bool,
}
impl Host for Parent {
    type Error = ();
    fn is_operational(_: &()) -> bool {
        true
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, ()> {
        self.reads.push(epoch.clone());
        if self.unavailable {
            return Err(());
        }
        let h: BigInt = (epoch - BigInt::from(3)).max(0.into()) * 5 + 1;
        Ok(Some(key(1000 + u64::try_from(h).unwrap()).to_vec()))
    }
    fn pin(&mut self, _: &[u8]) -> Result<Option<hc::Pin>, ()> {
        Ok(None)
    }
    fn key_hash(&mut self, _: &BigInt) -> Result<Option<Vec<u8>>, ()> {
        Ok(None)
    }
}
fn key(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn tree(j: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for row in j.as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&row["key"])),
            TrieValue::Bytes(bytes(&row["encoded"])),
        )
        .unwrap();
    }
    t
}
fn read_config() -> repo::ReadConfig {
    repo::ReadConfig {
        mode: HeaderMode::Hyperchain,
        ..Default::default()
    }
}
pub fn block(j: &J) -> Block {
    Block::decode(&bytes(&j["block"]), read_config().block()).unwrap()
}
fn setup(
    path: &super::Temporary,
    corpus: &J,
    index: usize,
    profile_name: &str,
) -> (Arc<Database>, Config<Otp26>) {
    setup_events(path, corpus, index, profile_name, Default::default())
}
fn setup_events(
    path: &super::Temporary,
    corpus: &J,
    index: usize,
    profile_name: &str,
    events: ae_storage::events::Config,
) -> (Arc<Database>, Config<Otp26>) {
    setup_fork(path, corpus, index, profile_name, events, None)
}
fn setup_fork(
    path: &super::Temporary,
    corpus: &J,
    index: usize,
    profile_name: &str,
    events: ae_storage::events::Config,
    fork: Option<ae_protocol::signaling::Fork>,
) -> (Arc<Database>, Config<Otp26>) {
    let row = &corpus["rows"][index];
    let network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        row["activations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| Activation {
                height: a["height"].as_u64().unwrap(),
                protocol: Protocol::try_from(a["protocol"].as_u64().unwrap() as u32).unwrap(),
            })
            .collect(),
    )
    .unwrap()
    .with_community_fork(fork)
    .unwrap();
    let Header::Key(header) =
        Header::decode(&bytes(&row["genesis"]["header"]), HeaderMode::Hyperchain).unwrap()
    else {
        panic!()
    };
    let owner = AccountPublicKey(key(1));
    let staking = contract_id(owner, Nonce::Number(&Unsigned::from(1u64))).0;
    let election = contract_id(owner, Nonce::Number(&Unsigned::from(2u64))).0;
    let profile = chain::Profile {
        network: network.clone(),
        contracts: Contracts {
            owner,
            staking,
            election,
            rewards: election,
        },
        context: machine::Context {
            current_contract: election,
            vm_version: ae_vm_fate::types::Version::Three,
            in_auth: false,
            on_chain: true,
            reserved_gas: 0.into(),
            arithmetic_word_size: WordSize::Bits64,
            unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
            bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
        },
        order: otp_order::profile(&corpus[profile_name]),
        overrides: Default::default(),
        diagnostic: ae_vm_fate::diagnostic::Profile::Release,
        limits: Default::default(),
        maximum_steps: 1_000_000,
        maximum_history_reads: 1000,
    };
    let hash = Header::Key(header.clone()).hash().unwrap();
    let mut rules = ae_chain::hyperchain::Rules::new(
        hash,
        hc::Config {
            parent_epoch_length: 5.into(),
            parent_start: 1.into(),
            parent_finality: 2.into(),
            pin_sync_margin: 1.into(),
            child_block_time: 4.into(),
            cache_limits: Default::default(),
        },
    );
    rules.reward_delay = 2;
    rules.fee_distribution = [10.into(), 70.into(), 20.into()];
    rules.fixed_coinbase = Unsigned::from_field_bytes(
        &row["fixed_coinbase"]
            .as_str()
            .unwrap()
            .parse::<BigInt>()
            .unwrap()
            .to_bytes_be()
            .1,
    )
    .unwrap();
    let config = Config {
        identity: repo::Identity::for_consensus(&network, hash, Hash256::ZERO, ConsensusType::Pos),
        profile: Arc::new(profile),
        rules,
        aevm: None,
        limits: Default::default(),
    };
    let db = Arc::new(
        Database::open_with_events(
            &path.0,
            config.identity.clone(),
            read_config(),
            Default::default(),
            events,
        )
        .unwrap(),
    );
    let t = &row["genesis"]["trees"];
    let a = tree(&t["accounts"]);
    let c = tree(&t["contracts"]);
    let calls = tree(&t["calls"]);
    let trees = Trees {
        accounts: ae_state::accounts::Accounts::from_root(a.root_hash(), a.store().clone())
            .unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(c.root_hash(), c.store().clone())
            .unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.store().clone()).unwrap(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    if db.view().unwrap().status().tip.is_none() {
        db.commit(
            repo::bootstrap(
                &db.view().unwrap(),
                &network,
                &header,
                &trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
    (db, config)
}

pub struct Node {
    pub db: Arc<Database>,
    pub config: Config<Otp26>,
    pub index: usize,
    pub corpus: &'static J,
    profile_name: &'static str,
    temporary: super::Temporary,
}
impl Node {
    pub fn with_events(events: ae_storage::events::Config) -> Self {
        let temporary = super::Temporary(std::env::temp_dir().join(format!(
            "ae-hc-events-{}-{}",
            std::process::id(),
            super::NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        let (db, config) = setup_events(&temporary, &DATA, 0, "profile", events);
        Self {
            db,
            config,
            index: 0,
            corpus: &DATA,
            profile_name: "profile",
            temporary,
        }
    }
    pub fn new(index: usize) -> Self {
        Self::from_corpus(&DATA, index)
    }
    pub fn from_corpus(corpus: &'static J, index: usize) -> Self {
        Self::from_profile(corpus, index, "profile")
    }
    pub fn with_fork(index: usize, fork: ae_protocol::signaling::Fork) -> Self {
        let temporary = super::Temporary(std::env::temp_dir().join(format!(
            "ae-hc-fork-{}-{}",
            std::process::id(),
            super::NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        let (db, config) = setup_fork(
            &temporary,
            &DATA,
            index,
            "profile",
            Default::default(),
            Some(fork),
        );
        Self {
            db,
            config,
            index,
            corpus: &DATA,
            profile_name: "profile",
            temporary,
        }
    }
    pub fn from_profile(corpus: &'static J, index: usize, profile_name: &'static str) -> Self {
        let temporary = super::Temporary(std::env::temp_dir().join(format!(
            "ae-hc-import-{}-{}",
            std::process::id(),
            super::NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        let (db, config) = setup(&temporary, corpus, index, profile_name);
        Self {
            db,
            config,
            index,
            corpus,
            profile_name,
            temporary,
        }
    }
    pub fn reopen(self) -> Self {
        let Self {
            db,
            config,
            index,
            corpus,
            profile_name,
            temporary,
        } = self;
        drop(db);
        let (db, config) = setup_fork(
            &temporary,
            corpus,
            index,
            profile_name,
            Default::default(),
            config.profile.network.community_fork(),
        );
        Self {
            db,
            config,
            index,
            corpus,
            profile_name,
            temporary,
        }
    }
    pub fn blocks(&self) -> Vec<Block> {
        self.corpus["rows"][self.index]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(block)
            .collect()
    }
    pub fn check(&self, index: usize) {
        let row = &self.corpus["rows"][self.index]["rows"][index];
        let b = block(row);
        let hash = b.hash().unwrap();
        let view = self.db.view().unwrap();
        view.verify_tip().unwrap();
        assert_eq!(view.status().tip, Some(hash));
        let record = view.require_record(hash).unwrap();
        assert_eq!(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, record.fees.as_bytes()).to_string(),
            row["fees"]
        );
        assert_eq!(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, record.difficulty.as_bytes()).to_string(),
            row["score"]
        );
        let trees = view.trees(hash, Default::default()).unwrap();
        assert_eq!(trees.roots().hash().0, b.header().common().state_hash);
        for expected in row["calls"].as_array().unwrap() {
            let id = bytes(&expected["id"]).try_into().unwrap();
            let call =
                ae_state::calls::Call::deserialize(id, &bytes(&expected["encoded"])).unwrap();
            assert_eq!(trees.calls.lookup(&call.contract, id).unwrap(), Some(call));
        }
        let checkpoint = view.hyperchain_checkpoint().unwrap();
        assert_eq!(checkpoint.revision, view.status().revision);
        assert!(
            view.hyperchain_event(view.status().revision)
                .unwrap()
                .is_some()
        );
    }
}
type CaptureHook = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;
#[derive(Clone, Default)]
pub struct Inputs {
    pub reads: Arc<Mutex<Vec<(u64, u64)>>>,
    pub fail: Arc<AtomicBool>,
    hook: CaptureHook,
}
impl Inputs {
    pub fn hook(&self, f: impl FnOnce() + Send + 'static) {
        *self.hook.lock().unwrap() = Some(Box::new(f));
    }
}
impl ae_sync::chain::hyperchain::ParentInputs for Inputs {
    type Host = Parent;
    type Error = &'static str;
    fn capture(&self, view: &repo::View<ae_storage::Snapshot<'_>>) -> Result<Parent, Self::Error> {
        self.reads
            .lock()
            .unwrap()
            .push((view.status().revision, view.resilience().generation));
        let hook = self.hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        if self.fail.load(Ordering::Acquire) {
            return Err("parent input unavailable");
        }
        Ok(Parent::default())
    }
}
