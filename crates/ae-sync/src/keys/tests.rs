use super::*;
use ae_codec::headers::{CommonHeader, Header};
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ae-key-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> Config {
        Config::new(self.0.join("keys"), b"synthetic fixture password".to_vec())
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn header() -> MicroHeader {
    MicroHeader {
        common: CommonHeader {
            version: 6,
            height: 42,
            previous_hash: [1; 32],
            previous_key_hash: [2; 32],
            state_hash: [3; 32],
        },
        transactions_hash: [4; 32],
        time: 12345,
        proof_of_fraud: Some([5; 32]),
        signature: [9; 64],
    }
}
fn verify(signed: &SignedMicro) {
    assert!(ae_crypto::verify_ed25519(
        &signed.signer,
        &Header::Micro(signed.header.clone())
            .signature_bytes()
            .unwrap(),
        &ae_types::Signature(signed.header.signature)
    ));
}

#[test]
fn key_worker_promotes_only_current_candidate_and_keeps_peer_identity_across_restart() {
    let temp = Temp::new();
    let config = temp.config();
    assert!(!format!("{config:?}").contains("synthetic fixture password"));
    let worker = Worker::spawn(config.clone()).unwrap();
    let client = worker.client();
    let first = client.info().keys;
    let identity = client.peer_identity().unwrap().blocking_wait().unwrap();
    assert_eq!(identity.public_key(), first.peer);
    let before = client
        .sign_micro(header())
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(before.signer, first.signing);
    verify(&before);
    assert_eq!(
        client
            .promote_candidate(first.signing)
            .unwrap()
            .blocking_wait(),
        Err(Error::KeyNotFound)
    );
    let next = client
        .promote_candidate(first.candidate)
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(next.signing, first.candidate);
    assert_ne!(next.candidate, first.candidate);
    assert!(matches!(
        client
            .sign_micro_for(first.signing, header())
            .unwrap()
            .blocking_wait(),
        Err(Error::KeyChanged)
    ));
    let after = client
        .sign_micro_for(next.signing, header())
        .unwrap()
        .blocking_wait()
        .unwrap();
    verify(&after);
    assert_eq!(after.signer, first.candidate);
    assert!(matches!(
        Worker::spawn(config.clone()),
        Err(Error::AlreadyRunning)
    ));
    // A discarded reply does not cancel the accepted promotion.
    drop(client.promote_candidate(next.candidate).unwrap());
    client.peer_identity().unwrap().blocking_wait().unwrap();
    assert_eq!(client.info().keys.signing, next.candidate);
    assert_eq!(client.info().promotions, 2);
    worker.shutdown().unwrap();
    assert!(!client.info().running);
    assert!(matches!(client.sign_micro(header()), Err(Error::Stopped)));
    let replacement = Worker::spawn(config).unwrap();
    assert_eq!(replacement.info().keys.peer, first.peer);
    assert_ne!(replacement.info().keys.signing, first.signing);
    assert_eq!(identity.public_key(), replacement.info().keys.peer);
    replacement.shutdown().unwrap();
}

#[test]
fn key_worker_bounds_pending_requests_and_drains_accepted_work_before_join() {
    let temp = Temp::new();
    let mut config = temp.config();
    config.requests = 4;
    let worker = Worker::spawn(config).unwrap();
    let client = worker.client();
    let (ready, release) = worker::pause(&client);
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let promotion = client
        .promote_candidate(client.info().keys.candidate)
        .unwrap();
    let signed = client.sign_micro(header()).unwrap();
    drop(client.sign_micro(header()).unwrap());
    assert_eq!(client.info().pending, 4);
    assert!(matches!(client.sign_micro(header()), Err(Error::Busy)));
    worker.request_shutdown();
    assert!(matches!(client.peer_identity(), Err(Error::Stopped)));
    release.send(()).unwrap();
    worker.shutdown().unwrap();
    let keys = promotion.blocking_wait().unwrap();
    let signed = signed.blocking_wait().unwrap();
    assert_eq!(signed.signer, keys.signing);
    verify(&signed);
    let status = client.info();
    assert_eq!(
        (status.pending, status.submitted, status.completed),
        (0, 4, 4)
    );
    assert!(status.failure.is_none() && !status.running);
}

struct Sequence {
    next: u8,
    fail: bool,
}
impl keeper::Entropy for Sequence {
    fn seed(&mut self) -> Result<Zeroizing<[u8; 32]>, Error> {
        if self.fail {
            return Err(Error::Entropy);
        }
        let mut seed = [0; 32];
        seed[31] = self.next;
        self.next += 1;
        Ok(Zeroizing::new(seed))
    }
}
#[test]
fn failed_candidate_generation_preserves_both_signing_keys_and_peer_files() {
    let temp = Temp::new();
    let config = temp.config();
    let mut random = Sequence {
        next: 1,
        fail: false,
    };
    let mut state = keeper::State::open(&config, &mut random).unwrap();
    let before = state.public();
    random.fail = true;
    assert_eq!(
        state.promote(before.candidate, &mut random),
        Err(Error::Entropy)
    );
    assert_eq!(state.public(), before);
    let signed = state.sign(header(), Some(before.signing)).unwrap();
    verify(&signed);
    random.fail = false;
    assert_eq!(
        state
            .promote(before.candidate, &mut random)
            .unwrap()
            .signing,
        before.candidate
    );
}

#[test]
fn wrong_password_partial_invalid_and_symlink_files_are_never_overwritten() {
    let temp = Temp::new();
    let config = temp.config();
    Worker::spawn(config.clone()).unwrap().shutdown().unwrap();
    let public = config.directory.join("peer_key.pub");
    let private = config.directory.join("peer_key");
    let public_bytes = fs::read(&public).unwrap();
    let private_bytes = fs::read(&private).unwrap();
    let wrong = Config::new(
        config.directory.clone(),
        b"wrong synthetic password".to_vec(),
    );
    assert!(matches!(Worker::spawn(wrong), Err(Error::InvalidPeerPair)));
    assert_eq!(fs::read(&public).unwrap(), public_bytes);
    assert_eq!(fs::read(&private).unwrap(), private_bytes);
    fs::remove_file(&private).unwrap();
    assert!(matches!(
        Worker::spawn(config.clone()),
        Err(Error::IncompletePeerFiles)
    ));
    assert!(!private.exists());
    assert_eq!(fs::read(&public).unwrap(), public_bytes);
    fs::write(&private, vec![0; 33]).unwrap();
    assert!(matches!(
        Worker::spawn(config.clone()),
        Err(Error::InvalidPeerFile)
    ));
    assert_eq!(fs::read(&private).unwrap(), vec![0; 33]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        fs::remove_file(&private).unwrap();
        let target = temp.0.join("synthetic-secret-target");
        fs::write(&target, &private_bytes).unwrap();
        symlink(&target, &private).unwrap();
        assert!(matches!(
            Worker::spawn(config.clone()),
            Err(Error::InvalidPeerFile)
        ));
        assert_eq!(fs::read(&target).unwrap(), private_bytes);
        assert_eq!(
            fs::metadata(&public).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&config.directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

fn native() -> serde_json::Value {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_key_owner.json")).unwrap()
}
fn bytes(value: &serde_json::Value) -> Vec<u8> {
    hex::decode(value.as_str().unwrap()).unwrap()
}
fn assert_public(actual: PublicKeys, expected: &serde_json::Value) {
    assert_eq!(
        hex::encode(actual.signing.0),
        expected["signing"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(actual.candidate.0),
        expected["candidate"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(actual.peer.0),
        expected["peer"].as_str().unwrap()
    );
}
#[test]
fn peer_key_conversion_and_encrypted_file_bytes_match_original_enacl_and_openssl() {
    for row in native()["rows"].as_array().unwrap() {
        let seed: [u8; 32] = bytes(&row["seed"]).try_into().unwrap();
        let signing = ae_crypto::Ed25519SecretKey::from_seed(&seed);
        assert_eq!(signing.public_key().0.as_slice(), bytes(&row["ed_public"]));
        let secret = keeper::peer_secret(&seed);
        assert_eq!(secret.as_slice(), bytes(&row["peer_private"]));
        let public = noise::Identity::from_secret(*secret).public_key().0;
        assert_eq!(public.as_slice(), bytes(&row["peer_public"]));
        let password = bytes(&row["password"]);
        let encrypted_public = files::transform(&password, &public, true);
        let encrypted_private = files::transform(&password, &secret, true);
        assert_eq!(encrypted_public.as_slice(), bytes(&row["encrypted_public"]));
        assert_eq!(
            encrypted_private.as_slice(),
            bytes(&row["encrypted_private"])
        );
        assert_eq!(
            *files::transform(&password, &encrypted_public, false),
            public
        );
        assert_eq!(
            *files::transform(&password, &encrypted_private, false),
            *secret
        );
    }
}
fn assert_signatures(state: &keeper::State, rows: &serde_json::Value) {
    for row in rows.as_array().unwrap() {
        let raw = bytes(&row["signing_bytes"]);
        let Header::Micro(header) =
            Header::decode(&raw, ae_codec::headers::HeaderMode::ProofOfWork).unwrap()
        else {
            panic!()
        };
        let signed = state.sign(header, None).unwrap();
        verify(&signed);
        assert_eq!(
            Header::Micro(signed.header).encode().unwrap(),
            bytes(&row["header"])
        );
    }
}
#[test]
fn keeper_files_promotions_and_protocol_one_through_eight_signatures_match_original_server() {
    let fixture = native();
    let row = &fixture["lifecycle"];
    let temp = Temp::new();
    let config = Config::new(temp.0.join("keys"), bytes(&row["password"]));
    let mut rng = Sequence {
        next: 1,
        fail: false,
    };
    let mut state = keeper::State::open(&config, &mut rng).unwrap();
    assert_public(state.public(), &row["initial"]);
    assert_signatures(&state, &row["before"]);
    assert_eq!(
        state.promote(AccountPublicKey([0; 32]), &mut rng),
        Err(Error::KeyNotFound)
    );
    let candidate = state.public().candidate;
    state.promote(candidate, &mut rng).unwrap();
    assert_public(state.public(), &row["promoted"]);
    assert_signatures(&state, &row["after_promote"]);
    assert_eq!(
        fs::read(config.directory.join("peer_key.pub")).unwrap(),
        bytes(&row["encrypted_public"])
    );
    assert_eq!(
        fs::read(config.directory.join("peer_key")).unwrap(),
        bytes(&row["encrypted_private"])
    );
    drop(state);
    let mut state = keeper::State::open(&config, &mut rng).unwrap();
    assert_public(state.public(), &row["restarted"]);
    assert_eq!(state.promote(candidate, &mut rng), Err(Error::KeyNotFound));
    assert_signatures(&state, &row["after_restart"]);
}

#[test]
fn key_crash_child() {
    let Some(directory) = std::env::var_os("AE_KEY_OWNER_CRASH_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let config = Config::new(directory.join("keys"), b"synthetic crash password".to_vec());
    let worker = Worker::spawn(config).unwrap();
    let client = worker.client();
    if std::env::var("AE_KEY_OWNER_CRASH_PROMOTE").unwrap() == "true" {
        client
            .promote_candidate(client.info().keys.candidate)
            .unwrap()
            .blocking_wait()
            .unwrap();
    }
    let keys = client.info().keys;
    fs::write(directory.join("ready.json"),serde_json::json!({"signing":hex::encode(keys.signing.0),"candidate":hex::encode(keys.candidate.0),"peer":hex::encode(keys.peer.0)}).to_string()).unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_key_owner_releases_lease_and_reopens_same_peer_with_fresh_signing_keys() {
    use std::{
        process::{Command, Stdio},
        time::Instant,
    };
    for promoted in [false, true] {
        let temp = Temp::new();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "keys::tests::key_crash_child", "--nocapture"])
            .env("AE_KEY_OWNER_CRASH_DIRECTORY", &temp.0)
            .env("AE_KEY_OWNER_CRASH_PROMOTE", promoted.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let ready = temp.0.join("ready.json");
        let started = Instant::now();
        let before: serde_json::Value = loop {
            if let Ok(raw) = fs::read(&ready)
                && let Ok(value) = serde_json::from_slice(&raw)
            {
                break value;
            }
            if child.try_wait().unwrap().is_some() {
                panic!("key crash worker exited before readiness");
            }
            if started.elapsed() > Duration::from_secs(30) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("key crash readiness timeout");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let worker = Worker::spawn(Config::new(
            temp.0.join("keys"),
            b"synthetic crash password".to_vec(),
        ))
        .unwrap();
        let keys = worker.info().keys;
        assert_eq!(hex::encode(keys.peer.0), before["peer"].as_str().unwrap());
        assert_ne!(
            hex::encode(keys.signing.0),
            before["signing"].as_str().unwrap()
        );
        assert_ne!(
            hex::encode(keys.candidate.0),
            before["candidate"].as_str().unwrap()
        );
        verify(
            &worker
                .client()
                .sign_micro(header())
                .unwrap()
                .blocking_wait()
                .unwrap(),
        );
        worker.shutdown().unwrap();
        println!(
            "Key owner crash recovery promoted={promoted}: peer identity retained and lease reacquired"
        );
    }
}
