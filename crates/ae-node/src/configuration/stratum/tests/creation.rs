use super::*;
use std::fs;
use zeroize::Zeroizing;

fn read(path: &Path) -> Result<Vec<u8>, std::io::ErrorKind> {
    fs::read(path).map_err(|e| e.kind())
}
fn state(value: &Result<Vec<u8>, std::io::ErrorKind>) -> Value {
    match value {
        Ok(bytes) => json!({"kind":"file","bytes":bytes.len()}),
        Err(std::io::ErrorKind::NotFound) => json!({"kind":"missing"}),
        Err(_) => json!({"kind":"unreadable"}),
    }
}

#[test]
fn original_stratum_key_creation_preserves_pairs_and_partial_write_order() {
    let fixture: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_stratum_keys.json"
    ))
    .unwrap();
    assert_eq!(fixture.len(), 15);
    for expected in fixture {
        let name = expected["name"].as_str().unwrap();
        let lab = Lab::new();
        let directory = match name {
            "nested" => lab.0.join("nested/keys"),
            "parent_file" => {
                fs::write(lab.0.join("parent"), [0]).unwrap();
                lab.0.join("parent/keys")
            }
            _ => lab.0.clone(),
        };
        let public = directory.join("sign_key.pub");
        let secret = directory.join("sign_key");
        match name {
            "empty" | "nested" | "parent_file" => (),
            "valid" | "wrong_public" | "wrong_suffix" | "short_secret" => {
                lab.keys(name);
            }
            "short_public" => {
                lab.keys("valid");
                fs::write(&public, [0]).unwrap();
            }
            "empty_both" => {
                fs::write(&public, []).unwrap();
                fs::write(&secret, []).unwrap();
            }
            "only_public" | "invalid_public_missing_secret" => {
                lab.keys("valid");
                fs::remove_file(&secret).unwrap();
                if name == "invalid_public_missing_secret" {
                    fs::write(&public, [0]).unwrap();
                }
            }
            "only_secret" | "missing_public_short_secret" => {
                lab.keys(if name == "only_secret" {
                    "valid"
                } else {
                    "short_secret"
                });
                fs::remove_file(&public).unwrap();
            }
            "public_directory" | "secret_directory" => {
                lab.keys("valid");
                let path = if name == "public_directory" {
                    &public
                } else {
                    &secret
                };
                fs::remove_file(path).unwrap();
                fs::create_dir(path).unwrap();
            }
            _ => panic!("unknown native key fixture"),
        }
        let before_public = read(&public);
        let before_secret = read(&secret);
        let mut user = lab.user();
        user["stratum"]["reward"]["keys"]["dir"] = json!(directory);
        let result = lab.resolve(user).unwrap().unwrap().load_or_create();
        let after_public = read(&public);
        let after_secret = read(&secret);
        assert_eq!(
            json!({
                "name":name,"ok":result.is_ok(),
                "public":state(&after_public),"secret":state(&after_secret),
                "public_unchanged":before_public==after_public,
                "secret_unchanged":before_secret==after_secret
            }),
            expected,
            "{name}: {:?}",
            result.as_ref().err()
        );
        if let Ok(prepared) = result {
            let key = keys::load(&directory).unwrap();
            assert_eq!(key.public_key(), prepared.caller());
            assert!(ae_crypto::verify_ed25519(
                &prepared.caller(),
                b"generated",
                &key.sign(b"generated")
            ));
        }
    }
}

#[test]
fn stratum_key_creation_requests_entropy_only_after_a_file_read_error() {
    let lab = Lab::new();
    let public = lab.keys("valid");
    let key =
        keys::load_or_create_with(&lab.0, || panic!("existing pair needs no entropy")).unwrap();
    assert_eq!(key.public_key(), public);
    lab.keys("wrong_public");
    assert!(matches!(
        keys::load_or_create_with(&lab.0, || panic!("invalid complete pair must be preserved")),
        Err(Error::KeyPair)
    ));
    let missing = lab.0.join("not-created");
    assert!(matches!(
        keys::load_or_create_with(&missing, || Err(Error::Entropy)),
        Err(Error::Entropy)
    ));
    assert!(!missing.exists());
    lab.keys("valid");
    fs::remove_file(lab.0.join("sign_key")).unwrap();
    assert!(matches!(
        keys::load_or_create_with(&lab.0, || Err(Error::Entropy)),
        Err(Error::Entropy)
    ));
    assert_eq!(fs::read(lab.0.join("sign_key.pub")).unwrap(), public.0);
    assert!(!lab.0.join("sign_key").exists());
    let mut seed = Zeroizing::new([0; 32]);
    seed[31] = 19;
    let generated = keys::load_or_create_with(&lab.0, || Ok(seed)).unwrap();
    let secret = fs::read(lab.0.join("sign_key")).unwrap();
    assert_ne!(generated.public_key(), public);
    assert_eq!(secret.len(), 64);
    assert_eq!(secret[..31], [0; 31]);
    assert_eq!(secret[31], 19);
    assert_eq!(secret[32..], generated.public_key().0);
}

#[cfg(unix)]
#[test]
fn created_stratum_key_files_are_private_and_readonly_existing_files_are_reused() {
    use std::os::unix::fs::PermissionsExt;
    let lab = Lab::new();
    let directory = lab.0.join("private/keys");
    let key = keys::load_or_create(&directory).unwrap();
    assert_eq!(
        fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["sign_key.pub", "sign_key"] {
        let path = directory.join(name);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(path, fs::Permissions::from_mode(0o400)).unwrap();
    }
    let again =
        keys::load_or_create_with(&directory, || panic!("readable files remain intact")).unwrap();
    assert_eq!(again.public_key(), key.public_key());
    for name in ["sign_key.pub", "sign_key"] {
        assert_eq!(
            fs::metadata(directory.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
    }
}

#[test]
fn stratum_key_wait_retries_directory_read_errors_without_creating_files() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            let lab = Lab::new();
            let public = lab.keys("valid");
            let path = lab.0.join("sign_key.pub");
            fs::remove_file(&path).unwrap();
            fs::create_dir(&path).unwrap();
            let mut waiting = Box::pin(
                lab.resolve(lab.user())
                    .unwrap()
                    .unwrap()
                    .load_when_available(),
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                    .await
                    .is_err()
            );
            assert!(path.is_dir());
            fs::remove_dir(&path).unwrap();
            fs::write(path, public.0).unwrap();
            let prepared = tokio::time::timeout(Duration::from_secs(2), waiting)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(prepared.caller(), public);
        });
}
