// aestratum_config:read_keys/1,2; native create ordering is significant.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Ed25519SecretKey, Error};
use rand::RngCore;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
use zeroize::Zeroizing;

type Bytes = Zeroizing<Vec<u8>>;
fn io(e: std::io::Error) -> Error {
    Error::Keys(e.kind())
}
fn bytes(path: &Path, expected: usize) -> Result<Bytes, Error> {
    fn regular(meta: &fs::Metadata) -> Result<(), Error> {
        if meta.is_dir() {
            return Err(Error::Keys(std::io::ErrorKind::IsADirectory));
        }
        if !meta.is_file() {
            return Err(Error::KeyPair);
        }
        Ok(())
    }
    regular(&fs::metadata(path).map_err(io)?)?;
    let file = File::open(path).map_err(io)?;
    regular(&file.metadata().map_err(io)?)?;
    let mut value = Zeroizing::new(Vec::with_capacity(expected + 1));
    file.take((expected + 1) as u64)
        .read_to_end(&mut value)
        .map_err(io)?;
    Ok(value)
}
fn read(directory: &Path) -> Result<(Bytes, Bytes), Error> {
    // Read both files before checking lengths. Native creation retries a
    // missing secret even when the readable public file has a bad length.
    let public = bytes(&directory.join("sign_key.pub"), 32)?;
    let secret = bytes(&directory.join("sign_key"), 64)?;
    Ok((public, secret))
}
fn validate(public: &[u8], secret: &[u8]) -> Result<Ed25519SecretKey, Error> {
    if public.len() != 32 || secret.len() != 64 {
        return Err(Error::KeyPair);
    }
    let key = Ed25519SecretKey::from_seed(secret[..32].try_into().map_err(|_| Error::KeyPair)?);
    let caller = key.public_key();
    if public != caller.0
        || secret[32..] != caller.0
        || !ae_crypto::verify_ed25519(&caller, b"roundtrip", &key.sign(b"roundtrip"))
    {
        return Err(Error::KeyPair);
    }
    Ok(key)
}
pub(super) fn load(directory: &Path) -> Result<Ed25519SecretKey, Error> {
    let (public, secret) = read(directory)?;
    validate(&public, &secret)
}
fn write(path: &Path, value: &[u8]) -> Result<(), Error> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io)?;
    file.write_all(value).map_err(io)?;
    file.sync_all().map_err(io)
}
pub(super) fn load_or_create(directory: &Path) -> Result<Ed25519SecretKey, Error> {
    load_or_create_with(directory, || {
        let mut seed = Zeroizing::new([0; 32]);
        rand::rngs::OsRng
            .try_fill_bytes(seed.as_mut())
            .map_err(|_| Error::Entropy)?;
        Ok(seed)
    })
}
pub(super) fn load_or_create_with(
    directory: &Path,
    entropy: impl FnOnce() -> Result<Zeroizing<[u8; 32]>, Error>,
) -> Result<Ed25519SecretKey, Error> {
    match read(directory) {
        Ok((public, secret)) => return validate(&public, &secret),
        Err(Error::Keys(_)) => (),
        Err(error) => return Err(error),
    }
    // Generate before attempting directory creation, as in the conductor.
    let seed = entropy()?;
    let key = Ed25519SecretKey::from_seed(&seed);
    let public = key.public_key();
    let mut secret = Zeroizing::new([0; 64]);
    secret[..32].copy_from_slice(seed.as_ref());
    secret[32..].copy_from_slice(&public.0);
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory).map_err(io)?;
    // Preserve native public-then-secret writes and partial failure evidence.
    write(&directory.join("sign_key.pub"), &public.0)?;
    write(&directory.join("sign_key"), secret.as_ref())?;
    #[cfg(unix)]
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(io)?;
    Ok(key)
}
