use super::*;
use aes::{
    Aes256,
    cipher::{BlockDecrypt, BlockEncrypt, KeyInit},
};
use keeper::Entropy;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

fn io(operation: FileOperation, error: std::io::Error) -> Error {
    Error::Io {
        operation,
        kind: error.kind(),
    }
}
pub(super) fn transform(password: &[u8], value: &[u8; 32], encrypt: bool) -> Zeroizing<[u8; 32]> {
    let key = Zeroizing::new(ae_crypto::sha256(password).0);
    let cipher = Aes256::new((&*key).into());
    let mut bytes = Zeroizing::new(*value);
    for chunk in bytes.chunks_exact_mut(16) {
        if encrypt {
            cipher.encrypt_block(chunk.into());
        } else {
            cipher.decrypt_block(chunk.into());
        }
    }
    bytes
}
fn ordinary(path: &Path, operation: FileOperation) -> Result<Option<fs::Metadata>, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err(Error::InvalidPeerFile),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io(operation, e)),
    }
}
pub(super) struct Lease(File);
impl Drop for Lease {
    fn drop(&mut self) {
        // Close alone can leave ownership alive in a descriptor inherited by
        // a concurrently spawning child. Release the lock before closing this
        // owner's descriptor so joined shutdown permits immediate replacement.
        let _ = self.0.unlock();
    }
}
#[cfg(test)]
impl Lease {
    pub(super) fn try_clone(&self) -> std::io::Result<File> {
        self.0.try_clone()
    }
}
pub(super) fn lease(directory: &Path) -> Result<Lease, Error> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(directory)
        .map_err(|e| io(FileOperation::Directory, e))?;
    let path = directory.join(".aeternity-key-owner.lock");
    ordinary(&path, FileOperation::Lease)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| io(FileOperation::Lease, e))?;
    file.try_lock().map_err(|e| match e {
        std::fs::TryLockError::WouldBlock => Error::AlreadyRunning,
        std::fs::TryLockError::Error(e) => io(FileOperation::Lease, e),
    })?;
    Ok(Lease(file))
}
fn read(path: &Path, operation: FileOperation) -> Result<Option<Zeroizing<[u8; 32]>>, Error> {
    let Some(metadata) = ordinary(path, operation)? else {
        return Ok(None);
    };
    if metadata.len() != 32 {
        return Err(Error::InvalidPeerFile);
    }
    let mut file = File::open(path).map_err(|e| io(operation, e))?;
    if !file.metadata().map_err(|e| io(operation, e))?.is_file() {
        return Err(Error::InvalidPeerFile);
    }
    let mut bytes = Zeroizing::new([0; 32]);
    file.read_exact(bytes.as_mut())
        .map_err(|e| io(operation, e))?;
    let mut extra = [0];
    if file.read(&mut extra).map_err(|e| io(operation, e))? != 0 {
        return Err(Error::InvalidPeerFile);
    }
    Ok(Some(bytes))
}
fn write(path: &Path, value: &[u8; 32], operation: FileOperation) -> Result<(), Error> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|e| io(operation, e))?;
    file.write_all(value).map_err(|e| io(operation, e))?;
    file.sync_all().map_err(|e| io(operation, e))
}
pub(super) fn load_or_create(
    config: &Config,
    rng: &mut impl Entropy,
) -> Result<noise::Identity, Error> {
    let public_path = config.directory.join("peer_key.pub");
    let private_path = config.directory.join("peer_key");
    match (
        read(&public_path, FileOperation::ReadPublic)?,
        read(&private_path, FileOperation::ReadPrivate)?,
    ) {
        (Some(public), Some(private)) => {
            let public = transform(&config.password, &public, false);
            let secret = transform(&config.password, &private, false);
            let identity = noise::Identity::from_secret(*secret);
            if identity.public_key().0 != *public {
                return Err(Error::InvalidPeerPair);
            }
            Ok(identity)
        }
        (None, None) => {
            let seed = rng.seed()?;
            let secret = keeper::peer_secret(&seed);
            let identity = noise::Identity::from_secret(*secret);
            let public = transform(&config.password, &identity.public_key().0, true);
            let private = transform(&config.password, &secret, true);
            // Never overwrite an existing or partially created pair. Any I/O
            // failure leaves visible evidence for explicit operator recovery.
            write(&public_path, &public, FileOperation::WritePublic)?;
            write(&private_path, &private, FileOperation::WritePrivate)?;
            #[cfg(unix)]
            File::open(&config.directory)
                .and_then(|f| f.sync_all())
                .map_err(|e| io(FileOperation::SyncDirectory, e))?;
            Ok(identity)
        }
        _ => Err(Error::IncompletePeerFiles),
    }
}
