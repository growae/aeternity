use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

// Rotation bounds newly written log files. Existing operator-provided files
// are preserved as backups even if they already exceed the configured limit.
pub struct RotatingFile {
    path: PathBuf,
    file: Option<File>,
    bytes: u64,
    maximum: u64,
    backups: usize,
}
impl RotatingFile {
    pub fn open(path: impl AsRef<Path>, maximum: u64, backups: usize) -> io::Result<Self> {
        if maximum == 0 || maximum > 1 << 40 || backups == 0 || backups > 32 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let path = path.as_ref().to_owned();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path,
            file: Some(file),
            bytes,
            maximum,
            backups,
        })
    }
    fn backup(&self, n: usize) -> PathBuf {
        let mut path = self.path.as_os_str().to_owned();
        path.push(format!(".{n}"));
        path.into()
    }
    fn rotate(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.take();
        match fs::remove_file(self.backup(self.backups)) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        for n in (1..self.backups).rev() {
            match fs::rename(self.backup(n), self.backup(n + 1)) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
        }
        fs::rename(&self.path, self.backup(1))?;
        self.bytes = 0;
        self.file = Some(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&self.path)?,
        );
        Ok(())
    }
}
impl Write for RotatingFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.maximum {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if self.bytes > self.maximum - bytes.len() as u64 {
            self.rotate()?;
        }
        let file = self.file.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
        let written = file.write(bytes)?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().ok_or(io::ErrorKind::BrokenPipe)?.flush()
    }
}
