//! Durable local-filesystem implementation of the storage contract.
//!
//! Mutations are serialized with one advisory interprocess lock file. New
//! content is written to a unique file in the target directory, flushed with
//! `fsync`, published with a same-filesystem atomic operation, and followed by
//! a directory `fsync`. Release guarantees target Linux/POSIX filesystem
//! semantics; advisory locks require every writer to follow this protocol.

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use fs4::FileExt;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{ObjectKey, Storage, StorageError, StorageOperation, StoredObject, VersionToken};

const LOCK_FILE_NAME: &str = ".snapshotstore.lock";
const TEMP_FILE_PREFIX: &str = ".snapshotstore-tmp-";

/// A storage backend rooted at one local directory.
#[derive(Debug, Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    /// Creates or opens a local storage root and its interprocess lock file.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let requested = root.as_ref();
        fs::create_dir_all(requested)?;
        let root = requested.canonicalize()?;
        if !root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("local storage root is not a directory: {}", root.display()),
            ));
        }

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(LOCK_FILE_NAME))?;
        lock.sync_all()?;
        sync_directory(&root)?;
        if let Some(parent) = root.parent() {
            sync_directory(parent)?;
        }

        Ok(Self { root })
    }

    /// Returns the canonical storage root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(key.as_str())
    }

    fn acquire_lock(&self, operation: StorageOperation) -> Result<File, StorageError> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.join(LOCK_FILE_NAME))
            .map_err(|error| unavailable(operation, error))?;
        FileExt::lock(&lock).map_err(|error| unavailable(operation, error))?;
        Ok(lock)
    }

    fn read_sync(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        let operation = StorageOperation::GetWithToken;
        let path = self.object_path(key);
        let mut file = File::open(&path).map_err(|error| read_error(key, operation, error))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| unavailable(operation, error))?;
        let bytes = Bytes::from(bytes);
        let version = token_for(&bytes);
        Ok(StoredObject { bytes, version })
    }

    fn create_sync(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        operation: StorageOperation,
    ) -> Result<VersionToken, StorageError> {
        let _lock = self.acquire_lock(operation)?;
        let target = self.object_path(key);
        let parent = target.parent().expect("object paths always have a parent");
        self.ensure_directories(parent, operation)?;

        if target.exists() {
            return Err(StorageError::AlreadyExists { key: key.clone() });
        }

        let mut temp = self.write_temp(parent, &bytes, operation)?;
        match fs::hard_link(temp.path(), &target) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(StorageError::AlreadyExists { key: key.clone() });
            }
            Err(error) => return Err(unavailable(operation, error)),
        }

        if fs::remove_file(temp.path()).is_ok() {
            temp.disarm();
        }
        sync_directory(parent).map_err(|error| unknown_outcome(operation, key, error))?;

        Ok(token_for(&bytes))
    }

    fn replace_sync(
        &self,
        key: &ObjectKey,
        expected: &VersionToken,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let operation = StorageOperation::ReplaceIfTokenMatches;
        let _lock = self.acquire_lock(operation)?;
        let current = self.read_sync(key)?;
        if &current.version != expected {
            return Err(StorageError::PreconditionFailed { key: key.clone() });
        }

        let target = self.object_path(key);
        let parent = target.parent().expect("object paths always have a parent");
        self.ensure_directories(parent, operation)?;
        let mut temp = self.write_temp(parent, &bytes, operation)?;
        fs::rename(temp.path(), &target).map_err(|error| unavailable(operation, error))?;
        temp.disarm();
        sync_directory(parent).map_err(|error| unknown_outcome(operation, key, error))?;

        Ok(token_for(&bytes))
    }

    fn ensure_directories(
        &self,
        directory: &Path,
        operation: StorageOperation,
    ) -> Result<(), StorageError> {
        let relative =
            directory
                .strip_prefix(&self.root)
                .map_err(|_| StorageError::InvalidResponse {
                    operation,
                    message: format!(
                        "resolved directory escaped local root: {}",
                        directory.display()
                    ),
                })?;
        let mut current = self.root.clone();

        for component in relative.components() {
            let next = current.join(component);
            if !next.exists() {
                match fs::create_dir(&next) {
                    Ok(()) => {
                        sync_directory(&current).map_err(|error| unavailable(operation, error))?
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(unavailable(operation, error)),
                }
            }
            if !next.is_dir() {
                return Err(StorageError::InvalidResponse {
                    operation,
                    message: format!("object parent is not a directory: {}", next.display()),
                });
            }
            current = next;
        }

        Ok(())
    }

    fn write_temp(
        &self,
        directory: &Path,
        bytes: &Bytes,
        operation: StorageOperation,
    ) -> Result<TempFileGuard, StorageError> {
        for _ in 0..8 {
            let path = directory.join(format!("{TEMP_FILE_PREFIX}{}", Uuid::new_v4()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(bytes)
                        .map_err(|error| unavailable(operation, error))?;
                    file.sync_all()
                        .map_err(|error| unavailable(operation, error))?;
                    drop(file);
                    return Ok(TempFileGuard::new(path));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(unavailable(operation, error)),
            }
        }

        Err(StorageError::Unavailable {
            operation,
            message: "could not allocate a unique temporary file".to_owned(),
        })
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.read_sync(key)
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.create_sync(key, bytes, StorageOperation::PutImmutable)
    }

    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.create_sync(key, bytes, StorageOperation::CreateIfAbsent)
    }

    async fn replace_if_token_matches(
        &self,
        key: &ObjectKey,
        expected: &VersionToken,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.replace_sync(key, expected, bytes)
    }
}

fn read_error(key: &ObjectKey, operation: StorageOperation, error: io::Error) -> StorageError {
    if error.kind() == io::ErrorKind::NotFound {
        StorageError::NotFound { key: key.clone() }
    } else {
        unavailable(operation, error)
    }
}

fn unavailable(operation: StorageOperation, error: io::Error) -> StorageError {
    StorageError::Unavailable {
        operation,
        message: error.to_string(),
    }
}

fn unknown_outcome(operation: StorageOperation, key: &ObjectKey, error: io::Error) -> StorageError {
    StorageError::UnknownWriteOutcome {
        operation,
        key: key.clone(),
        message: error.to_string(),
    }
}

fn token_for(bytes: &Bytes) -> VersionToken {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity("sha256:".len() + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    VersionToken::from_opaque(value).expect("SHA-256 tokens are never empty")
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_replace_and_immutable_put_follow_contract() {
        let directory = tempdir().expect("temporary directory");
        let storage = LocalStorage::open(directory.path()).expect("open local storage");
        let head = ObjectKey::parse("HEAD").expect("valid key");
        let manifest = ObjectKey::parse("manifests/commit.json").expect("valid key");

        let first = storage
            .create_if_absent(&head, Bytes::from_static(b"version-0"))
            .await
            .expect("create HEAD");
        assert_eq!(
            storage
                .create_if_absent(&head, Bytes::from_static(b"duplicate"))
                .await,
            Err(StorageError::AlreadyExists { key: head.clone() })
        );

        let second = storage
            .replace_if_token_matches(&head, &first, Bytes::from_static(b"version-1"))
            .await
            .expect("replace matching HEAD");
        assert_ne!(first, second);
        assert_eq!(
            storage
                .replace_if_token_matches(&head, &first, Bytes::from_static(b"stale"))
                .await,
            Err(StorageError::PreconditionFailed { key: head.clone() })
        );
        assert_eq!(
            storage
                .get_with_token(&head)
                .await
                .expect("read HEAD")
                .bytes,
            Bytes::from_static(b"version-1")
        );

        storage
            .put_immutable(&manifest, Bytes::from_static(b"manifest"))
            .await
            .expect("write immutable manifest");
        assert_eq!(
            storage
                .put_immutable(&manifest, Bytes::from_static(b"overwrite"))
                .await,
            Err(StorageError::AlreadyExists {
                key: manifest.clone()
            })
        );
        assert_eq!(
            storage
                .get_with_token(&manifest)
                .await
                .expect("read manifest")
                .bytes,
            Bytes::from_static(b"manifest")
        );
    }

    #[tokio::test]
    async fn successful_writes_leave_no_temporary_files() {
        let directory = tempdir().expect("temporary directory");
        let storage = LocalStorage::open(directory.path()).expect("open local storage");
        let key = ObjectKey::parse("data/attempt/file.parquet").expect("valid key");

        storage
            .put_immutable(&key, Bytes::from_static(b"data"))
            .await
            .expect("write object");

        let data_directory = directory.path().join("data/attempt");
        let names: Vec<_> = fs::read_dir(data_directory)
            .expect("read data directory")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect();
        assert_eq!(names, vec!["file.parquet"]);
    }
}
