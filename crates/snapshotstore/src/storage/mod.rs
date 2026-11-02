//! Storage primitives required by the publication protocol.
//!
//! A backend must provide atomic create-if-absent and token-matched replacement.
//! Listing is intentionally absent: committed state is discovered only through
//! `HEAD` and immutable manifests.

pub mod local;

use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A relative object key within one table's configured storage root.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Parses and validates a relative object key.
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidObjectKey> {
        let value = value.into();

        if value.is_empty() {
            return Err(InvalidObjectKey::Empty);
        }
        if value.starts_with('/') || value.starts_with('\\') {
            return Err(InvalidObjectKey::Absolute(value));
        }
        if value.contains('\\') {
            return Err(InvalidObjectKey::Backslash(value));
        }
        if value.chars().any(char::is_control) {
            return Err(InvalidObjectKey::ControlCharacter(value));
        }
        if value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(InvalidObjectKey::InvalidSegment(value));
        }

        Ok(Self(value))
    }

    /// Returns the backend-independent relative key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ObjectKey").field(&self.0).finish()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ObjectKey {
    type Err = InvalidObjectKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for ObjectKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// Why a relative object key was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidObjectKey {
    #[error("object key must not be empty")]
    Empty,

    #[error("object key must be relative: {0}")]
    Absolute(String),

    #[error("object key must use forward slashes: {0}")]
    Backslash(String),

    #[error("object key must not contain control characters: {0:?}")]
    ControlCharacter(String),

    #[error("object key contains an empty, dot, or parent segment: {0}")]
    InvalidSegment(String),
}

/// An opaque backend concurrency token returned by a successful read or write.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct VersionToken(String);

impl VersionToken {
    /// Wraps a backend token without interpreting its contents.
    pub fn from_opaque(value: impl Into<String>) -> Result<Self, InvalidVersionToken> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidVersionToken);
        }
        Ok(Self(value))
    }

    /// Exposes the token only so an adapter can return it to its backend.
    pub fn as_opaque(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for VersionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VersionToken(<opaque>)")
    }
}

/// An empty backend token cannot safely participate in compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("backend version token must not be empty")]
pub struct InvalidVersionToken;

/// The bytes and opaque version token observed by one atomic read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub bytes: Bytes,
    pub version: VersionToken,
}

/// Storage operation names used in typed availability and outcome errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOperation {
    GetWithToken,
    PutImmutable,
    CreateIfAbsent,
    ReplaceIfTokenMatches,
}

impl fmt::Display for StorageOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::GetWithToken => "get_with_token",
            Self::PutImmutable => "put_immutable",
            Self::CreateIfAbsent => "create_if_absent",
            Self::ReplaceIfTokenMatches => "replace_if_token_matches",
        })
    }
}

/// Failures exposed by a storage adapter.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageError {
    #[error("object not found: {key}")]
    NotFound { key: ObjectKey },

    #[error("object already exists: {key}")]
    AlreadyExists { key: ObjectKey },

    #[error("conditional write rejected for {key}")]
    PreconditionFailed { key: ObjectKey },

    /// The operation definitely did not complete because storage was unavailable.
    #[error("storage unavailable during {operation}: {message}")]
    Unavailable {
        operation: StorageOperation,
        message: String,
    },

    /// A write was attempted, but the adapter cannot prove whether it took effect.
    #[error("unknown write outcome during {operation} for {key}: {message}")]
    UnknownWriteOutcome {
        operation: StorageOperation,
        key: ObjectKey,
        message: String,
    },

    #[error("invalid backend response during {operation}: {message}")]
    InvalidResponse {
        operation: StorageOperation,
        message: String,
    },
}

/// Minimal storage behavior required for transactional publication.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Reads an object and its version token from the same backend response.
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError>;

    /// Creates an immutable object and must never overwrite an existing key.
    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError>;

    /// Creates a mutable publication object only when its key is absent.
    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError>;

    /// Atomically replaces an object only when its current token matches.
    async fn replace_if_token_matches(
        &self,
        key: &ObjectKey,
        expected: &VersionToken,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct MemoryStorage {
        state: Mutex<HashMap<ObjectKey, (Bytes, u64)>>,
    }

    impl MemoryStorage {
        fn token(generation: u64) -> VersionToken {
            VersionToken::from_opaque(format!("generation-{generation}"))
                .expect("test generations are never empty")
        }

        fn insert_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<VersionToken, StorageError> {
            let mut state = self.state.lock().expect("memory store lock poisoned");
            if state.contains_key(key) {
                return Err(StorageError::AlreadyExists { key: key.clone() });
            }
            state.insert(key.clone(), (bytes, 1));
            Ok(Self::token(1))
        }
    }

    #[async_trait]
    impl Storage for MemoryStorage {
        async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
            let state = self.state.lock().expect("memory store lock poisoned");
            let (bytes, generation) = state
                .get(key)
                .ok_or_else(|| StorageError::NotFound { key: key.clone() })?;
            Ok(StoredObject {
                bytes: bytes.clone(),
                version: Self::token(*generation),
            })
        }

        async fn put_immutable(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<VersionToken, StorageError> {
            self.insert_absent(key, bytes)
        }

        async fn create_if_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<VersionToken, StorageError> {
            self.insert_absent(key, bytes)
        }

        async fn replace_if_token_matches(
            &self,
            key: &ObjectKey,
            expected: &VersionToken,
            bytes: Bytes,
        ) -> Result<VersionToken, StorageError> {
            let mut state = self.state.lock().expect("memory store lock poisoned");
            let (_, generation) = state
                .get_mut(key)
                .ok_or_else(|| StorageError::NotFound { key: key.clone() })?;
            if &Self::token(*generation) != expected {
                return Err(StorageError::PreconditionFailed { key: key.clone() });
            }
            *generation += 1;
            let next = *generation;
            state.insert(key.clone(), (bytes, next));
            Ok(Self::token(next))
        }
    }

    #[test]
    fn object_keys_are_relative_and_normalized() {
        assert_eq!(
            ObjectKey::parse("manifests/commit.json")
                .expect("valid key")
                .as_str(),
            "manifests/commit.json"
        );

        for invalid in [
            "",
            "/HEAD",
            "data//file",
            "data/../HEAD",
            "data\\file",
            "data/line\nbreak",
        ] {
            assert!(ObjectKey::parse(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn token_debug_output_does_not_expose_backend_contents() {
        let token = VersionToken::from_opaque("etag-value").expect("valid token");
        assert_eq!(format!("{token:?}"), "VersionToken(<opaque>)");
    }

    #[tokio::test]
    async fn contract_distinguishes_create_conflict_and_stale_replace() {
        let storage = MemoryStorage::default();
        let head = ObjectKey::parse("HEAD").expect("valid key");

        let first = storage
            .create_if_absent(&head, Bytes::from_static(b"version-0"))
            .await
            .expect("initial create succeeds");
        assert_eq!(
            storage
                .create_if_absent(&head, Bytes::from_static(b"other"))
                .await,
            Err(StorageError::AlreadyExists { key: head.clone() })
        );

        let second = storage
            .replace_if_token_matches(&head, &first, Bytes::from_static(b"version-1"))
            .await
            .expect("matching token succeeds");
        assert_ne!(first, second);
        assert_eq!(
            storage
                .replace_if_token_matches(&head, &first, Bytes::from_static(b"lost-update"))
                .await,
            Err(StorageError::PreconditionFailed { key: head.clone() })
        );

        let observed = storage
            .get_with_token(&head)
            .await
            .expect("committed object remains readable");
        assert_eq!(observed.bytes, Bytes::from_static(b"version-1"));
        assert_eq!(observed.version, second);
    }

    #[tokio::test]
    async fn immutable_put_never_overwrites() {
        let storage = MemoryStorage::default();
        let manifest = ObjectKey::parse("manifests/commit.json").expect("valid key");

        storage
            .put_immutable(&manifest, Bytes::from_static(b"first"))
            .await
            .expect("first immutable write succeeds");
        assert_eq!(
            storage
                .put_immutable(&manifest, Bytes::from_static(b"second"))
                .await,
            Err(StorageError::AlreadyExists {
                key: manifest.clone()
            })
        );
        assert_eq!(
            storage
                .get_with_token(&manifest)
                .await
                .expect("original object remains")
                .bytes,
            Bytes::from_static(b"first")
        );
    }
}
