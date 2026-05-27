use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use uuid::Uuid;

use crate::backing_store::{BackingStoreT, Strategy};

const BLOBS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("file_backed_blobs");

/// An implementation of [`BackingStoreT`] and [`Strategy`] that stores blobs in
/// a redb embedded key-value database.
///
/// This backend is intended for values that are too small to deserve individual
/// files. It stores each item under its UUID bytes in a single redb table.
pub struct RedbStore<C> {
    codec: C,
    db: RedbPath,
}

/// Trait defining how to encode and decode a type `T` for byte-oriented stores.
pub trait BlobCodec<T>: Send + Sync + 'static {
    /// Encodes `data` into an owned byte vector.
    fn encode(&self, data: &T) -> anyhow::Result<Vec<u8>>;

    /// Decodes `data` from bytes.
    fn decode(&self, data: &[u8]) -> anyhow::Result<T>;
}

/// A [`BlobCodec`] implementation using the `bincode` crate.
///
/// Requires the `redb-bincodec` feature flag.
#[cfg(feature = "redb-bincodec")]
pub struct BinCodec;

#[cfg(feature = "redb-bincodec")]
impl<T: serde::Serialize + serde::de::DeserializeOwned> BlobCodec<T> for BinCodec {
    fn encode(&self, data: &T) -> anyhow::Result<Vec<u8>> {
        Ok(bincode::serde::encode_to_vec(
            data,
            bincode::config::legacy(),
        )?)
    }

    fn decode(&self, data: &[u8]) -> anyhow::Result<T> {
        Ok(bincode::serde::decode_from_slice(data, bincode::config::legacy())?.0)
    }
}

/// A [`BlobCodec`] implementation using the `prost` crate.
///
/// Requires the `redb-prostcodec` feature flag.
#[cfg(feature = "redb-prostcodec")]
pub struct ProstCodec;

#[cfg(feature = "redb-prostcodec")]
impl<T: Default + prost::Message> BlobCodec<T> for ProstCodec {
    fn encode(&self, data: &T) -> anyhow::Result<Vec<u8>> {
        Ok(data.encode_to_vec())
    }

    fn decode(&self, data: &[u8]) -> anyhow::Result<T> {
        Ok(T::decode(data)?)
    }
}

/// A prepared redb database path.
///
/// We expect exclusive access to this database file for writes. Multiple
/// [`RedbPath`] clones share the same opened database handle.
#[derive(Clone)]
pub struct RedbPath {
    path: Arc<PathBuf>,
    db: Arc<Database>,
}

impl RedbPath {
    /// Opens the specified redb database file, creating it if needed, and ensures
    /// the blob table exists.
    pub fn new(path: PathBuf) -> Self {
        let db = Database::create(&path).unwrap_or_else(|err| {
            panic!("Failed to open redb database {}: {err:?}", path.display())
        });
        let this = Self {
            path: Arc::new(path),
            db: Arc::new(db),
        };
        this.ensure_table();
        this
    }

    /// Returns the database file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ensure_table(&self) {
        let write_txn = self.db.begin_write().unwrap_or_else(|err| {
            panic!(
                "Failed to begin redb write transaction for {}: {err:?}",
                self.path.display()
            )
        });
        {
            write_txn.open_table(BLOBS).unwrap_or_else(|err| {
                panic!(
                    "Failed to open redb table in {}: {err:?}",
                    self.path.display()
                )
            });
        }
        write_txn.commit().unwrap_or_else(|err| {
            panic!(
                "Failed to commit redb table initialization for {}: {err:?}",
                self.path.display()
            )
        });
    }
}

impl<C> RedbStore<C> {
    /// Creates a new redb-backed store.
    pub fn new(codec: C, db: RedbPath) -> Self {
        Self { codec, db }
    }
}

impl<C: Send + Sync + 'static> BackingStoreT for RedbStore<C> {
    type PersistPath = RedbPath;

    fn delete(&self, key: Uuid) {
        remove_key(&self.db, key, "temporary");
    }

    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) {
        remove_key(path, key, "persisted");
    }

    fn register(&self, src_path: &Self::PersistPath, key: Uuid) {
        let bytes = read_bytes(src_path, key, "persisted");
        insert_bytes(&self.db, key, &bytes, "temporary");
    }

    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid) {
        let bytes = read_bytes(&self.db, key, "temporary");
        insert_bytes(dest_path, key, &bytes, "persisted");
    }

    fn sanitize_path(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
        let read_txn = path.db.begin_read().unwrap_or_else(|err| {
            panic!(
                "Failed to begin redb read transaction for {}: {err:?}",
                path.path.display()
            )
        });
        let table = read_txn.open_table(BLOBS).unwrap_or_else(|err| {
            panic!(
                "Failed to open redb table in {}: {err:?}",
                path.path.display()
            )
        });
        table
            .iter()
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to iterate redb table in {}: {err:?}",
                    path.path.display()
                )
            })
            .map(|entry| {
                let (key, _) = entry.unwrap_or_else(|err| {
                    panic!(
                        "Failed to read redb table entry in {}: {err:?}",
                        path.path.display()
                    )
                });
                Uuid::from_bytes(*key.value())
            })
            .collect::<Vec<_>>()
    }

    fn sync_persisted(&self, _path: &Self::PersistPath) {
        // Each mutation is committed durably as part of the operation that performs it.
    }
}

impl<T, C: BlobCodec<T>> Strategy<T> for RedbStore<C> {
    fn store(&self, key: Uuid, data: &T) {
        let bytes = self.codec.encode(data).unwrap_or_else(|err| {
            panic!("Failed to encode data for redb key {key}: {err:?}");
        });
        insert_bytes(&self.db, key, &bytes, "temporary");
    }

    fn load(&self, key: Uuid) -> T {
        let bytes = read_bytes(&self.db, key, "temporary");
        self.codec.decode(&bytes).unwrap_or_else(|err| {
            panic!("Failed to decode data for redb key {key}: {err:?}");
        })
    }
}

fn read_bytes(path: &RedbPath, key: Uuid, label: &str) -> Vec<u8> {
    let read_txn = path.db.begin_read().unwrap_or_else(|err| {
        panic!(
            "Failed to begin redb read transaction for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
    let table = read_txn.open_table(BLOBS).unwrap_or_else(|err| {
        panic!(
            "Failed to open redb table for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
    table
        .get(key.as_bytes())
        .unwrap_or_else(|err| {
            panic!(
                "Failed to read redb key {} from {} store {}: {err:?}",
                key,
                label,
                path.path.display()
            )
        })
        .unwrap_or_else(|| {
            panic!(
                "Attempted to read missing redb key {} from {} store {}",
                key,
                label,
                path.path.display()
            )
        })
        .value()
        .to_vec()
}

fn insert_bytes(path: &RedbPath, key: Uuid, bytes: &[u8], label: &str) {
    let write_txn = path.db.begin_write().unwrap_or_else(|err| {
        panic!(
            "Failed to begin redb write transaction for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
    {
        let mut table = write_txn.open_table(BLOBS).unwrap_or_else(|err| {
            panic!(
                "Failed to open redb table for {} store {}: {err:?}",
                label,
                path.path.display()
            )
        });
        let old = table.insert(key.as_bytes(), bytes).unwrap_or_else(|err| {
            panic!(
                "Failed to insert redb key {} into {} store {}: {err:?}",
                key,
                label,
                path.path.display()
            )
        });
        assert!(
            old.is_none(),
            "Attempted to overwrite existing redb key {} in {} store {}",
            key,
            label,
            path.path.display()
        );
    }
    write_txn.commit().unwrap_or_else(|err| {
        panic!(
            "Failed to commit redb insert for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
}

fn remove_key(path: &RedbPath, key: Uuid, label: &str) {
    let write_txn = path.db.begin_write().unwrap_or_else(|err| {
        panic!(
            "Failed to begin redb write transaction for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
    {
        let mut table = write_txn.open_table(BLOBS).unwrap_or_else(|err| {
            panic!(
                "Failed to open redb table for {} store {}: {err:?}",
                label,
                path.path.display()
            )
        });
        let old = table.remove(key.as_bytes()).unwrap_or_else(|err| {
            panic!(
                "Failed to remove redb key {} from {} store {}: {err:?}",
                key,
                label,
                path.path.display()
            )
        });
        assert!(
            old.is_some(),
            "Attempted to delete missing redb key {} from {} store {}",
            key,
            label,
            path.path.display()
        );
    }
    write_txn.commit().unwrap_or_else(|err| {
        panic!(
            "Failed to commit redb delete for {} store {}: {err:?}",
            label,
            path.path.display()
        )
    });
}

#[cfg(all(test, feature = "redb-bincodec"))]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use tokio::runtime::Handle;

    use crate::{BackingStore, FBPool};

    use super::{BinCodec, RedbPath, RedbStore};

    #[tokio::test]
    async fn persists_registers_and_loads() {
        let cache_dir = tempdir().unwrap();
        let persist_dir = tempdir().unwrap();
        let cache_path = cache_dir.path().join("cache.redb");
        let persist_path = persist_dir.path().join("persist.redb");
        let persisted_key;

        {
            let redb_store = RedbStore::new(BinCodec, RedbPath::new(cache_path.clone()));
            let store = Arc::new(BackingStore::new(redb_store, Handle::current()));
            let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));
            let tracked_persist = Arc::new(
                store
                    .track_path(RedbPath::new(persist_path.clone()))
                    .await
                    .unwrap(),
            );

            let item = pool.insert("Persisted Data".to_string());
            persisted_key = item.key();
            item.spawn_persist(&tracked_persist).await.await.unwrap();
            drop(item);
            store.finished().await;
        }

        {
            let redb_store = RedbStore::new(BinCodec, RedbPath::new(cache_path));
            let store = Arc::new(BackingStore::new(redb_store, Handle::current()));
            let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));
            let tracked_persist =
                Arc::new(store.track_path(RedbPath::new(persist_path)).await.unwrap());

            assert!(tracked_persist.all_keys().contains(&persisted_key));

            let item = pool
                .register(&tracked_persist, persisted_key)
                .await
                .expect("registered item");
            let guard = item.load().await;

            assert_eq!(*guard, "Persisted Data");
            drop(guard);
            drop(item);
            store.finished().await;
        }
    }
}
