use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use parking_lot::{Condvar, Mutex};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use uuid::Uuid;

pub use self::redb_bytes::RedbBytes;
use crate::backing_store::{BackingStoreT, Strategy};

mod redb_bytes;

const BLOBS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("file_backed_blobs");

static WRITE_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);

/// Total number of redb write transactions committed by this process, across all
/// redb-backed stores.
pub fn total_write_transactions() -> u64 {
    WRITE_TRANSACTIONS.load(Ordering::Relaxed)
}

fn count_committed_write_transaction() {
    WRITE_TRANSACTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Name of the database file a redb-backed store keeps inside its directory.
///
/// [`RedbPath::in_dir`] opens this file, and archived copies of a store (e.g. inside a tar)
/// conventionally carry it under this name.
pub const REDB_FILE_NAME: &str = "file_backed.redb";

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

    /// Encodes `data` and passes the encoded bytes to `use_bytes`.
    ///
    /// The bytes are only valid for the duration of the callback, so implementations that
    /// keep their encoded bytes in a pooled or borrowed buffer can override this to avoid
    /// allocating a fresh `Vec` per call (and define [`encode`](Self::encode) in terms of it).
    fn encode_with<R>(&self, data: &T, use_bytes: impl FnOnce(&[u8]) -> R) -> anyhow::Result<R> {
        Ok(use_bytes(&self.encode(data)?))
    }

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
///
/// Writes are group-committed: a caller applies its own operation to a shared open write
/// transaction and returns once the transaction containing it commits. Operations that
/// arrive while a commit is in flight accumulate into the next transaction, so concurrent
/// writers share commits instead of serializing one commit each behind redb's
/// single-writer lock.
#[derive(Clone)]
pub struct RedbPath {
    inner: Arc<RedbInner>,
}

struct RedbInner {
    path: PathBuf,
    db: Database,
    group: Mutex<GroupState>,
    cv: Condvar,
}

struct GroupState {
    /// The shared open batch that submitted operations apply to. `None` while a
    /// commit is in flight (or at quiescence — the last applier always commits).
    batch: Option<OpenBatch>,
    committing: bool,
    /// Callers parked waiting for the in-flight commit to finish so they can apply into
    /// the next transaction. The last applier to find this at zero commits the batch.
    appliers_waiting: usize,
    poisoned: bool,
}

struct OpenBatch {
    txn: redb::WriteTransaction,
    /// Shared by every operation applied into this batch; the applier that commits
    /// (or aborts) the batch sets it, and the others clone their result out of it.
    outcome: Arc<BatchOutcome>,
    ops: usize,
}

/// Set exactly once, when the batch resolves. `Err` means nothing in the batch was
/// applied: a failed operation or commit aborts the whole transaction.
struct BatchOutcome(OnceLock<Result<(), Arc<anyhow::Error>>>);

impl BatchOutcome {
    /// The resolved outcome; must not be called before the batch resolves.
    fn to_result(&self) -> anyhow::Result<()> {
        match self.0.get().unwrap() {
            Ok(()) => Ok(()),
            Err(err) => Err(batch_error(err)),
        }
    }
}

fn batch_error(err: &Arc<anyhow::Error>) -> anyhow::Error {
    anyhow::anyhow!("{err:#}")
}

/// Marks the queue poisoned if dropped while armed (i.e. during an unwind out of an apply
/// or commit), so every caller sharing the failed transaction panics rather than
/// reporting success.
struct PoisonArm<'a> {
    inner: &'a RedbInner,
    armed: bool,
}

impl<'a> PoisonArm<'a> {
    fn new(inner: &'a RedbInner) -> Self {
        Self { inner, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonArm<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut group = self.inner.group.lock();
            group.poisoned = true;
            self.inner.cv.notify_all();
        }
    }
}

#[cfg(test)]
/// Serializes tests that either assert on the process-global write-transaction counter
/// or commit enough transactions to skew it. An upper-bound assertion on a global
/// counter is only sound when heavy writers cannot run concurrently.
static WRITE_COUNT_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Upper bound on operations sharing one commit; bounds the dirty pages pinned by the
/// shared transaction.
const MAX_OPS_PER_COMMIT: usize = 4096;

impl RedbInner {
    fn assert_not_poisoned(&self, group: &GroupState) {
        assert!(
            !group.poisoned,
            "A previous redb write batch for {} panicked",
            self.path.display()
        );
    }

    /// Waits out any in-flight commit, then leaves a shared batch open in
    /// `group.batch`, opening one if needed.
    fn wait_for_open_batch<'a>(
        &self,
        group: &mut parking_lot::MutexGuard<'a, GroupState>,
        label: &str,
    ) -> anyhow::Result<()> {
        self.assert_not_poisoned(group);
        while group.committing {
            group.appliers_waiting += 1;
            self.cv.wait(group);
            group.appliers_waiting -= 1;
            self.assert_not_poisoned(group);
        }
        if group.batch.is_none() {
            let mut txn = self.db.begin_write().with_context(|| {
                format!(
                    "Failed to begin redb write transaction for {} store {}",
                    label,
                    self.path.display()
                )
            })?;
            txn.set_durability(Durability::None).with_context(|| {
                format!(
                    "Failed to set redb durability for {} store {}",
                    label,
                    self.path.display()
                )
            })?;
            group.batch = Some(OpenBatch {
                txn,
                outcome: Arc::new(BatchOutcome(OnceLock::new())),
                ops: 0,
            });
        }
        Ok(())
    }

    /// Applies `apply` to the shared open write transaction and returns once the
    /// transaction containing it has committed.
    ///
    /// An `Err` from `apply` (or from the commit) aborts the whole batch: every
    /// operation sharing the transaction gets the same `Err`, and none of them are
    /// applied. Error returns leave the queue usable; only panics poison it.
    fn submit_write(
        &self,
        label: &str,
        apply: impl FnOnce(&mut Table<'_, &[u8; 16], &[u8]>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let poison_arm = PoisonArm::new(self);
        let result = self.submit_write_inner(label, apply);
        poison_arm.disarm();
        result
    }

    fn submit_write_inner(
        &self,
        label: &str,
        apply: impl FnOnce(&mut Table<'_, &[u8; 16], &[u8]>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut group = self.group.lock();
        self.wait_for_open_batch(&mut group, label)?;
        let apply_result = {
            let batch = group.batch.as_mut().unwrap();
            batch
                .txn
                .open_table(BLOBS)
                .with_context(|| {
                    format!(
                        "Failed to open redb table for {} store {}",
                        label,
                        self.path.display()
                    )
                })
                .and_then(|mut table| apply(&mut table))
        };
        if let Err(err) = apply_result {
            return Err(self.abort_batch(&mut group, err));
        }
        let batch = group.batch.as_mut().unwrap();
        batch.ops += 1;
        let full = batch.ops >= MAX_OPS_PER_COMMIT;
        let outcome = Arc::clone(&batch.outcome);
        if group.appliers_waiting == 0 || full {
            // Nobody is lined up to extend this batch (or it's full): commit it ourselves.
            let batch = group.batch.take().unwrap();
            self.commit_batch(group, batch, label)
        } else {
            // A parked applier will extend this transaction and eventually commit it.
            while outcome.0.get().is_none() {
                self.assert_not_poisoned(&group);
                self.cv.wait(&mut group);
            }
            outcome.to_result()
        }
    }

    /// Durably commits everything submitted so far: takes over the shared transaction
    /// (or opens an empty one) and commits it with [`Durability::Immediate`].
    fn commit_durable(&self) -> anyhow::Result<()> {
        let poison_arm = PoisonArm::new(self);
        let result = self.commit_durable_inner();
        poison_arm.disarm();
        result
    }

    fn commit_durable_inner(&self) -> anyhow::Result<()> {
        let mut group = self.group.lock();
        self.wait_for_open_batch(&mut group, "sync")?;
        let set_result = group
            .batch
            .as_mut()
            .unwrap()
            .txn
            .set_durability(Durability::Immediate)
            .with_context(|| {
                format!(
                    "Failed to set redb sync durability for {}",
                    self.path.display()
                )
            });
        if let Err(err) = set_result {
            return Err(self.abort_batch(&mut group, err));
        }
        let batch = group.batch.take().unwrap();
        self.commit_batch(group, batch, "sync")
    }

    /// Drops (aborts) the open batch and resolves its outcome to `err`, so every
    /// operation that applied into it reports failure.
    fn abort_batch(
        &self,
        group: &mut parking_lot::MutexGuard<'_, GroupState>,
        err: anyhow::Error,
    ) -> anyhow::Error {
        let OpenBatch {
            txn,
            outcome,
            ops: _,
        } = group.batch.take().unwrap();
        drop(txn); // dropping an uncommitted redb transaction aborts it
        let shared = Arc::new(err);
        assert!(outcome.0.set(Err(Arc::clone(&shared))).is_ok());
        self.cv.notify_all();
        batch_error(&shared)
    }

    fn commit_batch(
        &self,
        mut group: parking_lot::MutexGuard<'_, GroupState>,
        batch: OpenBatch,
        label: &str,
    ) -> anyhow::Result<()> {
        group.committing = true;
        drop(group);
        let OpenBatch {
            txn,
            outcome,
            ops: _,
        } = batch;
        // Committing outside the lock lets the next batch accumulate concurrently.
        let commit_result = txn.commit().with_context(|| {
            format!(
                "Failed to commit redb write batch for {} store {}",
                label,
                self.path.display()
            )
        });
        let mut group = self.group.lock();
        group.committing = false;
        let shared = match commit_result {
            Ok(()) => {
                count_committed_write_transaction();
                Ok(())
            }
            Err(err) => Err(Arc::new(err)),
        };
        assert!(outcome.0.set(shared).is_ok());
        self.cv.notify_all();
        drop(group);
        outcome.to_result()
    }
}

impl RedbPath {
    /// Opens the specified redb database file, creating it if needed, and ensures
    /// the blob table exists.
    pub fn new(path: PathBuf) -> Self {
        Self::try_new(path).unwrap()
    }

    /// [`Self::new`], but returns errors instead of panicking.
    pub fn try_new(path: PathBuf) -> anyhow::Result<Self> {
        let db = Database::create(&path)
            .with_context(|| format!("Failed to open redb database {}", path.display()))?;
        Self::try_from_db(path, db)
    }

    /// Opens the specified redb database file with a configured cache size, creating it
    /// if needed, and ensures the blob table exists.
    pub fn new_with_cache_size(path: PathBuf, cache_size_bytes: usize) -> Self {
        Self::try_new_with_cache_size(path, cache_size_bytes).unwrap()
    }

    /// [`Self::new_with_cache_size`], but returns errors instead of panicking.
    pub fn try_new_with_cache_size(path: PathBuf, cache_size_bytes: usize) -> anyhow::Result<Self> {
        let db = Database::builder()
            .set_cache_size(cache_size_bytes)
            .create(&path)
            .with_context(|| format!("Failed to open redb database {}", path.display()))?;
        Self::try_from_db(path, db)
    }

    /// Opens the [`REDB_FILE_NAME`] database inside `dir`, creating the directory and the
    /// database if needed.
    ///
    /// This is the store's single exclusive handle: the contents are enumerated once when the
    /// path is tracked and membership is tracked in memory rather than via per-element
    /// syscalls. redb allows one live handle per file per process (clones share it), so a
    /// second open while a previous handle for the same file is alive panics — keep
    /// open→use→drop lifetimes disjoint.
    ///
    /// # Panics
    /// Panics if the directory or database cannot be created/opened.
    pub fn in_dir(dir: &Path) -> Self {
        Self::new(db_path_in(dir))
    }

    /// [`Self::in_dir`] with a configured cache size.
    ///
    /// # Panics
    /// Panics if the directory or database cannot be created/opened.
    pub fn in_dir_with_cache_size(dir: &Path, cache_size_bytes: usize) -> Self {
        Self::new_with_cache_size(db_path_in(dir), cache_size_bytes)
    }

    /// [`Self::in_dir_with_cache_size`], but returns errors instead of panicking.
    pub fn try_in_dir_with_cache_size(dir: &Path, cache_size_bytes: usize) -> anyhow::Result<Self> {
        Self::try_new_with_cache_size(try_db_path_in(dir)?, cache_size_bytes)
    }

    fn try_from_db(path: PathBuf, db: Database) -> anyhow::Result<Self> {
        let this = Self {
            inner: Arc::new(RedbInner {
                path,
                db,
                group: Mutex::new(GroupState {
                    batch: None,
                    committing: false,
                    appliers_waiting: 0,
                    poisoned: false,
                }),
                cv: Condvar::new(),
            }),
        };
        this.try_ensure_table()?;
        Ok(this)
    }

    /// Returns the database file path.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn db(&self) -> &Database {
        &self.inner.db
    }

    /// Writes a canonical copy of this database to `dest`: a fresh database holding the same
    /// blobs, written in a single sorted transaction. The output bytes are a deterministic
    /// function of the contents, independent of this database's transaction history (for a
    /// fixed redb version).
    ///
    /// # Panics
    /// Panics if `dest` already exists or the copy fails.
    #[deprecated(
        note = "the output bytes are only deterministic on a single machine; they differ \
                across machines, so this cannot provide canonical bytes for cross-machine \
                comparison"
    )]
    pub fn write_canonical(&self, dest: &Path) {
        assert!(
            !dest.exists(),
            "Canonical copy destination already exists: {}",
            dest.display()
        );
        let read_txn = self.db().begin_read().unwrap_or_else(|err| {
            panic!(
                "Failed to begin redb read transaction for {}: {err:?}",
                self.path().display()
            )
        });
        let src_table = read_txn.open_table(BLOBS).unwrap_or_else(|err| {
            panic!(
                "Failed to open redb table in {}: {err:?}",
                self.path().display()
            )
        });

        let dest_db = Database::create(dest).unwrap_or_else(|err| {
            panic!(
                "Failed to create canonical redb database {}: {err:?}",
                dest.display()
            )
        });
        let mut write_txn = dest_db.begin_write().unwrap_or_else(|err| {
            panic!(
                "Failed to begin redb write transaction for {}: {err:?}",
                dest.display()
            )
        });
        write_txn
            .set_durability(Durability::Immediate)
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to set redb durability for {}: {err:?}",
                    dest.display()
                )
            });
        {
            let mut dest_table = write_txn.open_table(BLOBS).unwrap_or_else(|err| {
                panic!("Failed to open redb table in {}: {err:?}", dest.display())
            });
            // Table iteration is in key order, so identical contents yield an identical
            // sequence of inserts.
            for entry in src_table.iter().unwrap_or_else(|err| {
                panic!(
                    "Failed to iterate redb table in {}: {err:?}",
                    self.path().display()
                )
            }) {
                let (key, value) = entry.unwrap_or_else(|err| {
                    panic!(
                        "Failed to read redb table entry in {}: {err:?}",
                        self.path().display()
                    )
                });
                let old = dest_table
                    .insert(key.value(), value.value())
                    .unwrap_or_else(|err| {
                        panic!(
                            "Failed to insert into canonical redb {}: {err:?}",
                            dest.display()
                        )
                    });
                assert!(old.is_none());
            }
        }
        write_txn.commit().unwrap_or_else(|err| {
            panic!(
                "Failed to commit canonical redb copy {}: {err:?}",
                dest.display()
            )
        });
        count_committed_write_transaction();
    }

    fn try_ensure_table(&self) -> anyhow::Result<()> {
        let write_txn = self.db().begin_write().with_context(|| {
            format!(
                "Failed to begin redb write transaction for {}",
                self.path().display()
            )
        })?;
        {
            write_txn.open_table(BLOBS).with_context(|| {
                format!("Failed to open redb table in {}", self.path().display())
            })?;
        }
        write_txn.commit().with_context(|| {
            format!(
                "Failed to commit redb table initialization for {}",
                self.path().display()
            )
        })?;
        count_committed_write_transaction();
        Ok(())
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

    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) -> anyhow::Result<()> {
        try_remove_key(path, key, "persisted")
    }

    fn register(&self, src_path: &Self::PersistPath, key: Uuid) {
        with_bytes(src_path, key, "persisted", |bytes| {
            insert_bytes(&self.db, key, bytes, "temporary");
        });
    }

    fn register_many(&self, src_path: &Self::PersistPath, keys: &[Uuid]) {
        // A serial caller registering per key pays one commit each (group commit only
        // batches concurrent callers). Copy in chunks instead: one source read
        // transaction and one group operation — hence one commit — per chunk.
        for chunk in keys.chunks(MAX_OPS_PER_COMMIT) {
            let read_txn = src_path.db().begin_read().unwrap_or_else(|err| {
                panic!(
                    "Failed to begin redb read transaction for persisted store {}: {err:?}",
                    src_path.path().display()
                )
            });
            let src_table = read_txn.open_table(BLOBS).unwrap_or_else(|err| {
                panic!(
                    "Failed to open redb table for persisted store {}: {err:?}",
                    src_path.path().display()
                )
            });
            self.db
                .inner
                .submit_write("temporary", |table| {
                    for &key in chunk {
                        let guard = src_table
                            .get(key.as_bytes())
                            .unwrap_or_else(|err| {
                                panic!(
                                    "Failed to read redb key {} from persisted store {}: {err:?}",
                                    key,
                                    src_path.path().display()
                                )
                            })
                            .unwrap_or_else(|| {
                                panic!(
                                    "Attempted to register missing redb key {} from persisted store {}",
                                    key,
                                    src_path.path().display()
                                )
                            });
                        let old = table
                            .insert(key.as_bytes(), guard.value())
                            .unwrap_or_else(|err| {
                                panic!(
                                    "Failed to insert redb key {} into temporary store {}: {err:?}",
                                    key,
                                    self.db.path().display()
                                )
                            });
                        assert!(
                            old.is_none(),
                            "Attempted to overwrite existing redb key {} in temporary store {}",
                            key,
                            self.db.path().display()
                        );
                    }
                    Ok(())
                })
                .unwrap();
        }
    }

    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid) -> anyhow::Result<()> {
        with_bytes(&self.db, key, "temporary", |bytes| {
            try_insert_bytes(dest_path, key, bytes, "persisted")
        })
    }

    fn sanitize_path(
        &self,
        path: &Self::PersistPath,
    ) -> anyhow::Result<impl IntoIterator<Item = Uuid>> {
        let read_txn = path.db().begin_read().with_context(|| {
            format!(
                "Failed to begin redb read transaction for {}",
                path.path().display()
            )
        })?;
        let table = read_txn
            .open_table(BLOBS)
            .with_context(|| format!("Failed to open redb table in {}", path.path().display()))?;
        table
            .iter()
            .with_context(|| format!("Failed to iterate redb table in {}", path.path().display()))?
            .map(|entry| {
                let (key, _) = entry.with_context(|| {
                    format!(
                        "Failed to read redb table entry in {}",
                        path.path().display()
                    )
                })?;
                Ok(Uuid::from_bytes(*key.value()))
            })
            .collect::<anyhow::Result<Vec<_>>>()
    }

    fn sync_persisted(&self, path: &Self::PersistPath) -> anyhow::Result<()> {
        path.inner.commit_durable()
    }
}

impl<T, C: BlobCodec<T>> Strategy<T> for RedbStore<C> {
    fn store(&self, key: Uuid, data: &T) {
        self.codec
            .encode_with(data, |bytes| {
                insert_bytes(&self.db, key, bytes, "temporary");
            })
            .unwrap_or_else(|err| {
                panic!("Failed to encode data for redb key {key}: {err:?}");
            });
    }

    fn load(&self, key: Uuid) -> T {
        with_bytes(&self.db, key, "temporary", |bytes| self.codec.decode(bytes)).unwrap_or_else(
            |err| {
                panic!("Failed to decode data for redb key {key}: {err:?}");
            },
        )
    }
}

/// Reads the blob stored under `key` into an owned byte vector. Panics if the key is absent.
pub fn read_blob(path: &RedbPath, key: Uuid) -> Vec<u8> {
    with_blob(path, key, <[u8]>::to_vec)
}

/// Reads the blob stored under `key` and passes it to `use_bytes` without copying it out of
/// redb's page cache. Panics if the key is absent.
pub fn with_blob<R>(path: &RedbPath, key: Uuid, use_bytes: impl FnOnce(&[u8]) -> R) -> R {
    with_bytes(path, key, "persisted", use_bytes)
}

fn with_bytes<R>(path: &RedbPath, key: Uuid, label: &str, use_bytes: impl FnOnce(&[u8]) -> R) -> R {
    let read_txn = path.db().begin_read().unwrap_or_else(|err| {
        panic!(
            "Failed to begin redb read transaction for {} store {}: {err:?}",
            label,
            path.path().display()
        )
    });
    let table = read_txn.open_table(BLOBS).unwrap_or_else(|err| {
        panic!(
            "Failed to open redb table for {} store {}: {err:?}",
            label,
            path.path().display()
        )
    });
    let guard = table
        .get(key.as_bytes())
        .unwrap_or_else(|err| {
            panic!(
                "Failed to read redb key {} from {} store {}: {err:?}",
                key,
                label,
                path.path().display()
            )
        })
        .unwrap_or_else(|| {
            panic!(
                "Attempted to read missing redb key {} from {} store {}",
                key,
                label,
                path.path().display()
            )
        });
    use_bytes(guard.value())
}

fn insert_bytes(path: &RedbPath, key: Uuid, bytes: &[u8], label: &str) {
    try_insert_bytes(path, key, bytes, label).unwrap()
}

fn try_insert_bytes(path: &RedbPath, key: Uuid, bytes: &[u8], label: &str) -> anyhow::Result<()> {
    path.inner.submit_write(label, |table| {
        let old = table.insert(key.as_bytes(), bytes).with_context(|| {
            format!(
                "Failed to insert redb key {} into {} store {}",
                key,
                label,
                path.path().display()
            )
        })?;
        assert!(
            old.is_none(),
            "Attempted to overwrite existing redb key {} in {} store {}",
            key,
            label,
            path.path().display()
        );
        Ok(())
    })
}

fn remove_key(path: &RedbPath, key: Uuid, label: &str) {
    try_remove_key(path, key, label).unwrap()
}

fn try_remove_key(path: &RedbPath, key: Uuid, label: &str) -> anyhow::Result<()> {
    path.inner.submit_write(label, |table| {
        let old = table.remove(key.as_bytes()).with_context(|| {
            format!(
                "Failed to remove redb key {} from {} store {}",
                key,
                label,
                path.path().display()
            )
        })?;
        assert!(
            old.is_some(),
            "Attempted to delete missing redb key {} from {} store {}",
            key,
            label,
            path.path().display()
        );
        Ok(())
    })
}

fn db_path_in(dir: &Path) -> PathBuf {
    try_db_path_in(dir).unwrap()
}

fn try_db_path_in(dir: &Path) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("Failed to create directory {}", dir.display()))?;
    Ok(dir.join(REDB_FILE_NAME))
}

#[cfg(test)]
mod small_cache_tests {
    use std::time::Instant;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{RedbPath, insert_bytes, with_bytes};

    /// Persist stores are written one commit per blob and scanned once at startup; confirm
    /// that workload stays correct and non-pathological with an arbitrarily small page cache.
    #[test]
    fn scan_once_workload_with_tiny_cache() {
        let _count_guard = crate::redbstore::WRITE_COUNT_TEST_LOCK.lock();
        const BLOB: usize = 32 * 1024;
        const N: usize = 2000;
        for cache in [64 * 1024, 1024 * 1024, 50 * 1024 * 1024] {
            let dir = tempdir().unwrap();
            let keys: Vec<Uuid> = (0..N).map(|i| Uuid::from_u128(i as u128 + 1)).collect();
            let start_write = Instant::now();
            {
                let db = RedbPath::in_dir_with_cache_size(dir.path(), cache);
                for (i, &key) in keys.iter().enumerate() {
                    insert_bytes(&db, key, &vec![(i % 251) as u8; BLOB], "persisted");
                }
            } // dropping the handle durably commits
            let write = start_write.elapsed();
            let start_scan = Instant::now();
            {
                let db = RedbPath::in_dir_with_cache_size(dir.path(), cache);
                for (i, &key) in keys.iter().enumerate() {
                    assert_eq!(
                        with_bytes(&db, key, "persisted", <[u8]>::to_vec),
                        vec![(i % 251) as u8; BLOB]
                    );
                }
            }
            let scan = start_scan.elapsed();
            eprintln!("cache {cache:>9}B: insert {N}x{BLOB}B = {write:?}, full scan = {scan:?}");
        }
    }
}

#[cfg(test)]
mod register_many_tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use tokio::runtime::Handle;
    use uuid::Uuid;

    use crate::BackingStore;

    use super::{BinCodec, RedbPath, RedbStore, total_write_transactions};

    /// A serial caller bulk-registering a whole tracked path must not pay one commit
    /// per key (group commit cannot batch a lone caller).
    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_register_batches_commits() {
        let _count_guard = crate::redbstore::WRITE_COUNT_TEST_LOCK.lock();
        const N: usize = 512;
        let cache_dir = tempdir().unwrap();
        let persist_dir = tempdir().unwrap();
        let mut keys: Vec<Uuid> = Vec::new();

        {
            let store = Arc::new(BackingStore::new(
                RedbStore::new(BinCodec, RedbPath::in_dir(cache_dir.path())),
                Handle::current(),
            ));
            let tracked = Arc::new(
                store
                    .track_path(RedbPath::in_dir(persist_dir.path()))
                    .await
                    .unwrap(),
            );
            let pool: Arc<crate::FBPool<Vec<u8>, _>> =
                Arc::new(crate::FBPool::new(Arc::clone(&store), 4));
            for i in 0..N {
                let item = pool.insert(vec![(i % 251) as u8; 64]);
                keys.push(item.key());
                item.spawn_persist(&tracked).await.await.unwrap();
                drop(item);
            }
            store.finished().await;
        }

        // Fresh process-equivalent: new cache store registering everything persisted.
        let store = Arc::new(BackingStore::new(
            RedbStore::new(BinCodec, RedbPath::in_dir(cache_dir.path())),
            Handle::current(),
        ));
        let tracked = store
            .track_path(RedbPath::in_dir(persist_dir.path()))
            .await
            .unwrap();
        assert_eq!(tracked.all_keys().len(), N);
        let before = total_write_transactions();
        let registered = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            move || store.blocking_register_all(&tracked)
        })
        .await
        .unwrap();
        let txns = total_write_transactions() - before;
        assert_eq!(registered.len(), N);
        assert!(
            txns < (N / 8) as u64,
            "bulk register of {N} keys took {txns} write transactions"
        );
        drop(registered);
        store.finished().await;
    }
}

#[cfg(test)]
mod group_commit_tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{RedbPath, insert_bytes, total_write_transactions, with_bytes};

    /// Concurrent writers must share commits (group commit) instead of paying one
    /// serialized transaction each behind redb's single-writer lock.
    #[test]
    fn concurrent_writes_share_commits() {
        let _count_guard = crate::redbstore::WRITE_COUNT_TEST_LOCK.lock();
        const THREADS: usize = 16;
        const OPS_PER_THREAD: usize = 64;
        let dir = tempdir().unwrap();
        let db = RedbPath::in_dir(dir.path());
        let keys: Vec<Uuid> = (0..THREADS * OPS_PER_THREAD)
            .map(|i| Uuid::from_u128(i as u128 + 1))
            .collect();
        let before = total_write_transactions();
        std::thread::scope(|s| {
            for chunk in keys.chunks(OPS_PER_THREAD) {
                let db = &db;
                s.spawn(move || {
                    for &key in chunk {
                        insert_bytes(db, key, key.as_bytes(), "test");
                    }
                });
            }
        });
        let txns = total_write_transactions() - before;
        eprintln!("{} ops committed in {txns} transactions", keys.len());
        assert!(
            txns < (keys.len() / 2) as u64,
            "expected group commit to share transactions: {txns} txns for {} ops",
            keys.len()
        );
        for &key in &keys {
            with_bytes(&db, key, "test", |bytes| assert_eq!(bytes, key.as_bytes()));
        }
    }
}

#[cfg(test)]
// Exercises the deprecated write_canonical; its single-machine determinism still holds.
#[allow(deprecated)]
mod canonical_tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{RedbPath, insert_bytes, remove_key};

    /// Two databases with identical contents but different transaction histories produce
    /// byte-identical canonical copies.
    #[test]
    fn canonical_copy_is_history_independent() {
        let dir = tempdir().unwrap();
        let keys: Vec<Uuid> = (0..64).map(|_| Uuid::new_v4()).collect();
        let blob = |key: &Uuid| key.as_bytes().repeat(100);

        let straight = RedbPath::new(dir.path().join("straight.redb"));
        for key in &keys {
            insert_bytes(&straight, *key, &blob(key), "test");
        }

        let churned = RedbPath::new(dir.path().join("churned.redb"));
        for key in keys.iter().rev() {
            insert_bytes(&churned, *key, &blob(key), "test");
        }
        for key in &keys[..32] {
            remove_key(&churned, *key, "test");
        }
        for key in &keys[..32] {
            insert_bytes(&churned, *key, &blob(key), "test");
        }

        straight.write_canonical(&dir.path().join("straight_canonical.redb"));
        churned.write_canonical(&dir.path().join("churned_canonical.redb"));
        let straight_bytes = std::fs::read(dir.path().join("straight_canonical.redb")).unwrap();
        let churned_bytes = std::fs::read(dir.path().join("churned_canonical.redb")).unwrap();
        assert_ne!(
            std::fs::read(straight.path()).unwrap(),
            std::fs::read(churned.path()).unwrap()
        );
        assert_eq!(straight_bytes, churned_bytes);
    }
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
