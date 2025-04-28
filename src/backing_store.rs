use std::sync::{Arc, Weak};

use dashmap::{DashMap, Entry};
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// Defines the low-level interface for physically storing, retrieving,
/// and managing keyed data blobs in a backing medium (like a filesystem).
///
/// Implementors handle the raw operations on keys and persistence paths.
/// The `BackingStore` wrapper manages concurrency, deduplication, and task scheduling,
/// so implementations of this trait typically don't need to handle races directly.
/// All methods are expected to be called from a blocking context.
pub trait BackingStoreT: Send + Sync + 'static {
    /// The type representing a path or location for persistent storage (e.g., `std::path::PathBuf`).
    type PersistPath: Send + Sync;

    /// Deletes the data associated with `key` from the primary (potentially temporary)
    /// storage managed by the `BackingStore`.
    fn delete(&self, key: Uuid);

    /// Deletes the data associated with `key` from the persisted location `path`.
    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid);

    /// Registers an existing item at `src_path` with the given `key`, making it known
    /// to the `BackingStore`. For filesystems, this is typically implemented via hard-linking
    /// the file from `src_path` into the store's managed temporary directory.
    /// This does not load the item into memory.
    fn register(&self, src_path: &Self::PersistPath, key: Uuid);

    /// Persists the data associated with `key` (currently managed by the store)
    /// to the specified `dest_path`. For filesystems, this is typically implemented
    /// via hard-linking from the store's managed temporary directory to `dest_path`.
    /// The backing data is never mutated after creation.
    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid);

    /// Returns an iterator over all keys known to exist at the persisted location `path`.
    fn all_persisted_keys(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid>;

    /// Ensures that all previous operations related to `path` are durably stored
    /// (e.g., by calling `syncfs` on the file system containing the directory).
    fn sync_persisted(&self, path: &Self::PersistPath);
}

/// Extends `BackingStoreT` with methods to load and store the actual data (`T`)
/// associated with a key. This defines the serialization/deserialization strategy.
///
/// Methods are generally called from a blocking context.
pub trait Strategy<T>: BackingStoreT {
    /// Stores (or serializes) the `data` for the given `key` into the backing store's
    /// primary (temporary) location.
    fn store(&self, key: Uuid, data: &T);

    /// Loads (or deserializes) the data `T` for the given `key` from the backing store.
    fn load(&self, key: Uuid) -> T;
}

// --- Blanket Implementations for Arc ---

impl<B: BackingStoreT> BackingStoreT for Arc<B> {
    type PersistPath = B::PersistPath;

    fn delete(&self, key: Uuid) {
        B::delete(self, key)
    }

    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) {
        B::delete_persisted(self, path, key)
    }

    fn register(&self, src_path: &Self::PersistPath, key: Uuid) {
        B::register(self, src_path, key)
    }

    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid) {
        B::persist(self, dest_path, key)
    }

    fn all_persisted_keys(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
        B::all_persisted_keys(self, path)
    }

    fn sync_persisted(&self, path: &Self::PersistPath) {
        B::sync_persisted(self, path)
    }
}

impl<T, B: Strategy<T>> Strategy<T> for Arc<B> {
    fn store(&self, key: Uuid, data: &T) {
        B::store(self, key, data)
    }

    fn load(&self, key: Uuid) -> T {
        B::load(self, key)
    }
}

// --- BackingStore Wrapper ---

/// A manager that wraps a `BackingStoreT` implementation, providing concurrency control,
/// task management via a Tokio runtime, and tracking of persisted paths.
///
/// It handles potential races and deduplication of tasks, ensuring that
/// multiple tasks can safely operate on the same backing store without conflicts.
pub struct BackingStore<B: BackingStoreT> {
    backing: B,
    use_counts: DashMap<Uuid, Weak<Token<B>>>,
    runtime: tokio::runtime::Handle,
    task_tracker: TaskTracker,
}

pub(super) struct Token<B: BackingStoreT> {
    key: Uuid,
    store: Arc<BackingStore<B>>,
}

/// Represents a persistence path being tracked by the `BackingStore`.
/// It holds the path itself and the keys known to be persisted there.
pub struct TrackedPath<P> {
    path: P,
    present: DashMap<Uuid, ()>,
}

impl<P> TrackedPath<P> {
    /// Returns a reference to the underlying path object.
    pub fn path(&self) -> &P {
        &self.path
    }

    /// Returns a clone of the list of keys known to be persisted at this path.
    pub fn all_keys(&self) -> Vec<Uuid> {
        self.present.iter().map(|entry| *entry.key()).collect()
    }
}

impl<B: BackingStoreT> Drop for Token<B> {
    fn drop(&mut self) {
        let name = self.key;
        let store = Arc::clone(&self.store);
        self.store.spawn_blocking(move || {
            let Entry::Occupied(entry) = store.use_counts.entry(name) else {
                panic!("Token not found for key: {}", name);
            };
            if entry.get().strong_count() > 0 {
                return;
            }
            store.backing.delete(name);
            entry.remove();
        });
    }
}

impl<B: BackingStoreT> BackingStore<B> {
    /// Creates a new `BackingStore` manager.
    ///
    /// # Arguments
    /// * `backing` - The low-level `BackingStoreT` implementation.
    /// * `runtime` - A handle to a Tokio runtime used for spawning background tasks
    ///   and managing async operations.
    pub fn new(backing: B, runtime: tokio::runtime::Handle) -> Self {
        Self {
            backing,
            use_counts: DashMap::new(),
            runtime,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Asynchronously begins tracking the given persistence `path`.
    ///
    /// It retrieves all keys currently persisted at the path using `BackingStoreT::all_persisted_keys`
    /// and returns a `JoinHandle` resolving to a `TrackedPath` containing the path and keys.
    pub fn track_path(
        self: &Arc<Self>,
        path: B::PersistPath,
    ) -> JoinHandle<TrackedPath<B::PersistPath>> {
        let this = Arc::clone(self);
        self.spawn_blocking(move || this.blocking_track_path(path))
    }

    /// Blocking version of `track_path`. Waits for the path tracking to complete.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_track_path(
        self: &Arc<Self>,
        path: B::PersistPath,
    ) -> TrackedPath<B::PersistPath> {
        let all_keys = self.backing.all_persisted_keys(&path);
        let present = key_map(all_keys);
        TrackedPath { path, present }
    }

    /// Spawns a blocking function `f` onto the store's managed Tokio runtime's blocking pool.
    /// Returns a `JoinHandle` to await the result `R`.
    pub fn spawn_blocking<R: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> JoinHandle<R> {
        self.task_tracker.spawn_blocking_on(f, &self.runtime)
    }

    /// Returns a reference to the Tokio runtime handle used by this store.
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    /// Returns a reference to the underlying `TaskTracker` used for detecting
    /// when all background tasks have completed.
    pub fn task_tracker(&self) -> &TaskTracker {
        &self.task_tracker
    }

    /// Returns a future that completes when all background tasks currently queued or
    /// running within this `BackingStore` instance have finished.
    /// This includes tasks like delayed deletions or background flushes.
    pub async fn finished(&self) {
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    pub(super) fn store<T>(self: &Arc<Self>, key: Uuid, data: &T) -> Arc<Token<B>>
    where
        B: Strategy<T>,
    {
        let entry = match self.use_counts.entry(key) {
            Entry::Vacant(entry) => entry,
            Entry::Occupied(_) => panic!("Token already exists for key: {}", key),
        };
        self.backing.store(key, data);
        let store = Arc::clone(self);
        let token = Arc::new(Token { key, store });
        entry.insert(Arc::downgrade(&token));
        token
    }

    pub(super) fn load<T>(&self, token: &Token<B>) -> T
    where
        B: Strategy<T>,
    {
        self.backing.load(token.key)
    }

    pub(super) fn persist(&self, token: &Token<B>, tracked: &TrackedPath<B::PersistPath>) {
        let entry = match tracked.present.entry(token.key) {
            Entry::Occupied(_) => return,
            Entry::Vacant(entry) => entry,
        };
        self.backing.persist(&tracked.path, token.key);
        entry.insert(());
    }

    pub(super) fn register(
        self: &Arc<Self>,
        key: Uuid,
        tracked: &TrackedPath<B::PersistPath>,
    ) -> Option<Arc<Token<B>>> {
        let _exists_guard = tracked.present.get(&key)?;
        let mut entry = match self.use_counts.entry(key) {
            Entry::Vacant(entry) => {
                self.backing.register(&tracked.path, key);
                entry.insert(Weak::new())
            }
            Entry::Occupied(entry) => match entry.get().upgrade() {
                Some(token) => return Some(token),
                None => entry.into_ref(),
            },
        };
        let store = Arc::clone(self);
        let new_token = Arc::new(Token { key, store });
        *entry = Arc::downgrade(&new_token);
        Some(new_token)
    }

    /// Asynchronously triggers the underlying `BackingStoreT::sync_persisted`
    /// operation for the given `tracked` path.
    /// Returns a `JoinHandle` that completes when the sync operation is done.
    pub fn sync(self: &Arc<Self>, tracked: Arc<TrackedPath<B::PersistPath>>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        self.spawn_blocking(move || this.blocking_sync(tracked.path()))
    }

    /// Blocking version of `sync`. Calls `BackingStoreT::sync_persisted` and waits for completion.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_sync(&self, path: &B::PersistPath) {
        self.backing.sync_persisted(path);
    }

    /// Asynchronously triggers the underlying `BackingStoreT::delete_persisted`
    /// operation for the given `key` at the `tracked` path.
    /// Returns a `JoinHandle` that completes when the deletion is done.
    pub fn delete_persisted(
        self: &Arc<Self>,
        tracked: Arc<TrackedPath<B::PersistPath>>,
        key: Uuid,
    ) -> JoinHandle<()> {
        let this = Arc::clone(self);
        self.spawn_blocking(move || this.blocking_delete_persisted(&tracked, key))
    }

    /// Blocking version of `delete_persisted`. Calls `BackingStoreT::delete_persisted`
    /// and waits for completion.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_delete_persisted(&self, tracked: &TrackedPath<B::PersistPath>, key: Uuid) {
        let entry = match tracked.present.entry(key) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return,
        };
        self.backing.delete_persisted(tracked.path(), key);
        entry.remove();
    }
}

fn key_map(all_keys: impl IntoIterator<Item = Uuid>) -> DashMap<Uuid, ()> {
    DashMap::from_iter(all_keys.into_iter().map(|key| (key, ())))
}
