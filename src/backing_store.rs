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

    /// Bulk [`register`](Self::register). Backends whose per-key registration pays a
    /// per-operation cost (e.g. a database transaction) should override this to amortize
    /// it; a serial caller registering one key at a time cannot be batched any other way.
    fn register_many(&self, src_path: &Self::PersistPath, keys: &[Uuid]) {
        for &key in keys {
            self.register(src_path, key);
        }
    }

    /// Persists the data associated with `key` (currently managed by the store)
    /// to the specified `dest_path`. For filesystems, this is typically implemented
    /// via hard-linking from the store's managed temporary directory to `dest_path`.
    /// The backing data is never mutated after creation.
    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid);

    /// Clean up and prepare the contents of the provided path to be used as a persistent
    /// store and return an iterator over all keys known to exist at the persisted location `path`.
    fn sanitize_path(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid>;

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

    fn register_many(&self, src_path: &Self::PersistPath, keys: &[Uuid]) {
        B::register_many(self, src_path, keys)
    }

    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid) {
        B::persist(self, dest_path, key)
    }

    fn sanitize_path(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
        B::sanitize_path(self, path)
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

/// Tokens returned by [`BackingStore::blocking_register_all`], keeping every key at a
/// tracked path registered. Dropping this releases the bulk registration; keys that
/// acquired other token holders in the meantime stay registered.
pub struct RegisteredTokens<B: BackingStoreT>(Vec<Arc<Token<B>>>);

impl<B: BackingStoreT> RegisteredTokens<B> {
    /// Number of keys held registered.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the tracked path had no keys.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
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

    /// Returns `true` if the key is known to be persisted at this path.
    pub fn contains_key(&self, key: Uuid) -> bool {
        self.present.contains_key(&key)
    }
}

impl<B: BackingStoreT> Drop for Token<B> {
    fn drop(&mut self) {
        let name = self.key;
        let store = Arc::clone(&self.store);
        self.store.spawn_blocking(move || {
            let Entry::Occupied(entry) = store.use_counts.entry(name) else {
                // Another token's cleanup task already removed this entry.
                // This happens when register() reuses a key with a pending cleanup.
                return;
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
    /// * `backing` - The low-level [BackingStoreT] implementation.
    /// * `runtime` - A handle to a Tokio runtime used for spawning background tasks
    ///   and managing async operations.
    ///
    /// If all available blocking threads on this runtime are simultaneously attempting a
    /// [blocking_load][bl], there can potentially be a deadlock. Either don't directly call
    /// spawn_blocking on this runtime or at least don't saturate the blocking thread pool.
    /// Alternatively don't use this runtime to call the `blocking_*` functions within file-backed,
    /// or else use a separate tokio runtime.
    ///
    /// [bl]: crate::Fb::blocking_load
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
    /// It retrieves all keys currently persisted at the path using the iterator returned from
    /// [BackingStoreT::sanitize_path] and returns a `JoinHandle` resolving to a [TrackedPath]
    /// containing the path and keys.
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
        let all_keys = self.backing.sanitize_path(&path);
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
    pub(crate) fn runtime_handle(&self) -> &tokio::runtime::Handle {
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

    /// Registers every key persisted at `tracked` in one bulk operation and returns
    /// tokens keeping them all registered. Dropping the returned guard releases them;
    /// keys that gained other references in the meantime (e.g. a pool `register`
    /// reusing the live token) survive.
    ///
    /// This exists for bulk-load paths that would otherwise `register` keys one at a
    /// time from a single thread, where per-key backing costs cannot amortize. It must
    /// not race per-key `register` calls for the same path: the bulk backing copy and
    /// the token bookkeeping are not atomic with respect to each other.
    ///
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_register_all(
        self: &Arc<Self>,
        tracked: &TrackedPath<B::PersistPath>,
    ) -> RegisteredTokens<B> {
        let keys = tracked.all_keys();
        // Only keys with no `use_counts` entry need the backing copy: a live token means
        // the bytes are already registered, and a dead weak means the bytes are still
        // present with a cleanup pending — which our new token below will cancel.
        let to_copy: Vec<Uuid> = keys
            .iter()
            .copied()
            .filter(|key| !self.use_counts.contains_key(key))
            .collect();
        self.backing.register_many(tracked.path(), &to_copy);
        let tokens = keys
            .into_iter()
            .map(|key| match self.use_counts.entry(key) {
                Entry::Vacant(entry) => {
                    let token = Arc::new(Token {
                        key,
                        store: Arc::clone(self),
                    });
                    entry.insert(Arc::downgrade(&token));
                    token
                }
                Entry::Occupied(mut entry) => match entry.get().upgrade() {
                    Some(token) => token,
                    None => {
                        let token = Arc::new(Token {
                            key,
                            store: Arc::clone(self),
                        });
                        *entry.get_mut() = Arc::downgrade(&token);
                        token
                    }
                },
            })
            .collect();
        RegisteredTokens(tokens)
    }

    /// Asynchronously triggers the underlying `BackingStoreT::sync_persisted`
    /// operation for the given `tracked` path.
    /// Returns a `JoinHandle` that completes when the sync operation is done.
    pub fn sync(self: &Arc<Self>, tracked: &Arc<TrackedPath<B::PersistPath>>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        let tracked = Arc::clone(tracked);
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
        tracked: &Arc<TrackedPath<B::PersistPath>>,
        key: Uuid,
    ) -> JoinHandle<()> {
        let this = Arc::clone(self);
        let tracked = Arc::clone(tracked);
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

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopBacking;

    impl BackingStoreT for NoopBacking {
        type PersistPath = ();
        fn delete(&self, _key: Uuid) {}
        fn delete_persisted(&self, _path: &(), _key: Uuid) {}
        fn register(&self, _src_path: &(), _key: Uuid) {}
        fn persist(&self, _dest_path: &(), _key: Uuid) {}
        fn sanitize_path(&self, _path: &()) -> impl IntoIterator<Item = Uuid> {
            []
        }
        fn sync_persisted(&self, _path: &()) {}
    }

    /// Two register-then-drop cycles for the same key spawn two deferred cleanup
    /// tasks. The first cleanup removes the `use_counts` entry; the second finds
    /// it missing before the cleanup task runs.
    ///
    /// Timeline:
    ///   1. register(key) → Token_A, use_counts[key] = Weak(A)
    ///   2. drop(Token_A)  → spawns cleanup_A  (deferred, hasn't run yet)
    ///   3. register(key) → finds Occupied + dead Weak(A), overwrites with Weak(B)
    ///   4. drop(Token_B)  → spawns cleanup_B  (deferred)
    ///   5. cleanup_A runs → Weak(B).strong_count()==0 → delete + remove entry
    ///   6. cleanup_B runs → entry is Vacant
    #[tokio::test]
    async fn register_drop_race() {
        let store = Arc::new(BackingStore::new(
            NoopBacking,
            tokio::runtime::Handle::current(),
        ));
        let key = Uuid::new_v4();
        let tracked = TrackedPath {
            path: (),
            present: DashMap::from_iter([(key, ())]),
        };

        drop(store.register(key, &tracked).unwrap());
        drop(store.register(key, &tracked).unwrap());

        store.finished().await;
    }
}
