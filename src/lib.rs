use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use consume_on_drop::{Consume, ConsumeOnDrop};
use cutoff_list::CutoffList;
use parking_lot::RwLock;
use tokio::{
    sync::{RwLockMappedWriteGuard, RwLockReadGuard},
    task::JoinHandle,
};

use uuid::Uuid;

mod entries;

pub mod backing_store;
pub mod convenience;
#[cfg(feature = "fbstore")]
pub mod fbstore;

use self::backing_store::{Strategy, TrackedPath};
use self::entries::{FullEntry, LimitedEntry};

pub use self::backing_store::{BackingStore, BackingStoreT};

/// A handle to data managed by an `FBPool`.
///
/// This acts like a `Box` but for potentially large data (`T`) that might
/// reside either in memory (in an LRU cache) or on disk (in the backing store) or
/// both.
/// Accessing the underlying data requires calling one of the `load` methods.
///
/// When dropped:
/// - If the item was never persisted or written to the backing store's temporary
///   location, it is simply dropped.
/// - If the item exists in the backing store's temporary location, a background
///   task is spawned via the `BackingStore` to delete it using `BackingStoreT::delete`.
// Field ordering is important to remove entries from the cutoff list when
// the last reference to the entry is dropped
pub struct Fb<T, B: BackingStoreT> {
    entry: FullEntry<T, B>,
    inner: FbInner<T, B>,
}

struct FbInner<T, B: BackingStoreT> {
    index: cutoff_list::Index,
    pool: Arc<FBPool<T, B>>,
}

impl<T, B: BackingStoreT> Drop for FbInner<T, B> {
    fn drop(&mut self) {
        let mut write_guard = self.pool.entries.write();
        write_guard.remove(self.index).unwrap();
    }
}

/// Manages a pool of `Fb` instances, backed by an in-memory cache
/// and a `BackingStore` for disk persistence.
///
/// ## Caching Strategy (Segmented LRU Variant)
///
/// The pool utilizes a variation of an LRU (Least Recently Used) cache designed
/// to reduce overhead for frequently accessed items. The cache is conceptually
/// divided into two segments (e.g., a "front half" and a "back half", typically
/// split evenly based on the total `mem_size`).
///
/// - **Promotion:** Items loaded from the backing store (`Strategy::load`) or
///   accessed while residing in the "back half" of the cache are promoted
///   to the most recently used position (the front of the "front half").
/// - **No Movement:** Items accessed while *already* in the "front half" do
///   **not** change position. This avoids the overhead of cache entry shuffling
///   for frequently hit "hot" items that are already near the front.
/// - **Insertion:** New items inserted via [`FBPool::insert`] also enter the
///   front of the "front half".
/// - **Eviction:** Items are eventually evicted from the least recently used
///   end of the "back half" when the cache is full.
///
/// This approach aims to provide LRU-like behavior while optimizing for workloads
/// where a subset of items is accessed very frequently.
///
/// Internally, we store each item in an `Option<T>`. When an item is evicted from cache,
/// we will replace `Some(val)` with None. Note that the size of `None::<T>`` on the
/// stack is the exact same as that of Some(val). What this means is that if T's resources
/// are primarily represented by its space on the stack (e.g. when `T` is `[f32; 4096]`),
/// there will be zero savings when it's removed from cache. In these cases, you should
/// use `Box<T>` instead.
pub struct FBPool<T, B: BackingStoreT> {
    entries: RwLock<CutoffList<LimitedEntry<T, B>>>,
    store: Arc<BackingStore<B>>,
}

impl<T, B: BackingStoreT> FBPool<T, B> {
    /// Creates a new pool managing items of type `T`.
    ///
    /// # Arguments
    /// * `store` - The configured `BackingStore` manager.
    /// * `mem_size` - The maximum number of items to keep loaded in the in-memory cache.
    ///   This size is divided internally (50/50) to implement the
    ///   two-segment caching strategy (see main [`FBPool`] documentation for details).
    pub fn new(store: Arc<BackingStore<B>>, mem_size: usize) -> Self {
        let entries = RwLock::new(CutoffList::new(vec![mem_size / 2, mem_size]));
        Self { entries, store }
    }

    /// Returns a reference to the underlying `BackingStore`.
    pub fn store(&self) -> &Arc<BackingStore<B>> {
        &self.store
    }

    /// Inserts new `data` into the pool, returning an `Fb` handle.
    ///
    /// The data is initially placed only in the in-memory LRU cache. It will only be
    /// written to the backing store's temporary location if it's evicted from the cache
    /// or explicitly written via`persist`/`blocking_persist`/`spawn_write_now`/`blocking_write_now`.
    ///
    /// Whenever the data is evicted from memory, after being written to the backing store
    /// with `B::store`, the data will be dropped normally, which means if there's a custom `Drop`
    /// implementation, it will be called. Each time the data is loaded back into memory, this
    /// could happen again if the data is evicted again.
    pub fn insert(self: &Arc<Self>, data: T) -> Fb<T, B>
    where
        T: Send + Sync + 'static,
        B: Strategy<T>,
    {
        let entry = FullEntry::new(data);
        let mut guard = self.entries.write();
        let index = guard.insert_first(entry.limited());
        let dump_entry = guard.get(guard.index_following_qth_cutoff(1));
        if let Some(entry) = dump_entry {
            entry.try_dump_to_disk(&self.store);
        }
        drop(guard);
        Fb {
            entry,
            inner: FbInner {
                index,
                pool: Arc::clone(self),
            },
        }
    }

    /// Asynchronously registers an existing item from a persistent path into the pool.
    ///
    /// Creates an `Fb` handle for an item identified by `key` located at the
    /// tracked persistent `path`. This typically involves calling `BackingStoreT::register`
    /// (e.g., hard-linking the file into the managed temporary area).
    ///
    /// The item data is *not* loaded into memory by this call.
    /// Returns `None` if the registration fails (e.g., the underlying store fails to find the key).
    pub async fn register(
        self: &Arc<Self>,
        path: &Arc<TrackedPath<B::PersistPath>>,
        key: Uuid,
    ) -> Option<Fb<T, B>>
    where
        T: Send + Sync + 'static,
    {
        let entry = FullEntry::register(key, &self.store, path).await?;
        let index = self.entries.write().insert_last(entry.limited());
        Some(Fb {
            entry,
            inner: FbInner {
                index,
                pool: Arc::clone(self),
            },
        })
    }

    /// Blocking version of `register`. Waits for the registration to complete.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_register(
        self: &Arc<Self>,
        path: &TrackedPath<B::PersistPath>,
        key: Uuid,
    ) -> Option<Fb<T, B>> {
        let entry = FullEntry::blocking_register(key, &self.store, path)?;
        let index = self.entries.write().insert_last(entry.limited());
        Some(Fb {
            entry,
            inner: FbInner {
                index,
                pool: Arc::clone(self),
            },
        })
    }

    /// Returns the current number of items managed by the pool (both in memory and on disk).
    pub fn size(&self) -> usize {
        self.entries.read().len()
    }
}

impl<T, B: BackingStoreT> Fb<T, B> {
    /// Returns the unique identifier (`Uuid`) for the data associated with this handle.
    /// This key will change if the data is mutated via `try_load_mut` or `make_mut`.
    pub fn key(&self) -> Uuid {
        self.entry.key()
    }

    /// Returns a reference to the `FBPool` this `Fb` belongs to.
    pub fn pool(&self) -> &Arc<FBPool<T, B>> {
        &self.inner.pool
    }
}

impl<T: Send + Sync + 'static, B: Strategy<T>> Fb<T, B> {
    /// Asynchronously loads the data and returns a read guard.
    ///
    /// Returns a `Future` that resolves to a `ReadGuard` once the data is available
    /// in memory (either immediately or after loading from the backing store).
    /// Suitable for use within `async` functions and tasks.
    pub async fn load(&self) -> ReadGuard<'_, T, B> {
        // We do this _before_ loading the backing value so that if the caller
        // cancels the operation, we don't waste the work done to load it by
        // immediately dumping it back to disk.
        shift_forward(&self.inner.pool, self.inner.index);
        // Construct before loading so that if cancelled, the object will be dumped
        // if necessary (possible if a lot of things are loaded simultaneously).
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        let data_guard = self.entry.load(&self.inner.pool.store).await;
        ReadGuard {
            data_guard,
            _on_drop: on_drop,
        }
    }

    /// Attempts to load the data and return a read guard, returning None if the data is not
    /// already in memory or is currently being evicted.
    /// The entry will only be potentially shifted in the LRU cache on success.
    pub fn try_load(&self) -> Option<ReadGuard<'_, T, B>> {
        let guard = self.entry.try_load()?;
        shift_forward(&self.inner.pool, self.inner.index);
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        Some(ReadGuard {
            data_guard: guard,
            _on_drop: on_drop,
        })
    }

    /// Loads the data and returns a read guard, performing blocking I/O if necessary.
    ///
    /// - If the data is already in the memory cache, returns immediately.
    /// - If the data is not in memory, it performs a blocking load operation via
    ///   `Strategy::load`.
    ///
    /// This method should only be called from a context where blocking is acceptable
    /// (e.g., outside a Tokio runtime, or within `spawn_blocking` or `block_in_place`).
    pub fn blocking_load(&self) -> ReadGuard<'_, T, B> {
        shift_forward(&self.inner.pool, self.inner.index);
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        let data_guard = self.entry.blocking_load(&self.inner.pool.store);
        ReadGuard {
            data_guard,
            _on_drop: on_drop,
        }
    }

    /// Loads the data and returns a read guard for immutable access.
    ///
    /// - If the data is already in the memory cache, returns immediately.
    /// - If the data is not in memory, it uses `tokio::task::block_in_place` to
    ///   call `blocking_load` to load it from the backing store.
    ///
    /// This is for the somewhat niche situation where you need to load an FBArc in a
    /// blocking function nested many blocking calls deep within an async task running
    /// on a tokio multithreaded runtime. Ideally you would propagate async down and use
    /// `load` instead.
    ///
    /// # Panics
    /// This method will panic if called from within a `tokio::runtime::Runtime`
    /// created using `Runtime::new_current_thread`, as `block_in_place` is not
    /// supported there. Use `load` instead in async contexts and
    /// `blocking_load` in known blocking contexts.
    pub fn load_in_place(&self) -> ReadGuard<'_, T, B> {
        shift_forward(&self.inner.pool, self.inner.index);
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        let data_guard = self.entry.load_in_place(&self.inner.pool.store);
        ReadGuard {
            data_guard,
            _on_drop: on_drop,
        }
    }

    /// Asynchronously acquires mutable access to the data.
    ///
    /// On return:
    /// 1. The data is ensured to be in memory.
    /// 2. The corresponding file in the backing store's temporary location (if any) is deleted.
    /// 3. The internal `Uuid` key for this data is changed.
    /// 4. A `WriteGuard` providing mutable access is returned.
    pub async fn load_mut(&mut self) -> WriteGuard<'_, T, B> {
        // We do this _before_ loading the backing value so that if the caller
        // cancels the operation, we don't waste the work done to load it by
        // immediately dumping it back to disk.
        shift_forward(&self.inner.pool, self.inner.index);
        // Construct before loading so that if cancelled, the object will be dumped
        // if necessary (possible if a lot of things are loaded simultaneously).
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        let data_guard = self.entry.load_mut(&self.inner.pool.store).await;
        WriteGuard {
            data_guard,
            _on_drop: on_drop,
        }
    }

    /// Attempts to load the data and return a write guard, returning None if the data is not
    /// already in memory or is currently being evicted.
    /// The entry will only be potentially shifted in the LRU cache on success.
    pub fn try_load_mut(&mut self) -> Option<WriteGuard<'_, T, B>> {
        let guard = self.entry.try_load_mut()?;
        shift_forward(&self.inner.pool, self.inner.index);
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        Some(WriteGuard {
            data_guard: guard,
            _on_drop: on_drop,
        })
    }

    /// Blocking version of `load_mut`. Waits for the operation to complete.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_load_mut(&mut self) -> WriteGuard<'_, T, B> {
        shift_forward(&self.inner.pool, self.inner.index);
        let on_drop = GuardDropper::new(&self.inner.pool, self.inner.index);
        let data_guard = self.entry.blocking_load_mut(&self.inner.pool.store);
        WriteGuard {
            data_guard,
            _on_drop: on_drop,
        }
    }

    /// Spawns a background task to immediately write the data to the backing store's
    /// temporary location if it isn't already there.
    ///
    /// Acquires the read guard and then returns a `JoinHandle` that completes when the write
    /// operation finishes.
    pub async fn spawn_write_now(&self) -> JoinHandle<()> {
        self.entry.spawn_write_now(&self.inner.pool.store).await
    }

    /// Performs a blocking write of the data to the backing store's temporary location
    /// if it is isn't already there. Waits for the write to complete.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_write_now(&self) {
        self.entry.blocking_write_now(&self.inner.pool.store);
    }

    /// Spawns a background task to persist the data to the specified `TrackedPath`.
    ///
    /// This calls `BackingStoreT::persist` (typically a hard-link). If the data
    /// is currently only in memory, it ensures it's written to the temporary
    /// location first before attempting persistence.
    /// If the data is already in the persistent location, this is a no-op.
    ///
    /// Acquires the read guard and then returns a `JoinHandle` that completes when the persistence
    /// operation finishes.
    pub async fn spawn_persist(&self, path: &Arc<TrackedPath<B::PersistPath>>) -> JoinHandle<()> {
        self.entry.spawn_persist(&self.inner.pool.store, path).await
    }

    /// Performs a blocking persistence of the data to the specified `TrackedPath`.
    /// Waits for the operation (including any preliminary writes) to complete.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_persist(&self, path: &TrackedPath<B::PersistPath>) {
        self.entry.blocking_persist(&self.inner.pool.store, path)
    }
}

fn shift_forward<T: Send + Sync + 'static, B: Strategy<T>>(
    pool: &FBPool<T, B>,
    index: cutoff_list::Index,
) {
    let read_guard = pool.entries.read();
    let preceding_cutoffs = read_guard.preceding_cutoffs(index).unwrap();
    if preceding_cutoffs == 0 {
        return;
    }
    drop(read_guard);
    let mut write_guard = pool.entries.write();
    let preceding_cutoffs = write_guard.preceding_cutoffs(index).unwrap();
    if preceding_cutoffs == 0 {
        return;
    }
    write_guard.shift_to_front(index);
    if preceding_cutoffs == 1 {
        return;
    }
    assert!(preceding_cutoffs == 2);
    let read_guard = parking_lot::RwLockWriteGuard::downgrade(write_guard);
    let dump_entry = read_guard
        .get(read_guard.index_following_qth_cutoff(1))
        .unwrap();
    dump_entry.try_dump_to_disk(&pool.store);
}

/// An RAII guard providing immutable access (`Deref`) to the underlying data `T`.
///
/// While this guard is alive, the data is guaranteed to remain loaded in memory
/// and will not be immediately evicted if it leaves the LRU cache.
// Field ordering is important for try_dump_to_disk to succeed on drop
pub struct ReadGuard<'a, T: Send + Sync + 'static, B: Strategy<T>> {
    data_guard: RwLockReadGuard<'a, T>,
    _on_drop: ConsumeOnDrop<GuardDropper<'a, T, B>>,
}

impl<T: Send + Sync + 'static, B: Strategy<T>> Deref for ReadGuard<'_, T, B> {
    type Target = T;

    /// Dereferences to the immutable underlying data `T`.
    fn deref(&self) -> &Self::Target {
        &self.data_guard
    }
}

/// An RAII guard providing mutable access (`DerefMut`) to the underlying data `T`.
///
/// While this guard is alive, the data is guaranteed to remain loaded in memory.
// Field ordering is important for try_dump_to_disk to succeed on drop
pub struct WriteGuard<'a, T: Send + Sync + 'static, B: Strategy<T>> {
    data_guard: RwLockMappedWriteGuard<'a, T>,
    _on_drop: ConsumeOnDrop<GuardDropper<'a, T, B>>,
}

impl<T: Send + Sync + 'static, B: Strategy<T>> Deref for WriteGuard<'_, T, B> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data_guard
    }
}

impl<T: Send + Sync + 'static, B: Strategy<T>> DerefMut for WriteGuard<'_, T, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data_guard
    }
}

struct GuardDropper<'a, T: Send + Sync + 'static, B: Strategy<T>> {
    pool: &'a FBPool<T, B>,
    index: cutoff_list::Index,
}

impl<T: Send + Sync + 'static, B: Strategy<T>> Consume for GuardDropper<'_, T, B> {
    fn consume(self) {
        let entry_guard = self.pool.entries.read();
        let preceding_cutoffs = entry_guard.preceding_cutoffs(self.index).unwrap();
        assert!(preceding_cutoffs <= 2);
        if preceding_cutoffs == 2 {
            entry_guard
                .get(self.index)
                .unwrap()
                .try_dump_to_disk(&self.pool.store);
        }
    }
}

impl<'a, T: Send + Sync + 'static, B: Strategy<T>> GuardDropper<'a, T, B> {
    pub fn new(pool: &'a FBPool<T, B>, index: cutoff_list::Index) -> ConsumeOnDrop<Self> {
        ConsumeOnDrop::new(GuardDropper { pool, index })
    }
}
