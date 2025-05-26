//! This module presents an "intermediate" interface to be wrapped by FBPool/FBStore which is guaranteed
//! never to deadlock. This allows the top-level lib to focus on integrating the backing store with the
//! LRU cache without needing to carefully consider synchronization issues and possible race conditions.

use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLockReadGuard, RwLockUpgradableReadGuard,
    RwLockWriteGuard,
};
use uuid::Uuid;

use crate::backing_store::{self, BackingStore, BackingStoreT, Strategy, TrackedPath};

pub(super) struct LimitedEntry<T, B: BackingStoreT> {
    backing: Weak<parking_lot::RwLock<Backing<T, B>>>,
    meta: Arc<EntryMetadata>,
}

pub(super) struct FullEntry<T, B: BackingStoreT> {
    backing: Arc<parking_lot::RwLock<Backing<T, B>>>,
    meta: Arc<EntryMetadata>,
}

struct EntryMetadata {
    // This is never contended
    //
    // There's one place where we read the key that isn't guarded by the `backing` RwLock
    // lock (`FullEntry::key`) and a separate place where we read/write the key without
    // having the corresponding correct reference (&self, &mut self) to the FullEntry
    // (`LimitedEntry::try_dump_to_disk`). These only both only grab read guards, not write
    // guards, so they won't contend with themselves or each other. For all other pairings,
    // we're either guaranteed to avoid contention by already holding the backing lock guard
    // or by rust's borrow checker or both. So we can safely use `try_read`/`try_write` and
    // unwrap the result everywhere this is accessed.
    key: parking_lot::RwLock<Uuid>,
}

impl<T, B: BackingStoreT> LimitedEntry<T, B> {
    /// Returns None if there are currently open handles or the full entry no longer exists
    pub(super) fn try_dump_to_disk(&self, store: &Arc<BackingStore<B>>)
    where
        T: Send + Sync + 'static,
        B: Strategy<T>,
    {
        let Some(arc) = self.backing.upgrade() else {
            return;
        };
        let Some(mut guard) = arc.try_write() else {
            return;
        };
        // Make sure we're holding the write guard before we read the key so that it doesn't change
        let key = *self.meta.key.try_read().unwrap();
        let store_clone = Arc::clone(store);
        guard.blocking_store(&store_clone, key);
        let old_value = guard.memory.take();
        drop(guard);
        drop(old_value);
    }
}

impl<T, B: BackingStoreT> FullEntry<T, B> {
    pub(super) fn new(data: T) -> Self {
        Self {
            backing: Arc::new(parking_lot::RwLock::new(Backing {
                memory: Some(data),
                stored: OnceLock::new(),
            })),
            meta: Arc::new(EntryMetadata {
                key: parking_lot::RwLock::new(Uuid::new_v4()),
            }),
        }
    }

    pub(super) fn key(&self) -> Uuid {
        *self.meta.key.try_read().unwrap()
    }

    pub(super) fn limited(&self) -> LimitedEntry<T, B> {
        LimitedEntry {
            backing: Arc::downgrade(&self.backing),
            meta: Arc::clone(&self.meta),
        }
    }

    pub(super) fn blocking_register(
        key: Uuid,
        store: &Arc<BackingStore<B>>,
        path: &TrackedPath<B::PersistPath>,
    ) -> Option<Self> {
        Some(Self {
            backing: Arc::new(parking_lot::RwLock::new(Backing {
                memory: None,
                stored: OnceLock::from(store.register(key, path)?),
            })),
            meta: Arc::new(EntryMetadata {
                key: parking_lot::RwLock::new(key),
            }),
        })
    }
}

impl<T: Send + Sync + 'static, B: Strategy<T>> FullEntry<T, B> {
    /// Returns None if the backing value is not in memory or is currently being
    /// evicted from memory.
    pub(super) fn try_load(&self) -> Option<MappedRwLockReadGuard<'_, T>> {
        let guard = self.backing.try_read()?;
        guard.memory.as_ref()?;
        Some(RwLockReadGuard::map(guard, |b| b.memory.as_ref().unwrap()))
    }

    pub(super) fn blocking_load(&self, store: &BackingStore<B>) -> MappedRwLockReadGuard<T> {
        let read_guard = self.backing.read();
        if read_guard.memory.is_some() {
            return RwLockReadGuard::map(read_guard, |b| b.memory.as_ref().unwrap());
        }
        drop(read_guard);
        let upgradeable_guard = self.backing.upgradable_read();
        let read_guard = if upgradeable_guard.memory.is_none() {
            let mut write_guard = RwLockUpgradableReadGuard::upgrade(upgradeable_guard);
            let data = store.load(write_guard.stored.get().unwrap());
            write_guard.memory = Some(data);
            RwLockWriteGuard::downgrade(write_guard)
        } else {
            RwLockUpgradableReadGuard::downgrade(upgradeable_guard)
        };
        RwLockReadGuard::map(read_guard, |b| b.memory.as_ref().unwrap())
    }

    /// Returns None if the backing value is not in memory or is currently being
    /// evicted from memory.
    // &mut necessary to avoid possibility of deadlock
    pub(super) fn try_load_mut(&mut self) -> Option<MappedRwLockWriteGuard<T>> {
        let mut guard = self.backing.try_write()?;
        guard.memory.as_ref()?;
        guard.stored.take();
        *self.meta.key.try_write().unwrap() = Uuid::new_v4();
        Some(RwLockWriteGuard::map(guard, |b| b.memory.as_mut().unwrap()))
    }

    pub(super) fn blocking_load_mut(
        &mut self, // &mut necessary to avoid possibility of deadlock
        store: &BackingStore<B>,
    ) -> MappedRwLockWriteGuard<T> {
        let mut guard = self.backing.write();
        let token = guard.stored.take();
        if guard.memory.is_none() {
            let data = store.load(&token.unwrap());
            guard.memory = Some(data);
        }
        *self.meta.key.try_write().unwrap() = Uuid::new_v4();
        RwLockWriteGuard::map(guard, |b| b.memory.as_mut().unwrap())
    }

    pub(super) fn blocking_write_now(&self, store: &Arc<BackingStore<B>>) {
        let read_guard = self.backing.read();
        read_guard.blocking_store(store, self.key());
    }

    pub(super) fn blocking_persist(
        &self,
        store: &Arc<BackingStore<B>>,
        path: &TrackedPath<B::PersistPath>,
    ) {
        let guard = self.backing.read();
        let token = Arc::clone(guard.blocking_store(store, *self.meta.key.try_read().unwrap()));
        drop(guard);
        store.persist(&token, path);
    }
}

struct Backing<T, B: BackingStoreT> {
    memory: Option<T>,
    stored: OnceLock<Arc<backing_store::Token<B>>>,
}

impl<T, B: Strategy<T>> Backing<T, B> {
    fn blocking_store(
        &self,
        store: &Arc<BackingStore<B>>,
        key: Uuid,
    ) -> &Arc<backing_store::Token<B>> {
        self.stored.get_or_init(|| {
            let data = self.memory.as_ref().unwrap();
            store.store(key, data)
        })
    }
}
