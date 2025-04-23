use std::sync::{Arc, OnceLock, Weak};

use derive_where::derive_where;
use tokio::select;
use tokio::sync::{Notify, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::backing_store::{self, BackingStore, BackingStoreT, Strategy, TrackedPath};

#[derive_where(Clone)]
pub struct LimitedEntry<T, B: BackingStoreT> {
    backing: Weak<tokio::sync::RwLock<Backing<T, B>>>,
    key: Arc<parking_lot::RwLock<Uuid>>, // Never contended
}

pub struct FullEntry<T, B: BackingStoreT> {
    backing: Arc<tokio::sync::RwLock<Backing<T, B>>>,
    key: Arc<parking_lot::RwLock<Uuid>>, // Never contended
    in_memory: Notify,
}

impl<T, B: BackingStoreT> LimitedEntry<T, B> {
    pub fn has_full(&self) -> bool {
        self.backing.strong_count() > 0
    }

    /// Returns None if there are currently open handles or the full entry no longer exists
    pub fn try_dump_to_disk(&self, store: &Arc<BackingStore<B>>) -> Option<JoinHandle<()>>
    where
        T: Send + Sync + 'static,
        B: Strategy<T>,
    {
        let arc = self.backing.upgrade()?;
        let mut guard = arc.try_write_owned().ok()?;
        // Make sure we're holding the write lock before we read the key so that it doesn't change
        let key = *self.key.try_read().unwrap();
        let store_clone = Arc::clone(store);
        Some(store.spawn_blocking(move || {
            guard.blocking_store(&store_clone, key);
            let old_value = guard.memory.take();
            drop(guard);
            drop(old_value);
        }))
    }
}

impl<T, B: BackingStoreT> FullEntry<T, B> {
    pub fn new(data: T) -> Self {
        Self {
            backing: Arc::new(tokio::sync::RwLock::new(Backing {
                memory: Some(data),
                stored: OnceLock::new(),
            })),
            key: Arc::new(parking_lot::RwLock::new(Uuid::new_v4())),
            in_memory: Notify::new(),
        }
    }

    pub fn key(&self) -> Uuid {
        *self.key.try_read().unwrap()
    }

    pub fn limited(&self) -> LimitedEntry<T, B> {
        LimitedEntry {
            backing: Arc::downgrade(&self.backing),
            key: Arc::clone(&self.key),
        }
    }

    pub async fn register(
        key: Uuid,
        store: &Arc<BackingStore<B>>,
        path: &Arc<TrackedPath<B::PersistPath>>,
    ) -> Option<Self>
    where
        T: Send + Sync + 'static,
    {
        let store_clone = Arc::clone(store);
        let path = Arc::clone(path);
        store
            .spawn_blocking(move || Self::blocking_register(key, &store_clone, &path))
            .await
            .unwrap()
    }

    pub fn blocking_register(
        key: Uuid,
        store: &Arc<BackingStore<B>>,
        path: &TrackedPath<B::PersistPath>,
    ) -> Option<Self> {
        Some(Self {
            backing: Arc::new(tokio::sync::RwLock::new(Backing {
                memory: None,
                stored: OnceLock::from(store.register(key, path)?),
            })),
            key: Arc::new(parking_lot::RwLock::new(key)),
            in_memory: Notify::new(),
        })
    }
}

impl<T: Send + Sync + 'static, B: Strategy<T>> FullEntry<T, B> {
    pub fn load(&self, store: &Arc<BackingStore<B>>) -> RwLockReadGuard<T> {
        if let Ok(read_guard) = self.backing.try_read() {
            if read_guard.memory.is_some() {
                return RwLockReadGuard::map(read_guard, |b| b.memory.as_ref().unwrap());
            }
        }
        tokio::task::block_in_place(|| self.blocking_load(store))
    }

    /// If aborted, the backing value may or may not have been loaded into memory.
    pub async fn load_async(&self, store: &Arc<BackingStore<B>>) -> RwLockReadGuard<T> {
        let guard = loop {
            let read_guard = self.backing.read().await;
            if read_guard.memory.is_some() {
                break read_guard;
            }
            let notified = self.in_memory.notified();
            drop(read_guard);
            let mut write_guard = select! {
                () = notified => continue,
                guard = Arc::clone(&self.backing).write_owned() => guard,
            };
            if write_guard.memory.is_some() {
                continue;
            }
            {
                let store_clone = Arc::clone(store);
                store
                    .spawn_blocking(move || {
                        let data = store_clone.load(write_guard.stored.get().unwrap());
                        write_guard.memory = Some(data);
                    })
                    .await
                    .unwrap();
            }
            self.in_memory.notify_waiters();
        };
        RwLockReadGuard::map(guard, |b| b.memory.as_ref().unwrap())
    }

    pub fn blocking_load(&self, store: &BackingStore<B>) -> RwLockReadGuard<T> {
        let guard = loop {
            let read_guard = self.backing.blocking_read();
            if read_guard.memory.is_some() {
                break read_guard;
            }
            let notified = self.in_memory.notified();
            drop(read_guard);
            let write_guard = store.runtime_handle().block_on(async {
                select! {
                    () = notified => None,
                    guard = self.backing.write() => Some(guard),
                }
            });
            let Some(mut write_guard) = write_guard else {
                continue;
            };
            if write_guard.memory.is_none() {
                let data = store.load(write_guard.stored.get().unwrap());
                write_guard.memory = Some(data);
                self.in_memory.notify_waiters();
            }
            break write_guard.downgrade();
        };
        RwLockReadGuard::map(guard, |b| b.memory.as_ref().unwrap())
    }

    /// If aborted, the backing value may or may not have been loaded into memory.
    pub async fn load_mut(
        &mut self, // &mut necessary to avoid possibility of deadlock
        store: &Arc<BackingStore<B>>,
    ) -> RwLockMappedWriteGuard<T> {
        let mut guard = loop {
            let borrowed_guard = self.backing.write().await;
            if borrowed_guard.memory.is_some() {
                break borrowed_guard;
            }
            drop(borrowed_guard);
            let mut owned_guard = Arc::clone(&self.backing).write_owned().await;
            // Since we have exclusive access to the full entry, no other task can
            // load the value (limited entries can only store, not load).
            assert!(owned_guard.memory.is_none());
            {
                let store_clone = Arc::clone(store);
                store
                    .spawn_blocking(move || {
                        let data = store_clone.load(owned_guard.stored.get().unwrap());
                        owned_guard.memory = Some(data);
                    })
                    .await
                    .unwrap();
            }
        };
        guard.stored.take();
        *self.key.try_write().unwrap() = Uuid::new_v4();
        RwLockWriteGuard::map(guard, |b| b.memory.as_mut().unwrap())
    }

    pub fn blocking_load_mut(
        &mut self, // &mut necessary to avoid possibility of deadlock
        store: &BackingStore<B>,
    ) -> RwLockMappedWriteGuard<T> {
        let mut guard = self.backing.blocking_write();
        let token = guard.stored.take();
        if guard.memory.is_none() {
            let data = store.load(&token.unwrap());
            guard.memory = Some(data);
        }
        *self.key.try_write().unwrap() = Uuid::new_v4();
        RwLockWriteGuard::map(guard, |b| b.memory.as_mut().unwrap())
    }

    pub fn spawn_write_now(&self, store: &Arc<BackingStore<B>>) -> JoinHandle<()> {
        let backing = Arc::clone(&self.backing);
        let key = Arc::clone(&self.key);
        let store_clone = Arc::clone(store);
        store.spawn_blocking(move || {
            let guard = backing.blocking_read();
            guard.blocking_store(&store_clone, *key.try_read().unwrap());
        })
    }

    pub fn blocking_write_now(&self, store: &Arc<BackingStore<B>>) {
        let read_guard = self.backing.blocking_read();
        read_guard.blocking_store(store, self.key());
    }

    pub fn spawn_persist(
        &self,
        store: &Arc<BackingStore<B>>,
        path: Arc<TrackedPath<B::PersistPath>>,
    ) -> JoinHandle<()> {
        let backing = Arc::clone(&self.backing);
        let key = Arc::clone(&self.key);
        let store_clone = Arc::clone(store);
        store.spawn_blocking(move || {
            blocking_persist(&backing, &store_clone, &key, &path);
        })
    }

    pub fn blocking_persist(
        &self,
        store: &Arc<BackingStore<B>>,
        path: &TrackedPath<B::PersistPath>,
    ) {
        blocking_persist(&self.backing, store, &self.key, path);
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

fn blocking_persist<T, B: Strategy<T>>(
    backing: &Arc<tokio::sync::RwLock<Backing<T, B>>>,
    store: &Arc<BackingStore<B>>,
    key: &Arc<parking_lot::RwLock<Uuid>>,
    path: &TrackedPath<<B as BackingStoreT>::PersistPath>,
) {
    let guard = backing.blocking_read();
    let token = Arc::clone(guard.blocking_store(store, *key.try_read().unwrap()));
    drop(guard);
    store.persist(&token, path);
}
