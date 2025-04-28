use std::{collections::HashSet, sync::Arc};

use tokio::task::JoinSet;
use uuid::Uuid;

use crate::backing_store::{BackingStore, BackingStoreT, Strategy, TaskTracker, TrackedPath};
use crate::{FBItem, WriteGuard};

impl<T: Send + Sync + 'static, B: Strategy<T>> FBItem<T, B> {
    /// Asynchronously ensures unique access to the data, returning a `WriteGuard`.
    ///
    /// If this `FBArc` is not the unique reference (`full_count() > 1`), the underlying
    /// data `T` is cloned (requiring `T: Clone`). The `FBArc` is then updated to point
    /// to this new, unique clone before returning the `WriteGuard`.
    /// Similar to `Arc::make_mut`.
    ///
    /// If the operation is aborted (e.g., future dropped), this `FBArc` might or might
    /// not have been replaced with one wrapping the clone.
    pub async fn make_mut(self: &mut Arc<Self>) -> WriteGuard<T, B>
    where
        T: Clone,
    {
        if Arc::strong_count(self) > 1 {
            let read_guard = self.load_async().await;
            let new_arc = Arc::new(self.pool().insert(read_guard.clone()));
            drop(read_guard);
            *self = new_arc;
        }
        Arc::get_mut(self).unwrap().load_mut().await
    }

    /// Blocking version of `make_mut`. Waits for the operation (including potential cloning)
    /// to complete. Requires `T: Clone`.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_make_mut(self: &mut Arc<Self>) -> WriteGuard<T, B>
    where
        T: Clone,
    {
        if Arc::strong_count(self) > 1 {
            let read_guard = self.blocking_load();
            let new_arc = Arc::new(self.pool().insert(read_guard.clone()));
            drop(read_guard);
            *self = new_arc;
        }
        Arc::get_mut(self).unwrap().blocking_load_mut()
    }
}

/// Atomically persists a collection of `FBArc`s and updates an external key/state.
///
/// This convenience function handles the pattern of:
/// 1. Persisting multiple `FBArc` items to a `tracked` path (potentially in parallel).
/// 2. Ensuring the `tracked` path is synced to disk.
/// 3. Calling `change_key` (which should atomically update some external state, like a
///    master key reference file).
/// 4. Deleting any files which are no longer referenced by the master key file.
///
/// This function operates blockingly.
///
/// # Arguments
/// * `store`: The `BackingStore` manager.
/// * `arcs`: An iterator providing the `FBArc` handles to persist.
/// * `tracked`: The target persistent path information.
/// * `max_simultaneous_tasks`: Controls the parallelism of the persist operations.
/// * `change_key`: A closure executed *after* all persists succeed but *before* cleanup.
///   It should perform the atomic update of the external state.
///
/// # Returns
/// Returns `Ok(R)` if all persists and `change_key` succeed, otherwise returns `Err(E)`.
pub fn blocking_save<T: Send + Sync + 'static, B: Strategy<T>, R, E>(
    store: &Arc<BackingStore<B>>,
    arcs: impl IntoIterator<Item = Arc<FBItem<T, B>>>,
    tracked: &Arc<TrackedPath<B::PersistPath>>,
    max_simultaneous_tasks: usize,
    change_key: impl FnOnce() -> Result<R, E>,
) -> Result<R, E> {
    blocking_save_with(
        store,
        |persister| {
            for arc in arcs {
                persister.persist(&arc);
            }
            Ok::<_, E>(())
        },
        tracked,
        max_simultaneous_tasks,
        change_key,
    )
}

/// A more flexible version of `blocking_save` allowing custom logic for selecting items to persist.
///
/// Similar to `blocking_save`, this handles atomically persisting items and updating state.
/// Instead of an iterator of arcs, it takes a closure `persist_arcs` which receives a
/// mutable `Persister`. The closure should call `Persister::persist` for each `FBArc`
/// that needs to be included in this save operation.
///
/// This allows for more complex scenarios where the set of items to persist might
/// not all belong to the same FBPool or have the same type.
///
/// If `change_key` atomically replaces the master key, aborting the task after
/// `change_key` succeeds but before the final sync completes will leave a valid
/// save in place, though potentially alongside some unreferenced persisted files
/// from this operation.
///
/// This function operates blockingly.
///
/// # Arguments
/// * `store`: The `BackingStore` manager.
/// * `persist_arcs`: A closure that uses the provided `Persister` to specify which items to persist.
/// * `tracked`: The target persistent path information.
/// * `max_simultaneous_tasks`: Controls the parallelism of the persist operations.
/// * `change_key`: A closure executed *after* all persists succeed but *before* cleanup.
///
/// # Returns
/// Returns `Ok(R)` if all persists and `change_key` succeed, otherwise returns `Err(E)`.
pub fn blocking_save_with<B: BackingStoreT, R, E>(
    store: &Arc<BackingStore<B>>,
    persist_arcs: impl FnOnce(&mut Persister<B>) -> Result<(), E>,
    tracked: &Arc<TrackedPath<B::PersistPath>>,
    max_simultaneous_tasks: usize,
    change_key: impl FnOnce() -> Result<R, E>,
) -> Result<R, E> {
    assert!(max_simultaneous_tasks > 0);
    let old_keys = tracked.all_keys();
    let mut persister = Persister {
        backing_store: Arc::clone(store),
        tracked: Arc::clone(tracked),
        join_set: JoinSet::new(),
        new_keys_set: HashSet::new(),
        max_simultaneous_tasks,
    };
    persist_arcs(&mut persister)?;
    let new_keys_set = persister.new_keys_set;
    let runtime = store.runtime_handle();
    runtime.block_on(store.sync(Arc::clone(tracked))).unwrap();
    let output = change_key()?;
    assert!(max_simultaneous_tasks > 0);
    let mut join_set = JoinSet::new();
    for key in old_keys {
        if new_keys_set.contains(&key) {
            continue;
        }
        if join_set.len() == max_simultaneous_tasks {
            runtime.block_on(join_set.join_next()).unwrap().unwrap();
        }
        let task_tracker = TaskTracker::new(Arc::clone(store));
        let store = Arc::clone(store);
        let tracked = Arc::clone(tracked);
        join_set.spawn_blocking_on(
            move || {
                store.blocking_delete_persisted(&tracked, key);
                drop(task_tracker)
            },
            runtime,
        );
    }
    let _: Vec<()> = runtime.block_on(join_set.join_all());
    Ok(output)
}

/// A helper struct used within the `persist_arcs` closure of `blocking_save_with`.
/// It collects the `FBArc` handles that need to be persisted.
pub struct Persister<B: BackingStoreT> {
    backing_store: Arc<BackingStore<B>>,
    tracked: Arc<TrackedPath<B::PersistPath>>,
    join_set: JoinSet<()>,
    new_keys_set: HashSet<Uuid>,
    max_simultaneous_tasks: usize,
}

impl<B: BackingStoreT> Persister<B> {
    /// Registers an `FBArc` to be persisted as part of the `blocking_save_with` operation.
    pub fn persist<T: Send + Sync + 'static>(&mut self, arc: &Arc<FBItem<T, B>>)
    where
        B: Strategy<T>,
    {
        let runtime_handle = self.backing_store.runtime_handle();
        assert!(self.join_set.len() <= self.max_simultaneous_tasks);
        if self.join_set.len() == self.max_simultaneous_tasks {
            runtime_handle
                .block_on(self.join_set.join_next())
                .unwrap()
                .unwrap();
        }
        let task_tracker = TaskTracker::new(Arc::clone(&self.backing_store));
        let tracked = Arc::clone(&self.tracked);
        self.new_keys_set.insert(arc.key());
        let arc = Arc::clone(arc);
        self.join_set.spawn_blocking_on(
            move || {
                arc.blocking_persist(&tracked);
                drop(task_tracker);
            },
            runtime_handle,
        );
    }
}
