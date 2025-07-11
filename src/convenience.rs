use std::{collections::HashSet, sync::Arc};

use get_mut_drop_weak::get_mut_drop_weak;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::backing_store::{BackingStore, BackingStoreT, Strategy, TrackedPath};
use crate::{Fb, WriteGuard};

impl<T: Send + Sync + 'static, B: Strategy<T>> Fb<T, B> {
    /// Asynchronously ensures unique access to the data, returning a `WriteGuard`.
    ///
    /// If this `Arc` is not the unique reference (`full_count() > 1`), the underlying
    /// data `T` is cloned (requiring `T: Clone`). The `Arc` is then updated to point
    /// to this new, unique clone before returning the `WriteGuard`.
    /// Similar to `Arc::make_mut`.
    ///
    /// If the operation is aborted (e.g., future dropped), this `Arc` might or might
    /// not have been replaced with one wrapping the clone.
    pub async fn make_mut(self: &mut Arc<Self>) -> WriteGuard<T, B>
    where
        T: Clone,
    {
        let arc = match get_mut_drop_weak(self) {
            Ok(output) => return output.load_mut().await,
            Err(arc) => arc,
        };
        let read_guard = arc.load().await;
        let new_arc = Arc::new(arc.pool().insert(read_guard.clone()));
        drop(read_guard);
        *arc = new_arc;
        Arc::get_mut(arc).unwrap().load_mut().await
    }

    /// Blocking version of `make_mut`. Waits for the operation (including potential cloning)
    /// to complete. Requires `T: Clone`.
    /// Must not be called from an async context that isn't allowed to block.
    pub fn blocking_make_mut(self: &mut Arc<Self>) -> WriteGuard<T, B>
    where
        T: Clone,
    {
        let arc = match get_mut_drop_weak(self) {
            Ok(output) => return output.blocking_load_mut(),
            Err(arc) => arc,
        };
        let read_guard = arc.blocking_load();
        let new_arc = Arc::new(arc.pool().insert(read_guard.clone()));
        drop(read_guard);
        *arc = new_arc;
        Arc::get_mut(arc).unwrap().blocking_load_mut()
    }
}

/// Atomically persists a collection of `Arc<Fb>`s and updates an external key/state.
///
/// This convenience function handles the pattern of:
/// 1. Persisting multiple `Fb` items to a `tracked` path (potentially in parallel).
/// 2. Ensuring the `tracked` path is synced to disk.
/// 3. Calling `change_key` (which should atomically update some external state, like a
///    master key reference file).
/// 4. Deleting any files which are no longer referenced by the master key file.
///
/// This function operates blockingly.
///
/// # Arguments
/// * `store`: The `BackingStore` manager.
/// * `arcs`: An iterator providing the `Arc<Fb>` handles to persist.
/// * `tracked`: The target persistent path information.
/// * `max_simultaneous_tasks`: Controls the parallelism of the persist operations.
/// * `change_key`: A closure executed *after* all persists succeed but *before* cleanup.
///   It should perform the atomic update of the external state.
///
/// # Returns
/// Returns `Ok(R)` if all persists and `change_key` succeed, otherwise returns `Err(E)`.
pub fn blocking_save<T: Send + Sync + 'static, B: Strategy<T>, R, E>(
    store: &Arc<BackingStore<B>>,
    arcs: impl IntoIterator<Item = Arc<Fb<T, B>>>,
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
/// mutable `Persister`. The closure should call `Persister::persist` for each `Fb`
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
    let mut old_keys = tracked.all_keys();
    let new_keys_set = prepare_save(
        persist_arcs,
        tracked,
        max_simultaneous_tasks,
        store.runtime_handle(),
    )?;
    store.blocking_sync(tracked.path());
    let output = change_key()?;
    old_keys.retain(|key| !new_keys_set.contains(key));
    post_save_cleanup(store, tracked, max_simultaneous_tasks, &old_keys);
    Ok(output)
}

/// Helper function for implementing `blocking_save_with`. This collects the keys that we want to retain.
pub fn prepare_save<B: BackingStoreT, E>(
    persist_arcs: impl FnOnce(&mut Persister<B>) -> Result<(), E>,
    tracked: &Arc<TrackedPath<B::PersistPath>>,
    max_simultaneous_tasks: usize,
    runtime: &tokio::runtime::Handle,
) -> Result<HashSet<Uuid>, E> {
    assert!(max_simultaneous_tasks > 0);
    let mut persister = Persister {
        tracked: Arc::clone(tracked),
        join_set: JoinSet::new(),
        new_keys_set: HashSet::new(),
        max_simultaneous_tasks,
        runtime: runtime.clone(),
    };
    persist_arcs(&mut persister)?;
    let new_keys_set = persister.new_keys_set;
    let _: Vec<()> = runtime.block_on(persister.join_set.join_all());
    Ok(new_keys_set)
}

/// Helper function for implementing `blocking_save_with`. This deletes the keys that are no longer referenced
pub fn post_save_cleanup<B: BackingStoreT>(
    store: &Arc<BackingStore<B>>,
    tracked: &Arc<TrackedPath<B::PersistPath>>,
    max_simultaneous_tasks: usize,
    keys_to_delete: &[Uuid],
) {
    assert!(max_simultaneous_tasks > 0);
    let runtime = store.runtime_handle();
    let mut join_set = JoinSet::new();
    for &key in keys_to_delete {
        if join_set.len() == max_simultaneous_tasks {
            runtime.block_on(join_set.join_next()).unwrap().unwrap();
        }
        let store = Arc::clone(store);
        let tracked = Arc::clone(tracked);
        let task_tracker = store.task_tracker().clone();
        let runtime_clone = runtime.clone();
        join_set.spawn_on(
            async move {
                task_tracker
                    .spawn_blocking_on(
                        move || store.blocking_delete_persisted(&tracked, key),
                        &runtime_clone,
                    )
                    .await
                    .unwrap()
            },
            runtime,
        );
    }
    let _: Vec<()> = runtime.block_on(join_set.join_all());
}

/// A helper struct used within the `persist_arcs` closure of `blocking_save_with`.
/// It collects the `Arc<Fb>` handles that need to be persisted.
pub struct Persister<B: BackingStoreT> {
    tracked: Arc<TrackedPath<B::PersistPath>>,
    join_set: JoinSet<()>,
    new_keys_set: HashSet<Uuid>,
    max_simultaneous_tasks: usize,
    runtime: tokio::runtime::Handle,
}

impl<B: BackingStoreT> Persister<B> {
    /// Registers an `Arc<Fb>` to be persisted as part of the `blocking_save_with` operation.
    pub fn persist<T: Send + Sync + 'static>(&mut self, arc: &Arc<Fb<T, B>>)
    where
        B: Strategy<T>,
    {
        assert!(self.join_set.len() <= self.max_simultaneous_tasks);
        if self.join_set.len() == self.max_simultaneous_tasks {
            self.runtime
                .block_on(self.join_set.join_next())
                .unwrap()
                .unwrap();
        }
        let tracked = Arc::clone(&self.tracked);
        self.new_keys_set.insert(arc.key());
        let arc = Arc::clone(arc);
        self.join_set.spawn_on(
            async move { arc.spawn_persist(&tracked).await.await.unwrap() },
            &self.runtime,
        );
    }
}
