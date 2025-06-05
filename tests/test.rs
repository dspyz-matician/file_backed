use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use dashmap::{DashMap, DashSet};
use uuid::Uuid;

use file_backed::backing_store::{BackingStore, BackingStoreT, Strategy};
use file_backed::{FBPool, Fb, convenience::blocking_save};

// Simple clonable data structure for testing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestData {
    pub id: u32,
    pub content: String,
}

// --- Test Backing Store Implementation ---

#[derive(Debug, Clone)]
pub struct TestStore {
    // Simulates temporary storage (e.g., files in a cache dir)
    // Stores the actual data for load simulation
    temp_data: Arc<DashMap<Uuid, TestData>>,
    // Simulates persisted storage (e.g., files hard-linked to final destination)
    // Only stores keys associated with paths
    persisted_data: Arc<DashMap<PathBuf, DashSet<Uuid>>>,
    // Tracks calls for verification
    pub call_counts: Arc<CallCounts>,
    store_sleep_duration: Duration,
}

#[derive(Debug, Default)]
pub struct CallCounts {
    pub store: AtomicUsize,
    pub load: AtomicUsize,
    pub delete: AtomicUsize,
    pub delete_persisted: AtomicUsize,
    pub register: AtomicUsize,
    pub persist: AtomicUsize,
    pub all_persisted_keys: AtomicUsize,
    pub sync_persisted: AtomicUsize,
}

impl TestStore {
    pub fn new(store_sleep_duration: Duration) -> Self {
        Self {
            temp_data: Arc::new(DashMap::new()),
            persisted_data: Arc::new(DashMap::new()),
            call_counts: Arc::new(Default::default()),
            store_sleep_duration,
        }
    }

    // Helper to create with default zero sleep
    pub fn new_no_sleep() -> Self {
        Self::new(Duration::from_millis(0))
    }

    // Helper for tests to pre-populate persisted state
    pub fn _add_persisted(&self, path: &Path, key: Uuid, data: TestData) {
        self.temp_data.insert(key, data); // Make it loadable for register test
        self.persisted_data
            .entry(path.to_path_buf())
            .or_default()
            .insert(key);
        println!("TestStore: Pre-added persisted key {} at {:?}", key, path);
    }

    pub fn get_temp_keys(&self) -> HashSet<Uuid> {
        self.temp_data.iter().map(|entry| *entry.key()).collect()
    }

    pub fn get_persisted_keys(&self, path: &Path) -> HashSet<Uuid> {
        self.persisted_data
            .get(path)
            .map_or_else(HashSet::new, |set| set.iter().map(|k| *k).collect())
    }
}

impl BackingStoreT for TestStore {
    type PersistPath = PathBuf; // Use PathBuf for simplicity

    fn delete(&self, key: Uuid) {
        self.call_counts.delete.fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Deleting temp key {}", key);
        let removed_entry = self.temp_data.remove(&key);
        // Assert it existed - Fail Fast!
        assert!(
            removed_entry.is_some(),
            "Attempted to delete non-existent temp key: {}",
            key
        );
    }

    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) {
        self.call_counts
            .delete_persisted
            .fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Deleting persisted key {} from {:?}", key, path);
        let path_entry = self.persisted_data.get_mut(path).unwrap_or_else(|| {
            panic!(
                "Attempted to delete persisted key {} from non-tracked path {:?}",
                key, path
            )
        });
        let removed = path_entry.remove(&key);
        // Assert it existed - Fail Fast!
        assert!(
            removed.is_some(),
            "Attempted to delete non-existent persisted key {} from path {:?}",
            key,
            path
        );
    }

    fn register(&self, src_path: &Self::PersistPath, key: Uuid) {
        self.call_counts.register.fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Registering key {} from {:?}", key, src_path);
        // Simulate hard link: ensure it's in persisted and make it available in temp
        // Fail Fast: Assert the key is actually persisted at src_path
        assert!(
            self.persisted_data
                .get(src_path)
                .is_some_and(|s| s.contains(&key)),
            "Attempted to register key {} which is not persisted at {:?}",
            key,
            src_path
        );
        // In a real FS store, we wouldn't load here, just link.
        // But for the mock, load needs *something* in temp_data.
        // We assume _add_persisted already put it there.
        // Fail Fast: Assert the key is now loadable (was pre-added)
        assert!(
            self.temp_data.contains_key(&key),
            "Registered key {} not found in temp data (should have been pre-added for mock)",
            key
        );
    }

    fn persist(&self, dest_path: &Self::PersistPath, key: Uuid) {
        self.call_counts.persist.fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Persisting key {} to {:?}", key, dest_path);
        // Fail Fast: Assert the key exists in temp store before persisting
        assert!(
            self.temp_data.contains_key(&key),
            "Attempted to persist non-existent temp key: {}",
            key
        );
        self.persisted_data
            .entry(dest_path.to_path_buf())
            .or_default()
            .insert(key);
    }

    fn sanitize_path(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
        self.call_counts
            .all_persisted_keys
            .fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Getting all persisted keys for {:?}", path);
        match self.persisted_data.get(path) {
            Some(keys) => keys.iter().map(|k| *k).collect::<Vec<_>>(),
            None => vec![], // Return empty Vec directly
        }
    }

    fn sync_persisted(&self, path: &Self::PersistPath) {
        self.call_counts
            .sync_persisted
            .fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Syncing persisted path {:?}", path);
        // No-op for mock, just count
    }
}

// Implement the Strategy trait for our TestStore and TestData
impl Strategy<TestData> for TestStore {
    fn store(&self, key: Uuid, data: &TestData) {
        println!("TestStore: Storing key {} data {:?} - STARTING", key, data);

        if !self.store_sleep_duration.is_zero() {
            println!("TestStore: Sleeping for {:?}", self.store_sleep_duration);
            std::thread::sleep(self.store_sleep_duration);
        }

        self.temp_data.insert(key, data.clone());
        self.call_counts.store.fetch_add(1, Ordering::SeqCst);

        println!("TestStore: Storing key {} data {:?} - COMPLETED", key, data);
    }

    fn load(&self, key: Uuid) -> TestData {
        self.call_counts.load.fetch_add(1, Ordering::SeqCst);
        println!("TestStore: Loading key {}", key);
        let entry = self
            .temp_data
            .get(&key)
            .unwrap_or_else(|| panic!("Attempted to load non-existent temp key: {}", key));
        // Fail Fast: unwrap is sufficient here per guidelines
        entry.value().clone()
    }
}

// Helper struct to bundle setup
pub struct TestSetup {
    pub runtime: tokio::runtime::Handle,
    pub store_impl: Arc<TestStore>,
    pub backing_store: Arc<BackingStore<Arc<TestStore>>>,
    pub pool: Arc<FBPool<TestData, Arc<TestStore>>>,
    pub calls: Arc<CallCounts>,
}

pub fn setup(mem_size: usize) -> TestSetup {
    setup_with_store_sleep(mem_size, Duration::from_millis(0)) // Default to zero sleep
}

pub fn setup_with_store_sleep(mem_size: usize, store_sleep: Duration) -> TestSetup {
    let runtime = tokio::runtime::Handle::current();

    let store_impl = Arc::new(TestStore::new(store_sleep));
    let backing_store = Arc::new(BackingStore::new(store_impl.clone(), runtime.clone()));
    let pool = Arc::new(FBPool::new(backing_store.clone(), mem_size));
    let calls = store_impl.call_counts.clone();

    TestSetup {
        runtime,
        store_impl,
        backing_store,
        pool,
        calls,
    }
}

// Helper to wait for background tasks
pub async fn wait_for_store(backing_store: &Arc<BackingStore<Arc<TestStore>>>) {
    backing_store.finished().await;
}

fn test_path_a() -> PathBuf {
    PathBuf::from("/test/persist/a")
}
fn test_path_b() -> PathBuf {
    PathBuf::from("/test/persist/b")
}

#[tokio::test]
async fn test_insert_and_load_async() {
    let setup = setup(10);
    let data1 = TestData {
        id: 1,
        content: "hello".to_string(),
    };

    // --- Action: Insert ---
    let arc1 = Arc::new(setup.pool.insert(data1.clone()));
    // --- Verification ---
    // store: 0 (Insert only puts in cache)
    // load: 0
    // delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert_eq!(setup.pool.size(), 1);
    assert_eq!(Arc::strong_count(&arc1), 1);

    // --- Action: Load (from cache) ---
    let guard1 = arc1.load().await;
    assert_eq!(*guard1, data1);
    drop(guard1);
    // --- Verification ---
    // store: 0
    // load: 0 (Was in cache)
    // delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);

    let arc2 = arc1.clone(); // No calls triggered

    // --- Action: Drop Arcs ---
    drop(arc1);
    drop(arc2);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // store: 0
    // load: 0
    // delete: 0 (Item was never written to temp store)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert_eq!(setup.pool.size(), 0);
    assert!(setup.store_impl.get_temp_keys().is_empty());
}

#[tokio::test]
async fn test_eviction_triggers_store_reload_triggers_load() {
    let setup = setup(1); // Cache size 1
    let data1 = TestData {
        id: 1,
        content: "data1".to_string(),
    };
    let data2 = TestData {
        id: 2,
        content: "data2".to_string(),
    };

    // --- Action: Insert data1 ---
    let arc1 = setup.pool.insert(data1.clone());
    let key1 = arc1.key();
    // --- Verification ---
    // store: 0, load: 0, delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // --- Action: Insert data2 (evicts data1) ---
    let arc2 = setup.pool.insert(data2.clone());
    let key2 = arc2.key();
    // --- Verification ---
    // store: 1 (data1 written on eviction)
    // load: 0
    // delete: 0
    wait_for_store(&setup.backing_store).await; // Allow background write on eviction
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_temp_keys().contains(&key1));
    assert!(!setup.store_impl.get_temp_keys().contains(&key2)); // data2 not written yet

    // --- Action: Load data1 (not in cache, evicts data2) ---
    let guard1 = arc1.load().await; // Load data1
    assert_eq!(*guard1, data1);
    drop(guard1);
    // --- Verification ---
    // store: 2 (data2 written on eviction)
    // load: 1 (data1 was loaded)
    // delete: 0
    wait_for_store(&setup.backing_store).await; // Allow background write on eviction
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 2);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&key1));
    assert!(setup.store_impl.get_temp_keys().contains(&key2));

    // --- Action: Load data2 (not in cache, evicts data1) ---
    let guard2 = arc2.load().await; // Load data2
    assert_eq!(*guard2, data2);
    drop(guard2);
    // --- Verification ---
    // store: 2 (data1 already written, eviction likely checks)
    // load: 2 (data2 was loaded)
    // delete: 0
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 2); // No new store
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 2);

    // --- Action: Drop Arcs ---
    drop(arc1);
    drop(arc2);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 2 (Both data1 and data2 were written to temp store)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 2);
    assert_eq!(setup.pool.size(), 0);
    assert!(setup.store_impl.get_temp_keys().is_empty());
}

#[tokio::test]
async fn test_write_now_triggers_store_drop_triggers_delete() {
    let setup = setup(10);
    let data1 = TestData {
        id: 10,
        content: "write_now_delete".to_string(),
    };

    let arc1 = setup.pool.insert(data1.clone());
    let key1 = arc1.key();
    assert!(!setup.store_impl.get_temp_keys().contains(&key1)); // Not written yet

    // --- Action: Explicit Write ---
    let write_handle = arc1.spawn_write_now().await; // Use async version
    write_handle.await.unwrap();
    // --- Verification ---
    // store: 1 (Written explicitly)
    // load: 0
    // delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_temp_keys().contains(&key1));

    // --- Action: Write again (should be no-op for store) ---
    let write_handle2 = arc1.spawn_write_now().await;
    write_handle2.await.unwrap();
    // --- Verification ---
    // store: 1 (Already written, no new store call)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);

    // --- Action: Drop Arc ---
    drop(arc1);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Item was written to temp store)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&key1));
    assert_eq!(setup.pool.size(), 0);
}

#[tokio::test]
async fn test_register_does_not_load_or_store_first_load_does() {
    let setup = setup(10);
    let reg_key = Uuid::new_v4();
    let reg_path = test_path_a();
    let reg_data = TestData {
        id: 99,
        content: "registered".to_string(),
    };

    // Prepare mock state: item exists persisted, AND loadable via temp store
    // because `register` implementation needs load to work later.
    setup
        .store_impl
        ._add_persisted(&reg_path, reg_key, reg_data.clone());
    assert!(setup.store_impl.get_temp_keys().contains(&reg_key)); // Mock setup check
    setup.calls.store.store(0, Ordering::SeqCst); // Reset calls from mock setup

    // --- Action: Track Path ---
    let tracked_path = setup
        .backing_store
        .track_path(reg_path.clone())
        .await
        .unwrap();
    let tracked_path = Arc::new(tracked_path);
    assert!(tracked_path.all_keys().contains(&reg_key));
    assert_eq!(setup.calls.all_persisted_keys.load(Ordering::SeqCst), 1);

    // --- Action: Register ---
    let maybe_arc = setup.pool.register(&tracked_path, reg_key).await;
    let arc = maybe_arc.unwrap();
    // --- Verification ---
    // store: 0 (Register does not store)
    // load: 0 (Register does not load)
    // register: 1
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);
    assert_eq!(setup.pool.size(), 1);
    assert_eq!(arc.key(), reg_key);

    // --- Action: Load (first time) ---
    let guard = arc.load().await;
    assert_eq!(*guard, reg_data);
    drop(guard);
    // --- Verification ---
    // load: 1 (First load triggered Strategy::load)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);

    // --- Action: Load (second time - should be cached) ---
    let guard2 = arc.load().await;
    assert_eq!(*guard2, reg_data);
    drop(guard2);
    // --- Verification ---
    // load: 1 (No new load call, was cached)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);

    // --- Action: Drop Arc ---
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Mock register simulates adding to temp store, so delete occurs)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&reg_key));
    assert_eq!(setup.pool.size(), 0);
}

#[tokio::test]
async fn test_persist_triggers_store_if_not_written_noop_if_done() {
    // Renamed slightly
    let setup = setup(10);
    let data = TestData {
        id: 5,
        content: "persist_store".to_string(),
    };
    let persist_path = test_path_b();

    // --- Action: Insert ---
    let arc = setup.pool.insert(data.clone());
    let key = arc.key();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 0);
    assert!(!setup.store_impl.get_temp_keys().contains(&key));

    // --- Action: Track Path ---
    // We need to track *before* persisting so the TrackedPath knows the initial state.
    let tracked_path = setup
        .backing_store
        .track_path(persist_path.clone())
        .await
        .unwrap();
    let tracked_path = Arc::new(tracked_path);
    // Assume initially empty for this test path
    assert!(!tracked_path.all_keys().contains(&key));

    // --- Action: Persist (first time, not written yet, not persisted yet) ---
    let persist_handle = arc.spawn_persist(&tracked_path).await;
    persist_handle.await.unwrap();
    // --- Verification ---
    // store: 1 (Writes to temp first)
    // persist: 1 (BackingStoreT::persist called)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&key));
    assert!(
        setup
            .store_impl
            .get_persisted_keys(&persist_path)
            .contains(&key)
    );

    // Update tracked path state conceptually (real impl might need re-tracking or updates)
    // For the test, we know the key *is* persisted now. The existing `tracked_path` Arc
    // might not reflect this if it's immutable. Let's assume the *logic* inside
    // spawn_persist checks against the *current* state known to the store, or perhaps
    // checks the TrackedPath passed in. Let's assume it checks the path.

    // --- Action: Persist Again (already written to temp, *and* already persisted at path) ---
    let persist_handle2 = arc.spawn_persist(&tracked_path).await; // Use same tracked_path
    persist_handle2.await.unwrap();
    // --- Verification ---
    // store: 1 (No new store call needed)
    // persist: 1 (NO-OP: Already persisted at the target path) <--- CHANGE HERE
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 1); // Count remains 1

    // --- Action: Drop Arc ---
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Item was written to temp store)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&key));
    assert!(
        setup
            .store_impl
            .get_persisted_keys(&persist_path)
            .contains(&key)
    );

    // Cleanup persisted
    let delete_persisted_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let tracked_clone = tracked_path.clone();
        move || store.blocking_delete_persisted(&tracked_clone, key)
    });
    delete_persisted_handle.await.unwrap();
    assert!(
        !setup
            .store_impl
            .get_persisted_keys(&persist_path)
            .contains(&key)
    );
}

#[tokio::test]
async fn test_persist_is_noop_if_already_persisted() {
    let setup = setup(10);
    let key = Uuid::new_v4();
    let path = test_path_a();
    let data = TestData {
        id: 100,
        content: "already_persisted".to_string(),
    };

    // --- Setup: Simulate item already persisted ---
    // Add to mock persisted *and* temp store (so register -> load works)
    setup.store_impl._add_persisted(&path, key, data.clone());
    assert!(setup.store_impl.get_persisted_keys(&path).contains(&key));
    assert!(setup.store_impl.get_temp_keys().contains(&key)); // For loading later
    setup.calls.store.store(0, Ordering::SeqCst); // Reset calls from mock setup

    // --- Action: Track Path (path now contains the key) ---
    let tracked_path = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path = Arc::new(tracked_path);
    assert!(tracked_path.all_keys().contains(&key)); // Verify tracking found the key
    assert_eq!(setup.calls.all_persisted_keys.load(Ordering::SeqCst), 1);

    // --- Action: Register the Arc ---
    let maybe_arc = setup.pool.register(&tracked_path, key).await;
    let arc = maybe_arc.unwrap();
    // Reset calls after setup phase
    setup.calls.store.store(0, Ordering::SeqCst);
    setup.calls.persist.store(0, Ordering::SeqCst);
    setup.calls.load.store(0, Ordering::SeqCst);
    setup.calls.register.store(0, Ordering::SeqCst); // Reset this too

    // --- Action: Persist (item is already known persisted at this path) ---
    let persist_handle = arc.spawn_persist(&tracked_path).await;
    persist_handle.await.unwrap();

    // --- Verification ---
    // store: 0 (No-op)
    // persist: 0 (No-op)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 0);

    // --- Action: Load (just to double check item is valid) ---
    let guard = arc.load().await;
    assert_eq!(*guard, data);
    drop(guard);
    // load: 1 (First load triggers Strategy::load because register doesn't load)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);

    // --- Action: Drop Arc ---
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Mock register simulates adding to temp store)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&key)); // Temp deleted
    assert!(setup.store_impl.get_persisted_keys(&path).contains(&key)); // Still persisted

    // Cleanup persisted
    let delete_persisted_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let tracked_clone = tracked_path.clone();
        move || store.blocking_delete_persisted(&tracked_clone, key)
    });
    delete_persisted_handle.await.unwrap();
    assert!(!setup.store_impl.get_persisted_keys(&path).contains(&key));
}

#[tokio::test]
async fn test_try_load_mut_unique_deletes_original_temp() {
    let setup = setup(10);
    let data = TestData {
        id: 20,
        content: "mutate_unique_del".to_string(),
    };

    let mut item = setup.pool.insert(data.clone());
    let original_key = item.key();

    // --- Action: Write original to temp store ---
    let write_handle = item.spawn_write_now().await;
    write_handle.await.unwrap();
    // store: 1, delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key));
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);

    // --- Action: Mutate (unique) ---
    let mut guard = item.load_mut().await; // Async version
    guard.content = "mutated_content".to_string();
    drop(guard); // Original temp file deleted *during* try_load_mut
    // --- Verification ---
    // store: 1 (No new store call)
    // delete: 1 (Original key's temp file deleted)
    let new_key = item.key();
    assert_ne!(original_key, new_key);
    wait_for_store(&setup.backing_store).await; // Allow delete task to run
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key)); // Original gone
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key)); // New not written yet

    // --- Action: Drop Mutated Arc ---
    drop(item);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Mutated version was never written to temp, so no new delete)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key));
    assert_eq!(setup.pool.size(), 0);
}

#[tokio::test]
async fn test_try_load_mut_unique_original_not_written() {
    let setup = setup(10);
    let data = TestData {
        id: 23,
        content: "mutate_unique_not_written".to_string(),
    };

    let mut item = setup.pool.insert(data.clone());
    let original_key = item.key();
    // store: 0, delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key)); // Original not written

    // --- Action: Mutate (unique) ---
    let mut guard = item.load_mut().await;
    guard.content = "mutated_content".to_string();
    drop(guard);
    // --- Verification ---
    // store: 0 (No store call)
    // delete: 0 (Original key's temp file didn't exist, so nothing to delete)
    let new_key = item.key();
    assert_ne!(original_key, new_key);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // Delete NOT called
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key));
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key));

    // --- Action: Drop Mutated Arc ---
    drop(item);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 0 (Mutated version never written)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert_eq!(setup.pool.size(), 0);
}

#[tokio::test]
async fn test_make_mut_shared_clones_no_delete_during_call() {
    let setup = setup(10);
    let data = TestData {
        id: 31,
        content: "make_mut_shared_clone".to_string(),
    };

    let mut arc1 = Arc::new(setup.pool.insert(data.clone()));
    let arc2 = arc1.clone(); // Shared reference
    let original_key = arc1.key();

    // --- Action: Write original ---
    let write_handle = arc1.spawn_write_now().await;
    write_handle.await.unwrap();
    // store: 1, delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key));

    // --- Action: make_mut (shared -> clones) ---
    let mut guard = arc1.make_mut().await; // Async version
    guard.id = 32;
    guard.content = "mutated_via_clone".to_string();
    drop(guard);
    // --- Verification ---
    // store: 1 (No new store)
    // delete: 0 (Clone happens, original temp file NOT deleted as arc2 exists)
    let new_key = arc1.key();
    assert_ne!(original_key, new_key);
    assert_eq!(arc2.key(), original_key); // arc2 still points to original
    assert!(!Arc::ptr_eq(&arc1, &arc2));
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // Delete NOT called
    assert!(setup.store_impl.get_temp_keys().contains(&original_key)); // Original still in temp
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key)); // Clone not written

    // --- Action: Drop mutated arc1 ---
    drop(arc1);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 0 (Cloned data was never written to temp)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key));

    // --- Action: Drop original arc2 ---
    drop(arc2);
    wait_for_store(&setup.backing_store).await;
    // --- Verification ---
    // delete: 1 (Original data WAS written and this is the last Arc)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key));
    assert_eq!(setup.pool.size(), 0);
}

#[tokio::test]
async fn test_blocking_save_convenience() {
    // Assuming blocking_save is implemented correctly using spawn_blocking internally
    // for its I/O, but the function itself blocks the caller.
    // So we call it via spawn_blocking from the test.
    let setup = setup(10);
    let path = test_path_a();
    let data1 = TestData {
        id: 101,
        content: "save1".to_string(),
    };
    let data2 = TestData {
        id: 102,
        content: "save2".to_string(),
    };
    let existing_key = Uuid::new_v4();
    let existing_data = TestData {
        id: 100,
        content: "existing".to_string(),
    };

    setup
        .store_impl
        ._add_persisted(&path, existing_key, existing_data.clone());

    let arc1 = Arc::new(setup.pool.insert(data1));
    let arc2 = Arc::new(setup.pool.insert(data2));
    let key1 = arc1.key();
    let key2 = arc2.key();

    let tracked = Arc::new(setup.backing_store.blocking_track_path(path.clone())); // Assuming tracking itself is safe here
    assert!(tracked.all_keys().contains(&existing_key));

    let change_key_called = Arc::new(AtomicBool::new(false));

    let save_handle = tokio::task::spawn_blocking({
        // Clone everything needed for the blocking task
        let store = setup.backing_store.clone();
        let arcs_to_save = vec![arc1.clone(), arc2.clone()];
        let tracked_clone = tracked.clone();
        let flag = change_key_called.clone();

        move || {
            let change_key_closure = move || -> Result<i32, String> {
                println!("Change Key Closure Called!");
                flag.store(true, Ordering::SeqCst);
                Ok(42)
            };

            blocking_save(
                &store,
                arcs_to_save, // Use the cloned arcs
                &tracked_clone,
                4,
                change_key_closure,
            )
        }
    });

    let result = save_handle.await.unwrap(); // Unwrap potential panic
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);

    setup.backing_store.finished().await;

    assert!(change_key_called.load(Ordering::SeqCst));

    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 2);
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 1);

    let persisted_keys = setup.store_impl.get_persisted_keys(&path);
    assert!(persisted_keys.contains(&key1));
    assert!(persisted_keys.contains(&key2));
    assert!(!persisted_keys.contains(&existing_key));

    drop(arc1);
    drop(arc2);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_guard_held_item_evicted_then_dumped_on_guard_drop() {
    // Renamed for clarity
    let setup = setup(2); // Cache size 2
    let data_a = TestData {
        id: 1,
        content: "Item A".to_string(),
    };
    let data_b = TestData {
        id: 2,
        content: "Item B".to_string(),
    };
    let data_c = TestData {
        id: 3,
        content: "Item C".to_string(),
    };

    // --- Action: Insert A ---
    let arc_a = setup.pool.insert(data_a.clone()); // Cache = [A]
    let key_a = arc_a.key();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

    // --- Action: Load A and hold guard ---
    let guard_a = arc_a.load().await; // Cache = [A], A is held
    assert_eq!(*guard_a, data_a);
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // --- Action: Insert B ---
    let arc_b = setup.pool.insert(data_b.clone()); // Cache = [B, A], A is held
    let key_b = arc_b.key(); // To check later
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

    // --- Action: Insert C (A is evicted, B remains) ---
    let arc_c = setup.pool.insert(data_c.clone()); // Cache = [C, B], A is evicted but held by guard
    let key_c = arc_c.key(); // To check later
    // --- Verification ---
    // store: 0 (A is evicted but held by guard, store deferred until guard drop)
    wait_for_store(&setup.backing_store).await; // Ensure no unexpected stores happened
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert!(!setup.store_impl.get_temp_keys().contains(&key_a)); // A not stored yet

    // --- Action: Drop guard_a ---
    // A was evicted, now the guard is dropped. A should be stored now.
    drop(guard_a);
    wait_for_store(&setup.backing_store).await; // Wait for the deferred store task
    // --- Verification ---
    // store: 1 (A should be stored now)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&key_a)); // A should now be in temp store

    // --- Action: Insert D (evicts B) ---
    let data_d = TestData {
        id: 4,
        content: "Item D".to_string(),
    };
    let _arc_d = setup.pool.insert(data_d.clone()); // Cache = [D, C], B evicted.
    wait_for_store(&setup.backing_store).await; // Wait for B's eviction store
    // --- Verification ---
    // store: 2 (B stored on eviction - no guard was held for B)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 2);
    assert!(setup.store_impl.get_temp_keys().contains(&key_b)); // B stored

    // --- Cleanup ---
    drop(arc_a); // A was stored, last ref drop -> delete A scheduled
    drop(arc_b); // B was stored, last ref drop -> delete B scheduled
    drop(arc_c); // C not stored (still in cache), last ref drop -> no delete C scheduled
    // arc_d also not stored
    wait_for_store(&setup.backing_store).await;
    // delete: 2 (A and B deleted)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 2);
    assert!(!setup.store_impl.get_temp_keys().contains(&key_a));
    assert!(!setup.store_impl.get_temp_keys().contains(&key_b));
    assert!(!setup.store_impl.get_temp_keys().contains(&key_c)); // Verify C wasn't stored/deleted
}

#[tokio::test]
async fn test_lru_strict_half_behavior_on_access() {
    // Renamed
    // Scenario 1: Access Back(B), Front(C); Oldest(A) gets evicted
    {
        println!("--- LRU Strict Half Test: Scenario 1 (Oldest A Evicted) ---");
        let setup = setup(4); // Cache size 4 (Front={0,1}, Back={2,3})
        let items = (0..5)
            .map(|i| TestData {
                id: i,
                content: format!("Item {}", i),
            })
            .collect::<Vec<_>>();

        // --- Action: Fill cache A, B, C, D ---
        // Order: [D(0), C(1), B(2), A(3)]
        let arcs = items[0..4]
            .iter()
            .map(|d| setup.pool.insert(d.clone()))
            .collect::<Vec<_>>();
        let keys: Vec<Uuid> = arcs.iter().map(|a| a.key()).collect(); // [kA, kB, kC, kD]
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

        // --- Action: Access C (pos 1, front half) ---
        // Rule: Access front half -> no position change.
        let _guard_c = arcs[2].load().await;
        drop(_guard_c);
        // Expected Cache: [D(0), C(1), B(2), A(3)]

        // --- Action: Access B (pos 2, back half) ---
        // Rule: Access back half -> move to front.
        let _guard_b = arcs[1].load().await;
        drop(_guard_b);
        // Expected Cache: [B(0), D(1), C(2), A(3)] (A is still oldest)

        // Verification: No disk I/O yet
        assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

        // --- Action: Insert E (evicts oldest: A) ---
        let _arc_e = setup.pool.insert(items[4].clone()); // Cache: [E(0), B(1), D(2), C(3)], A evicted
        wait_for_store(&setup.backing_store).await; // Wait for A's eviction store

        // --- Verification ---
        // store: 1 (A evicted and stored)
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
        assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
        assert!(setup.store_impl.get_temp_keys().contains(&keys[0])); // key_a stored
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[1])); // key_b not stored
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[2])); // key_c not stored
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[3])); // key_d not stored
    } // End Scenario 1 scope

    // Scenario 2: Access Back(B), Front(C), Back(A); Oldest(C) gets evicted
    {
        println!("--- LRU Strict Half Test: Scenario 2 (Oldest C Evicted) ---");
        let setup = setup(4); // New setup
        let items = (0..5)
            .map(|i| TestData {
                id: i,
                content: format!("Item {}", i),
            })
            .collect::<Vec<_>>();

        // --- Action: Fill cache A, B, C, D ---
        // Order: [D(0), C(1), B(2), A(3)]
        let arcs = items[0..4]
            .iter()
            .map(|d| setup.pool.insert(d.clone()))
            .collect::<Vec<_>>();
        let keys: Vec<Uuid> = arcs.iter().map(|a| a.key()).collect(); // [kA, kB, kC, kD]
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

        // --- Action: Access C (pos 1, front half) ---
        // Rule: Access front half -> no position change.
        let _guard_c = arcs[2].load().await;
        drop(_guard_c);
        // Expected Cache: [D(0), C(1), B(2), A(3)]

        // --- Action: Access B (pos 2, back half) ---
        // Rule: Access back half -> move to front.
        let _guard_b = arcs[1].load().await;
        drop(_guard_b);
        // Expected Cache: [B(0), D(1), C(2), A(3)]

        // --- Action: Access A (pos 3, back half) ---
        // Rule: Access back half -> move to front.
        let _guard_a = arcs[0].load().await;
        drop(_guard_a);
        // Expected Cache: [A(0), B(1), D(2), C(3)] (C is now oldest)

        // Verification: No disk I/O yet
        assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

        // --- Action: Insert E (evicts oldest: C) ---
        let _arc_e = setup.pool.insert(items[4].clone()); // Cache: [E(0), A(1), B(2), D(3)], C evicted
        wait_for_store(&setup.backing_store).await; // Wait for C's eviction store

        // --- Verification ---
        // store: 1 (C evicted and stored)
        assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
        assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[0])); // key_a not stored
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[1])); // key_b not stored
        assert!(setup.store_impl.get_temp_keys().contains(&keys[2])); // key_c IS stored
        assert!(!setup.store_impl.get_temp_keys().contains(&keys[3])); // key_d not stored
    } // End Scenario 2 scope
}

#[tokio::test]
async fn test_register_fails_if_key_not_in_tracked_path() {
    let setup = setup(10);
    let path = test_path_a();
    let missing_key = Uuid::new_v4(); // A key we won't add

    // --- Action: Track Path (will be empty or not contain missing_key) ---
    let tracked_path = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path = Arc::new(tracked_path);
    assert!(!tracked_path.all_keys().contains(&missing_key)); // Verify key isn't tracked
    let initial_register_calls = setup.calls.register.load(Ordering::SeqCst);
    let initial_pool_size = setup.pool.size();

    // --- Action: Attempt to register the missing key (async) ---
    let result_arc = setup.pool.register(&tracked_path, missing_key).await;

    // --- Verification ---
    assert!(result_arc.is_none()); // Expect None
    assert_eq!(setup.pool.size(), initial_pool_size); // Pool size shouldn't change
    // Assuming register checks TrackedPath first or BackingStoreT::register indicates failure cleanly
    // We expect the underlying BackingStoreT::register not to be called, or to fail without side effects.
    // Strictest check is just on the Option result and pool size.
    // Let's assume the FBPool::register implementation avoids calling BackingStoreT::register
    // if the key isn't in the tracked path's known keys.
    assert_eq!(
        setup.calls.register.load(Ordering::SeqCst),
        initial_register_calls
    ); // No successful register call counted

    // --- Action: Attempt to register the missing key (blocking) ---
    let register_handle = tokio::task::spawn_blocking({
        let pool = setup.pool.clone();
        let tracked = tracked_path.clone();
        move || pool.blocking_register(&tracked, missing_key)
    });
    let result_blocking = register_handle.await.unwrap();

    // --- Verification ---
    assert!(result_blocking.is_none()); // Expect None
    assert_eq!(setup.pool.size(), initial_pool_size);
    assert_eq!(
        setup.calls.register.load(Ordering::SeqCst),
        initial_register_calls
    );
}

#[tokio::test]
async fn test_try_load_mut_triggers_load_if_not_cached() {
    let setup = setup(1); // Cache size 1
    let data_a = TestData {
        id: 50,
        content: "Load For Mut A".to_string(),
    };
    let data_b = TestData {
        id: 51,
        content: "Load For Mut B".to_string(),
    };

    // --- Action: Insert A, Write A ---
    let mut item_a = setup.pool.insert(data_a.clone());
    let original_key_a = item_a.key();
    let write_handle = item_a.spawn_write_now().await;
    write_handle.await.unwrap(); // A is now in temp store
    // store: 1, load: 0, delete: 0
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key_a));

    // --- Action: Insert B (evicts A from memory cache) ---
    let _arc_b = setup.pool.insert(data_b.clone());
    // B's eviction might write B, wait for it. A is no longer cached.
    wait_for_store(&setup.backing_store).await; // Wait for potential eviction store of B
    let stores_after_b_insert = setup.calls.store.load(Ordering::SeqCst); // Might be 1 or 2

    // --- Action: try_load_mut on A (unique, but not cached) ---
    let mut guard = item_a.load_mut().await;

    // --- Verification ---
    // load: 1 (A had to be loaded from disk)
    // delete: 1 (Original temp file for A deleted during try_load_mut)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);

    assert!(!setup.store_impl.get_temp_keys().contains(&original_key_a)); // Original temp gone

    // Modify data
    guard.content = "Mutated A after load".to_string();
    drop(guard);

    let new_key_a = item_a.key(); // Key changes upon getting guard
    assert_ne!(original_key_a, new_key_a);

    // Final check: Load again (async) to verify content
    let final_guard = item_a.load().await;
    assert_eq!(final_guard.content, "Mutated A after load");
    drop(final_guard);

    // Check store count didn't increase further unless B was evicted again
    assert!(setup.calls.store.load(Ordering::SeqCst) <= stores_after_b_insert + 1); // Allow for potential re-eviction store

    drop(item_a); // Drop mutated item (not written, no delete)
    wait_for_store(&setup.backing_store).await;
    // Final delete count depends if B was stored and dropped. Should be >= 1.
    assert!(setup.calls.delete.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn test_blocking_save_skips_already_persisted() {
    let setup = setup(10);
    let path = test_path_b(); // Use a distinct path
    let data_a = TestData {
        id: 70,
        content: "Save A".to_string(),
    };
    let data_b = TestData {
        id: 71,
        content: "Save B".to_string(),
    };

    // --- Setup: Insert A, B ---
    let arc_a = Arc::new(setup.pool.insert(data_a.clone()));
    let arc_b = Arc::new(setup.pool.insert(data_b.clone()));
    let key_a = arc_a.key();
    let key_b = arc_b.key();

    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 0);

    // --- Action 1: Track Path (initially empty) & Save only A ---
    let tracked_path_1 = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path_1 = Arc::new(tracked_path_1);
    assert!(tracked_path_1.all_keys().is_empty());

    let save_handle_1 = tokio::task::spawn_blocking({
        let store_clone = setup.backing_store.clone();
        let arcs_clone = vec![arc_a.clone()]; // Only save A first
        let tracked_clone = tracked_path_1.clone();
        move || -> Result<(), String> {
            blocking_save(&store_clone, arcs_clone, &tracked_clone, 1, || Ok(()))
        }
    });
    let result1 = save_handle_1.await.unwrap();
    assert!(result1.is_ok());

    setup.backing_store.finished().await;

    // --- Verification 1 ---
    // store: 1 (A written to temp)
    // persist: 1 (A persisted)
    // sync: 1
    // delete_persisted: 0 (No old keys)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_persisted_keys(&path).contains(&key_a)); // A persisted
    assert!(!setup.store_impl.get_persisted_keys(&path).contains(&key_b)); // B not persisted
    assert!(setup.store_impl.get_temp_keys().contains(&key_a)); // A in temp

    // --- Action 2: Re-Track Path (now contains A) ---
    let tracked_path_2 = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path_2 = Arc::new(tracked_path_2);
    assert!(tracked_path_2.all_keys().contains(&key_a)); // Path now tracked with A
    assert!(!tracked_path_2.all_keys().contains(&key_b));

    // --- Action 3: Save both A and B using updated tracked_path_2 ---
    // blocking_save should see A is already tracked and only persist B, then cleanup.
    let save_handle_2 = tokio::task::spawn_blocking({
        let store_clone = setup.backing_store.clone();
        let arcs_clone = vec![arc_a.clone(), arc_b.clone()]; // Save A and B
        let tracked_clone = tracked_path_2.clone();
        move || -> Result<(), String> {
            blocking_save(&store_clone, arcs_clone, &tracked_clone, 2, || Ok(()))
        }
    });
    let result2 = save_handle_2.await.unwrap();
    assert!(result2.is_ok());

    setup.backing_store.finished().await;

    // --- Verification 2 ---
    // store: 1 + 1 = 2 (B written to temp)
    // persist: 1 + 1 = 2 (B persisted, A skipped)
    // sync: 1 + 1 = 2
    // delete_persisted: 0 + 0 = 0 (No keys in tracked_path_2 were removed from the final set)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 2);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 2);
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 2);
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 0);

    // Final state checks
    let final_persisted_keys = setup.store_impl.get_persisted_keys(&path);
    assert_eq!(final_persisted_keys.len(), 2);
    assert!(final_persisted_keys.contains(&key_a)); // A still persisted
    assert!(final_persisted_keys.contains(&key_b)); // B now persisted
    assert!(setup.store_impl.get_temp_keys().contains(&key_a)); // A still in temp
    assert!(setup.store_impl.get_temp_keys().contains(&key_b)); // B now in temp

    // --- Cleanup ---
    drop(arc_a);
    drop(arc_b);
    wait_for_store(&setup.backing_store).await;
    // delete: 2 (Both A and B were written to temp store)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_blocking_save_more_items_than_max_simultaneous() {
    let setup = setup(10); // Cache size doesn't matter much here
    let path = test_path_a();
    let num_items = 5;
    let max_simultaneous = 2; // Limit parallelism

    // --- Setup: Insert items ---
    let items: Vec<TestData> = (0..num_items)
        .map(|i| TestData {
            id: 100 + i,
            content: format!("Item {}", i),
        })
        .collect();
    let arcs: Vec<Arc<Fb<TestData, Arc<TestStore>>>> = items
        .iter()
        .map(|d| Arc::new(setup.pool.insert(d.clone())))
        .collect();
    let keys: Vec<Uuid> = arcs.iter().map(|a| a.key()).collect();

    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

    // --- Action: Track Path (empty) ---
    let tracked_path = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path = Arc::new(tracked_path);
    assert!(tracked_path.all_keys().is_empty());

    // --- Action: Call blocking_save with parallelism limit ---
    let save_handle = tokio::task::spawn_blocking({
        // Clone Arcs needed for the blocking task
        let store_clone = setup.backing_store.clone();
        let arcs_clone = arcs.clone();
        let tracked_clone = tracked_path.clone();
        move || -> Result<usize, String> {
            let change_key_closure = || -> Result<usize, String> { Ok(num_items as usize) };
            blocking_save(
                &store_clone,
                arcs_clone,
                &tracked_clone,
                max_simultaneous, // Apply limit here
                change_key_closure,
            )
        }
    });

    let result = save_handle.await.unwrap(); // Unwrap JoinError
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), num_items as usize);

    setup.backing_store.finished().await; // Ensure all tasks are finished

    // --- Verification ---
    // Ensure all items were processed despite parallelism limit
    assert_eq!(
        setup.calls.persist.load(Ordering::SeqCst),
        num_items as usize
    );
    // Each item likely needed storing to temp first
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), num_items as usize);
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 0); // No old keys to delete

    // Check mock persisted state
    let persisted_keys_set = setup.store_impl.get_persisted_keys(&path);
    assert_eq!(persisted_keys_set.len(), num_items as usize);
    for key in &keys {
        assert!(persisted_keys_set.contains(key));
    }

    // Cleanup
    drop(arcs); // Drop all FBArcs
    wait_for_store(&setup.backing_store).await;
    assert_eq!(
        setup.calls.delete.load(Ordering::SeqCst),
        num_items as usize
    ); // All temp items deleted

    // Cleanup persisted
    let cleanup_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let tracked_clone = tracked_path.clone();
        let keys_clone = keys;
        move || {
            for key in keys_clone {
                store.blocking_delete_persisted(&tracked_clone, key);
            }
        }
    });
    cleanup_handle.await.unwrap();
}

// tests/basic.rs (Add these tests to the existing file)

#[tokio::test]
async fn test_blocking_register_success() {
    let setup = setup(10);
    let reg_key = Uuid::new_v4();
    let reg_path = test_path_a();
    let reg_data = TestData {
        id: 200,
        content: "blocking_register_data".to_string(),
    };

    // Setup mock state
    setup
        .store_impl
        ._add_persisted(&reg_path, reg_key, reg_data.clone());
    setup.calls.store.store(0, Ordering::SeqCst); // Reset calls

    // Track path (using blocking version correctly)
    let track_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let path = reg_path.clone();
        move || store.blocking_track_path(path)
    });
    let tracked_path = Arc::new(track_handle.await.unwrap());
    assert!(tracked_path.all_keys().contains(&reg_key));
    let initial_pool_size = setup.pool.size();

    // --- Action: blocking_register ---
    let register_handle = tokio::task::spawn_blocking({
        let pool = setup.pool.clone();
        let tracked = tracked_path.clone();
        move || pool.blocking_register(&tracked, reg_key)
    });
    let result_arc = register_handle.await.unwrap();

    // --- Verification ---
    assert!(result_arc.is_some()); // Expect success
    let arc = result_arc.unwrap();
    assert_eq!(arc.key(), reg_key);
    assert_eq!(setup.pool.size(), initial_pool_size + 1);
    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);
    // Register doesn't load
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // Cleanup
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Temp file deleted on drop
}

#[tokio::test]
async fn test_blocking_load_success() {
    let setup = setup(1); // Cache size 1
    let data_a = TestData {
        id: 210,
        content: "blocking_load_A".to_string(),
    };
    let data_b = TestData {
        id: 211,
        content: "blocking_load_B".to_string(),
    };

    // Insert A, write A to temp store
    let arc_a = Arc::new(setup.pool.insert(data_a.clone()));
    let key_a = arc_a.key();
    tokio::task::spawn_blocking({
        let a = arc_a.clone();
        move || a.blocking_write_now()
    })
    .await
    .unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&key_a));

    // Insert B to evict A from cache
    let _item_b = setup.pool.insert(data_b.clone());
    wait_for_store(&setup.backing_store).await; // Wait for potential eviction store

    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // --- Action: blocking_load for A (not cached) ---
    let load_handle = tokio::task::spawn_blocking({
        let arc_clone = arc_a.clone();
        move || {
            let guard = arc_clone.blocking_load();
            guard.clone() // Clone data out
        }
    });
    let loaded_data = load_handle.await.unwrap();

    // --- Verification ---
    assert_eq!(loaded_data, data_a);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1); // Load occurred

    // --- Cleanup ---
    drop(arc_a);
    wait_for_store(&setup.backing_store).await; // Wait for delete task
}

#[tokio::test]
async fn test_blocking_write_now_success() {
    let setup = setup(10);
    let data = TestData {
        id: 220,
        content: "blocking_write_now".to_string(),
    };

    let arc = Arc::new(setup.pool.insert(data.clone()));
    let key = arc.key();
    assert!(!setup.store_impl.get_temp_keys().contains(&key));
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);

    // --- Action: blocking_write_now ---
    let write_handle = tokio::task::spawn_blocking({
        let arc_clone = arc.clone();
        move || arc_clone.blocking_write_now()
    });
    write_handle.await.unwrap();

    // --- Verification ---
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // Store occurred
    assert!(setup.store_impl.get_temp_keys().contains(&key));

    // --- Action: blocking_write_now again (no-op for store) ---
    let write_handle2 = tokio::task::spawn_blocking({
        let arc_clone = arc.clone();
        move || arc_clone.blocking_write_now()
    });
    write_handle2.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // Store count unchanged

    // Cleanup
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Written, so deleted
}

#[tokio::test]
async fn test_blocking_persist_success() {
    let setup = setup(10);
    let data = TestData {
        id: 230,
        content: "blocking_persist".to_string(),
    };
    let path = test_path_a();

    let arc = Arc::new(setup.pool.insert(data.clone()));
    let key = arc.key();
    assert!(!setup.store_impl.get_temp_keys().contains(&key));
    assert!(!setup.store_impl.get_persisted_keys(&path).contains(&key));
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 0);

    // Track path
    let tracked_path = Arc::new(setup.backing_store.blocking_track_path(path.clone())); // Okay to call blocking track directly here

    // --- Action: blocking_persist ---
    let persist_handle = tokio::task::spawn_blocking({
        let arc_clone = arc.clone();
        let tracked_clone = tracked_path.clone();
        move || arc_clone.blocking_persist(&tracked_clone)
    });
    persist_handle.await.unwrap();

    // --- Verification ---
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // Written to temp first
    assert_eq!(setup.calls.persist.load(Ordering::SeqCst), 1); // Persisted
    assert!(setup.store_impl.get_temp_keys().contains(&key));
    assert!(setup.store_impl.get_persisted_keys(&path).contains(&key));

    // Cleanup
    drop(arc);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Temp deleted
    let cleanup_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let tracked_clone = tracked_path.clone();
        move || store.blocking_delete_persisted(&tracked_clone, key)
    });
    cleanup_handle.await.unwrap(); // Cleanup persisted
}

#[tokio::test]
async fn test_blocking_try_load_mut_success() {
    let setup = setup(10);
    let data = TestData {
        id: 240,
        content: "blocking_try_mut".to_string(),
    };

    let mut arc = Arc::new(setup.pool.insert(data.clone()));
    let original_key = arc.key();

    // Write to temp store first
    tokio::task::spawn_blocking({
        let a = arc.clone();
        move || a.blocking_write_now()
    })
    .await
    .unwrap();
    assert!(setup.store_impl.get_temp_keys().contains(&original_key));
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // Not loaded yet if cached

    // --- Action: blocking_try_load_mut ---
    let mutate_handle = tokio::task::spawn_blocking(move || {
        let maybe_guard = Arc::get_mut(&mut arc).map(Fb::blocking_load_mut);
        assert!(maybe_guard.is_some());
        let mut guard = maybe_guard.unwrap();
        guard.content = "mutated_blocking".to_string();
        drop(guard);
        arc // move arc back
    });
    let arc_mutated = mutate_handle.await.unwrap();

    // --- Verification ---
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // Assume was cached, no load needed
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Original temp deleted
    let new_key = arc_mutated.key();
    assert_ne!(original_key, new_key);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key)); // Original temp gone

    // Verify content (use blocking load)
    let load_handle = tokio::task::spawn_blocking({
        let a = arc_mutated.clone();
        move || a.blocking_load().content.clone()
    });
    assert_eq!(load_handle.await.unwrap(), "mutated_blocking");

    // Cleanup
    drop(arc_mutated);
    wait_for_store(&setup.backing_store).await;
    // Mutated version not written, no second delete
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_blocking_make_mut_success_unique() {
    let setup = setup(10);
    let data = TestData {
        id: 250,
        content: "blocking_make_mut".to_string(),
    };

    let mut arc = Arc::new(setup.pool.insert(data.clone()));
    let original_key = arc.key();

    tokio::task::spawn_blocking({
        let a = arc.clone();
        move || a.blocking_write_now()
    })
    .await
    .unwrap();
    assert!(setup.store_impl.get_temp_keys().contains(&original_key));
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);

    // --- Action: blocking_make_mut (unique) ---
    let mutate_handle = tokio::task::spawn_blocking(move || {
        // Unique case, should be like try_load_mut
        let mut guard = arc.blocking_make_mut();
        guard.content = "mutated_make_mut_blocking".to_string();
        drop(guard);
        arc // move arc back
    });
    let arc_mutated = mutate_handle.await.unwrap();

    // --- Verification ---
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Original temp deleted
    let new_key = arc_mutated.key();
    assert_ne!(original_key, new_key);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key));

    // Verify content
    let load_handle = tokio::task::spawn_blocking({
        let a = arc_mutated.clone();
        move || a.blocking_load().content.clone()
    });
    assert_eq!(load_handle.await.unwrap(), "mutated_make_mut_blocking");

    // Cleanup
    drop(arc_mutated);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // No second delete
}

#[tokio::test]
async fn test_try_load_mut_fails_if_not_unique() {
    let setup = setup(10);
    let data = TestData {
        id: 300,
        content: "try_mut_fail".to_string(),
    };

    // --- Setup: Create shared Arc ---
    let mut arc1 = Arc::new(setup.pool.insert(data.clone()));
    let arc2 = arc1.clone(); // Create a second reference
    let original_key = arc1.key();
    let initial_delete_count = setup.calls.delete.load(Ordering::SeqCst);

    assert_eq!(Arc::strong_count(&arc1), 2); // Verify it's shared

    // --- Action: try_load_mut (async) ---
    let result_async = Arc::get_mut(&mut arc1);

    // --- Verification (async) ---
    assert!(result_async.is_none()); // Should fail
    assert_eq!(arc1.key(), original_key); // Key should not change
    assert_eq!(
        setup.calls.delete.load(Ordering::SeqCst),
        initial_delete_count
    ); // No delete call

    // --- Action: blocking_try_load_mut ---
    let handle_blocking = tokio::task::spawn_blocking({
        // Need to clone BOTH Arcs if we want to keep arc2 alive outside,
        // but we only need arc1 inside the closure.
        // Move arc1 into the closure. arc2 keeps the count > 1.
        let mut arc1_clone = arc1;
        move || Arc::get_mut(&mut arc1_clone).is_some()
    });
    let succeeded = handle_blocking.await.unwrap(); // Unwrap JoinError
    // Get arc1 back - its state shouldn't have changed
    // let arc1 = handle_blocking.await.unwrap().1; // This doesn't work as maybe_guard doesn't hold arc

    // --- Verification (blocking) ---
    assert!(!succeeded); // Should fail
    // We can't easily check arc1's key here without returning it,
    // but we can check the delete count hasn't changed.
    assert_eq!(
        setup.calls.delete.load(Ordering::SeqCst),
        initial_delete_count
    ); // No delete call

    // Keep arc2 alive until end of test
    drop(arc2);
}

#[tokio::test]
async fn test_blocking_save_many_deletions_exceeding_limit() {
    let setup = setup(20); // Ensure enough space if needed
    let path = test_path_a();
    let num_initial = 5;
    let num_new = 2;
    let max_simultaneous = 2; // Limit for tasks (including deletions)

    // --- Setup: Insert and Save initial items ---
    let initial_items: Vec<TestData> = (0..num_initial)
        .map(|i| TestData {
            id: 400 + i,
            content: format!("Initial {}", i),
        })
        .collect();
    let initial_arcs: Vec<_> = initial_items
        .iter()
        .map(|d| Arc::new(setup.pool.insert(d.clone())))
        .collect();
    let initial_keys: HashSet<Uuid> = initial_arcs.iter().map(|a| a.key()).collect();

    let tracked_path_1 = Arc::new(setup.backing_store.track_path(path.clone()).await.unwrap());
    assert!(tracked_path_1.all_keys().is_empty());

    let save_handle_1 = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let arcs = initial_arcs.clone();
        let tracked = tracked_path_1.clone();
        move || -> Result<(), String> {
            blocking_save(&store, arcs, &tracked, max_simultaneous, || Ok(()))
        }
    });
    assert!(save_handle_1.await.unwrap().is_ok());

    setup.backing_store.finished().await; // Ensure all tasks are finished

    // Verification 1
    assert_eq!(
        setup.calls.persist.load(Ordering::SeqCst),
        num_initial as usize
    );
    assert_eq!(setup.calls.delete_persisted.load(Ordering::SeqCst), 0);
    assert_eq!(setup.store_impl.get_persisted_keys(&path), initial_keys);

    // --- Setup: Insert new items ---
    let new_items: Vec<TestData> = (0..num_new)
        .map(|i| TestData {
            id: 500 + i,
            content: format!("New {}", i),
        })
        .collect();
    let new_arcs: Vec<_> = new_items
        .iter()
        .map(|d| Arc::new(setup.pool.insert(d.clone())))
        .collect();
    let new_keys: HashSet<Uuid> = new_arcs.iter().map(|a| a.key()).collect();

    // --- Action: Re-Track Path (contains initial keys) & Save new items ---
    let tracked_path_2 = Arc::new(setup.backing_store.track_path(path.clone()).await.unwrap());
    assert_eq!(HashSet::from_iter(tracked_path_2.all_keys()), initial_keys);

    let save_handle_2 = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let arcs = new_arcs.clone(); // Only save the new set
        let tracked = tracked_path_2.clone();
        move || -> Result<(), String> {
            blocking_save(&store, arcs, &tracked, max_simultaneous, || Ok(()))
        }
    });
    assert!(save_handle_2.await.unwrap().is_ok());

    setup.backing_store.finished().await; // Ensure all tasks are finished

    // --- Verification 2 ---
    // persist: 5 + 2 = 7 (New items persisted)
    // delete_persisted: 0 + 5 = 5 (Initial items deleted)
    // sync: 1 + 1 = 2
    // store: 5 + 2 = 7 (Assuming new items weren't written before)
    assert_eq!(
        setup.calls.persist.load(Ordering::SeqCst),
        num_initial as usize + num_new as usize
    );
    assert_eq!(
        setup.calls.delete_persisted.load(Ordering::SeqCst),
        num_initial as usize
    );
    assert_eq!(setup.calls.sync_persisted.load(Ordering::SeqCst), 2);
    assert_eq!(
        setup.calls.store.load(Ordering::SeqCst),
        num_initial as usize + num_new as usize
    );

    // Check final persisted state
    let final_persisted_keys = setup.store_impl.get_persisted_keys(&path);
    assert_eq!(final_persisted_keys, new_keys); // Only new keys should remain

    // --- Cleanup ---
    // Drop all arcs
    drop(initial_arcs);
    drop(new_arcs);
    wait_for_store(&setup.backing_store).await;
    // Verify temp deletes occurred for all stored items
    assert_eq!(
        setup.calls.delete.load(Ordering::SeqCst),
        num_initial as usize + num_new as usize
    );
}

#[tokio::test]
async fn test_blocking_make_mut_shared_clones() {
    let setup = setup(10);
    // Ensure the data type is Clone
    let data = TestData {
        id: 31,
        content: "make_mut_shared_blocking".to_string(),
    };

    // --- Setup: Create shared Arc and write original data ---
    let mut arc1 = Arc::new(setup.pool.insert(data.clone()));
    let arc2 = arc1.clone(); // Shared reference
    let original_key = arc1.key();

    let write_handle = tokio::task::spawn_blocking({
        let arc_clone = arc1.clone();
        move || arc_clone.blocking_write_now() // Write the original data to temp store
    });
    write_handle.await.unwrap();

    // --- Pre-Verification ---
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // Original stored
    assert!(Arc::ptr_eq(&arc1, &arc2)); // Point to same data initially
    assert_eq!(Arc::strong_count(&arc1), 2); // Shared count
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No deletes yet

    // --- Action: blocking_make_mut on shared Arc ---
    // This should trigger a clone within arc1.
    let mutate_handle = tokio::task::spawn_blocking(move || {
        // arc1 moved into closure
        let mut guard = arc1.blocking_make_mut(); // This clones arc1 internally
        guard.id = 32; // Modify the clone
        guard.content = "mutated_via_clone_blocking".to_string();
        drop(guard);
        arc1 // Return the mutated arc1
    });
    let arc1_mutated = mutate_handle.await.unwrap(); // Retrieve the (now unique) mutated arc1

    // --- Verification ---
    // Counts and Pointers: arc1 is now unique, arc2 is unique, they point to different data
    assert_eq!(Arc::strong_count(&arc1_mutated), 1);
    assert_eq!(Arc::strong_count(&arc2), 1); // arc2 still exists and holds original
    assert!(!Arc::ptr_eq(&arc1_mutated, &arc2)); // No longer point to the same Arc entry

    // Keys: arc1 has new key, arc2 has original key
    let new_key = arc1_mutated.key();
    assert_ne!(original_key, new_key);
    assert_eq!(arc2.key(), original_key);

    // Deletes: No delete should have occurred during make_mut when cloning
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key)); // Original still in temp

    // Content: Verify arc1 has mutated data, arc2 has original
    // Load arc1 (mutated) - use blocking load correctly
    let load1_handle = tokio::task::spawn_blocking({
        let a = arc1_mutated.clone();
        move || a.blocking_load().clone()
    });
    let data1_loaded = load1_handle.await.unwrap();
    assert_eq!(data1_loaded.id, 32);
    assert_eq!(data1_loaded.content, "mutated_via_clone_blocking");

    // Load arc2 (original) - use blocking load correctly
    let load2_handle = tokio::task::spawn_blocking({
        let a = arc2.clone();
        move || a.blocking_load().clone()
    });
    let data2_loaded = load2_handle.await.unwrap();
    assert_eq!(data2_loaded, data); // Should be original data

    // --- Cleanup: Drop arcs separately and verify deletes ---
    drop(arc1_mutated); // Drop mutated (clone wasn't written to temp)
    wait_for_store(&setup.backing_store).await;
    // delete: 0 (Mutated data was never stored, so drop does nothing)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
    assert!(!setup.store_impl.get_temp_keys().contains(&new_key));

    drop(arc2); // Drop original (WAS written to temp)
    wait_for_store(&setup.backing_store).await;
    // delete: 1 (Original data deleted now)
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key));
    assert_eq!(setup.pool.size(), 0); // Pool should be empty now
}

#[tokio::test]
async fn test_blocking_try_load_mut_triggers_load_if_not_cached() {
    let setup = setup(1); // Cache size 1
    let data_a = TestData {
        id: 55,
        content: "Blocking Load Mut A".to_string(),
    };
    let data_b = TestData {
        id: 56,
        content: "Blocking Load Mut B".to_string(),
    };

    // --- Setup: Insert A, write A, evict A ---
    let mut arc_a = Arc::new(setup.pool.insert(data_a.clone()));
    let original_key_a = arc_a.key();

    // Write A using blocking_write_now
    let write_handle = tokio::task::spawn_blocking({
        let a = arc_a.clone();
        move || a.blocking_write_now()
    });
    write_handle.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // A stored
    assert!(setup.store_impl.get_temp_keys().contains(&original_key_a));
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No deletes yet

    // Insert B to evict A
    let _arc_b = setup.pool.insert(data_b.clone());
    wait_for_store(&setup.backing_store).await; // Wait for potential eviction store of B
    let stores_after_b_insert = setup.calls.store.load(Ordering::SeqCst); // Might be 1 or 2

    // --- Action: blocking_try_load_mut on A (unique, not cached) ---
    assert_eq!(Arc::strong_count(&arc_a), 1); // Ensure unique reference
    let mutate_handle = tokio::task::spawn_blocking(move || {
        // arc_a moved into closure
        let maybe_entry = Arc::get_mut(&mut arc_a);
        assert!(maybe_entry.is_some());
        let mut guard = maybe_entry.unwrap().blocking_load_mut(); // This should trigger load 
        guard.content = "Mutated A after blocking load".to_string();
        drop(guard); // Deletes original temp file, assigns new key
        arc_a // Return mutated arc
    });
    let arc_a_mutated = mutate_handle.await.unwrap();

    // --- Verification ---
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    // load: 1 (A had to be loaded)
    // delete: 1 (Original temp file deleted during blocking_try_load_mut)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);

    let new_key_a = arc_a_mutated.key();
    assert_ne!(original_key_a, new_key_a);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key_a)); // Original temp gone
    // Store count shouldn't have increased unless B got re-evicted during A's load+mutate
    assert!(setup.calls.store.load(Ordering::SeqCst) <= stores_after_b_insert + 1);

    // --- Final check: Load content (blocking) ---
    let load_handle = tokio::task::spawn_blocking({
        let a = arc_a_mutated.clone();
        move || a.blocking_load().content.clone()
    });
    let final_content = load_handle.await.unwrap();
    assert_eq!(final_content, "Mutated A after blocking load");

    // --- Cleanup ---
    drop(arc_a_mutated); // Drop mutated arc (not written, no delete)
    // _arc_b drops implicitly
    wait_for_store(&setup.backing_store).await;
    // Final delete count depends if B was stored. Should be >= 1.
    assert!(setup.calls.delete.load(Ordering::SeqCst) >= 1);
}

#[tokio::test(flavor = "multi_thread")] // Explicitly use multi-thread runtime
async fn test_load_triggers_blocking_load_if_not_cached() {
    // This test relies on block_in_place, hence the multi_thread flavor.
    let setup = setup(1); // Cache size 1 to easily force eviction
    let data_a = TestData {
        id: 600,
        content: "Load A".to_string(),
    };
    let data_b = TestData {
        id: 601,
        content: "Load B".to_string(),
    };

    // --- Setup: Insert A, write A, evict A ---
    let arc_a = setup.pool.insert(data_a.clone());
    let key_a = arc_a.key();

    // Write A to temp store so it's loadable after eviction
    let write_handle = arc_a.spawn_write_now().await; // Use async version for setup convenience
    write_handle.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1); // A stored
    assert!(setup.store_impl.get_temp_keys().contains(&key_a));
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // Insert B to evict A from memory cache
    let _arc_b = setup.pool.insert(data_b.clone());
    wait_for_store(&setup.backing_store).await; // Wait for potential eviction store of B

    // --- Action: Call load() on A (unique, not cached) ---
    // This call will use block_in_place internally because A is not cached.
    // It should block the current async test thread temporarily but succeed
    // because we specified the multi_thread runtime flavor.
    let guard_a = arc_a.load_in_place(); // THE CALL BEING TESTED

    // --- Verification ---
    assert_eq!(*guard_a, data_a); // Verify data is correct
    // load: 1 (Strategy::load should have been called via block_in_place)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);

    // --- Cleanup ---
    drop(guard_a);
    drop(arc_a);
    // arc_b drops implicitly
    wait_for_store(&setup.backing_store).await;
    assert!(setup.calls.delete.load(Ordering::SeqCst) >= 1); // A should be deleted
}

#[tokio::test(flavor = "multi_thread")] // Use multi_thread for consistency, though not strictly needed here
async fn test_load_returns_from_cache_if_present() {
    let setup = setup(10); // Sufficient cache size
    let data_a = TestData {
        id: 610,
        content: "Load Cached A".to_string(),
    };

    // --- Setup: Insert A (it's now in cache) ---
    let arc_a = setup.pool.insert(data_a.clone());
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // --- Action: Call load() on A (cached) ---
    // This should return directly from the memory cache without
    // calling block_in_place or Strategy::load.
    let guard_a = arc_a.load_in_place(); // THE CALL BEING TESTED

    // --- Verification ---
    assert_eq!(*guard_a, data_a); // Verify data is correct
    // load: 0 (Strategy::load should NOT have been called)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // --- Cleanup ---
    drop(guard_a);
    drop(arc_a); // A was never stored, so no delete on drop
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);
}

#[tokio::test] // No specific flavor needed, assuming try_load is non-blocking
async fn test_try_load_success_when_cached() {
    let setup = setup(10); // Ensure space in cache
    let data_a = TestData {
        id: 700,
        content: "Try Load Cached A".to_string(),
    };

    // --- Setup: Insert A (it's now in cache) ---
    let item_a = setup.pool.insert(data_a.clone());
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // --- Action: Call try_load() on cached item ---
    let maybe_guard = item_a.try_load();

    // --- Verification ---
    assert!(maybe_guard.is_some()); // Expect success
    let guard = maybe_guard.unwrap();
    assert_eq!(*guard, data_a); // Verify data
    // load: 0 (Strategy::load should NOT have been called)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // --- Cleanup ---
    drop(guard);
    drop(item_a);
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // Not stored, no delete
}

#[tokio::test]
async fn test_try_load_fails_when_not_cached() {
    let setup = setup(1); // Cache size 1 to force eviction
    let data_a = TestData {
        id: 710,
        content: "Try Load Evicted A".to_string(),
    };
    let data_b = TestData {
        id: 711,
        content: "Try Load Evictor B".to_string(),
    };

    // --- Setup: Insert A, write A, evict A ---
    let item_a = setup.pool.insert(data_a.clone());
    // Write A to temp store so it *could* be loaded, but won't be by try_load
    let write_handle = item_a.spawn_write_now().await;
    write_handle.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);

    // Insert B to evict A
    let _item_b = setup.pool.insert(data_b.clone());
    wait_for_store(&setup.backing_store).await; // Wait for potential eviction store of B

    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // --- Action: Call try_load() on evicted item A ---
    let maybe_guard = item_a.try_load();

    // --- Verification ---
    assert!(maybe_guard.is_none()); // Expect failure (None)
    // load: 0 (Strategy::load should NOT have been called)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);

    // --- Cleanup ---
    drop(maybe_guard);
    drop(item_a); // A was stored, should be deleted
    wait_for_store(&setup.backing_store).await;
    assert!(setup.calls.delete.load(Ordering::SeqCst) >= 1); // Delete for A occurred
}

#[tokio::test]
async fn test_sync_try_load_mut_success_when_cached() {
    let setup = setup(10); // Cache size 10
    let data_a = TestData {
        id: 720,
        content: "Sync Try Mut A".to_string(),
    };

    // --- Setup: Insert A, write A. A is cached and unique. ---
    let mut item_a = setup.pool.insert(data_a.clone());
    let original_key_a = item_a.key();
    // Write A so delete behavior can be verified
    let write_handle = item_a.spawn_write_now().await;
    write_handle.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key_a));
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads needed yet
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No deletes needed yet

    // --- Action: Call sync try_load_mut() ---
    let maybe_guard = item_a.try_load_mut(); // The synchronous call being tested

    // --- Verification ---
    assert!(maybe_guard.is_some()); // Expect success
    // load: 0 (No load needed, was cached)
    // delete: 1 (Original temp file deleted)
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key_a)); // Original temp gone

    // Modify data via guard
    let mut guard = maybe_guard.unwrap();
    guard.content = "Sync Mutated A".to_string();
    drop(guard);

    let new_key_a = item_a.key(); // Key should change upon success
    assert_ne!(original_key_a, new_key_a);

    // Final check: Load content (async) to verify
    let final_guard = item_a.load().await;
    assert_eq!(final_guard.content, "Sync Mutated A");
    drop(final_guard);

    // Cleanup
    drop(item_a); // Mutated wasn't stored, no further delete
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_sync_try_load_mut_fails_when_not_cached() {
    let setup = setup(1); // Cache size 1
    let data_a = TestData {
        id: 730,
        content: "Sync Try Mut Evicted A".to_string(),
    };
    let data_b = TestData {
        id: 731,
        content: "Sync Try Mut Evictor B".to_string(),
    };

    // --- Setup: Insert A, write A, evict A ---
    let mut item_a = setup.pool.insert(data_a.clone());
    let original_key_a = item_a.key();
    let write_handle = item_a.spawn_write_now().await;
    write_handle.await.unwrap(); // A is in temp store
    let _arc_b = setup.pool.insert(data_b.clone()); // Evict A
    wait_for_store(&setup.backing_store).await;
    let initial_delete_count = setup.calls.delete.load(Ordering::SeqCst);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads yet

    // --- Action: Call sync try_load_mut() on evicted item ---
    let maybe_guard = item_a.try_load_mut();

    // --- Verification ---
    assert!(maybe_guard.is_none()); // Expect failure (None) because not cached
    drop(maybe_guard);
    assert_eq!(item_a.key(), original_key_a); // Key should not change
    // load: 0 (try_load_mut doesn't load)
    // delete: 0 (No delete occurred)
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(
        setup.calls.delete.load(Ordering::SeqCst),
        initial_delete_count
    );

    // Cleanup
    drop(item_a); // A was stored, delete should happen
    wait_for_store(&setup.backing_store).await;
    assert!(setup.calls.delete.load(Ordering::SeqCst) > initial_delete_count);
}

#[tokio::test]
async fn test_register_same_persisted_key_twice() {
    let setup = setup(10); // Cache size doesn't matter much here
    let path = test_path_a();
    let key = Uuid::new_v4();
    let data = TestData {
        id: 800,
        content: "Register Twice".to_string(),
    };

    // --- Setup: Simulate item already persisted ---
    setup.store_impl._add_persisted(&path, key, data.clone());
    assert!(setup.store_impl.get_persisted_keys(&path).contains(&key));
    assert!(setup.store_impl.get_temp_keys().contains(&key)); // For loading later
    setup.calls.store.store(0, Ordering::SeqCst); // Reset calls

    // --- Action: Track Path ---
    let tracked_path = setup.backing_store.track_path(path.clone()).await.unwrap();
    let tracked_path = Arc::new(tracked_path);
    assert!(tracked_path.all_keys().contains(&key));
    setup.calls.register.store(0, Ordering::SeqCst); // Reset register count specifically

    // --- Action: Register key for the first time (async) ---
    let maybe_item1 = setup.pool.register(&tracked_path, key).await;
    assert!(maybe_item1.is_some());
    let item1 = maybe_item1.unwrap();

    // --- Verification 1 ---
    assert_eq!(item1.key(), key);
    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // Register doesn't load

    // --- Action: Register same key for the second time (async) ---
    let maybe_item2 = setup.pool.register(&tracked_path, key).await;
    assert!(maybe_item2.is_some());
    let item2 = maybe_item2.unwrap();

    // --- Verification 2 ---
    assert_eq!(item2.key(), key);
    // BackingStoreT::register only called once.
    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // Still no loads

    // --- Verification 3: Distinct Items, Equal Keys ---
    assert_eq!(item1.key(), item2.key());

    // --- Verification 4: Loading yields equal data but requires separate loads ---
    // Load item1
    let guard1 = item1.load().await;
    assert_eq!(*guard1, data);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1); // First load occurred
    drop(guard1);

    // Load item2
    let guard2 = item2.load().await;
    assert_eq!(*guard2, data);
    // Load count should increase again, as item2 is a distinct entry
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 2); // Second load occurred
    drop(guard2);

    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No deletes yet

    // --- Action: Drop first Arc ---
    drop(item1);
    wait_for_store(&setup.backing_store).await;
    // --- Verification 5 ---
    // Delete should NOT have happened yet, as item2 still holds a reference managed by the pool
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);

    // --- Action: Drop second Arc ---
    drop(item2);
    wait_for_store(&setup.backing_store).await;
    // --- Verification 6 ---
    // Now the last reference managed by the pool is gone, delete should occur
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&key)); // Temp deleted

    // Cleanup persisted state
    let cleanup_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let tracked_clone = tracked_path.clone();
        move || store.blocking_delete_persisted(&tracked_clone, key)
    });
    cleanup_handle.await.unwrap();
}

#[tokio::test]
async fn test_blocking_register_same_persisted_key_twice() {
    // Similar test using the blocking variants
    let setup = setup(10);
    let path = test_path_b(); // Use different path
    let key = Uuid::new_v4();
    let data = TestData {
        id: 801,
        content: "Blocking Register Twice".to_string(),
    };

    setup.store_impl._add_persisted(&path, key, data.clone());
    setup.calls.store.store(0, Ordering::SeqCst);

    let tracked_path = Arc::new(setup.backing_store.blocking_track_path(path.clone()));
    assert!(tracked_path.all_keys().contains(&key));
    setup.calls.register.store(0, Ordering::SeqCst);

    // --- Action: Register key for the first time (blocking) ---
    let register1_handle = tokio::task::spawn_blocking({
        let p = setup.pool.clone();
        let t = tracked_path.clone();
        move || p.blocking_register(&t, key)
    });
    let item1 = register1_handle.await.unwrap().unwrap(); // Unwrap JoinError and Option

    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);

    // --- Action: Register same key for the second time (blocking) ---
    let register2_handle = tokio::task::spawn_blocking({
        let p = setup.pool.clone();
        let t = tracked_path.clone();
        move || p.blocking_register(&t, key)
    });
    let item2 = register2_handle.await.unwrap().unwrap(); // Unwrap JoinError and Option

    assert_eq!(setup.calls.register.load(Ordering::SeqCst), 1);

    // --- Verification: Distinct Arcs ---
    assert_eq!(item1.key(), item2.key());

    // --- Verification: Loading (use blocking load) ---
    let load1_handle = tokio::task::spawn_blocking(move || item1.blocking_load().clone());
    assert_eq!(load1_handle.await.unwrap(), data);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 1);

    let load2_handle = tokio::task::spawn_blocking(move || item2.blocking_load().clone());
    assert_eq!(load2_handle.await.unwrap(), data);
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 2); // Separate load needed

    // --- Cleanup ---
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Now deleted

    let cleanup_handle = tokio::task::spawn_blocking({
        let store = setup.backing_store.clone();
        let t = tracked_path.clone();
        move || store.blocking_delete_persisted(&t, key)
    });
    cleanup_handle.await.unwrap();
}

#[tokio::test]
async fn test_async_make_mut_unique_no_clone() {
    let setup = setup(10);
    // Ensure data is Clone, though it shouldn't be used here
    let data_a = TestData {
        id: 900,
        content: "Async Make Mut Unique".to_string(),
    };

    // --- Setup: Insert A, write A. A is cached and unique. ---
    let mut arc_a = Arc::new(setup.pool.insert(data_a.clone()));
    let original_key_a = arc_a.key();
    // Write A so delete behavior can be verified
    let write_handle = arc_a.spawn_write_now().await;
    write_handle.await.unwrap();
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert!(setup.store_impl.get_temp_keys().contains(&original_key_a));
    assert_eq!(Arc::strong_count(&arc_a), 1); // Verify unique
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0); // No loads needed yet
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No deletes needed yet

    // --- Action: Call async make_mut() ---
    // Since it's unique, this should behave like try_load_mut:
    // ensure cached (already is), delete original temp, change key, return guard.
    let mut guard = arc_a.make_mut().await; // THE CALL BEING TESTED

    // --- Verification ---
    // load: 0 (No load needed, was cached)
    // delete: 1 (Original temp file deleted because mutation occurred)
    wait_for_store(&setup.backing_store).await; // Wait for delete task
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);
    assert!(!setup.store_impl.get_temp_keys().contains(&original_key_a)); // Original temp gone

    // Modify data via guard
    guard.content = "Async Make Mut Unique - Modified".to_string();
    drop(guard);

    let new_key_a = arc_a.key(); // Key should change upon success
    assert_ne!(original_key_a, new_key_a);

    // Final check: Load content (async) to verify
    let final_guard = arc_a.load().await;
    assert_eq!(final_guard.content, "Async Make Mut Unique - Modified");
    drop(final_guard);

    // Cleanup
    drop(arc_a); // Mutated wasn't stored, no further delete
    wait_for_store(&setup.backing_store).await;
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1); // Delete count remains 1
}

#[tokio::test]
async fn test_try_load_fails_while_store_is_running_for_eviction() {
    let sleep_duration = Duration::from_millis(50);
    println!(
        "NOTE: This test uses mock TestStore::store with sleep: {:?}",
        sleep_duration
    );
    // --- USE SETUP WITH SLEEP ---
    let setup = setup_with_store_sleep(1, sleep_duration); // Cache size 1, 50ms sleep
    // ----------------------------
    let data_a = TestData {
        id: 960,
        content: "Eviction Target A Sleep".to_string(),
    };
    let data_b = TestData {
        id: 961,
        content: "Evictor B Sleep".to_string(),
    };

    // Insert A - cached
    let item_a = setup.pool.insert(data_a.clone());

    // Insert B - triggers store(A), which will sleep.
    let _item_b = setup.pool.insert(data_b.clone());

    // Give the background task a moment to start the store call.
    tokio::time::sleep(Duration::from_millis(10)).await; // Less than sleep_duration

    // --- Action: Call try_load() on A while store(A) is likely sleeping ---
    let maybe_guard = item_a.try_load();

    // --- Verification ---
    assert!(
        maybe_guard.is_none(),
        "try_load should fail if item is being evicted (store running)"
    );
    drop(maybe_guard);

    // --- Cleanup (Allow background tasks to finish now) ---
    // This await will now take at least ~40ms more.
    wait_for_store(&setup.backing_store).await;
    assert_eq!(
        setup.calls.store.load(Ordering::SeqCst),
        1,
        "Store for A should have completed"
    );

    drop(item_a);
    wait_for_store(&setup.backing_store).await;
    assert!(
        setup.calls.delete.load(Ordering::SeqCst) >= 1,
        "Delete for A should have occurred"
    );
}

#[tokio::test]
async fn test_sync_try_load_mut_fails_while_store_is_running_for_eviction() {
    let sleep_duration = Duration::from_millis(50);
    println!(
        "NOTE: This test uses mock TestStore::store with sleep: {:?}",
        sleep_duration
    );
    // --- USE SETUP WITH SLEEP ---
    let setup = setup_with_store_sleep(1, sleep_duration); // Cache size 1, 50ms sleep
    // ----------------------------
    let data_a = TestData {
        id: 970,
        content: "Eviction Target Mut A Sleep".to_string(),
    };
    let data_b = TestData {
        id: 971,
        content: "Evictor Mut B Sleep".to_string(),
    };

    // Insert A - cached and unique
    let mut item_a = setup.pool.insert(data_a.clone());

    // Insert B - should trigger eviction process for A, which calls store(A).
    // The store(A) call will sleep for 50ms due to our modification.
    let _arc_b = setup.pool.insert(data_b.clone());

    // Give the background task a brief moment to potentially start, but less than the sleep duration.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // --- Action: Call sync try_load_mut() on A while store(A) is likely sleeping ---
    // Even though arc_a is unique, it should fail because it's "being evicted" / not resident.
    let maybe_guard = item_a.try_load_mut(); // THE CALL BEING TESTED

    // --- Verification ---
    assert!(
        maybe_guard.is_none(),
        "sync try_load_mut should fail if item is being evicted (store running)"
    );
    drop(maybe_guard);
    // Verify no side effects from failed try_load_mut
    assert_eq!(setup.calls.load.load(Ordering::SeqCst), 0);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0); // No delete call triggered

    // --- Cleanup (Allow background tasks to finish now) ---
    // This await will now take at least ~40ms more because store(A) was sleeping.
    wait_for_store(&setup.backing_store).await;
    assert_eq!(
        setup.calls.store.load(Ordering::SeqCst),
        1,
        "Store for A should have completed"
    );

    drop(item_a);
    wait_for_store(&setup.backing_store).await;
    // Delete should happen eventually for A as it was stored
    assert!(
        setup.calls.delete.load(Ordering::SeqCst) >= 1,
        "Delete for A should have occurred"
    );
}

#[tokio::test]
async fn test_item_dropped_during_eviction_write_triggers_delete() {
    let sleep_duration = Duration::from_millis(100); // Use a slightly longer sleep
    println!(
        "NOTE: This test uses mock TestStore::store with sleep: {:?}",
        sleep_duration
    );
    // --- USE SETUP WITH SLEEP ---
    let setup = setup_with_store_sleep(1, sleep_duration); // Cache size 1, 100ms sleep
    // ----------------------------
    let data_a = TestData {
        id: 980,
        content: "Drop During Store A".to_string(),
    };
    let data_b = TestData {
        id: 981,
        content: "Drop During Store B".to_string(),
    };

    // Insert A - cached and unique
    let item_a = setup.pool.insert(data_a.clone());
    let key_a = item_a.key();

    // Insert B - should trigger eviction process for A, which calls store(A).
    // The store(A) call will sleep for 100ms.
    let _item_b = setup.pool.insert(data_b.clone());

    // Give the background store task a moment to start sleeping.
    tokio::time::sleep(Duration::from_millis(20)).await; // Less than sleep_duration

    // Check that store hasn't finished yet and delete hasn't started
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 0); // Store is likely still sleeping
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 0);

    // --- Action: Drop the last (only) reference to A while store(A) is running ---
    drop(item_a);

    // --- Action: Wait for all background tasks to complete ---
    // This will wait for store(A) to finish its sleep and complete the write.
    // Then, because item_a was dropped, it should *immediately* trigger delete(A).
    // wait_for_store should cover both.
    wait_for_store(&setup.backing_store).await;

    // --- Verification ---
    // store: 1 (The write operation for A finished)
    // delete: 1 (The delete operation for A was triggered right after store completed)
    assert_eq!(setup.calls.store.load(Ordering::SeqCst), 1);
    assert_eq!(setup.calls.delete.load(Ordering::SeqCst), 1);

    // Verify the item is actually gone from the mock temp store
    assert!(
        !setup.store_impl.get_temp_keys().contains(&key_a),
        "Item A should have been deleted from temp store immediately after being stored"
    );

    // _item_b drops implicitly, potentially triggering store/delete for B depending on cache state
    // Just ensure the counts for A are correct.
}
