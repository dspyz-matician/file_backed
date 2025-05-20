//! Minimal example demonstrating persisting and registering items.

use std::sync::Arc;

use tempfile::tempdir;
use tokio::runtime;

use file_backed::fbstore::{BinCodec, FBStore, PreparedPath};
use file_backed::{BackingStore, FBPool};

fn main() -> anyhow::Result<()> {
    let runtime = runtime::Builder::new_multi_thread().build()?;

    // 1. Setup distinct paths for cache and persistent storage
    let cache_dir = tempdir()?;
    let persist_dir = tempdir()?;
    let cache_store_path = cache_dir.path().to_path_buf();
    let persist_store_path = persist_dir.path().to_path_buf();
    println!("Cache path: {}", cache_store_path.display());
    println!("Persist path: {}", persist_store_path.display());

    let persisted_key;

    // --- Scope 1: Insert and Persist ---
    {
        let prepared_cache_path = PreparedPath::blocking_new(cache_store_path.clone(), vec![]);
        let prepared_persist_path = PreparedPath::blocking_new(persist_store_path.clone(), vec![]);

        let fb_store = FBStore::new(BinCodec, prepared_cache_path);
        let store = Arc::new(BackingStore::new(fb_store, runtime.handle().clone()));
        let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));

        // Track persistent path & insert
        let tracked_persist = Arc::new(store.blocking_track_path(prepared_persist_path));
        let item1 = pool.insert("Persisted Data".to_string());
        persisted_key = item1.key(); // Remember the key

        // Persist (e.g., hard-links to persist_dir)
        item1.blocking_persist(&tracked_persist);
        // Optional: store.sync(tracked_persist).await?; // For durability

        println!("Persisted item with key: {}", persisted_key);

        runtime.block_on(store.finished());
    }

    // --- Scope 2: Simulate Restart, Register, and Load ---
    {
        // Re-create store/pool pointing to the same paths
        let prepared_cache_path = PreparedPath::blocking_new(cache_store_path, vec![]);
        let prepared_persist_path = PreparedPath::blocking_new(persist_store_path, vec![]); // Must exist

        let fb_store = FBStore::new(BinCodec, prepared_cache_path);
        let store = Arc::new(BackingStore::new(fb_store, runtime.handle().clone()));
        let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));

        // Re-track persistent path
        let tracked_persist = Arc::new(store.blocking_track_path(prepared_persist_path));

        // Register the previously persisted item by its key
        let registered_item = pool
            .blocking_register(&tracked_persist, persisted_key)
            .expect("Failed to register item");

        println!("Registered item with key: {}", persisted_key);

        // Load the registered item (loads from persist_dir link into cache_dir)
        let guard = registered_item.blocking_load();
        println!("Loaded registered data: {}", *guard);
        assert_eq!(*guard, "Persisted Data");
        drop(guard);

        runtime.block_on(store.finished());
    }

    Ok(())
}
