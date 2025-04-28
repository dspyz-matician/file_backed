//! Minimal example demonstrating persisting and registering items.

use std::sync::Arc;

use tempfile::tempdir;
use tokio::runtime::Handle;

use file_backed::fbstore::{BinCodec, FBStore, PreparedPath};
use file_backed::{BackingStore, FBPool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        let prepared_cache_path = PreparedPath::new(cache_store_path.clone(), vec![]).await;
        let prepared_persist_path = PreparedPath::new(persist_store_path.clone(), vec![]).await;

        let fb_store = FBStore::new(BinCodec, prepared_cache_path);
        let store = Arc::new(BackingStore::new(fb_store, Handle::current()));
        let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));

        // Track persistent path & insert
        let tracked_persist = Arc::new(store.track_path(prepared_persist_path).await?);
        let item1 = pool.insert("Persisted Data".to_string());
        persisted_key = item1.key(); // Remember the key

        // Persist (e.g., hard-links to persist_dir)
        item1.spawn_persist(tracked_persist.clone()).await?;
        // Optional: store.sync(tracked_persist).await?; // For durability

        println!("Persisted item with key: {}", persisted_key);
        // item1, pool, store dropped here; cache file might be deleted later
        store.finished().await;
    }

    // --- Scope 2: Simulate Restart, Register, and Load ---
    {
        // Re-create store/pool pointing to the same paths
        let prepared_cache_path = PreparedPath::new(cache_store_path, vec![]).await;
        let prepared_persist_path = PreparedPath::new(persist_store_path, vec![]).await; // Must exist

        let fb_store = FBStore::new(BinCodec, prepared_cache_path);
        let store = Arc::new(BackingStore::new(fb_store, Handle::current()));
        let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 1));

        // Re-track persistent path
        let tracked_persist = Arc::new(store.track_path(prepared_persist_path).await?);

        // Register the previously persisted item by its key
        let registered_item = pool
            .register(&tracked_persist, persisted_key)
            .await
            .expect("Failed to register item");

        println!("Registered item with key: {}", persisted_key);

        // Load the registered item (loads from persist_dir link into cache_dir)
        let guard = registered_item.load();
        println!("Loaded registered data: {}", *guard);
        assert_eq!(*guard, "Persisted Data");
        drop(guard);

        // registered_item, pool, store dropped here
        store.finished().await;
    }

    Ok(())
}
