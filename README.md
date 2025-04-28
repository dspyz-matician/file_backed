# file_backed
Provides types for managing collections of large objects, using an in-memory LRU cache backed by persistent storage (typically the filesystem).

## Overview

This crate provides `FBPool` and `FBArc` (File-Backed Arc) to manage data (`T`) that might be too large or numerous to keep entirely in memory. It uses a strategy similar to a swap partition:

1.  **In-Memory Cache:** An LRU cache (`FBPool`) keeps frequently/recently used items readily available in memory.
2.  **Backing Store:** When items are evicted from the cache, or when explicitly requested, they are serialized and written to a temporary location in a backing store (like disk).
3.  **Lazy Loading:** When an item not in the cache is accessed via its `FBArc` handle (`.load()`, `.load_async()`, etc.), it's automatically read back from the backing store.
4.  **Reference Counting:** `FBArc` acts like `std::sync::Arc`, tracking references.
5.  **Automatic Cleanup:** When the last `FBArc` for an item is dropped, its corresponding data in the *temporary* backing store is automatically deleted via a background task.
6.  **Persistence:** Items can be explicitly "persisted" (e.g., hard-linked) to a separate, permanent location and later "registered" back into a pool, allowing data to survive application restarts.

## Core Components

* **`FBPool<T, B>`:** Manages the collection, including the LRU cache and interaction with the backing store.
* **`FBArc<T, B>`:** A smart pointer (like `Arc`) to an item managed by the pool. Access requires calling a `.load()` variant.
* **`BackingStoreT` Trait:** Defines the low-level storage operations (delete, persist, register, etc.). You typically implement this or use a provided one (like `FBStore`). This handles *where* and *how* raw data blobs (identified by `Uuid`) are physically stored.
* **`Strategy<T>` Trait:** Extends `BackingStoreT`. Defines *how* your specific data type `T` is serialized (`store`) and deserialized (`load`).
* **`BackingStore<B>` Struct:** A wrapper around a `BackingStoreT` implementation that manages concurrency, background tasks (using Tokio), and tracking of persistent paths. `FBPool` uses this internally.
* **`ReadGuard`/`WriteGuard`:** RAII guards returned by `load` methods, providing access to the data and ensuring it stays loaded. `WriteGuard` marks data as dirty for writing back to the store.

## Features

* **`bincodec`**: Provides `fbstore::BinCodec`, an implementation of `fbstore::Codec<T>` using `bincode` for serialization (requires `T: Serialize + DeserializeOwned`).
* **`prostcodec`**: Provides `fbstore::ProstCodec`, an implementation of `fbstore::Codec<T>` using `prost` for serialization (requires `T: prost::Message + Default`).
* **`dupe`**: Implements `dupe::Dupe` for `FBArc`.

## Basic Usage

This example shows basic insertion, loading (which may come from cache or disk), and automatic cleanup.

```rust
// examples/simple_usage.rs

use std::sync::Arc;

use tempfile::tempdir;
use tokio::runtime::Handle;

use file_backed::fbstore::{BinCodec, FBStore, PreparedPath};
use file_backed::{BackingStore, FBPool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Setup store and pool
    let temp_dir = tempdir()?;
    let prepared_path = PreparedPath::new(temp_dir.path().to_path_buf(), vec![]).await;
    let fb_store = FBStore::new(BinCodec, prepared_path); // Uses BinCodec for String
    let store = Arc::new(BackingStore::new(fb_store, Handle::current()));
    let pool: Arc<FBPool<String, _>> = Arc::new(FBPool::new(store.clone(), 2)); // Cache size 2

    // 2. Insert items
    let mut items = Vec::new();
    items.push(pool.insert("Hello".to_string()));
    items.push(pool.insert("World".to_string()));
    items.push(pool.insert("!".to_string())); // "Hello" starts being evicted now

    // 3. Load an item (might load from disk if evicted)
    let guard = items[0].load().await; // Load "Hello". Now "World" will be evicted.
    println!("Loaded: {}", *guard);
    assert_eq!(*guard, "Hello");
    drop(guard);

    // 4. Arcs automatically cleaned up when dropped
    drop(items);
    store.finished().await; // Wait for background tasks to finish (e.g., file deletions)

    Ok(())
}
```

## Persistence

Items can be persisted to survive application restarts using `persist` and brought back using `register`. This typically uses hard links in file-based stores.

```rust
// examples/persistence.rs

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
```

## Provided Filesystem Backing (`FBStore`)

`FBStore` is a filesystem-based implementation of `BackingStoreT`.

* It requires a `Codec<T>` (`BinCodec` or `ProstCodec` via features, or your own implementation) to handle serialization.
* It uses a `PreparedPath`, which manages a directory structure (subdirectories `00` to `ff`) for sharding files based on their `Uuid`.
* `persist` and `register` are implemented using `std::fs::hard_link`.

## Concurrency

* The `BackingStore` uses a Tokio runtime (`tokio::runtime::Handle`) provided during initialization to manage background tasks (like deletions, cache write-backs) and async operations.
* Methods often come in pairs:
    * `some_operation_async()` / `spawn_some_operation()`: Return a `Future` or `JoinHandle`, suitable for async contexts.
    * `blocking_some_operation()`: Perform the operation blockingly. Must not be called from an async context unless using `spawn_blocking` or `block_in_place`.
* `FBArc::load()`: Special case. If data isn't in memory, it uses `tokio::task::block_in_place` to call the blocking load logic. **Warning:** This will panic if called within a `tokio::runtime::Runtime` created using `Runtime::new_current_thread`. Use `load_async` in async code.
* The library aims to be thread-safe and select/cancel-safe; concurrent access via different `FBArc` handles (even for the same data) or pool operations from multiple threads should be handled correctly via internal synchronization.

## Running Examples

```
cargo run --example simple_usage --features=bincodec
cargo run --example persistence --features=bincodec
```
