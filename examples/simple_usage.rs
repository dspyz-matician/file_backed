//! Minimal example demonstrating FBPool usage.

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
    let guard = items[0].load(); // Load "Hello". Now "World" will be evicted.
    println!("Loaded: {}", *guard);
    assert_eq!(*guard, "Hello");
    drop(guard);

    // 4. Arcs automatically cleaned up when dropped
    drop(items);
    store.finished().await; // Wait for background tasks to finish (e.g., file deletions)

    Ok(())
}
