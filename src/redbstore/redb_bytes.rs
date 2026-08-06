use std::io;
use std::sync::RwLock;

use anyhow::Context;
use redb::{Database, ReadableDatabase, StorageBackend};
use uuid::Uuid;

use super::BLOBS;

/// Read-only access to a redb blob store held entirely in memory — e.g. the
/// [`REDB_FILE_NAME`](super::REDB_FILE_NAME) entry of an archive.
///
/// The database must have been durably committed (via
/// [`BackingStoreT::sync_persisted`](crate::backing_store::BackingStoreT::sync_persisted) or a
/// clean close) before its bytes were captured: non-durable commits are invisible to a fresh
/// open. Any writes redb performs on open (e.g. repair bookkeeping) stay in memory.
pub struct RedbBytes(Database);

impl RedbBytes {
    /// Opens a database from its file contents.
    pub fn parse(bytes: Vec<u8>) -> anyhow::Result<Self> {
        let db = Database::builder()
            .create_with_backend(BytesBackend(RwLock::new(bytes)))
            .context("Failed to open in-memory redb")?;
        Ok(Self(db))
    }

    /// Reads the blob stored under `key`, erroring if it is absent.
    pub fn blob(&self, key: Uuid) -> anyhow::Result<Vec<u8>> {
        self.with_blob(key, <[u8]>::to_vec)
    }

    /// Passes the blob stored under `key` to `use_bytes` without copying it out of the
    /// database, erroring if it is absent.
    pub fn with_blob<R>(&self, key: Uuid, use_bytes: impl FnOnce(&[u8]) -> R) -> anyhow::Result<R> {
        let table = self.0.begin_read()?.open_table(BLOBS)?;
        let value = table
            .get(key.as_bytes())?
            .with_context(|| format!("Missing key {key} in redb bytes"))?;
        Ok(use_bytes(value.value()))
    }

    /// All keys in the store, in key order.
    pub fn keys(&self) -> anyhow::Result<Vec<Uuid>> {
        use redb::ReadableTable;
        let table = self.0.begin_read()?.open_table(BLOBS)?;
        table
            .iter()?
            .map(|entry| {
                let (key, _) = entry?;
                Ok(Uuid::from_bytes(*key.value()))
            })
            .collect()
    }
}

/// [`redb::backends::InMemoryBackend`], except seeded with the bytes of an existing database.
#[derive(Debug)]
struct BytesBackend(RwLock<Vec<u8>>);

fn out_of_range() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "Index out-of-range")
}

impl StorageBackend for BytesBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.0.read().unwrap().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let data = self.0.read().unwrap();
        let offset = usize::try_from(offset).map_err(|_| out_of_range())?;
        let end = offset.checked_add(out.len()).ok_or_else(out_of_range)?;
        if end > data.len() {
            return Err(out_of_range());
        }
        out.copy_from_slice(&data[offset..end]);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let len = usize::try_from(len).map_err(|_| out_of_range())?;
        self.0.write().unwrap().resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut guard = self.0.write().unwrap();
        let offset = usize::try_from(offset).map_err(|_| out_of_range())?;
        let end = offset.checked_add(data.len()).ok_or_else(out_of_range)?;
        if end > guard.len() {
            return Err(out_of_range());
        }
        guard[offset..end].copy_from_slice(data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::RedbBytes;
    use crate::redbstore::{REDB_FILE_NAME, RedbPath, insert_bytes};

    #[test]
    fn reads_blobs_from_cleanly_closed_db_bytes() {
        let dir = tempdir().unwrap();
        let key = Uuid::new_v4();
        {
            let path = RedbPath::in_dir(dir.path());
            insert_bytes(&path, key, b"payload", "persisted");
        } // dropping the last handle durably commits

        let bytes = std::fs::read(dir.path().join(REDB_FILE_NAME)).unwrap();
        let db = RedbBytes::parse(bytes).unwrap();
        assert_eq!(db.blob(key).unwrap(), b"payload");
        assert!(db.blob(Uuid::new_v4()).is_err());
    }
}
