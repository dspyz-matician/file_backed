use std::fs::{self, File};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::Context;
use log::warn;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::backing_store::{BackingStoreT, Strategy};

/// An implementation of [`BackingStoreT`] and [`Strategy`] that uses the
/// local filesystem for storage.
///
/// It stores each item (`T`) as a separate file, serialized using a provided [`Codec`].
/// Files are sharded into subdirectories based on the first two characters of the
/// item's UUID to avoid overly large directories.
pub struct FBStore<C> {
    /// The codec used for serializing/deserializing data.
    codec: C,
    /// The prepared root directory path for this store's temporary/cache files.
    path: PreparedPath,
}

/// Trait defining how to encode and decode a type `T` for storage.
///
/// Used by [`FBStore`] to handle serialization to/from files.
pub trait Codec<T>: Send + Sync + 'static {
    /// Encodes the `data` and writes it to the provided `file`.
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()>;

    /// Reads from the `file` and decodes it back into an instance of `T`.
    fn decode(&self, file: &mut File) -> anyhow::Result<T>;
}

/// A [`Codec`] implementation using the `bincode` crate.
///
/// Requires the `bincodec` feature flag.
/// The type `T` must implement `serde::Serialize` and `serde::DeserializeOwned`.
#[cfg(feature = "bincodec")]
pub struct BinCodec;

#[cfg(feature = "bincodec")]
impl<T: serde::Serialize + serde::de::DeserializeOwned> Codec<T> for BinCodec {
    /// Encodes `data` using `bincode` (legacy config) into the `file`.
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()> {
        bincode::serde::encode_into_std_write(data, file, bincode::config::legacy())?;
        Ok(())
    }

    /// Decodes data from `file` into `T` using `bincode` (legacy config).
    fn decode(&self, file: &mut File) -> anyhow::Result<T> {
        Ok(bincode::serde::decode_from_std_read(
            file,
            bincode::config::legacy(),
        )?)
    }
}

/// A [`Codec`] implementation using the `prost` crate for protobuf serialization.
///
/// Requires the `prostcodec` feature flag.
/// The type `T` must implement `prost::Message` and `Default`.
#[cfg(feature = "prostcodec")]
pub struct ProstCodec;

#[cfg(feature = "prostcodec")]
impl<T: Default + prost::Message> Codec<T> for ProstCodec {
    /// Encodes the prost `Message` `data` into its byte representation and writes it to `file`.
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()> {
        use std::io::Write;

        let bytes = data.encode_to_vec();
        file.write_all(&bytes)?;
        Ok(())
    }

    /// Reads all bytes from `file` and decodes them into a prost `Message` `T`.
    fn decode(&self, file: &mut File) -> anyhow::Result<T> {
        use std::io::Read;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(T::decode(&*bytes)?)
    }
}

/// Represents a directory path prepared for use with [`FBStore`].
///
/// This involves creating sharding subdirectories (`00` through `ff`)
/// and storing a list of filenames to ignore when scanning for keys.
/// It dereferences to [`std::path::Path`].
/// 
/// We expect to have exclusive access to this directory. If another
/// process interacts with it, or if the caller modifies files in the
/// directory directly (besides those which are explicitly ignored), all bets are off.
#[derive(Debug, Clone)] // Added Debug, Clone assuming they are useful
pub struct PreparedPath {
    /// The root path of the prepared directory.
    path: PathBuf,
    /// A list of filenames to ignore during operations like `all_persisted_keys`.
    ignored: Vec<&'static str>,
}

impl PreparedPath {
    /// Creates a new `PreparedPath` asynchronously.
    ///
    /// Creates the root directory `path` if it doesn't exist, and ensures
    /// all sharding subdirectories (`00`..=`ff`) exist within it.
    ///
    /// # Arguments
    /// * `path` - The root directory path for the store.
    /// * `ignored` - A list of static filenames to ignore within this directory.
    ///
    /// We expect to have exclusive access to this directory. If another
    /// process interacts with it, or if the caller modifies files in the
    /// directory directly (besides those which are explicitly ignored), all bets are off.
    /// 
    /// # Panics
    /// Panics if any required directory cannot be created.
    pub async fn new(path: PathBuf, ignored: Vec<&'static str>) -> Self {
        for b in 0..=0xff {
            let dir = path.join(format!("{:02x}", b));
            tokio::fs::create_dir_all(&dir).await.unwrap_or_else(|err| {
                panic!("Failed to create directory {}: {:?}", dir.display(), err)
            });
        }
        Self { path, ignored }
    }

    /// Creates a new `PreparedPath` synchronously (blocking).
    ///
    /// Creates the root directory `path` if it doesn't exist, and ensures
    /// all sharding subdirectories (`00`..=`ff`) exist within it.
    /// Use this variant when not in an async context.
    ///
    /// # Arguments
    /// * `path` - The root directory path for the store.
    /// * `ignored` - A list of static filenames to ignore within this directory.
    ///
    /// # Panics
    /// Panics if any required directory cannot be created.
    pub fn blocking_new(path: PathBuf, ignored: Vec<&'static str>) -> Self {
        for b in 0..=0xff {
            let dir = path.join(format!("{:02x}", b));
            std::fs::create_dir_all(&dir).unwrap_or_else(|err| {
                panic!("Failed to create directory {}: {:?}", dir.display(), err)
            });
        }
        Self { path, ignored }
    }

    /// Returns a reference to the root path [`PathBuf`].
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns a reference to the list of ignored filenames.
    pub fn ignored(&self) -> &Vec<&'static str> {
        &self.ignored
    }
}

impl Deref for PreparedPath {
    type Target = Path;

    /// Dereferences to `&Path` for easier path manipulation.
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl<C> FBStore<C> {
    /// Creates a new filesystem-backed store.
    ///
    /// # Arguments
    /// * `codec` - The [`Codec`] used to serialize/deserialize data `T`.
    /// * `path` - The [`PreparedPath`] representing the root directory for this store's files.
    pub fn new(codec: C, path: PreparedPath) -> Self {
        Self { codec, path }
    }
}

impl<C: Send + Sync + 'static> BackingStoreT for FBStore<C> {
    /// Specifies that [`PreparedPath`] is used to represent persistent locations.
    type PersistPath = PreparedPath;

    /// Deletes the file associated with `key` from this store's directory (`self.path`).
    ///
    /// # Panics
    /// Panics if the file cannot be removed (e.g., permissions, not found).
    fn delete(&self, key: Uuid) {
        let file_path = key_path(&self.path, key);
        fs::remove_file(&file_path).unwrap_or_else(|err| {
            panic!("Failed to remove file {}: {:?}", file_path.display(), err)
        });
    }

    /// Deletes the file associated with `key` from the specified persistent `path`.
    ///
    /// # Panics
    /// Panics if the file cannot be removed from the *target* path.
    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) {
        let file_path = key_path(path, key);
        fs::remove_file(&file_path).unwrap_or_else(|err| {
            panic!("Failed to remove file {}: {:?}", file_path.display(), err)
        });
    }

    /// Registers an item by hard-linking its file from `src_path` into this store's directory.
    ///
    /// This makes an existing file (presumably in a persistent location `src_path`)
    /// known to this store instance without copying data.
    ///
    /// # Panics
    /// Panics if the hard link cannot be created (e.g., different filesystems,
    /// permissions, source not found, destination exists).
    fn register(&self, src_path: &PreparedPath, key: Uuid) {
        let src_path = key_path(src_path, key);
        let dest_path = key_path(&self.path, key);
        fs::hard_link(&src_path, &dest_path).unwrap_or_else(|err| {
            panic!(
                "Failed to create hard link from {} to {}: {:?}",
                src_path.display(),
                dest_path.display(),
                err
            )
        });
    }

    /// Persists an item managed by this store by hard-linking its file to `dest_path`.
    ///
    /// Creates a hard link from the file within this store's directory (`self.path`)
    /// to the target persistent directory (`dest_path`).
    ///
    /// # Panics
    /// Panics if the hard link cannot be created.
    fn persist(&self, dest_path: &PreparedPath, key: Uuid) {
        let src_path = key_path(&self.path, key);
        let dest_path = key_path(dest_path, key);
        fs::hard_link(&src_path, &dest_path).unwrap_or_else(|err| {
            panic!(
                "Failed to create hard link from {} to {}: {:?}",
                src_path.display(),
                dest_path.display(),
                err
            )
        });
    }

    /// Scans the `path` directory for files whose names are valid UUIDs and returns them.
    ///
    /// It recursively walks the directory `path`, ignoring subdirectories and any filenames
    /// present in `path.ignored`. It attempts to parse filenames as simple UUID strings.
    ///
    /// **Warning:** Files with non-UTF8 names or names that fail to parse as UUIDs
    /// will be logged as warnings and **deleted** from the filesystem during the scan.
    fn sanitize_path(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
        WalkDir::new(&**path).into_iter().filter_map(|entry| {
            let entry = entry.unwrap_or_else(|err| {
                panic!("Failed to read directory {}: {:?}", path.display(), err)
            });
            if entry.file_type().is_dir() {
                return None;
            }
            if let Some(file_name) = entry.file_name().to_str() {
                if path.ignored.contains(&file_name) {
                    return None;
                }
            }
            let file_name_os = entry.file_name();
            let Some(file_name_str) = file_name_os.to_str() else {
                let path = entry.path();
                warn!(
                    "Failed to convert file name to string at {}, deleting file: {:?}",
                    path.display(),
                    file_name_os
                );
                remove_file(path);
                return None;
            };
            match Uuid::parse_str(file_name_str) {
                Ok(key) => Some(key),
                Err(err) => {
                    let path = entry.path();
                    warn!(
                        "Failed to parse UUID from file name {}, deleting file: {:?}",
                        path.display(),
                        err
                    );
                    remove_file(path);
                    None
                }
            }
        })
    }

    /// Attempts to synchronize the filesystem containing the directory `path`.
    ///
    /// Ensures that previous writes within this directory are flushed to the
    /// underlying storage for durability.
    ///
    /// **Platform Specific:** Currently only implemented for Linux using `nix::unistd::syncfs`.
    /// On other platforms, this will be a no-op.
    ///
    /// # Panics
    /// Panics if the directory `path` cannot be opened.
    /// On Linux, panics if `syncfs` fails.
    fn sync_persisted(&self, path: &Self::PersistPath) {
        let file = File::open(&**path)
            .unwrap_or_else(|err| panic!("Failed to open dir {}: {:?}", path.display(), err));
        #[cfg(target_os = "linux")]
        nix::unistd::syncfs(std::os::fd::AsRawFd::as_raw_fd(&file)).unwrap_or_else(|err| {
            panic!(
                "Failed to sync file system of dir {}: {:?}",
                path.display(),
                err
            )
        });
    }
}

fn remove_file(path: &Path) {
    std::fs::remove_file(path)
        .unwrap_or_else(|err| panic!("Failed to remove file {}: {:?}", path.display(), err));
}

impl<T, C: Codec<T>> Strategy<T> for FBStore<C> {
    /// Stores `data` by serializing it with the [`Codec`] into a new file named after `key`.
    ///
    /// The file is created within the store's prepared path (`self.path`).
    /// Uses `OpenOptions::create_new` to avoid overwriting existing files.
    ///
    /// # Panics
    /// Panics if the file cannot be created (e.g., destination exists, permissions)
    /// or if encoding fails.
    fn store(&self, key: Uuid, data: &T) {
        let file_path = key_path(&self.path, key);
        let mut create_options = fs::OpenOptions::new();
        create_options.create_new(true).write(true);
        let mut file = create_options.open(&file_path).unwrap_or_else(|err| {
            panic!("Failed to create file {}: {:?}", file_path.display(), err)
        });
        self.codec
            .encode(data, &mut file)
            .with_context(|| format!("Failed to write data to {}", file_path.display(),))
            .unwrap()
    }

    /// Loads data `T` by reading the file associated with `key` and decoding it.
    ///
    /// Reads the file from the store's prepared path (`self.path`).
    ///
    /// # Panics
    /// Panics if the file cannot be opened (e.g., not found, permissions)
    /// or if decoding fails.
    fn load(&self, key: Uuid) -> T {
        let file_path = key_path(&self.path, key);
        let mut file = File::open(&file_path)
            .unwrap_or_else(|err| panic!("Failed to open file {}: {:?}", file_path.display(), err));
        self.codec
            .decode(&mut file)
            .with_context(|| format!("Failed to read data from {}", file_path.display(),))
            .unwrap()
    }
}

/// Calculates the sharded file path for a given `key` within a `root` directory.
///
/// The path is constructed as `root/<XX>/<UUID_SIMPLE>`, where `<XX>` is the
/// first two hex characters of the UUID's simple string representation.
/// Note this means the first two characters will appear twice in the full path
/// (once in the directory and once in the filename).
///
/// # Example
/// ```
/// # use uuid::Uuid;
/// # use std::path::{Path, PathBuf};
/// # use file_backed::fbstore::key_path;
/// let root = Path::new("/tmp/store");
/// let key = Uuid::parse_str("1c3a3c1a-a4a4-4a1a-8a8a-0a7a7a7a7a7a").unwrap();
/// let expected_path = PathBuf::from("/tmp/store/1c/1c3a3c1aa4a44a1a8a8a0a7a7a7a7a7a");
/// assert_eq!(key_path(root, key), expected_path);
/// ```
pub fn key_path(root: &Path, key: Uuid) -> PathBuf {
    let key_string = key.as_simple().to_string(); // Using simple format (no hyphens)
    let leading_chars = &key_string[0..2];
    root.join(leading_chars).join(key_string)
}
