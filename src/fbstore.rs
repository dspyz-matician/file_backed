use std::fs::{self, File};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::Context;
use log::warn;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::backing_store::{BackingStoreT, Strategy};

pub struct FBStore<C> {
    codec: C,
    path: PreparedPath,
}

pub trait Codec<T>: Send + Sync + 'static {
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()>;
    fn decode(&self, file: &mut File) -> anyhow::Result<T>;
}

#[cfg(feature = "bincodec")]
pub struct BinCodec;

#[cfg(feature = "bincodec")]
impl<T: serde::Serialize + serde::de::DeserializeOwned> Codec<T> for BinCodec {
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()> {
        bincode::serde::encode_into_std_write(data, file, bincode::config::legacy())?;
        Ok(())
    }

    fn decode(&self, file: &mut File) -> anyhow::Result<T> {
        Ok(bincode::serde::decode_from_std_read(
            file,
            bincode::config::legacy(),
        )?)
    }
}

#[cfg(feature = "prostcodec")]
pub struct ProstCodec;

#[cfg(feature = "prostcodec")]
impl<T: Default + prost::Message> Codec<T> for ProstCodec {
    fn encode(&self, data: &T, file: &mut File) -> anyhow::Result<()> {
        use std::io::Write;

        let bytes = data.encode_to_vec();
        file.write_all(&bytes)?;
        Ok(())
    }

    fn decode(&self, file: &mut File) -> anyhow::Result<T> {
        use std::io::Read;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(T::decode(&*bytes)?)
    }
}

pub struct PreparedPath {
    path: PathBuf,
    ignored: Vec<&'static str>,
}

impl PreparedPath {
    pub async fn new(path: PathBuf, ignored: Vec<&'static str>) -> Self {
        for b in 0..=0xff {
            let dir = path.join(format!("{:02x}", b));
            tokio::fs::create_dir_all(&dir).await.unwrap_or_else(|err| {
                panic!("Failed to create directory {}: {:?}", dir.display(), err)
            });
        }
        Self { path, ignored }
    }

    pub fn blocking_new(path: PathBuf, ignored: Vec<&'static str>) -> Self {
        for b in 0..=0xff {
            let dir = path.join(format!("{:02x}", b));
            std::fs::create_dir_all(&dir).unwrap_or_else(|err| {
                panic!("Failed to create directory {}: {:?}", dir.display(), err)
            });
        }
        Self { path, ignored }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn ignored(&self) -> &Vec<&'static str> {
        &self.ignored
    }
}

impl Deref for PreparedPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl<C> FBStore<C> {
    pub fn new(codec: C, path: PreparedPath) -> Self {
        Self { codec, path }
    }
}

impl<C: Send + Sync + 'static> BackingStoreT for FBStore<C> {
    type PersistPath = PreparedPath;

    fn delete(&self, key: Uuid) {
        let file_path = key_path(&self.path, key);
        fs::remove_file(&file_path).unwrap_or_else(|err| {
            panic!("Failed to remove file {}: {:?}", file_path.display(), err)
        });
    }

    fn delete_persisted(&self, path: &Self::PersistPath, key: Uuid) {
        let file_path = key_path(path, key);
        fs::remove_file(&file_path).unwrap_or_else(|err| {
            panic!("Failed to remove file {}: {:?}", file_path.display(), err)
        });
    }

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

    fn all_persisted_keys(&self, path: &Self::PersistPath) -> impl IntoIterator<Item = Uuid> {
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

pub fn key_path(root: &Path, key: Uuid) -> PathBuf {
    let key_string = key.as_simple().to_string();
    let leading_chars = &key_string[0..2];
    root.join(leading_chars).join(key_string)
}
