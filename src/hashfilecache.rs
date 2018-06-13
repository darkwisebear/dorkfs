use std::fs;
use std::vec;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::error::Error;
use std::iter::FromIterator;

use tiny_keccak::Keccak;
use serde_json;

use cache::{self, CacheError, ReadonlyFile, DirectoryEntry, Directory, CacheLayer, CacheRef,
            CacheObject, CacheObjectMetadata, ObjectType};

#[derive(Debug)]
pub struct HashFile {
    file: fs::File
}

impl ReadonlyFile for HashFile {
    fn metadata(&self) -> cache::Result<fs::Metadata> {
        self.file.metadata().map_err(|err| CacheError::from(err))
    }
}

impl Read for HashFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for HashFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let io::SeekFrom::Start(p) = pos {
            self.file.seek(SeekFrom::Start(p + 1))
        } else {
            self.file.seek(pos)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HashDirectory {
    entries: Vec<DirectoryEntry>
}

impl IntoIterator for HashDirectory {
    type Item = DirectoryEntry;
    type IntoIter = vec::IntoIter<DirectoryEntry>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        self.entries.into_iter()
    }
}

impl Directory for HashDirectory {}

#[derive(Debug)]
pub struct HashFileCache {
    base_path: Arc<PathBuf>
}

impl CacheLayer for HashFileCache {
    type File = HashFile;
    type Directory = HashDirectory;

    fn get(&self, cache_ref: &CacheRef) -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        let mut file = self.open_object_file(cache_ref)?;
        let objtype = Self::identify_object_type(&mut file)?;

        let cache_object = match objtype {
            ObjectType::File => {
                let hash_file = HashFile {
                    file
                };

                CacheObject::File(hash_file)
            }

            ObjectType::Directory => {
                let hash_dir = serde_json::from_reader(file)?;
                CacheObject::Directory(hash_dir)
            }

            ObjectType::Commit => {
                let commit = serde_json::from_reader(file)?;
                CacheObject::Commit(commit)
            }
        };

        Ok(cache_object)
    }

    fn add(&self, object: CacheObject<Self::File, Self::Directory>) -> cache::Result<CacheRef> {
        let obj_type = ObjectType::from(&object);
        let (mut target_file, rel_path) = self.create_staging_file(obj_type)?;

        match object {
            CacheObject::File(mut hash_file) => {
                hash_file.file.seek(SeekFrom::Start(0))?;
                // TODO: Use CoW if possible
                let copied = io::copy(&mut hash_file.file, &mut target_file)?;
                debug!("Bytes copied to staging file: {}", copied);
            }

            CacheObject::Directory(hash_dir, ..) => {
                serde_json::to_writer(&mut target_file, &hash_dir)?;
            }

            CacheObject::Commit(commit) => {
                serde_json::to_writer(&mut target_file, &commit)?;
            }
        };

        target_file.seek(SeekFrom::Start(0))?;

        let cache_ref = Self::build_cache_ref(target_file)?;
        let rel_target_path = Self::rel_path_from_ref(&cache_ref);

        let parent_path = rel_target_path.parent()
            .ok_or(CacheError::ObjectNotFound(cache_ref))?.to_owned();
        self.ensure_path(parent_path)?;

        let source_path = self.base_path.join(rel_path);
        let target_path = self.base_path.join(rel_target_path);
        info!("Adding object {} to {}", cache_ref, target_path.display());
        if !target_path.exists() {
            fs::hard_link(&source_path, target_path)?;
        }

        debug!("Removing staging file {}", source_path.display());
        if let Err(err) = fs::remove_file(&source_path) {
            warn!("Unable to remove staging file {}: {}", source_path.display(), err.description());
        }

        Ok(cache_ref)
    }

    fn create_file(&self, source_file: fs::File) -> cache::Result<Self::File> {
        let hash_file = HashFile {
            file: source_file
        };

        Ok(hash_file)
    }

    fn create_directory<I: Iterator<Item=DirectoryEntry>>(&self, entries: I)
                                                          -> cache::Result<Self::Directory> {
        let dir = HashDirectory {
            entries: Vec::from_iter(entries)
        };

        Ok(dir)
    }

    fn metadata(&self, cache_ref: &CacheRef) -> cache::Result<CacheObjectMetadata> {
        let mut file = self.open_object_file(cache_ref)?;
        let size = file.metadata()?.len() - 1;
        let object_type = Self::identify_object_type(&mut file)?;

        let result = CacheObjectMetadata {
            size,
            object_type
        };

        Ok(result)
    }
}


impl HashFileCache {
    pub fn new<P: AsRef<Path>>(cache_dir: P) -> cache::Result<Self> {
        let cache = HashFileCache {
            base_path: Arc::new(cache_dir.as_ref().to_owned())
        };

        // Actually this could be omitted, but it will tell us early if the target location is
        // accessible
        cache.ensure_path("objects")?;
        cache.ensure_path("staging")?;

        Ok(cache)
    }

    fn ensure_path<P: AsRef<Path>>(&self, rel_path: P) -> cache::Result<()> {
        let full_path = self.base_path.join(rel_path);
        debug!("Ensuring path {}", full_path.display());
        if !full_path.exists() {
            debug!("Creating path {}", full_path.display());
            ::std::fs::create_dir_all(&full_path)?;
        }
        if !full_path.is_dir() {
            Err(CacheError::InitializationFailed)
        } else {
            Ok(())
        }
    }

    fn build_cache_ref<R: Read>(mut reader: R) -> cache::Result<CacheRef> {
        let mut hash_writer = HashWriter::new();
        io::copy(&mut reader, &mut hash_writer)?;
        Ok(CacheRef(hash_writer.finish()))
    }

    fn rel_path_from_ref(cache_ref: &CacheRef) -> PathBuf {
        let hexstr = cache_ref.to_string();
        Path::new("objects")
            .join(&hexstr[0..2])
            .join(&hexstr[2..4])
            .join(&hexstr[4..])
    }

    fn generate_staging_path() -> PathBuf {
        use rand::{weak_rng, Rng};

        let rand_name: String = weak_rng().gen_ascii_chars().take(8).collect();
        Path::new("staging").join(rand_name)
    }

    fn create_staging_file(&self, object_type: ObjectType) -> cache::Result<(fs::File, PathBuf)> {
        self.ensure_path("staging")?;
        let rel_path = HashFileCache::generate_staging_path();
        let full_path = self.base_path.join(&rel_path);

        info!("Creating staging file {}", full_path.display());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(full_path)?;

        file.write(object_type.as_identifier())?;

        Ok((file, rel_path))
    }

    fn open_object_file(&self, cache_ref: &CacheRef) -> cache::Result<fs::File> {
        let rel_path = Self::rel_path_from_ref(cache_ref);
        let path = self.base_path.join(&rel_path);

        if path.exists() {
            let file = fs::File::open(path)?;
            Ok(file)
        } else {
            Err(CacheError::ObjectNotFound(cache_ref.clone()))
        }
    }

    fn identify_object_type<R: Read>(object_file: &mut R) -> cache::Result<ObjectType> {
        let mut objtype_identifier = [0u8; 1];
        object_file.read_exact(&mut objtype_identifier)?;
        ObjectType::from_identifier(&objtype_identifier)
    }
}

struct HashWriter {
    hasher: Keccak
}

impl HashWriter {
    fn new() -> Self {
        HashWriter {
            hasher: Keccak::new_shake256()
        }
    }

    fn finish(self) -> [u8; 32] {
        let mut result = unsafe {
            ::std::mem::uninitialized::<[u8; 32]>()
        };
        self.hasher.finalize(&mut result);
        result
    }
}

impl Write for HashWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.hasher.update(buf);
        Ok(())
    }
}

