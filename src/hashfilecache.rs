use std::fs;
use std::vec;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fmt::Debug;
use std::error::Error;
use std::iter::FromIterator;

use serde_json;
use serde::Serialize;

use cache::{Result, CacheError, ReadonlyFile, DirectoryEntry, Directory, CacheLayer, CacheRef,
            CacheObject, CacheObjectMetadata, ObjectType, Commit};

#[derive(Debug)]
pub struct HashFile {
    file: fs::File
}

impl ReadonlyFile for HashFile {}

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
pub struct HashFileCache<C: CacheLayer+Debug> {
    base_path: Arc<PathBuf>,
    cache: C
}

impl<C: CacheLayer+Debug> CacheLayer for HashFileCache<C> {
    type File = HashFile;
    type Directory = HashDirectory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        match self.open_object_file(cache_ref) {
            Ok(file) => Self::load_cache_object(file),

            Err(CacheError::ObjectNotFound(_)) => {
                let mut upstream_object =
                    self.cache.get(cache_ref)?;
                match upstream_object {
                    CacheObject::File(ref mut file) => {
                        self.store_file(cache_ref, file)
                            .and_then(|mut file|
                                file.seek(SeekFrom::Start(1))
                                    .map_err(Into::into)
                                    .map(|_| CacheObject::File(HashFile { file })))
                    }

                    CacheObject::Directory(ref dir) => {
                        let hash_dir = HashDirectory {
                            entries: Vec::from_iter(dir.clone().into_iter())
                        };
                        self.store_json(cache_ref, &hash_dir, ObjectType::Directory)
                            .map(|_| CacheObject::Directory(hash_dir))
                    }

                    CacheObject::Commit(ref commit) =>
                        self.store_json(cache_ref, commit, ObjectType::Commit)
                            .map(|_| CacheObject::Commit(commit.clone()))
                }
            }

            Err(e) => Err(e)
        }
    }

    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata> {
        let mut file = self.open_object_file(cache_ref)?;
        let size = file.metadata()?.len() - 1;
        let object_type = Self::identify_object_type(&mut file)?;

        let result = CacheObjectMetadata {
            size,
            object_type
        };

        Ok(result)
    }

    fn add_file_by_path(&self, source_path: &Path) -> Result<CacheRef> {
        let cache_ref = self.cache.add_file_by_path(&source_path)?;
        let mut source_file = fs::File::open(source_path)?;
        self.store_file(&cache_ref, source_file)?;
        Ok(cache_ref)
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> Result<CacheRef> {
        let entries = Vec::from_iter(items);
        let cache_ref = self.cache.add_directory(&mut entries.iter().cloned())?;

        let hash_dir = HashDirectory { entries };
        self.store_json(&cache_ref, &hash_dir, ObjectType::Directory)?;
        Ok(cache_ref)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        let cache_ref = self.cache.add_commit(commit.clone())?;
        self.store_json(&cache_ref, &commit, ObjectType::Commit)?;
        Ok(cache_ref)
    }

    fn get_head_commit(&self) -> Result<Option<CacheRef>> {
        self.cache.get_head_commit()
    }
}

impl<C: CacheLayer+Debug> HashFileCache<C> {
    pub fn new<P: AsRef<Path>>(cache: C, cache_dir: P) -> Result<Self> {
        let cache = HashFileCache {
            base_path: Arc::new(cache_dir.as_ref().to_owned()),
            cache
        };

        // Actually this could be omitted, but it will tell us early if the target location is
        // accessible
        cache.ensure_path("objects")?;
        cache.ensure_path("staging")?;

        Ok(cache)
    }

    fn add_object_file(&self, rel_path: PathBuf, cache_ref: &CacheRef) -> Result<()> {
        let rel_target_path = Self::rel_path_from_ref(&cache_ref);
        let parent_path = rel_target_path.parent()
            .ok_or(CacheError::ObjectNotFound(*cache_ref))?.to_owned();
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

        Ok(())
    }

    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata> {
        let mut file = self.open_object_file(cache_ref)?;
        let size = file.metadata()?.len() - 1;
        let object_type = Self::identify_object_type(&mut file)?;

        let result = CacheObjectMetadata {
            size,
            object_type
        };

        Ok(result)
    }

    fn ensure_path<P: AsRef<Path>>(&self, rel_path: P) -> Result<()> {
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

    fn create_staging_file(&self, object_type: ObjectType) -> Result<(fs::File, PathBuf)> {
        self.ensure_path("staging")?;
        let rel_path = Self::generate_staging_path();
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

    fn open_object_file(&self, cache_ref: &CacheRef) -> Result<fs::File> {
        let rel_path = Self::rel_path_from_ref(cache_ref);
        let path = self.base_path.join(&rel_path);

        if path.exists() {
            let file = fs::File::open(path)?;
            Ok(file)
        } else {
            Err(CacheError::ObjectNotFound(cache_ref.clone()))
        }
    }

    fn identify_object_type<R: Read>(object_file: &mut R) -> Result<ObjectType> {
        let mut objtype_identifier = [0u8; 1];
        object_file.read_exact(&mut objtype_identifier)?;
        ObjectType::from_identifier(&objtype_identifier)
    }

    fn load_cache_object(mut file: fs::File) -> Result<CacheObject<HashFile, HashDirectory>> {
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

    fn store_file<R: Read>(&self, cache_ref: &CacheRef, mut source: R) -> Result<fs::File> {
        let (mut target_file, rel_path) =
            self.create_staging_file(ObjectType::File)?;
        // TODO: Use CoW if possible
        let copied = io::copy(&mut source, &mut target_file)?;
        debug!("Bytes copied to staging file: {}", copied);
        self.add_object_file(rel_path, &cache_ref)?;
        Ok(target_file)
    }

    fn store_json<T>(&self, cache_ref: &CacheRef, value: &T, obj_type: ObjectType)
        -> Result<fs::File> where T: ?Sized+Serialize {
        let (mut target_file, rel_path) =
            self.create_staging_file(obj_type)?;
        serde_json::to_writer(&mut target_file, value)?;
        self.add_object_file(rel_path, &cache_ref)?;
        Ok(target_file)
    }
}
