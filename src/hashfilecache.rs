use std::fs;
use std::vec;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fmt::Debug;
use std::error::Error;
use std::iter::FromIterator;
use std::borrow::Borrow;
use std::ffi::OsStr;

use rand::{prelude::*, distributions::Alphanumeric};
use serde_json;
use serde::Serialize;

use crate::cache::{Result, CacheError, ReadonlyFile, DirectoryEntry, Directory, CacheLayer,
                   CacheRef, CacheObject, ObjectType, CacheObjectMetadata, Commit};

#[derive(Debug, Serialize, Deserialize)]
struct FileMetadata {
    size: u64,
    objtype: char
}

impl From<CacheObjectMetadata> for FileMetadata {
    fn from(val: CacheObjectMetadata) -> Self {
        FileMetadata {
            size: val.size,
            objtype: CacheFileType::from(val.object_type).as_identifier().into()
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum CacheFileType {
    File,
    Directory,
    Commit,
    Symlink
}

impl CacheFileType {
    fn from_identifier(id: u8) -> Result<Self> {
        match id {
            b'F' => Ok(CacheFileType::File),
            b'D' => Ok(CacheFileType::Directory),
            b'C' => Ok(CacheFileType::Commit),
            b'L' => Ok(CacheFileType::Symlink),
            _ => Err(CacheError::UnknownObjectType(id))
        }
    }

    fn as_identifier(self) -> u8 {
        match self {
            CacheFileType::File => b'F',
            CacheFileType::Directory => b'D',
            CacheFileType::Commit => b'C',
            CacheFileType::Symlink => b'L'
        }
    }
}

impl From<ObjectType> for CacheFileType {
    fn from(val: ObjectType) -> Self {
        match val {
            ObjectType::File => CacheFileType::File,
            ObjectType::Directory => CacheFileType::Directory,
            ObjectType::Commit => CacheFileType::Commit,
            ObjectType::Symlink => CacheFileType::Symlink
        }
    }
}

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

impl FromIterator<DirectoryEntry> for HashDirectory {
    fn from_iter<I: IntoIterator<Item=DirectoryEntry>>(iter: I) -> Self {
        HashDirectory { entries: Vec::from_iter(iter) }
    }
}

impl Directory for HashDirectory {
    fn find_entry<S: Borrow<OsStr>>(&self, name: &S) -> Option<&DirectoryEntry> {
        self.entries.find_entry(name)
    }
}

impl IntoIterator for HashDirectory {
    type Item = DirectoryEntry;
    type IntoIter = vec::IntoIter<DirectoryEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LinkData {
    target: String
}

#[derive(Debug)]
pub struct HashFileCache<C: CacheLayer+Debug> {
    base_path: Arc<PathBuf>,
    cache: C
}

impl<C: CacheLayer+Debug> CacheLayer for HashFileCache<C> {
    type File = HashFile;
    type Directory = HashDirectory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        match self.open_object_file(cache_ref)
            .and_then(|f| self.load_cache_object(cache_ref, f)) {
            Ok(object) => Ok(object),

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
                        let hash_dir = HashDirectory::from_iter(dir.clone());
                        self.store_json(cache_ref, &hash_dir, CacheFileType::Directory)
                            .map(|_| CacheObject::Directory(hash_dir))
                    }

                    CacheObject::Commit(ref commit) =>
                        self.store_json(cache_ref, commit, CacheFileType::Commit)
                            .map(|_| CacheObject::Commit(commit.clone())),

                    CacheObject::Symlink(ref symlink) => {
                        let linkdata = LinkData {
                            target: symlink.clone()
                        };
                        self.store_json(cache_ref, &linkdata, CacheFileType::Symlink)
                            .map(|_| CacheObject::Symlink(symlink.clone()))
                    }
                }
            }

            Err(e) => Err(e)
        }
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef> {
        let cache_ref = self.cache.add_file_by_path(&source_path)?;
        let source_file = fs::File::open(source_path)?;
        self.store_file(&cache_ref, source_file)?;
        Ok(cache_ref)
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef> {
        let entries = Vec::from_iter(items);
        let cache_ref = self.cache.add_directory(&mut entries.iter().cloned())?;

        let hash_dir = HashDirectory::from_iter(entries);
        self.store_json(&cache_ref, &hash_dir, CacheFileType::Directory)?;
        Ok(cache_ref)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        let cache_ref = self.cache.add_commit(commit.clone())?;
        self.store_json(&cache_ref, &commit, CacheFileType::Commit)?;
        Ok(cache_ref)
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>> {
        self.cache.get_head_commit(branch)
    }

    fn merge_commit<S: AsRef<str>>(&mut self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef> {
        self.cache.merge_commit(branch, cache_ref)
    }

    fn create_branch<S: AsRef<str>>(&mut self, branch: S, cache_ref: &CacheRef) -> Result<()> {
        self.cache.create_branch(branch, cache_ref)
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

    #[cfg(test)]
    pub fn inner_mut(&mut self) -> &mut C {
        &mut self.cache
    }

    fn add_object_file(&self, rel_path: PathBuf, cache_ref: &CacheRef) -> Result<()> {
        let rel_target_path = Self::rel_path_from_ref(&cache_ref);
        let parent_path = rel_target_path.parent()
            .ok_or_else(|| CacheError::ObjectNotFound(*cache_ref))?.to_owned();
        self.ensure_path(parent_path)?;

        let source_path = self.base_path.join(rel_path);
        let target_path = self.base_path.join(rel_target_path);

        info!("Adding object {} to {}", cache_ref, target_path.display());
        if !target_path.exists() {
            fs::hard_link(&source_path, target_path)?;
        } else {
            warn!("Target path already exists, unable to create link!")
        }

        debug!("Removing staging file {}", source_path.display());
        if let Err(err) = fs::remove_file(&source_path) {
            warn!("Unable to remove staging file {}: {}", source_path.display(), err.description());
        }

        Ok(())
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
        let rand_name: String = SmallRng::from_entropy().sample_iter(&Alphanumeric).take(8).collect();
        Path::new("staging").join(rand_name)
    }

    fn invalidate(&self, cache_ref: &CacheRef) {
        if let Err(e) = fs::remove_file(self.generate_cache_file_path(cache_ref)) {
            if let io::ErrorKind::NotFound = e.kind() {
                info!("Invalidated object {} not in cache", cache_ref);
            } else {
                error!("Unable to invalidate object {}: {}", cache_ref, e);
            }
        }
    }

    fn create_staging_file(&self, object_type: CacheFileType) -> Result<(fs::File, PathBuf)> {
        self.ensure_path("staging")?;
        let rel_path = Self::generate_staging_path();
        let full_path = self.base_path.join(&rel_path);

        info!("Creating staging file {}", full_path.display());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(full_path)?;

        file.write_all(&[object_type.as_identifier()])?;

        Ok((file, rel_path))
    }

    fn generate_cache_file_path(&self, cache_ref: &CacheRef) -> PathBuf {
        let rel_path = Self::rel_path_from_ref(cache_ref);
        self.base_path.join(&rel_path)
    }

    fn open_object_file(&self, cache_ref: &CacheRef) -> Result<fs::File> {
        let path = self.generate_cache_file_path(cache_ref);

        if path.exists() {
            let file = fs::File::open(path)?;
            Ok(file)
        } else {
            Err(CacheError::ObjectNotFound(*cache_ref))
        }
    }

    fn identify_object_type<R: Read>(object_file: &mut R) -> Result<CacheFileType> {
        let mut objtype_identifier = [0u8; 1];
        object_file.read_exact(&mut objtype_identifier)?;
        CacheFileType::from_identifier(objtype_identifier[0])
    }

    fn load_cache_object(&self, cache_ref: &CacheRef, mut file: fs::File)
        -> Result<CacheObject<HashFile, HashDirectory>> {
        let objtype = Self::identify_object_type(&mut file)?;
        match objtype {
            CacheFileType::File => {
                let hash_file = HashFile {
                    file
                };

                Ok(CacheObject::File(hash_file))
            }

            CacheFileType::Directory => {
                let hash_dir = serde_json::from_reader(file)?;
                Ok(CacheObject::Directory(hash_dir))
            }

            CacheFileType::Commit => {
                // If the returned error indicates that data is missing we force the upper layer to
                // retrieve the object from upstream again.
                match serde_json::from_reader(file) {
                    Ok(commit) => Ok(CacheObject::Commit(commit)),
                    Err(e) => if e.is_data() {
                        self.invalidate(cache_ref);
                        Err(CacheError::ObjectNotFound(*cache_ref))
                    } else {
                        Err(e.into())
                    }
                }
            }

            CacheFileType::Symlink => {
                let linkdata: LinkData = serde_json::from_reader(file)?;
                Ok(CacheObject::Symlink(linkdata.target))
            }
        }
    }

    fn store_file<R: Read>(&self, cache_ref: &CacheRef, mut source: R) -> Result<fs::File> {
        let (mut target_file, rel_path) =
            self.create_staging_file(CacheFileType::File)?;
        // TODO: Use CoW if possible
        let copied = io::copy(&mut source, &mut target_file)?;
        debug!("Bytes copied to staging file: {}", copied);
        self.add_object_file(rel_path, &cache_ref)?;
        Ok(target_file)
    }

    fn store_json<T>(&self, cache_ref: &CacheRef, value: &T, obj_type: CacheFileType)
        -> Result<fs::File> where T: ?Sized+Serialize {
        let (mut target_file, rel_path) =
            self.create_staging_file(obj_type)?;
        serde_json::to_writer(&mut target_file, value)?;
        self.add_object_file(rel_path, &cache_ref)?;
        Ok(target_file)
    }
}
