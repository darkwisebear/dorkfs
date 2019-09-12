use std::fs::{self, File};
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use std::io::{self, Read, Write, Seek, SeekFrom, BufReader};
use std::fmt::{self, Formatter, Debug};
use std::error::Error;
use std::iter::FromIterator;

use rand::{prelude::*, distributions::Alphanumeric};
use serde_json;
use lru::LruCache;
use futures::{future, prelude::*};
use serde::{Serialize, Deserialize};

use crate::{
    cache::{
        Result, CacheError, ReadonlyFile, DirectoryEntry, DirectoryImpl, CacheLayer,
        CacheRef, CacheObject, ObjectType, CacheObjectMetadata, Commit
    }
};

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

pub type HashDirectory = DirectoryImpl;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LinkData {
    target: String
}

#[derive(Clone, Debug)]
struct FileCachePath {
    base_path: Arc<PathBuf>
}

impl FileCachePath {
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

    fn invalidate(&self, cache_ref: &CacheRef) {
        if let Err(e) = fs::remove_file(self.generate_cache_file_path(cache_ref)) {
            if let io::ErrorKind::NotFound = e.kind() {
                info!("Invalidated object {} not in cache", cache_ref);
            } else {
                error!("Unable to invalidate object {}: {}", cache_ref, e);
            }
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
}

#[derive(Clone, Debug)]
enum LruCacheObject {
    Commit(Commit),
    Symlink(String),
    Directory(HashDirectory)
}

impl<F: ReadonlyFile> Into<CacheObject<F, HashDirectory>> for LruCacheObject {
    fn into(self) -> CacheObject<F, HashDirectory> {
        match self {
            LruCacheObject::Commit(commit) => CacheObject::Commit(commit),
            LruCacheObject::Symlink(symlink) => CacheObject::Symlink(symlink),
            LruCacheObject::Directory(dir) => CacheObject::Directory(dir)
        }
    }
}

struct ObjectCache {
    cache: LruCache<CacheRef, LruCacheObject>
}

impl Debug for ObjectCache {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_list().entries(self.cache.iter()).finish()
    }
}

impl ObjectCache {
    fn new(cache_size: usize) -> Self {
        Self {
            cache: LruCache::new(cache_size)
        }
    }

    fn put(&mut self, cache_ref: &CacheRef, obj: &CacheObject<HashFile, HashDirectory>) {
        match obj {
            CacheObject::Directory(ref dir) => {
                   self.cache.put(*cache_ref, LruCacheObject::Directory(dir.clone()));
            }
            CacheObject::Commit(ref commit) => {
                    self.cache.put(*cache_ref, LruCacheObject::Commit(commit.clone()));
            }
            CacheObject::Symlink(ref symlink) => {
                    self.cache.put(*cache_ref, LruCacheObject::Symlink(symlink.clone()));
            }
            CacheObject::File(..) => (),
        }
    }

    fn get(&mut self, cache_ref: &CacheRef) -> Option<CacheObject<HashFile, HashDirectory>> {
        self.cache.get(cache_ref).map(|obj| obj.clone().into())
    }
}

pub struct HashFileCache<C> where C: CacheLayer {
    cache_path: FileCachePath,
    obj_cache: Arc<Mutex<ObjectCache>>,
    cache: C
}

impl<C: CacheLayer+Debug> Debug for HashFileCache<C> {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.debug_struct("HashFileCache")
            .field("cache_path", &self.cache_path)
            .field("obj_cache", &self.obj_cache)
            .field("cache", &self.cache)
            .finish()
    }
}

impl<C> CacheLayer for HashFileCache<C>  where C: CacheLayer,
                                               C::GetFuture: Send+'static,
                                               <C::GetFuture as IntoFuture>::Future: Send+'static,
                                               C::File: Send,
                                               C::Directory: Send {
    type File = HashFile;
    type Directory = HashDirectory;
    type GetFuture = Box<dyn Future<Item=CacheObject<HashFile, HashDirectory>, Error=CacheError>+Send>;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let mut obj_cache = self.obj_cache.lock().unwrap();
        if let Some(cached) = obj_cache.get(cache_ref) {
            Ok(cached)
        } else {
            let result = match self.cache_path.open_object_file(cache_ref)
                .and_then(|f| self.load_cache_object(cache_ref, f)) {
                Ok(object) => Ok(object),

                Err(CacheError::ObjectNotFound(_)) => {
                    let mut upstream_object =
                        self.cache.get(cache_ref)?;
                    match upstream_object {
                        CacheObject::File(ref mut file) => {
                            self.cache_path.store_file(cache_ref, file)
                                .and_then(|mut file|
                                    file.seek(SeekFrom::Start(1))
                                        .map_err(Into::into)
                                        .map(|_| CacheObject::File(HashFile { file })))
                        }

                        CacheObject::Directory(ref dir) => {
                            let hash_dir = HashDirectory::from_iter(dir.clone());
                            self.cache_path.store_json(cache_ref, &hash_dir, CacheFileType::Directory)
                                .map(|_| CacheObject::Directory(hash_dir))
                        }

                        CacheObject::Commit(ref commit) => {
                            self.cache_path.store_json(cache_ref, commit, CacheFileType::Commit)
                                .map(|_| CacheObject::Commit(commit.clone()))
                        }

                        CacheObject::Symlink(ref symlink) => {
                            let linkdata = LinkData {
                                target: symlink.clone()
                            };
                            self.cache_path.store_json(cache_ref, &linkdata, CacheFileType::Symlink)
                                .map(|_| CacheObject::Symlink(symlink.clone()))
                        }
                    }
                }

                Err(e) => Err(e)
            };

            match result {
                Ok(ref obj) => obj_cache.put(cache_ref, obj),
                Err(_) => ()
            }

            result
        }
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef> {
        let cache_ref = self.cache.add_file_by_path(&source_path)?;
        let source_file = fs::File::open(source_path)?;
        self.cache_path.store_file(&cache_ref, source_file)?;
        Ok(cache_ref)
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef> {
        let entries = Vec::from_iter(items);
        let cache_ref = self.cache.add_directory(&mut entries.iter().cloned())?;

        let hash_dir = HashDirectory::from_iter(entries);
        self.cache_path.store_json(&cache_ref, &hash_dir, CacheFileType::Directory)?;
        Ok(cache_ref)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        let cache_ref = self.cache.add_commit(commit.clone())?;
        self.cache_path.store_json(&cache_ref, &commit, CacheFileType::Commit)?;
        Ok(cache_ref)
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>> {
        self.cache.get_head_commit(branch)
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef> {
        self.cache.merge_commit(branch, cache_ref)
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<()> {
        self.cache.create_branch(branch, cache_ref)
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        let cached_obj = self.obj_cache.lock().unwrap().get(cache_ref);
        let file_cached = cached_obj.map(Ok)
            .unwrap_or_else(|| {
                let file_cached = self.cache_path.open_object_file(cache_ref)
                    .and_then(|file| self.load_cache_object(cache_ref, file));
                if let Ok(ref cached_obj) = file_cached {
                    self.obj_cache.lock().unwrap().put(cache_ref, cached_obj);
                }
                file_cached
            });
        let result = match file_cached {
            Ok(object) => future::Either::A(future::ok(object)),

            Err(CacheError::ObjectNotFound(_)) => {
                let cache_path = self.cache_path.clone();
                let cache_ref = *cache_ref;
                let obj_cache = Arc::clone(&self.obj_cache);
                let upstream_object_future =
                    self.cache.get_poll(&cache_ref)
                        .into_future()
                        .and_then(move |mut upstream_object| {
                            let result = match upstream_object {
                                CacheObject::File(ref mut file) => {
                                    cache_path.store_file(&cache_ref, file)
                                        .and_then(|mut file|
                                            file.seek(SeekFrom::Start(1))
                                                .map_err(Into::into)
                                                .map(|_| CacheObject::File(HashFile { file })))
                                }

                                CacheObject::Directory(ref dir) => {
                                    let hash_dir = HashDirectory::from_iter(dir.clone());
                                    cache_path.store_json(&cache_ref, &hash_dir, CacheFileType::Directory)
                                        .map(|_| CacheObject::Directory(hash_dir))
                                }

                                CacheObject::Commit(ref commit) =>
                                    cache_path.store_json(&cache_ref, commit, CacheFileType::Commit)
                                        .map(|_| CacheObject::Commit(commit.clone())),

                                CacheObject::Symlink(ref symlink) => {
                                    let linkdata = LinkData {
                                        target: symlink.clone()
                                    };
                                    cache_path.store_json(&cache_ref, &linkdata, CacheFileType::Symlink)
                                        .map(|_| CacheObject::Symlink(symlink.clone()))
                                }
                            };

                            if let Ok(ref obj) = result {
                                obj_cache.lock().unwrap().put(&cache_ref, obj);
                            }

                            result
                        });

                future::Either::B(upstream_object_future)
            }

            Err(e) => future::Either::A(future::failed(e))
        };

        Box::new(result) as Self::GetFuture
    }
}

impl<C> HashFileCache<C>  where C: CacheLayer,
                                C::GetFuture: Send+'static,
                                <C::GetFuture as IntoFuture>::Future: Send+'static,
                                C::File: Send,
                                C::Directory: Send {
    pub fn new<P: AsRef<Path>>(cache: C, cache_dir: P) -> Result<Self> {
        let cache_path = FileCachePath {
            base_path: Arc::new(cache_dir.as_ref().to_owned())
        };

        // Actually this could be omitted, but it will tell us early if the target location is
        // accessible
        cache_path.ensure_path("objects")?;
        cache_path.ensure_path("staging")?;

        let cache = HashFileCache {
            cache_path,
            obj_cache: Arc::new(Mutex::new(ObjectCache::new(64))),
            cache
        };

        Ok(cache)
    }

    #[cfg(test)]
    pub fn inner(&self) -> &C {
        &self.cache
    }

    fn identify_object_type<R: Read>(object_file: &mut R) -> Result<CacheFileType> {
        let mut objtype_identifier = [0u8; 1];
        object_file.read_exact(&mut objtype_identifier)?;
        CacheFileType::from_identifier(objtype_identifier[0])
    }

    fn load_cache_object(&self, cache_ref: &CacheRef, mut file: File)
        -> Result<CacheObject<<Self as CacheLayer>::File, <Self as CacheLayer>::Directory>> {
        let objtype = Self::identify_object_type(&mut file)?;
        match objtype {
            CacheFileType::File => {
                let hash_file = HashFile {
                    file
                };

                Ok(CacheObject::File(hash_file))
            }

            CacheFileType::Directory => {
                let hash_dir = serde_json::from_reader(BufReader::new(file))?;
                Ok(CacheObject::Directory(hash_dir))
            }

            CacheFileType::Commit => {
                // If the returned error indicates that data is missing we force the upper layer to
                // retrieve the object from upstream again.
                match serde_json::from_reader(BufReader::with_capacity(1024, file)) {
                    Ok(commit) => Ok(CacheObject::Commit(commit)),
                    Err(e) => if e.is_data() {
                        self.cache_path.invalidate(cache_ref);
                        Err(CacheError::ObjectNotFound(*cache_ref))
                    } else {
                        Err(e.into())
                    }
                }
            }

            CacheFileType::Symlink => {
                let linkdata: LinkData = serde_json::from_reader(BufReader::with_capacity(256, file))?;
                Ok(CacheObject::Symlink(linkdata.target))
            }
        }
    }
}
