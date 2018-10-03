use std::fs::{self, DirEntry, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::{Arc, Weak, Mutex};
use std::collections::HashMap;
use std::error::Error as StdError;

use failure::Error;

use types::*;
use utility::*;
use cache::{self, DirectoryEntry, CacheLayer, CacheRef, Commit, ReferencedCommit, CacheObject};

#[derive(Debug)]
pub enum FSOverlayFile<C: CacheLayer+Debug> {
    FsFile(Arc<Mutex<File>>),
    CacheFile(C::File)
}

impl<C: CacheLayer+Debug> OverlayFile for FSOverlayFile<C> {
    fn close(self) -> Result<(), Error> {
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), Error> {
        if let FSOverlayFile::FsFile(ref mut file) = *self {
            let mut file = file.lock().unwrap();
            file.set_len(size).map_err(Error::from)
        } else {
            Ok(())
        }
    }
}

impl<C: CacheLayer+Debug> Read for FSOverlayFile<C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match *self {
            FSOverlayFile::FsFile(ref mut read) => {
                let mut file = read.lock().unwrap();
                file.read(buf)
            }
            FSOverlayFile::CacheFile(ref mut read) => read.read(buf),
        }
    }
}

impl<C: CacheLayer+Debug> Write for FSOverlayFile<C> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        match *self {
            FSOverlayFile::FsFile(ref mut write) => {
                let mut file = write.lock().unwrap();
                file.write(buf)
            }
            FSOverlayFile::CacheFile(..) => Err(io::Error::from(io::ErrorKind::PermissionDenied))
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        if let FSOverlayFile::FsFile(ref mut write) = *self {
            let mut file = write.lock().unwrap();
            file.flush()
        } else {
            Ok(())
        }
    }
}

impl<C: CacheLayer+Debug> Seek for FSOverlayFile<C> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match *self {
            FSOverlayFile::FsFile(ref mut seek) => {
                let mut file = seek.lock().unwrap();
                file.seek(pos)
            }
            FSOverlayFile::CacheFile(ref mut seek) => seek.seek(pos),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OverlayPath {
    base_path: PathBuf,
    abs_fs_path: PathBuf,
    rel_overlay_path: PathBuf
}

impl OverlayPath {
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Self {
        let base_path = base_path.into();
        Self {
            abs_fs_path: base_path.clone(),
            base_path,
            rel_overlay_path: PathBuf::new()
        }
    }

    pub fn with_overlay_path<P, Q>(base_path: P, rel_path: Q) -> Result<Self, Error>
        where P: Into<PathBuf>,
              Q: AsRef<Path> {
        let mut path = OverlayPath::new(base_path);
        for c in rel_path.as_ref().components() {
            match c {
                ::std::path::Component::Normal(c) => {
                    path.push_overlay(c.to_str()
                        .ok_or(format_err!("Unable to convert path component to UTF-8"))?
                        .to_owned());
                },
                i => debug!("Dropping {:?} during conversion", i)
            }
        }
        Ok(path)
    }

    pub fn overlay_path(&self) -> &Path {
        self.rel_overlay_path.as_path()
    }

    pub fn rel_fs_path(&self) -> &Path {
        self.abs_fs_path.strip_prefix(&self.base_path).unwrap()
    }

    pub fn abs_fs_path(&self) -> &Path {
        self.abs_fs_path.as_path()
    }

    pub fn push_overlay<P: Into<PathBuf>>(&mut self, subdir: P) {
        let mut subdir = subdir.into().into_os_string();
        self.rel_overlay_path.push(&subdir);
        subdir.push(".f");
        self.abs_fs_path.push(subdir);
    }

    pub fn push_fs<P: Into<PathBuf>>(&mut self, subdir: P) {
        let mut subdir = subdir.into();
        self.abs_fs_path.push(&subdir);
        subdir.set_extension("");
        self.rel_overlay_path.push(subdir);
    }

    pub fn pop(&mut self) -> bool {
        if self.rel_overlay_path.pop() {
            let fs_path_pop_succeeded = self.abs_fs_path.pop();
            assert!(fs_path_pop_succeeded);
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct FilesystemOverlay<C: CacheLayer> {
    cache: C,
    head: Option<CacheRef>,
    overlay_files: HashMap<PathBuf, Weak<Mutex<File>>>,
    base_path: PathBuf
}

impl<C: CacheLayer+Debug> FilesystemOverlay<C> {
    fn file_path<P: AsRef<Path>>(base_path: P) -> PathBuf {
        base_path.as_ref().join("files")
    }

    fn meta_path<P: AsRef<Path>>(base_path: P) -> PathBuf {
        base_path.as_ref().join("meta")
    }

    fn add_fs_file<P: AsRef<Path>>(&mut self, path: P, file: File) -> FSOverlayFile<C> {
        let file = Arc::new(Mutex::new(file));
        self.overlay_files.insert(path.as_ref().to_owned(), Arc::downgrade(&file));
        FSOverlayFile::FsFile(file)
    }

    pub fn new<P: AsRef<Path>>(cache: C, base_path: P) -> Result<Self, Error> {
        fs::create_dir_all(Self::file_path(base_path.as_ref()))?;
        let meta_path = Self::meta_path(base_path.as_ref());
        fs::create_dir_all(&meta_path)?;

        let new_head_file_path = meta_path.join("NEW_HEAD");
        if new_head_file_path.exists() {
            unimplemented!("TODO: Recover from a previous crash during commit");
        }

        let head_path = meta_path.join("HEAD");
        let head = match File::open(&head_path) {
            Ok(mut file) => {
                let mut head = String::new();
                file.read_to_string(&mut head)?;
                Some(head.parse()?)
            }

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    None
                } else {
                    return Err(ioerr.into());
                }
            }
        };

        Ok(FilesystemOverlay {
            cache,
            head,
            overlay_files: HashMap::new(),
            base_path: base_path.as_ref().to_owned()
        })
    }

    fn resolve_object_ref<P: AsRef<Path>>(&self, path: P) -> Result<Option<CacheRef>, Error> {
        match self.head {
            Some(head) => {
                let commit = self.cache.get(&head)?.into_commit()
                    .expect("Head ref is not a commit");
                cache::resolve_object_ref(&self.cache, &commit, path)
            }

            None => Ok(None)
        }
    }

    fn dir_entry_to_directory_entry(&self, dir_entry: DirEntry, mut overlay_path: OverlayPath)
        -> Result<cache::DirectoryEntry, Error> {
        let file_type = dir_entry.file_type()?;
        let fs_name = os_string_to_string(dir_entry.file_name())?;
        overlay_path.push_fs(fs_name);
        let name = os_string_to_string(
            overlay_path.overlay_path().file_name()
                // Unwrapping is ok here as we just added at least one path component,
                // so None cannot happen.
                .unwrap()
                .to_os_string())?;

        let (cache_ref, object_type) = if file_type.is_dir() {
            let cache_ref  = self.generate_tree(overlay_path)?;
            (cache_ref, cache::ObjectType::Directory)
        } else if file_type.is_file() {
            let cache_ref = self.cache.add_file_by_path(dir_entry.path())?;
            (cache_ref, cache::ObjectType::File)
        } else {
            unimplemented!("TODO: Implement tree generation for further file types, e.g. links!");
        };

        debug!("Add entry {}", name.as_str());
        Ok(cache::DirectoryEntry {
            cache_ref,
            object_type,
            name
        })
    }

    fn iter_directory<I,T,F,G>(&self, overlay_path: OverlayPath, mut from_directory_entry: F, mut from_dir_entry: G)
        -> Result<I, Error>
        where I: FromIterator<T>,
              T: Eq+Hash,
              F: FnMut(DirectoryEntry) -> Result<T, Error>,
              G: FnMut(DirEntry) -> Result<T, Error> {
        // Get the directory contents from the cache if it's already there
        let dir_entries_result: Result<HashSet<T>, Error> =
            if let Some(cache_dir_ref) = self.resolve_object_ref(overlay_path.overlay_path())? {
                debug!("Reading dir {:?} from cache", overlay_path.overlay_path());
                let dir = self.cache.get(&cache_dir_ref)?.into_directory()?;
                dir.into_iter().map(|e| from_directory_entry(e)).collect()
            } else {
                debug!("Directory not existing in cache");
                Ok(HashSet::new())
            };
        let mut dir_entries = dir_entries_result?;

        // Now merge the staged content into the directory
        let abs_path = overlay_path.abs_fs_path();
        if abs_path.exists() {
            info!("Reading overlay path {}", abs_path.to_string_lossy());
            for dir_entry in fs::read_dir(&*abs_path)? {
                let entry = from_dir_entry(dir_entry?)?;
                dir_entries.insert(entry);
            }
        }

        Ok(I::from_iter(dir_entries.into_iter()))
    }

    fn dir_entry_to_overlay_dir_entry(dir_entry: fs::DirEntry) -> Result<OverlayDirEntry, Error> {
        let mut overlay_path = OverlayPath::new("");
        overlay_path.push_fs(os_string_to_string(dir_entry.file_name())?);
        let name = os_string_to_string(overlay_path.overlay_path().as_os_str().to_owned())?;

        let entry = OverlayDirEntry {
            name,
            metadata: Metadata::from_fs_metadata(dir_entry.metadata()?)?
        };

        Ok(entry)
    }

    fn directory_entry_to_overlay_dir_entry(&self, dir_entry: DirectoryEntry)
        -> Result<OverlayDirEntry, Error> {
        self.cache.metadata(&dir_entry.cache_ref)
            .map_err(Error::from)
            .and_then(Metadata::from_cache_metadata)
            .map(move |metadata| OverlayDirEntry {
                name: dir_entry.name,
                metadata
            })
    }

    fn generate_tree(&self, overlay_path: OverlayPath) -> Result<CacheRef, Error> {
        let dir_entries: HashSet<DirectoryEntry> = self.iter_directory(
            overlay_path.clone(),
            |e| Ok(e),
            |e| {
                self.dir_entry_to_directory_entry(e, overlay_path.clone())
            })?;

        let directory = dir_entries.into_iter();
        self.cache.add_directory(directory).map_err(Into::into)
    }

    fn create_new_head_commit(&mut self, commit_ref: &CacheRef) -> Result<PathBuf, Error> {
        self.head = Some(commit_ref.clone());

        let meta_path = Self::meta_path(&self.base_path);
        let new_head_file_path = meta_path.join("NEW_HEAD");

        let mut new_head_file = File::create(&new_head_file_path)?;
        new_head_file.write(commit_ref.to_string().as_bytes())?;
        new_head_file.flush()?;
        new_head_file.sync_all()?;

        Ok(new_head_file_path)
    }

    fn clear_closed_files_in_path(&mut self, path: &mut OverlayPath) -> Result<(), Error> {
        debug!("Clearing overlay path {}", path.abs_fs_path().display());

        for entry_result in fs::read_dir(&*path.abs_fs_path())? {
            let entry = entry_result?;
            let file_type = entry.file_type()?;
            let file_name = os_string_to_string(entry.file_name())?;
            path.push_fs(file_name);

            if file_type.is_dir() {
                self.clear_closed_files_in_path(path)?;
            } else {
                if !self.overlay_files.contains_key(path.overlay_path()) {
                    debug!("Removing overlay file {}", path.abs_fs_path().display());
                    if let Err(ioerr) = fs::remove_file(&*path.abs_fs_path()) {
                        warn!("Unable to remove overlay file {} during cleanup: {}",
                              path.abs_fs_path().display(),
                              ioerr.description());
                    }
                }
            }
        }

        path.pop();
        if let Err(ioerr) = fs::remove_dir(&*path.abs_fs_path()) {
            warn!("Unable to remove overlay directory {} during cleanup: {}",
                  path.abs_fs_path().display(),
                  ioerr.description());
        }

        Ok(())
    }

    fn clear_closed_files(&mut self) -> Result<(), Error> {
        self.overlay_files.retain(|_, file| file.upgrade().is_some());

        debug!("Currently open files: {:?}", self.overlay_files.keys().collect::<Vec<&PathBuf>>());

        let mut base_path = OverlayPath::new(Self::file_path(&self.base_path));
        self.clear_closed_files_in_path(&mut base_path)
    }

    pub fn ensure_directory<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        let overlay_path = OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        fs::create_dir_all(&*overlay_path.abs_fs_path())
            .map_err(|e| e.into())
    }
}

pub struct CacheLayerLog<'a, C: CacheLayer+'a> {
    cache: &'a C,
    next_cache_ref: Option<CacheRef>
}

impl<'a, C: CacheLayer+'a> CacheLayerLog<'a, C> {
    fn new<'b>(cache: &'a C, commit_ref: &'b CacheRef) -> Result<Self, Error> {
        let log = CacheLayerLog {
            cache,
            next_cache_ref: Some(commit_ref.clone())
        };

        Ok(log)
    }
}

impl<'a, C: CacheLayer+'a> WorkspaceLog<'a> for CacheLayerLog<'a, C> { }

impl<'a, C: CacheLayer+'a> Iterator for CacheLayerLog<'a, C> {
    type Item = Result<ReferencedCommit, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cache_ref.take().and_then(|cache_ref| {
            match self.cache.get(&cache_ref) {
                Ok(CacheObject::Commit(commit)) => {
                    if !commit.parents.is_empty() {
                        if commit.parents.len() > 1 {
                            warn!("Non-linear history not supported yet!");
                        }

                        self.next_cache_ref = Some(commit.parents[0]);
                    }

                    Some(Ok(ReferencedCommit(cache_ref, commit)))
                }

                Ok(_) => Some(Err(format_err!("Current ref {} doesn't reference a commit", cache_ref))),
                Err(e) => Some(Err(format_err!("Error retrieving cache object for ref {}: {}", cache_ref, e)))
            }
        })
    }
}

impl<'a, C: CacheLayer+Debug+'a> WorkspaceController<'a> for FilesystemOverlay<C> {
    type Log = CacheLayerLog<'a, C>;

    fn commit(&mut self, message: &str) -> Result<CacheRef, Error> {
        let file_path = Self::file_path(&self.base_path);
        let meta_path = Self::meta_path(&self.base_path);
        let overlay_path = OverlayPath::new(file_path);

        let tree = self.generate_tree(overlay_path)?;
        let parents = Vec::from_iter(self.head.take().into_iter());
        let commit = Commit {
            parents,
            tree,
            message: message.to_owned()
        };

        let new_commit_ref = self.cache.add_commit(commit)?;

        // Stage 1: create file "NEW_HEAD"
        let new_head_path = self.create_new_head_commit(&new_commit_ref)?;

        let head_file_path = meta_path.join("HEAD");

        // Stage 2: Cleanup overlay content
        self.clear_closed_files()?;

        if let Err(e) = fs::remove_file(&head_file_path) {
            if e.kind() != io::ErrorKind::NotFound {
                error!("Unable to remove the old HEAD file: {}", e);
                return Err(e.into());
            } else {
                info!("Creating new HEAD file");
            }
        }

        // Stage 3: Replace old HEAD with NEW_HEAD
        fs::rename(new_head_path, head_file_path)?;

        Ok(new_commit_ref)
    }

    fn get_current_head_ref(&self) -> Result<Option<CacheRef>, Error> {
        Ok(self.head)
    }

    fn get_log<'b: 'a>(&'b self, start_commit: &CacheRef) -> Result<Self::Log, Error> {
        CacheLayerLog::new(&self.cache, start_commit)
    }
}

impl<C: CacheLayer+Debug> Overlay for FilesystemOverlay<C> {
    type File = FSOverlayFile<C>;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool)
        -> Result<FSOverlayFile<C>, Error> {
        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let abs_fs_path = overlay_path.abs_fs_path();
        let cache_path = overlay_path.overlay_path();

        debug!("Trying to open {} in the overlay", abs_fs_path.to_string_lossy());

        // Check if the file is already part of the overlay
        let overlay_file = fs::OpenOptions::new()
            .read(true)
            .write(writable)
            .create(false)
            .open(&*abs_fs_path);
        match  overlay_file {
            Ok(file) => Ok(self.add_fs_file(&cache_path, file)),

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    let cache_ref = self.resolve_object_ref(&cache_path)?;
                    debug!("Opening cache ref {:?}", cache_ref);
                    let mut cache_file = if let Some(existing_ref) = cache_ref {
                        let cache_object = self.cache.get(&existing_ref)?;
                        Some(cache_object.into_file()?)
                    } else {
                        None
                    };

                    if writable {
                        if let Some(parent_path) = abs_fs_path.parent() {
                            fs::create_dir_all(parent_path)?;
                        }

                        let mut new_file = fs::OpenOptions::new()
                            .create(true)
                            .read(true)
                            .write(true)
                            .open(&*abs_fs_path)?;
                        if let Some(mut cache_file) = cache_file {
                            io::copy(&mut cache_file, &mut new_file)?;
                            new_file.seek(SeekFrom::Start(0))?;
                        }
                        Ok(self.add_fs_file(&cache_path, new_file))
                    } else {
                        if let Some(cache_file) = cache_file {
                            Ok(FSOverlayFile::CacheFile(cache_file))
                        } else {
                            bail!("File not found!");
                        }
                    }
                } else {
                    Err(ioerr.into())
                }
            }
        }
    }

    fn list_directory<I,P>(&self, path: P) -> Result<I, Error>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path> {
        let overlay_path = OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        self.iter_directory(
            overlay_path,
            |dir_entry| {
                self.directory_entry_to_overlay_dir_entry(dir_entry)
            },
            Self::dir_entry_to_overlay_dir_entry)
    }

    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata, Error> {
        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let abs_fs_path = overlay_path.abs_fs_path();

        if abs_fs_path.exists() {
            fs::symlink_metadata(&*abs_fs_path)
                .map_err(|e| e.into())
                .and_then(Metadata::from_fs_metadata)
        } else {
            self.resolve_object_ref(overlay_path.overlay_path())
                .and_then(|cache_ref| cache_ref.ok_or(format_err!("File not found!")))
                .and_then(|cache_ref| self.cache.metadata(&cache_ref)
                    .map_err(Error::from))
                .and_then(Metadata::from_cache_metadata)
        }
    }
}

#[cfg(test)]
pub mod testutil {
    use hashfilecache::HashFileCache;
    use overlay::*;
    use std::io::{Read, Seek, SeekFrom};

    pub fn open_working_copy<P: AsRef<Path>>(path: P) -> FilesystemOverlay<HashFileCache> {
        let cache_dir = path.as_ref().join("cache");
        let overlay_dir = path.as_ref().join("overlay");

        let cache = HashFileCache::new(&cache_dir).expect("Unable to create cache");
        FilesystemOverlay::new(cache, &overlay_dir)
            .expect("Unable to create overlay")
    }

    pub fn check_file_content<F: Read+Seek>(file: &mut F, expected_content: &str) {
        file.seek(SeekFrom::Start(0)).expect("Unable to seek in test file");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("Unable to read from test file");
        assert_eq!(expected_content, content.as_str());
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use overlay::*;
    use std::io::{Read, Write, Seek, SeekFrom};
    use std::collections::HashSet;
    use super::testutil::*;

    #[cfg(target_os = "windows")]
    mod overlay_path_win {
        use overlay::OverlayPath;
        use std::path::{Path, PathBuf};

        #[test]
        fn create_and_push() {
            let mut path = OverlayPath::new("C:\\tmp");
            path.push_overlay("sub1");
            assert_eq!("C:\\tmp\\sub1.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1", path.overlay_path().to_str().unwrap());
            path.push_overlay("sub2");
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2", path.overlay_path().to_str().unwrap());
            path.push_overlay("file.txt");
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f\\file.txt.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f\\file.txt.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2\\file.txt", path.overlay_path().to_str().unwrap());
        }

        #[test]
        fn create_and_push_fs() {
            let mut path = OverlayPath::new("C:\\tmp");
            path.push_fs("sub1.f");
            assert_eq!("C:\\tmp\\sub1.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1", path.overlay_path().to_str().unwrap());
            path.push_fs("sub2.f");
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2", path.overlay_path().to_str().unwrap());
            path.push_fs("file.txt.f");
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f\\file.txt.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f\\file.txt.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2\\file.txt", path.overlay_path().to_str().unwrap());
        }

        #[test]
        fn create_from_existing() {
            let mut path = OverlayPath::with_overlay_path(Path::new("C:\\tmp"),
                                                          Path::new("sub1\\sub2"))
                .unwrap();
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2", path.overlay_path().to_str().unwrap());
            path.push_overlay("file.txt");
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f\\file.txt.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f\\file.txt.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2\\file.txt", path.overlay_path().to_str().unwrap());
        }

        #[test]
        fn  pop_path() {
            let mut path = OverlayPath::with_overlay_path(Path::new("C:\\tmp"),
                                                          Path::new("sub1\\sub2\\sub3\\file.txt"))
                .unwrap();
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f\\sub3.f\\file.txt.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f\\sub3.f\\file.txt.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2\\sub3\\file.txt", path.overlay_path().to_str().unwrap());
            path.pop();
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f\\sub3.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f\\sub3.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2\\sub3", path.overlay_path().to_str().unwrap());
            path.pop();
            assert_eq!("C:\\tmp\\sub1.f\\sub2.f", path.abs_fs_path().to_str().unwrap());
            assert_eq!("sub1.f\\sub2.f", path.rel_fs_path().to_str().unwrap());
            assert_eq!("sub1\\sub2", path.overlay_path().to_str().unwrap());
        }
    }

    #[test]
    fn create_root_commit() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let mut staged_file = overlay.open_file("test.txt", false)
            .expect("Unable to open staged file!");
        let mut contents = String::new();
        staged_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(staged_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut committed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(committed_file);

        let metadata = overlay.metadata("test.txt")
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);

        let mut editable_file = overlay.open_file("test.txt", true)
            .expect("Unable to open file for writing!");
        editable_file.seek(SeekFrom::End(0)).expect("Unable to seek to end!");
        write!(editable_file, "Yay!")
            .expect("Unable to append to file!");
        drop(editable_file);

        let mut changed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open changed file!");
        let mut contents = String::new();
        changed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!Yay!");
        drop(changed_file);

        let metadata = overlay.metadata("test.txt")
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!Yay!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);
    }

    #[test]
    fn two_commits() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut additional_file = overlay.open_file("test2.txt", true)
            .expect("Unable to create file");
        write!(additional_file, "Another file").expect("Couldn't write to test file");
        drop(additional_file);

        overlay.commit("A test commit with parent").expect("Unable to commit");

        let mut first_file = overlay.open_file("test.txt", false)
            .expect("Unable to open first file");
        let mut content = String::new();
        first_file.read_to_string(&mut content)
            .expect("Unable to read from first file");
        assert_eq!("What a test!", content.as_str());
    }

    #[test]
    fn reopen_workspace() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        drop(overlay);

        let mut overlay = open_working_copy(tempdir.path());

        let mut committed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
    }

    #[test]
    fn commit_directories() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());
        overlay.ensure_directory("a/nested/dir")
            .expect("Failed to create a nested directory");

        let mut test_file = overlay.open_file("a/nested/dir/test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("A test commit").expect("Unable to commit");

        for writable in &[true, false] {
            let mut committed_file = overlay.open_file("a/nested/dir/test.txt", *writable)
                .expect(format!("Unable to open committed file as {}",
                                if *writable { "writable" } else { "readonly" }).as_str());
            let mut contents = String::new();
            committed_file.read_to_string(&mut contents)
                .expect("Unable to read from committed file");
            drop(committed_file);
            assert_eq!("What a test!", contents.as_str());
        }

        let dir_entries: HashSet<OverlayDirEntry> = overlay.list_directory("a/nested/dir")
            .expect("Unable to get directory contents of a/nested/dir");
        assert_eq!(HashSet::<String>::from_iter(["test.txt"].iter().map(|s| s.to_string())),
                   HashSet::<String>::from_iter(dir_entries.iter().map(|e| e.name.clone())));
    }

    #[test]
    fn commit_empty_directory() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory("test")
            .expect("Unable to create directory");
        overlay.commit("Test message")
            .expect("Unable to commit empty directory");

        let dir: Vec<OverlayDirEntry> = overlay.list_directory("")
            .expect("Unable to list directory");
        let dir_names: HashSet<String> = dir.into_iter().map(|e| e.name).collect();
        assert_eq!(dir_names, HashSet::from_iter(["test"].iter().map(|s| s.to_string())));

        let subdir: Vec<OverlayDirEntry> = overlay.list_directory("test")
            .expect("Unable to list subdirectory test");
        assert!(subdir.is_empty());
    }

    #[test]
    fn modify_file_after_commit() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        check_file_content(&mut test_file, "What a test!");

        overlay.commit("A test commit").expect("Unable to create first commit");
        check_file_content(&mut test_file, "What a test!");

        debug!("Adding additional content");
        write!(test_file, "Incredible!").expect("Couldn't write to test file");
        debug!("Checking additional content");
        check_file_content(&mut test_file, "What a test!Incredible!");

        debug!("Committing additional content");
        overlay.commit("A test commit with parent")
            .expect("Unable to create second commit");
        check_file_content(&mut test_file, "What a test!Incredible!");
    }
}
