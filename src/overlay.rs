use std::fs::{self, ReadDir, DirEntry, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::hash::{Hash, Hasher};
use std::fmt::{self, Formatter, Display, Debug};
use std::sync::{Arc, Weak, Mutex};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::ffi::OsStr;

use crate::types::*;
use crate::utility::*;
use crate::cache::{
    self, DirectoryEntry, CacheLayer, CacheRef, Commit, ReferencedCommit, CacheObject, CacheError
};

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Directory not empty")]
    NonemptyDirectory,

    #[fail(display = "File not found")]
    FileNotFound,

    #[fail(display = "IO error: {}", _0)]
    IoError(io::Error),

    #[fail(display = "Cache error: {}", _0)]
    CacheError(CacheError),

    #[fail(display = "{}", _0)]
    Generic(failure::Error)
}

impl Error {
    pub fn from_fail<E: failure::Fail>(e: E) -> Self {
        Error::Generic(failure::Error::from(e))
    }

}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e)
    }
}

impl From<CacheError> for Error {
    fn from(e: CacheError) -> Self {
        Error::CacheError(e)
    }
}

impl From<failure::Error> for Error {
    fn from(e: failure::Error) -> Self {
        Error::Generic(e)
    }
}

impl From<&'static str> for Error {
    fn from(e: &'static str) -> Self {
        Error::Generic(failure::err_msg(e))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct OverlayDirEntry {
    pub name: String,
    pub metadata: Metadata
}

impl PartialEq for OverlayDirEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for OverlayDirEntry {}

impl Hash for OverlayDirEntry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.name.hash(hasher);
    }
}

impl<'a> From<(&'a str, &'a Metadata)> for OverlayDirEntry {
    fn from(val: (&'a str, &'a Metadata)) -> OverlayDirEntry {
        OverlayDirEntry {
            name: val.0.to_owned(),
            metadata: val.1.clone()
        }
    }
}

pub trait OverlayFile: Read+Write+Seek {
    fn close(&mut self) -> Result<()>;
    fn truncate(&mut self, size: u64) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileState {
    New,
    Modified,
    Deleted
}

impl Display for FileState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let literal = match self {
            FileState::New => "new",
            FileState::Modified => "modified",
            FileState::Deleted => "deleted"
        };
        f.write_str(literal)
    }
}

pub trait Overlay: Debug {
    type File: OverlayFile;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool) -> Result<Self::File>;
    fn list_directory<I,P>(&self, path: P) -> Result<I>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata>;
    fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        self.metadata(path).is_ok()
    }
    fn delete_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;
    fn revert_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;
}

pub trait WorkspaceLog<'a>: Iterator<Item=Result<ReferencedCommit>> { }

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceFileStatus(pub PathBuf, pub FileState);

impl<P: AsRef<Path>> From<(P, FileState)> for WorkspaceFileStatus {
    fn from(val: (P, FileState)) -> Self {
        WorkspaceFileStatus(val.0.as_ref().to_path_buf(), val.1)
    }
}

pub trait WorkspaceController<'a>: Debug {
    type Log: WorkspaceLog<'a>;
    type StatusIter: Iterator<Item=Result<WorkspaceFileStatus>>+'a;

    fn commit(&mut self, message: &str) -> Result<CacheRef>;
    fn get_current_head_ref(&self) -> Result<Option<CacheRef>>;
    fn get_current_branch(&self) -> Result<Option<&str>>;
    fn switch_branch(&mut self, branch: &str) -> Result<CacheRef>;
    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef<'a>>)
                     -> Result<()>;
    fn get_log(&'a self, start_commit: &CacheRef) -> Result<Self::Log>;
    fn get_status(&'a self) -> Result<Self::StatusIter>;
}
#[derive(Debug)]
pub enum FSOverlayFile<C: CacheLayer+Debug> {
    FsFile(Option<Arc<Mutex<File>>>),
    CacheFile(C::File)
}

impl<C: CacheLayer+Debug> OverlayFile for FSOverlayFile<C> {
    fn close(&mut self) -> Result<()> {
        if let FSOverlayFile::FsFile(ref mut file) = *self {
            file.take();
        }
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        if let FSOverlayFile::FsFile(ref mut file) = *self {
            let file = file.as_ref().unwrap().lock().unwrap();
            file.set_len(size).map_err(Error::from)
        } else {
            Ok(())
        }
    }
}

impl<C: CacheLayer+Debug> Read for FSOverlayFile<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.as_mut().unwrap().lock().unwrap();
                file.read(buf)
            }
            FSOverlayFile::CacheFile(ref mut read) => read.read(buf),
        }
    }
}

impl<C: CacheLayer+Debug> Write for FSOverlayFile<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.as_mut().unwrap().lock().unwrap();
                file.write(buf)
            }
            FSOverlayFile::CacheFile(..) => Err(io::Error::from(io::ErrorKind::PermissionDenied))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let FSOverlayFile::FsFile(ref mut file) = *self {
            let mut file = file.as_mut().unwrap().lock().unwrap();
            file.flush()
        } else {
            Ok(())
        }
    }
}

impl<C: CacheLayer+Debug> Seek for FSOverlayFile<C> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.as_mut().unwrap().lock().unwrap();
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

    pub fn with_overlay_path<P, Q>(base_path: P, rel_path: Q) -> Result<Self>
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

    pub fn into_abs_fs_path(self) -> PathBuf {
        self.abs_fs_path
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
enum OverlayOperation<T> {
    Add(T),
    Subtract(T)
}

#[derive(Debug)]
pub struct FilesystemOverlay<C: CacheLayer> {
    cache: C,
    head: Option<CacheRef>,
    overlay_files: HashMap<PathBuf, Weak<Mutex<File>>>,
    base_path: PathBuf,
    branch: String
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
        FSOverlayFile::FsFile(Some(file))
    }

    pub fn new<P: AsRef<Path>>(cache: C, base_path: P, branch: &str) -> Result<Self> {
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
                if head == CacheRef::null().to_string() {
                    None
                } else {
                    Some(head.parse()?)
                }
            }

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    cache.get_head_commit(branch)?
                } else {
                    return Err(ioerr.into());
                }
            }
        };

        Ok(FilesystemOverlay {
            cache,
            head,
            overlay_files: HashMap::new(),
            base_path: base_path.as_ref().to_owned(),
            branch: branch.to_owned()
        })
    }

    fn resolve_object_ref<P: AsRef<Path>>(&self, path: P) -> Result<Option<CacheRef>> {
        match self.head {
            Some(head) => {
                let commit = self.cache.get(&head)?.into_commit()
                    .expect("Head ref is not a commit");
                cache::resolve_object_ref(&self.cache, &commit, path)
                    .map_err(Error::Generic)
            }

            None => Ok(None)
        }
    }

    fn dir_entry_to_directory_entry(&self, dir_entry: DirEntry, mut overlay_path: OverlayPath)
        -> Result<OverlayOperation<cache::DirectoryEntry>> {
        let fs_name = os_string_to_string(dir_entry.file_name())?;
        let file_type = dir_entry.file_type()?;
        overlay_path.push_fs(&fs_name);
        let name = os_string_to_string(
            overlay_path.overlay_path().file_name()
                // Unwrapping is ok here as we just added at least one path component,
                // so None cannot happen.
                .unwrap()
                .to_os_string())?;

        if fs_name.ends_with("f") {
            let (cache_ref, object_type) = if file_type.is_dir() {
                let cache_ref = self.generate_tree(overlay_path)?;
                (cache_ref, cache::ObjectType::Directory)
            } else if file_type.is_file() {
                let cache_ref = self.cache.add_file_by_path(&dir_entry.path())?;
                (cache_ref, cache::ObjectType::File)
            } else {
                unimplemented!("TODO: Implement tree generation for further file types, e.g. links!");
            };

            let directory_entry = cache::DirectoryEntry {
                cache_ref,
                object_type,
                name
            };

            debug!("Add entry {}", directory_entry.name.as_str());
            Ok(OverlayOperation::Add(directory_entry))
        } else {
            let directory_entry = cache::DirectoryEntry {
                cache_ref: CacheRef::null(),
                object_type: if file_type.is_file() {
                    cache::ObjectType::File
                } else {
                    cache::ObjectType::Directory
                },
                name
            };

            debug!("Subtract entry {}", directory_entry.name.as_str());
            Ok(OverlayOperation::Subtract(directory_entry))
        }
    }

    fn iter_directory<I,T,F,G>(&self,
                               overlay_path: OverlayPath,
                               from_directory_entry: F,
                               mut from_dir_entry: G) -> Result<I>
        where I: FromIterator<T>,
              T: Eq+Hash,
              F: FnMut(DirectoryEntry) -> Result<T>,
              G: FnMut(DirEntry) -> Result<OverlayOperation<T>> {
        // Get the directory contents from the cache if it's already there
        let dir_entries_result: Result<HashSet<T>> =
            if let Some(cache_dir_ref) = self.resolve_object_ref(overlay_path.overlay_path())? {
                debug!("Reading dir {} with ref {} from cache", overlay_path.overlay_path().display(), &cache_dir_ref);
                let dir = self.cache.get(&cache_dir_ref)?
                    .into_directory()?;
                dir.into_iter().map(from_directory_entry).collect()
            } else {
                debug!("Directory \"{}\" not existing in cache",
                       overlay_path.overlay_path().display());
                Ok(HashSet::new())
            };
        let mut dir_entries = dir_entries_result?;

        // Now merge the staged content into the directory
        let abs_path = overlay_path.abs_fs_path();
        if abs_path.exists() {
            info!("Reading overlay path {}", abs_path.to_string_lossy());
            for dir_entry in fs::read_dir(abs_path)? {
                match from_dir_entry(dir_entry?)? {
                    OverlayOperation::Add(entry) => { dir_entries.replace(entry); }
                    OverlayOperation::Subtract(entry) => { dir_entries.remove(&entry); }
                }
            }
        } else {
            debug!("...but path doesn't exist");
        }

        Ok(I::from_iter(dir_entries.into_iter()))
    }

    fn dir_entry_to_overlay_dir_entry(dir_entry: fs::DirEntry) -> Result<OverlayOperation<OverlayDirEntry>> {
        let mut overlay_path = OverlayPath::new("");
        let fs_name = os_string_to_string(dir_entry.file_name())?;
        overlay_path.push_fs(&fs_name);
        let name = os_string_to_string(overlay_path.overlay_path().as_os_str().to_owned())?;

        let entry = OverlayDirEntry {
            name,
            metadata: Metadata::from_fs_metadata(dir_entry.metadata()?)?
        };

        if fs_name.ends_with("f") {
            Ok(OverlayOperation::Add(entry))
        } else {
            Ok(OverlayOperation::Subtract(entry))
        }
    }

    fn directory_entry_to_overlay_dir_entry(&self, dir_entry: DirectoryEntry)
        -> Result<OverlayDirEntry> {
        self.cache.metadata(&dir_entry.cache_ref)
            .map_err(Error::from)
            .and_then(|meta| Metadata::from_cache_metadata(meta)
                .map_err(Error::Generic))
            .map(move |metadata| OverlayDirEntry {
                name: dir_entry.name,
                metadata
            })
    }

    fn generate_tree(&self, overlay_path: OverlayPath) -> Result<CacheRef> {
        let dir_entries: HashSet<DirectoryEntry> = self.iter_directory(
            overlay_path.clone(),
            |e| Ok(e),
            |e| {
                self.dir_entry_to_directory_entry(e, overlay_path.clone())
            })?;

        let mut directory = dir_entries.into_iter();
        self.cache.add_directory(&mut directory).map_err(Into::into)
    }

    fn pin_current_head(&mut self) -> Result<PathBuf> {
        let meta_path = Self::meta_path(&self.base_path);
        let head_file_path = meta_path.join("HEAD");
        if !head_file_path.exists() {
            let new_head_file_path = meta_path.join("NEW_HEAD");
            fs::remove_file(&new_head_file_path).ok();

            let commit_ref = self.head.unwrap_or_else(|| CacheRef::null());

            info!("Pinning HEAD to {}", commit_ref);

            let mut new_head_file = File::create(&new_head_file_path)?;
            new_head_file.write(commit_ref.to_string().as_bytes())?;
            new_head_file.flush()?;
            new_head_file.sync_all()?;

            fs::rename(new_head_file_path, &head_file_path)?;
        }

        Ok(head_file_path)
    }

    fn clear_closed_files_in_path(&mut self, path: &mut OverlayPath) -> Result<()> {
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

    fn clear_closed_files(&mut self) -> Result<()> {
        self.overlay_files.retain(|_, file| file.upgrade().is_some());

        debug!("Currently open files: {:?}", self.overlay_files.keys().collect::<Vec<&PathBuf>>());

        let mut base_path = OverlayPath::new(Self::file_path(&self.base_path));
        self.clear_closed_files_in_path(&mut base_path)
    }

    pub fn ensure_directory<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let overlay_path = OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        fs::create_dir_all(&*overlay_path.abs_fs_path())
            .map_err(|e| e.into())
    }

    fn open_cache_file(&mut self, writable: bool, abs_fs_path: &Path, cache_path: &Path)
                       -> Result<FSOverlayFile<C>> {
        let cache_ref = self.resolve_object_ref(&cache_path)?;
        debug!("Opening cache ref {:?}", cache_ref);

        let cache_file = if let Some(existing_ref) = cache_ref {
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

            self.pin_current_head()?;

            Ok(self.add_fs_file(&cache_path, new_file))
        } else {
            if let Some(cache_file) = cache_file {
                Ok(FSOverlayFile::CacheFile(cache_file))
            } else {
                Err(format_err!("File not found!").into())
            }
        }
    }
}

pub struct CacheLayerLog<'a, C: CacheLayer+'a> {
    cache: &'a C,
    next_cache_ref: Option<CacheRef>
}

impl<'a, C: CacheLayer+'a> CacheLayerLog<'a, C> {
    fn new<'b>(cache: &'a C, commit_ref: &'b CacheRef) -> Result<Self> {
        let log = CacheLayerLog {
            cache,
            next_cache_ref: Some(commit_ref.clone())
        };

        Ok(log)
    }
}

impl<'a, C: CacheLayer+'a> WorkspaceLog<'a> for CacheLayerLog<'a, C> { }

impl<'a, C: CacheLayer+'a> Iterator for CacheLayerLog<'a, C> {
    type Item = Result<ReferencedCommit>;

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

                Ok(_) => Some(Err(format_err!("Current ref {} doesn't reference a commit", cache_ref).into())),
                Err(e) => Some(Err(format_err!("Error retrieving cache object for ref {}: {}", cache_ref, e).into()))
            }
        })
    }
}

impl<'a, C: CacheLayer+Debug+'a> WorkspaceController<'a> for FilesystemOverlay<C> {
    type Log = CacheLayerLog<'a, C>;
    type StatusIter = FSStatusIter<'a, C>;

    fn commit(&mut self, message: &str) -> Result<CacheRef> {
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

        let new_commit_ref = self.cache.add_commit(commit)
            .and_then(|cache_ref|
                self.cache.merge_commit(&self.branch, cache_ref))?;

        // Stage 1: Cleanup overlay content
        self.clear_closed_files()?;

        // Stage 2: Remove version pinning if there is no open file left
        if self.overlay_files.is_empty() {
            // If there is no open file, we can assume that moving to the latest HEAD is just fine.
            // In this case we unpin and get the latest HEAD from the cache if the cache supports
            // that.
            info!("Unpinning HEAD");
            let head_file_path = meta_path.join("HEAD");
            if let Err(ioerr) = fs::remove_file(head_file_path) {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    info!("HEAD file couldn't be found");
                } else {
                    warn!("Unable to delete HEAD file: {}", ioerr);
                }
            }
            self.head = self.cache.get_head_commit(&self.branch)?.or(Some(new_commit_ref));
        } else {
            // If there are still open files, we just use our added commit as the new HEAD and pin
            // the workspace to that reference so that open files do not accidentally overwrite
            // stuff other people have committed in the meantime.
            info!("Not unpinning as there are still open files in the overlay");
            self.head = Some(new_commit_ref);
            self.pin_current_head()?;
        }

        Ok(new_commit_ref)
    }

    fn get_current_head_ref(&self) -> Result<Option<CacheRef>> {
        Ok(self.head)
    }

    fn get_current_branch(&self) -> Result<Option<&str>> {
        Ok(Some(self.branch.as_ref()))
    }

    fn switch_branch(&mut self, new_branch: &str) -> Result<CacheRef> {
        self.get_status()
            .and_then(|mut status_iter|
                match status_iter.next() {
                    Some(item) => {
                        Err(format_err!("Cannot switch if the workspace isn't clean: {:?}", item)
                            .into())
                    }
                    None => Ok(())
                })
            .and_then(|_| self.cache.get_head_commit(new_branch)
                .map_err(Into::into)
                .and_then(|cache_ref|
                    cache_ref.ok_or(format_err!("Branch doesn't exist").into())))
            .map(|new_cache_ref| {
                self.branch = new_branch.to_string();
                self.head = Some(new_cache_ref);
                new_cache_ref
            })
    }

    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef<'a>>)
        -> Result<()> {
        let base_ref = match repo_ref {
            Some(RepoRef::CacheRef(cache_ref)) => cache_ref,
            Some(RepoRef::Branch(branch)) => self.cache.get_head_commit(branch)
                .map_err(Error::CacheError)
                .and_then(|cache_ref|
                    cache_ref.ok_or(format_err!("Branch {} doesn't exsit", branch).into()))?,
            None => self.head.ok_or(Error::Generic(format_err!("No HEAD in current workspace set")))?,
        };

        self.cache.create_branch(new_branch, base_ref)
            .map_err(Into::into)
    }

    fn get_log(&'a self, start_commit: &CacheRef) -> Result<Self::Log> {
        CacheLayerLog::new(&self.cache, start_commit)
    }

    fn get_status(&'a self) -> Result<FSStatusIter<'a, C>> {
        let path = OverlayPath::new(Self::file_path(&self.base_path));
        Ok(FSStatusIter::new(path, self))
    }
}

pub struct FSStatusIter<'a, C: CacheLayer+'a> {
    cur_dir: Option<(ReadDir, OverlayPath)>,
    sub_dirs: Vec<OverlayPath>,
    overlay: &'a FilesystemOverlay<C>
}

impl<'a, C: CacheLayer> FSStatusIter<'a, C> {
    fn new(base_path: OverlayPath, overlay: &'a FilesystemOverlay<C>) -> Self {
        let sub_dirs = if base_path.abs_fs_path().exists() {
            vec![base_path]
        } else {
            Vec::new()
        };

        Self {
            cur_dir: None,
            sub_dirs,
            overlay
        }
    }
}

impl<'a, C: CacheLayer> Iterator for FSStatusIter<'a, C> {
    type Item = Result<WorkspaceFileStatus>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_item =
                match self.cur_dir {
                    Some((ref mut read_dir, ref path)) => read_dir.next()
                        .map(|item|
                            item.map(|item| (item, path.clone()))),
                    None => None
                };

            match next_item {
                Some(Ok((entry, mut path))) => {
                    let file_name = PathBuf::from(entry.file_name());
                    debug!("Regular file entry {}", file_name.display());
                    path.push_fs(&file_name);

                    match file_name.extension() {
                        Some(ext) if ext == OsStr::new("f") => {
                            match entry.file_type() {
                                Ok(file_type) => if file_type.is_dir() {
                                    self.sub_dirs.push(path);
                                } else if file_type.is_file() {
                                    let object_ref =
                                        self.overlay.resolve_object_ref(path.overlay_path());
                                    let status =
                                        match object_ref {
                                            Ok(obj_ref) => if obj_ref.is_some() {
                                                FileState::Modified
                                            } else {
                                                FileState::New
                                            },
                                            Err(err) => break Some(Err(err.into()))
                                        };
                                    let workspace_file_status = WorkspaceFileStatus(
                                        path.overlay_path().to_path_buf(),
                                        status);
                                    break Some(Ok(workspace_file_status));
                                } else if file_type.is_symlink() {
                                    warn!("Cannot handle symlinks yet");
                                }

                                Err(err) => break Some(Err(err.into()))
                            }
                        }

                        Some(ext) if ext == OsStr::new("d") => {
                            let workspace_file_status = WorkspaceFileStatus(
                                path.overlay_path().to_path_buf(),
                                FileState::Deleted);
                            break Some(Ok(workspace_file_status));
                        }

                        _ => warn!("Cannot handle file \"{}\" in the overlay", path.abs_fs_path().display())
                    }
                }

                Some(Err(err)) => break Some(Err(err.into())),

                None => match self.sub_dirs.pop() {
                    Some(sub_dir) => {
                        match fs::read_dir(sub_dir.abs_fs_path()) {
                            Ok(read_dir) => self.cur_dir = Some((read_dir, sub_dir)),
                            Err(err) => break Some(Err(err.into()))
                        }
                    }
                    None => break None
                }
            }
        }
    }
}

impl<C: CacheLayer+Debug> Overlay for FilesystemOverlay<C> {
    type File = FSOverlayFile<C>;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool)
        -> Result<FSOverlayFile<C>> {
        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let abs_fs_path = overlay_path.abs_fs_path();
        let cache_path = overlay_path.overlay_path();

        debug!("Trying to open {} in the overlay", abs_fs_path.display());

        // Check if the file is already part of the overlay
        let overlay_file = fs::OpenOptions::new()
            .read(true)
            .write(writable)
            .create(false)
            .open(&*abs_fs_path);
        match overlay_file {
            Ok(file) => Ok(self.add_fs_file(&cache_path, file)),

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    let mut whiteout_path = abs_fs_path.to_owned();
                    whiteout_path.set_extension("d");
                    if whiteout_path.exists() {
                        Err(format_err!("File was deleted in the workspace").into())
                    } else {
                        self.open_cache_file(writable, abs_fs_path, cache_path)
                            .map_err(Into::into)
                    }
                } else {
                    Err(ioerr.into())
                }
            }
        }
    }

    fn list_directory<I,P>(&self, path: P) -> Result<I>
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

    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata> {
        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let abs_fs_path = overlay_path.abs_fs_path();

        if abs_fs_path.exists() {
            fs::symlink_metadata(&*abs_fs_path)
                .map_err(|e| e.into())
                .and_then(|meta| Metadata::from_fs_metadata(meta)
                    .map_err(Into::into))
        } else {
            self.resolve_object_ref(overlay_path.overlay_path())
                .and_then(|cache_ref|
                    cache_ref.ok_or(format_err!("File not found!").into()))
                .and_then(|cache_ref| self.cache.metadata(&cache_ref)
                    .map_err(Error::from))
                .and_then(|meta| Metadata::from_cache_metadata(meta).map_err(Into::into))
        }
    }

    fn delete_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let source_path = overlay_path.abs_fs_path();
        let mut target_path = source_path.to_owned();
        target_path.set_extension("d");
        if source_path.exists() {
            // Check if the underlying cache already contains the file. If yes, we just delete the
            // overlay file. Otherwise we rename the file such that a whiteout file exists.
            if self.resolve_object_ref(overlay_path.overlay_path())?.is_none() {
                debug!("Deleting: Remove overlay file {}", source_path.display());
                if source_path.is_dir() {
                    fs::remove_dir_all(source_path)
                } else {
                    fs::remove_file(source_path)
                }
            } else {
                debug!("Deleting: Moving {} to {}", source_path.display(), target_path.display());
                fs::rename(source_path, target_path)
            }.map_err(Into::into)
        } else {
            debug!("Create whiteout file {}", target_path.display());
            target_path.parent()
                .map(fs::create_dir_all).unwrap_or(Ok(()))
                .and_then(|_| fs::File::create(target_path))
                .map(|_| ())
                .map_err(Into::into)
        }
    }

    fn revert_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        OverlayPath::with_overlay_path(
            Self::file_path(&self.base_path), path.as_ref())
            .map(OverlayPath::into_abs_fs_path)
            .and_then(|fs_path|
                if fs_path.is_dir() {
                    info!("Reverting directory {} in {}",
                          path.as_ref().display(), fs_path.display());
                    fs::remove_dir_all(fs_path)
                } else {
                    info!("Reverting file {} in {}",
                          path.as_ref().display(), fs_path.display());
                    fs::remove_file(fs_path)
                }.map_err(Into::into))
    }
}

#[cfg(test)]
pub mod testutil {
    use std::{
        io::{Read, Seek, SeekFrom},
        path::{Path, PathBuf},
        collections::HashMap
    };

    use crate::hashfilecache::HashFileCache;
    use crate::overlay::*;
    use crate::nullcache::NullCache;

    use failure::Error;

    pub fn open_working_copy<P: AsRef<Path>>(path: P) -> FilesystemOverlay<HashFileCache<NullCache>> {
        let cache_dir = path.as_ref().join("cache");
        let overlay_dir = path.as_ref().join("overlay");

        let cache = HashFileCache::new(NullCache::default(), &cache_dir)
            .expect("Unable to create cache");
        FilesystemOverlay::new(cache, &overlay_dir, "master")
            .expect("Unable to create overlay")
    }

    pub fn check_file_content<F: Read+Seek>(file: &mut F, expected_content: &str) {
        file.seek(SeekFrom::Start(0)).expect("Unable to seek in test file");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("Unable to read from test file");
        assert_eq!(expected_content, content.as_str());
    }

    pub fn workspace_status_from_controller<'a, W: WorkspaceController<'a>>(controller: &'a W)
        -> Result<HashMap<PathBuf, FileState>> {
        controller.get_status()
            .and_then(|status|
                          status.map(|status|
                              status.map(|status| (status.0, status.1)))
                              .collect())
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Write, Seek, SeekFrom},
        collections::HashSet,
        path::Path,
        iter::FromIterator
    };
    use tempfile::tempdir;
    use crate::overlay::{Overlay, OverlayDirEntry, FileState, WorkspaceController};
    use super::testutil::*;

    #[cfg(target_os = "windows")]
    mod overlay_path_win {
        use crate::overlay::OverlayPath;
        use std::path::Path;

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
        crate::init_logging();

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
        crate::init_logging();

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
        crate::init_logging();

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
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());
        overlay.ensure_directory("a/nested/dir")
            .expect("Failed to create a nested directory");

        let mut test_file = overlay.open_file("a/nested/dir/test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let status =
            workspace_status_from_controller(&overlay).unwrap();
        assert_eq!(Some(&FileState::New), status.get(Path::new("a/nested/dir/test.txt")));

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
        crate::init_logging();

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
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        check_file_content(&mut test_file, "What a test!");

        let status =
            workspace_status_from_controller(&overlay).unwrap();
        assert_eq!(Some(&FileState::New),
                   status.get(Path::new("test.txt")));

        overlay.commit("A test commit").expect("Unable to create first commit");
        check_file_content(&mut test_file, "What a test!");

        debug!("Adding additional content");
        write!(test_file, "Incredible!").expect("Couldn't write to test file");
        debug!("Checking additional content");
        check_file_content(&mut test_file, "What a test!Incredible!");

        let status =
            workspace_status_from_controller(&overlay).unwrap();
        assert_eq!(Some(&FileState::Modified),
                   status.get(Path::new("test.txt")));

        drop(test_file);

        debug!("Committing additional content");
        overlay.commit("A test commit with parent")
            .expect("Unable to create second commit");

        let mut test_file = overlay.open_file("test.txt", false)
            .expect("Unable to create file");
        check_file_content(&mut test_file, "What a test!Incredible!");
    }

    #[test]
    fn delete_file_after_commit() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");

        overlay.delete_file("test.txt").expect("File deletion failed");

        assert!(overlay.get_status().unwrap()
            .any(|status|
                status.ok() == Some(("test.txt", FileState::Deleted).into())));

        overlay.open_file("test.txt", false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_file_from_workspace() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.delete_file("test.txt").expect("File deletion failed");
        overlay.open_file("test.txt", false).expect_err("File can still be opened");
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.open_file("test.txt", false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_file_from_repository() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.delete_file("test.txt").expect("File deletion failed");
        overlay.commit("Committing deleted file").expect("Unable to create second commit");
        overlay.open_file("test.txt", false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_dir_from_workspace() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory("a/dir").expect("Unable to create dir");
        let mut test_file = overlay.open_file("a/dir/test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.delete_file("a/dir").expect("Dir deletion failed");
        overlay.open_file("a/dr/test.txt", false)
            .expect_err("File can still be opened");
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.open_file("a/dir/test.txt", false)
            .expect_err("File can still be opened");
    }

    #[test]
    fn delete_directory_in_overlay() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory("a/dir").expect("Unable to create dir");
        let mut test_file = overlay.open_file("a/dir/test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.delete_file("a/dir").expect("Dir deletion failed");
        overlay.commit("Committing deleted dir").expect("Unable to create second commit");
        overlay.open_file("a/dir/test.txt", false)
            .expect_err("File can still be opened");
    }
}
