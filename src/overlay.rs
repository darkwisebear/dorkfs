use std::fs::{self, ReadDir, DirEntry, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::collections::hash_set;
use std::iter::{self, FromIterator};
use std::hash::{Hash, Hasher};
use std::fmt::{self, Formatter, Display, Debug};
use std::sync::{Arc, Weak, Mutex};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::cmp::max;
use std::borrow::Cow;
use std::vec;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

use chrono::{Utc, Offset};
use either::Either;
use futures::{
    Future, FutureExt, Stream, task::{Poll, Context},
};
use failure::{Fail, format_err};
use log::{debug, warn, info};
use difference;

use crate::{
    cache::{self, DirectoryEntry, CacheLayer, CacheRef, Commit, ReferencedCommit, CacheObject,
            CacheError, Directory},
    dispatch::{self, PathDispatcher},
    types::*,
    utility::*
};
use difference::Difference;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Directory not empty")]
    NonemptyDirectory,

    #[fail(display = "Workspace unclean")]
    UncleanWorkspace,

    #[fail(display = "File not found")]
    FileNotFound,

    #[fail(display = "IO error: {}", _0)]
    IoError(io::Error),

    #[fail(display = "Branch {} doesn't have a HEAD ref", _0)]
    MissingHeadRef(String),

    #[fail(display = "Detached HEAD")]
    DetachedHead,

    #[fail(display = "Cache error: {}", _0)]
    CacheError(CacheError),

    #[fail(display = "{}", _0)]
    Generic(failure::Error)
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
    pub size: u64,
    pub object_type: ObjectType
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
            object_type: val.1.object_type,
            size: val.1.size
        }
    }
}

pub trait OverlayFile: Read+Write+Seek {
    fn truncate(&mut self, size: u64) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileState {
    New,
    Modified,
    Deleted,
    SubmoduleUpdated
}

impl Display for FileState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let literal = match self {
            FileState::New => "new",
            FileState::Modified => "modified",
            FileState::Deleted => "deleted",
            FileState::SubmoduleUpdated => "submodule modified"
        };
        f.write_str(literal)
    }
}

pub trait Overlay {
    type File: OverlayFile+'static;
    type DirIter: Iterator<Item=Result<OverlayDirEntry>>+'static;

    fn open_file(&mut self, path: &Path, writable: bool) -> Result<Self::File>;
    fn list_directory(&self, path: &Path) -> Result<Self::DirIter>;
    fn ensure_directory(&self, path: &Path) -> Result<()>;
    fn metadata(&self, path: &Path) -> Result<Metadata>;
    fn exists(&self, path: &Path) -> bool {
        self.metadata(path).is_ok()
    }
    fn delete_file(&self, path: &Path) -> Result<()>;
    fn revert_file(&mut self, path: &Path) -> Result<()>;
}

pub trait WorkspaceLog: Iterator<Item=Result<ReferencedCommit>> { }

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceFileStatus(pub PathBuf, pub FileState);

impl<P: AsRef<Path>> From<(P, FileState)> for WorkspaceFileStatus {
    fn from(val: (P, FileState)) -> Self {
        WorkspaceFileStatus(val.0.as_ref().to_path_buf(), val.1)
    }
}

impl WorkspaceFileStatus {
    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }
}

impl Display for WorkspaceFileStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let WorkspaceFileStatus(path, state) = self;
        let state = match state {
            FileState::New => 'N',
            FileState::Modified => 'M',
            FileState::Deleted => 'D',
            FileState::SubmoduleUpdated => 'S',
        };
        write!(f, "{} {}", state, path.display())
    }
}

pub trait WorkspaceController<'a>: Debug {
    type Log: WorkspaceLog+'a;
    type LogStream: Stream<Item=Result<ReferencedCommit>>+Send+Unpin+'static;
    type StatusIter: Iterator<Item=Result<WorkspaceFileStatus>>+'a;
    type DiffFuture: Future<Output=Result<Vec<difference::Difference>>>+Send+'static;

    fn commit(&mut self, message: &str) -> Result<CacheRef>;
    fn get_current_head_ref(&self) -> Result<Option<CacheRef>>;
    fn get_current_branch(&self) -> Result<Option<Cow<str>>>;
    // TODO: Check why branch is optional
    fn switch(&mut self, target: RepoRef) -> Result<CacheRef>;
    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef<'a>>) -> Result<()>;
    fn get_log(&'a self, start_commit: &CacheRef) -> Result<Self::Log>;
    fn get_log_stream(&self, start_commit: CacheRef) -> Self::LogStream;
    fn get_status(&'a self) -> Result<Self::StatusIter>;
    fn update_head(&mut self) -> Result<CacheRef>;
    fn get_diff(&mut self, path: &Path) -> Self::DiffFuture;

    fn is_clean(&'a self) -> Result<bool> {
        self.get_status()
            .map(|mut status|
                status.next().is_none())
    }
}

#[derive(Debug)]
pub enum FSOverlayFile<C: CacheLayer+Debug> {
    FsFile(Arc<Mutex<File>>),
    CacheFile(C::File),
    BoxedFile(BoxedOverlayFile)
}

impl<C: CacheLayer+Debug> OverlayFile for FSOverlayFile<C> {
    fn truncate(&mut self, size: u64) -> Result<()> {
        if let FSOverlayFile::FsFile(ref mut file) = *self {
            let file = file.lock().unwrap();
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
                let mut file = file.lock().unwrap();
                file.read(buf)
            }
            FSOverlayFile::CacheFile(ref mut read) => read.read(buf),
            FSOverlayFile::BoxedFile(ref mut read) => read.read(buf),
        }
    }
}

impl<C: CacheLayer+Debug> Write for FSOverlayFile<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.lock().unwrap();
                file.write(buf)
            }
            FSOverlayFile::CacheFile(..) => Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            FSOverlayFile::BoxedFile(ref mut file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.lock().unwrap();
                file.flush()
            }
            FSOverlayFile::CacheFile(..) => Ok(()),
            FSOverlayFile::BoxedFile(ref mut file) => file.flush(),
        }
    }
}

impl<C: CacheLayer+Debug> Seek for FSOverlayFile<C> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match *self {
            FSOverlayFile::FsFile(ref mut file) => {
                let mut file = file.lock().unwrap();
                file.seek(pos)
            }
            FSOverlayFile::CacheFile(ref mut seek) => seek.seek(pos),
            FSOverlayFile::BoxedFile(ref mut file) => file.seek(pos),
        }
    }
}

pub trait Repository<'a>: Overlay+WorkspaceController<'a> {}
pub trait DebuggableOverlayFile: OverlayFile+Debug {}

pub type BoxedOverlayFile = Box<dyn DebuggableOverlayFile+Send+Sync>;
pub type BoxedDirIter = Box<dyn Iterator<Item=Result<OverlayDirEntry>>>;

pub type BoxedRepository = Box<dyn for<'a> Repository<'a,
    Log=Box<dyn WorkspaceLog+'a>,
    LogStream=Box<dyn Stream<Item=Result<ReferencedCommit>>+Send+Unpin>,
    StatusIter=Box<dyn Iterator<Item=Result<WorkspaceFileStatus>>+'a>,
    DiffFuture=Pin<Box<dyn Future<Output=Result<Vec<difference::Difference>>>+Send>>,
    File=BoxedOverlayFile,
    DirIter=BoxedDirIter>
+Send+Sync>;

#[derive(Debug)]
pub struct RepositoryWrapper<R>(R) where for<'a> R: Repository<'a>+Debug;

impl<R> RepositoryWrapper<R> where for<'a> R: Repository<'a>+Debug {
    pub fn new(repo: R) -> Self {
        RepositoryWrapper(repo)
    }
}

impl<T> OverlayFile for Box<T> where T: OverlayFile+?Sized {
    fn truncate(&mut self, size: u64) -> Result<()> {
        self.as_mut().truncate(size)
    }
}

impl<R> Overlay for RepositoryWrapper<R> where for <'a> R: Repository<'a>+Debug,
                                               <R as Overlay>::File: DebuggableOverlayFile+Send+Sync {
    type File = Box<dyn DebuggableOverlayFile+Send+Sync>;
    type DirIter = Box<dyn Iterator<Item=Result<OverlayDirEntry>>>;

    fn open_file(&mut self, path: &Path, writable: bool) -> Result<Self::File> {
        self.0.open_file(path, writable)
            .map(|f| Box::new(f) as Box<dyn DebuggableOverlayFile+Send+Sync>)
    }

    fn list_directory(&self, path: &Path) -> Result<Self::DirIter> {
        self.0.list_directory(path).map(|dir| Box::new(dir) as Self::DirIter)
    }

    fn ensure_directory(&self, path: &Path) -> Result<()> {
        self.0.ensure_directory(path)
    }

    fn metadata(&self, path: &Path) -> Result<Metadata> {
        self.0.metadata(path)
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        self.0.delete_file(path)
    }

    fn revert_file(&mut self, path: &Path) -> Result<()> {
        self.0.revert_file(path)
    }
}

impl<T> WorkspaceLog for Box<T> where T: WorkspaceLog+?Sized {}

impl<'a, R> WorkspaceController<'a> for RepositoryWrapper<R> where for<'b> R: Repository<'b>+Debug {
    type Log = Box<dyn WorkspaceLog+'a>;
    type LogStream = Box<dyn Stream<Item=Result<ReferencedCommit>>+Send+Unpin>;
    type StatusIter = Box<dyn Iterator<Item=Result<WorkspaceFileStatus>>+'a>;
    type DiffFuture = Pin<Box<dyn Future<Output=Result<Vec<difference::Difference>>>+Send>>;

    fn commit(&mut self, message: &str) -> Result<CacheRef> {
        self.0.commit(message)
    }

    fn get_current_head_ref(&self) -> Result<Option<CacheRef>> {
        self.0.get_current_head_ref()
    }

    fn get_current_branch(&self) -> Result<Option<Cow<str>>> {
        self.0.get_current_branch()
    }

    fn switch(&mut self, target: RepoRef) -> Result<CacheRef> {
        self.0.switch(target)
    }

    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef<'a>>) -> Result<()> {
        self.0.create_branch(new_branch, repo_ref)
    }

    fn get_log(&'a self, start_commit: &CacheRef) -> Result<Self::Log> {
        self.0.get_log(start_commit).map(|log|
            Box::new(log) as Box<dyn WorkspaceLog>)
    }

    fn get_log_stream(&self, start_commit: CacheRef) -> Self::LogStream {
        Box::new(self.0.get_log_stream(start_commit)) as Self::LogStream
    }

    fn get_status(&'a self) -> Result<Self::StatusIter> {
        self.0.get_status().map(|iter|
            Box::new(iter) as Box<dyn Iterator<Item=Result<WorkspaceFileStatus>>>)
    }

    fn update_head(&mut self) -> Result<CacheRef> {
        self.0.update_head()
    }

    fn get_diff(&mut self, path: &Path) -> Self::DiffFuture {
        Box::pin(self.0.get_diff(path)) as Self::DiffFuture
    }
}

impl<'a, R> Repository<'a> for RepositoryWrapper<R>
    where for<'b> R: Repository<'b>,
          <R as Overlay>::File: DebuggableOverlayFile+Send+Sync {}

impl Overlay for BoxedRepository {
    type File = Box<dyn DebuggableOverlayFile+Send+Sync>;
    type DirIter = Box<dyn Iterator<Item=Result<OverlayDirEntry>>>;

    fn open_file(&mut self, path: &Path, writable: bool) -> Result<Self::File> {
        self.deref_mut().open_file(path, writable)
    }

    fn list_directory(&self, path: &Path) -> Result<Self::DirIter> {
        self.deref().list_directory(path)
    }

    fn ensure_directory(&self, path: &Path) -> Result<()> {
        self.deref().ensure_directory(path)
    }

    fn metadata(&self, path: &Path) -> Result<Metadata> {
        self.deref().metadata(path)
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        self.deref().delete_file(path)
    }

    fn revert_file(&mut self, path: &Path) -> Result<()> {
        self.deref_mut().revert_file(path)
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
                        .ok_or_else(|| format_err!("Unable to convert path component to UTF-8"))?
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

    #[cfg(test)]
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
enum OverlayOperation<T> {
    Add(T),
    Subtract(T)
}

#[derive(Debug, Clone)]
struct WorkspaceHead {
    cache_ref: Option<CacheRef>,
    meta_path: PathBuf
}

impl WorkspaceHead {
    fn new(meta_path: PathBuf) -> Result<WorkspaceHead> {
        let new_head_file_path = meta_path.join("NEW_HEAD");
        if new_head_file_path.exists() {
            unimplemented!("TODO: Recover from a previous crash during commit");
        }

        let head_path = meta_path.join("HEAD");
        let cache_ref  = if head_path.exists() {
            File::open(&head_path)
                .and_then(|mut file| {
                    let mut head = String::new();
                    file.read_to_string(&mut head).map(|_| head)
                })
                .map_err(Error::IoError)
                .and_then(|head|
                    if head == CacheRef::null().to_string() {
                        Ok(None)
                    } else {
                        head.parse()
                            .map_err(Error::Generic)
                            .map(Option::Some)
                    })
        } else {
            Ok(None)
        };

        cache_ref.map(move |cache_ref|
            WorkspaceHead {
                cache_ref,
                meta_path
            })
    }

    fn get_ref(&self) -> Option<CacheRef> {
        self.cache_ref
    }

    fn set_ref(&mut self, new_ref: Option<CacheRef>) -> Result<()> {
        let head_file_path = self.meta_path.join("HEAD");
        let new_head_file_path = self.meta_path.join("NEW_HEAD");
        fs::remove_file(&new_head_file_path).ok();

        let commit_ref = new_ref.unwrap_or_else(CacheRef::null);

        let mut new_head_file = File::create(&new_head_file_path)?;
        new_head_file.write_all(commit_ref.to_string().as_bytes())?;
        new_head_file.flush()?;
        new_head_file.sync_all()?;

        match fs::remove_file(&head_file_path) {
            Ok(_) => (),
            Err(ref err) if err.kind() == io::ErrorKind::NotFound => (),
            Err(err) => return Err(Error::IoError(err))
        }
        fs::rename(new_head_file_path, head_file_path)?;

        self.cache_ref = new_ref;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FilesystemOverlay<C> where C: CacheLayer {
    cache: Arc<C>,
    head: WorkspaceHead,
    overlay_files: HashMap<PathBuf, Weak<Mutex<File>>>,
    base_path: PathBuf,
    branch: Option<String>,
    submodules: PathDispatcher<BoxedRepository>
}

impl<C> FilesystemOverlay<C> where C: CacheLayer+Debug+Send+Sync+'static,
                                   <C as CacheLayer>::File: Send {
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

    pub fn add_submodule<P>(&mut self, path: P, submodule: BoxedRepository) -> Result<()>
        where P: AsRef<Path> {
        self.submodules.add_overlay(path.as_ref(), submodule)
            .map(|s| if s.is_some() {
                warn!("Submodule replaced");
            })
            .map_err(Error::Generic)
    }

    pub fn new(cache: C, base_path: Cow<Path>, start_ref: RepoRef) -> Result<Self> {
        fs::create_dir_all(Self::file_path(base_path.as_ref()))?;
        let meta_path = Self::meta_path(base_path.as_ref());
        fs::create_dir_all(&meta_path)?;
        let head = WorkspaceHead::new(meta_path)?;

        let branch = match start_ref {
            RepoRef::Branch(branch) => Some(branch.to_string()),
            RepoRef::CacheRef(_) => None
        };

        let mut fs = FilesystemOverlay {
            cache: Arc::new(cache),
            head,
            overlay_files: HashMap::new(),
            base_path: base_path.into_owned(),
            branch,
            submodules: PathDispatcher::new()
        };

        if fs.is_clean()? {
            if let RepoRef::CacheRef(cache_ref) = start_ref {
                info!("Updating repository to {}", cache_ref);
                fs.head.set_ref(Some(cache_ref))?;
            }
        }

        Ok(fs)
    }

    fn resolve_object_ref<P: AsRef<Path>>(&self, path: P) -> Result<Option<CacheRef>> {
        match self.head.get_ref() {
            Some(head) => {
                let commit = self.cache.get(&head)?.into_commit()
                    .expect("Head ref is not a commit");
                cache::resolve_object_ref(self.cache.as_ref(), &commit, path)
                    .map_err(Error::Generic)
            }

            None => Ok(None)
        }
    }

    fn dir_entry_to_directory_entry(&self, dir_entry: &DirEntry, mut overlay_path: OverlayPath)
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

        if fs_name.ends_with('f') {
            let (cache_ref, object_type) = if file_type.is_dir() {
                let cache_ref = self.generate_tree(&overlay_path)?;
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
                name,
                size: dir_entry.metadata()?.len()
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
                name,
                size: 0u64
            };

            debug!("Subtract entry {}", directory_entry.name.as_str());
            Ok(OverlayOperation::Subtract(directory_entry))
        }
    }

    fn iter_directory<T,F,G>(&self,
                             overlay_path: &OverlayPath,
                             mut from_directory_entry: F,
                             mut from_dir_entry: G) -> Result<HashSet<T>>
        where T: Eq+Hash,
              F: FnMut(DirectoryEntry) -> Result<T>,
              G: FnMut(&DirEntry) -> Result<OverlayOperation<T>> {
        // Get the directory contents from the cache if it's already there
        let dir_entries_result: Result<HashSet<T>> =
            if let Some(cache_dir_ref) = self.resolve_object_ref(overlay_path.overlay_path())? {
                debug!("Reading dir {} with ref {} from cache", overlay_path.overlay_path().display(), &cache_dir_ref);
                let dir = self.cache.get(&cache_dir_ref)?
                    .into_directory()?;
                dir.into_iter().map(&mut from_directory_entry).collect()
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
                match from_dir_entry(&dir_entry?)? {
                    OverlayOperation::Add(entry) => { dir_entries.replace(entry); }
                    OverlayOperation::Subtract(entry) => { dir_entries.remove(&entry); }
                }
            }
        } else {
            debug!("...but path doesn't exist");
        }

        // Now check for changed submodules
        for (overlay, subpath) in &self.submodules {
            let parent = match subpath.parent() {
                Some(parent) => parent,
                None => {
                    warn!("Unable to get parent directory of submodule {}. Skipping.",
                          subpath.display());
                    continue;
                }
            };

            // If the submodule is located in this directory
            if parent == overlay_path.overlay_path() {
                let submodule_head_ref = match overlay.get_current_head_ref()? {
                    Some(cache_ref) => cache_ref,
                    None => {
                        info!("Submodule {} doesn't have a commit yet. Skipping.", subpath.display());
                        continue;
                    }
                };

                let gitlink_name = match subpath.file_name() {
                    Some(filename) => filename,
                    None => {
                        warn!("Unable to get directory name of submodule {}. Skipping.",
                              subpath.display());
                        continue;
                    }
                };

                let directory_entry = DirectoryEntry {
                    name: os_string_to_string(gitlink_name.to_os_string())?,
                    cache_ref: submodule_head_ref,
                    object_type: cache::ObjectType::Commit,
                    size: 0
                };
                dir_entries.replace(from_directory_entry(directory_entry)?);
            }
        }

        Ok(dir_entries)
    }

    fn dir_entry_to_overlay_dir_entry(dir_entry: &fs::DirEntry)
        -> Result<OverlayOperation<OverlayDirEntry>> {
        let mut overlay_path = OverlayPath::new("");
        let fs_name = os_string_to_string(dir_entry.file_name())?;
        overlay_path.push_fs(&fs_name);
        let name = os_string_to_string(overlay_path.overlay_path().as_os_str().to_owned())?;

        let metadata = Metadata::from_fs_metadata(&dir_entry.metadata()?)?;
        let entry = OverlayDirEntry {
            name,
            size: metadata.size,
            object_type: metadata.object_type
        };

        if fs_name.ends_with('f') {
            Ok(OverlayOperation::Add(entry))
        } else {
            Ok(OverlayOperation::Subtract(entry))
        }
    }

    fn directory_entry_to_overlay_dir_entry(&self, dir_entry: DirectoryEntry)
        -> Result<OverlayDirEntry> {
        let object_type = ObjectType::from_cache_object_type(dir_entry.object_type)?;
        Ok(OverlayDirEntry {
            name: dir_entry.name,
            object_type,
            size: dir_entry.size
        })
    }

    fn generate_tree(&self, overlay_path: &OverlayPath) -> Result<CacheRef> {
        let mut directory = self.iter_directory(
            &overlay_path,
            Ok,
            |e|
                self.dir_entry_to_directory_entry(&e, overlay_path.clone()))?.into_iter();

        self.cache.add_directory(&mut directory as &mut dyn Iterator<Item=DirectoryEntry>)
            .map_err(Into::into)
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
                debug!("Removing overlay dir {}", path.abs_fs_path().display());
                if let Err(ioerr) = fs::remove_dir(&*path.abs_fs_path()) {
                    warn!("Unable to remove overlay directory {} during cleanup: {}",
                          path.abs_fs_path().display(),
                          ioerr);
                }
            } else if !self.overlay_files.contains_key(path.overlay_path()) {
                debug!("Removing overlay file {}", path.abs_fs_path().display());
                if let Err(ioerr) = fs::remove_file(&*path.abs_fs_path()) {
                    warn!("Unable to remove overlay file {} during cleanup: {}",
                          path.abs_fs_path().display(),
                          ioerr);
                }
            }

            path.pop();
        }

        Ok(())
    }

    fn clear_closed_files(&mut self) -> Result<()> {
        self.overlay_files.retain(|_, file| file.upgrade().is_some());

        debug!("Currently open files: {:?}", self.overlay_files.keys().collect::<Vec<&PathBuf>>());

        let mut base_path = OverlayPath::new(Self::file_path(&self.base_path));
        self.clear_closed_files_in_path(&mut base_path)
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

            Ok(self.add_fs_file(&cache_path, new_file))
        } else if let Some(cache_file) = cache_file {
                Ok(FSOverlayFile::CacheFile(cache_file))
        } else {
            Err(Error::FileNotFound)
        }
    }

    fn contains_files<P: AsRef<Path>>(path: P) -> Result<bool> {
        path.as_ref().read_dir().map_err(Error::IoError)
            .map(|mut dir_iter| dir_iter.next().and_then(io::Result::ok))
            .map(|entry| entry.is_some())
    }

    #[cfg(test)]
    pub fn set_head(&mut self, cache_ref: CacheRef) -> Result<()> {
        self.head.set_ref(Some(cache_ref))
    }

    pub fn get_cache<'a>(&'a self) -> impl Deref<Target=C>+'a {
        self.cache.as_ref()
    }

    fn get_head_commit(&self) -> Result<Option<Commit>> {
        self.head.cache_ref
            .map(|head_ref| self.cache.get(&head_ref)
                .and_then(CacheObject::into_commit)
                .map_err(Error::CacheError))
            .transpose()
    }

    fn create_diff_future<P: AsRef<Path>>(&mut self, path: P) -> impl Future<Output=Result<Vec<Difference>>> {
        let repo_file =
            self.open_file(path.as_ref(), false);
        let head = self.head.cache_ref;
        let cache = Arc::clone(&self.cache);
        async move {
            let head = head
                .ok_or_else(|| Error::MissingHeadRef("Unable to generate diff wthout HEAD".to_string()))?;
            let head_commit = cache.get_poll(&head).await?.into_commit()?;
            let base_file_ref = cache::resolve_object_ref(cache.as_ref(), &head_commit, path.as_ref())?
                .ok_or_else(|| format_err!("Unable to resolve {} to create diff", path.as_ref().display()))?;
            let base_file = cache.get_poll(&base_file_ref).await?;

            let differences = match base_file {
                CacheObject::File(mut file) => {
                    let mut base_bytes = Vec::new();
                    file.read_to_end(&mut base_bytes)?;
                    let mut modified_bytes = Vec::new();
                    repo_file?.read_to_end(&mut modified_bytes)?;
                    let base_str = String::from_utf8_lossy(&base_bytes[..]);
                    let modified_str = String::from_utf8_lossy(&modified_bytes[..]);
                    difference::Changeset::new(base_str.as_ref(), modified_str.as_ref(), "\n").diffs
                }
                CacheObject::Directory(_) =>
                    unreachable!("Directories shouldn't be part of a workspace status."),
                // TODO: Get current commit ref and display as Difference::Add
                CacheObject::Commit(_) => vec![Difference::Rem(base_file_ref.to_string())],
                // TODO: Get current symlink contents and display as Difference::Add
                CacheObject::Symlink(symlink) => vec![Difference::Rem(symlink)]
            };

            Ok(differences)
        }
    }
}

pub struct CacheLayerLog<C: CacheLayer, T: Deref<Target=C>> {
    cache: T,
    next_cache_ref: Option<CacheRef>
}

impl<C: CacheLayer, T: Deref<Target=C>> CacheLayerLog<C, T> {
    fn new(cache: T, commit_ref: &CacheRef) -> Result<Self> {
        let log = CacheLayerLog {
            cache,
            next_cache_ref: Some(*commit_ref)
        };

        Ok(log)
    }
}

impl<C: CacheLayer, T: Deref<Target=C>> WorkspaceLog for CacheLayerLog<C, T> { }

impl<C: CacheLayer, T: Deref<Target=C>> Iterator for CacheLayerLog<C, T> {
    type Item = Result<ReferencedCommit>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cache_ref.take().and_then(|cache_ref| {
            match self.cache.deref().get(&cache_ref) {
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

pub struct CacheLayerLogStream<C> where C: CacheLayer {
    cache: Arc<C>,
    current_poll: Option<(CacheRef, <C as CacheLayer>::GetFuture)>
}

impl<C> Debug for CacheLayerLogStream<C> where C: CacheLayer+Debug {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("CacheLayerLogStream")
            .field("cache", &self.cache)
            .field("current_poll",
                   &self.current_poll.as_ref().map(|(cache_ref, _)| cache_ref))
            .finish()
    }
}

impl<C> CacheLayerLogStream<C> where C: CacheLayer {
    fn new(start_commit: CacheRef, cache: Arc<C>) -> Self {
        let curr_poll = cache.get_poll(&start_commit);
        Self {
            cache,
            current_poll: Some((start_commit, curr_poll))
        }
    }
}

impl<C> Stream for CacheLayerLogStream<C> where C: CacheLayer {
    type Item = Result<ReferencedCommit>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.current_poll.take() {
            Some((cur_ref, mut cur_poll)) => {
                match cur_poll.poll_unpin(cx) {
                    Poll::Ready(Ok(obj)) => {
                        let commit = obj.into_commit()?;

                        if !commit.parents.is_empty() {
                            if commit.parents.len() > 1 {
                                warn!("Non-linear history not supported yet!");
                            }

                            let new_poll =
                                self.cache.get_poll(&commit.parents[0]);
                            self.current_poll = Some((commit.parents[0], new_poll));
                        }

                        Poll::Ready(Some(Ok(ReferencedCommit(cur_ref, commit))))
                    }

                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(Error::from(e)))),

                    Poll::Pending => {
                        self.current_poll = Some((cur_ref, cur_poll));
                        Poll::Pending
                    }
                }
            }

            None => Poll::Ready(None)
        }
    }
}

impl<'a, C> WorkspaceController<'a> for FilesystemOverlay<C>
    where C: CacheLayer+Debug+Send+Sync+'static, <C as CacheLayer>::File: Send {
    type Log = CacheLayerLog<C, &'a C>;
    type LogStream = CacheLayerLogStream<C>;
    type StatusIter = iter::Chain<FSStatusIter<'a, C>, BoxedRepoDispatcherIter<'a, C>>;
    type DiffFuture = Pin<Box<dyn Future<Output=Result<Vec<Difference>>>+Send>>;

    fn commit(&mut self, message: &str) -> Result<CacheRef> {
        // lock all opened files to block writes as long as the commit is pending
        let open_files = self.overlay_files.drain()
            .filter_map(|(path, file)|
                file.upgrade().map(move |file| (path, file)))
            .collect::<Vec<_>>();
        let mut locks = open_files.iter()
            .map(|(path, ptr)| (path, ptr.lock().unwrap()))
            .collect::<Vec<_>>();

        let file_path = Self::file_path(&self.base_path);
        let overlay_path = OverlayPath::new(&file_path);

        let tree = self.generate_tree(&overlay_path)?;
        let parents = Vec::from_iter(self.head.get_ref().into_iter());
        let time = Utc::now();
        let commit = Commit {
            parents,
            tree,
            message: message.to_owned(),
            committed_date: time.with_timezone(&Utc.fix())
        };

        // Commit
        let new_commit_ref = self.cache.add_commit(commit)
            .and_then(|cache_ref| match self.branch {
                Some(ref branch_name) =>
                    self.cache.merge_commit(branch_name, &cache_ref),
                None => Ok(cache_ref)
            })?;

        // Set HEAD to (possibly merged) new head commit
        let result = self.head.set_ref(Some(new_commit_ref))
            .map(|_| new_commit_ref);

        debug!("Updating overlay");
        for (path, overlay_file) in locks.iter_mut() {
            // If the file is still open, update its contents
            debug!("Updating file {}", path.display());
            self.resolve_object_ref(path)
                .and_then(|cache_ref| match cache_ref {
                    Some(cache_ref) => self.cache.get(&cache_ref)
                        .and_then(CacheObject::into_file)
                        .map_err(Error::CacheError)
                        .and_then(|mut cache_file| {
                            let cur_pos = overlay_file.seek(SeekFrom::Current(0))?;
                            overlay_file.seek(SeekFrom::Start(0))?;
                            let amount = io::copy(&mut cache_file,
                                                  &mut **overlay_file)?;
                            // truncate file if copied amount is less than the current size or
                            // enlarge it if it has become smaller than the previous write position
                            overlay_file.set_len(max(amount, cur_pos))?;
                            overlay_file.seek(SeekFrom::Start(cur_pos))
                                .map(|_| ())
                                .map_err(Error::IoError)
                        }),
                    None => Ok(())
                })?;
        }

        drop(locks);
        self.overlay_files.extend(open_files.into_iter()
            .map(|(path, file)| (path, Arc::downgrade(&file))));

        self.clear_closed_files()?;

        result
    }

    fn get_current_head_ref(&self) -> Result<Option<CacheRef>> {
        Ok(self.head.get_ref())
    }

    fn get_current_branch(&self) -> Result<Option<Cow<str>>> {
        Ok(self.branch.as_ref().map(|b| Cow::Borrowed(b.as_str())))
    }

    fn switch(&mut self, target: RepoRef) -> Result<CacheRef> {
        match target {
            RepoRef::Branch(branch) => {
                if !self.is_clean()? {
                    return Err(Error::UncleanWorkspace);
                }

                self.branch = Some(branch.to_string());
                self.cache.get_head_commit(branch)
                    .map_err(Into::into)
                    .and_then(|cache_ref|
                        cache_ref.ok_or_else(|| Error::MissingHeadRef(self.branch.clone().unwrap())))
                    .and_then(|cache_ref|
                        self.head.set_ref(Some(cache_ref)).map(|_| cache_ref))
            }

            RepoRef::CacheRef(cache_ref) => {
                self.branch = None;
                self.head.set_ref(Some(cache_ref))?;
                Ok(cache_ref)
            }
        }
    }

    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef<'a>>)
        -> Result<()> {
        let base_ref = match repo_ref {
            Some(RepoRef::CacheRef(cache_ref)) => cache_ref,
            Some(RepoRef::Branch(branch)) => self.cache.get_head_commit(branch)
                .map_err(Error::CacheError)
                .and_then(|cache_ref|
                    cache_ref.ok_or_else(|| format_err!("Branch {} doesn't exsit", branch).into()))?,
            None => self.head.get_ref()
                .ok_or_else(|| Error::Generic(format_err!("No HEAD in current workspace set")))?,
        };

        self.cache.create_branch(new_branch, &base_ref)
            .map_err(Into::into)
    }

    fn get_log(&'a self, start_commit: &CacheRef) -> Result<Self::Log> {
        CacheLayerLog::new(&self.cache, start_commit)
    }

    fn get_log_stream(&self, start_commit: CacheRef) -> Self::LogStream {
        CacheLayerLogStream::new(start_commit, Arc::clone(&self.cache))
    }

    fn get_status(&'a self) -> Result<Self::StatusIter> {
        let path = OverlayPath::new(Self::file_path(&self.base_path));
        Ok(FSStatusIter::new(path, self)
            .chain(BoxedRepoDispatcherIter::new(self)?))
    }

    fn update_head(&mut self) -> Result<CacheRef> {
        if !self.is_clean()? {
            return Err(Error::UncleanWorkspace);
        }

        self.branch.as_ref().ok_or(Error::DetachedHead)
            .and_then(|branch|
                self.cache.get_head_commit(branch.as_str())
                    .map_err(Into::into))
            .and_then(|cache_ref|
                cache_ref.ok_or_else(|| Error::MissingHeadRef(self.branch.clone().unwrap())))
            .and_then(|cache_ref|
                self.head.set_ref(Some(cache_ref)).map(|_| cache_ref))
    }

    fn get_diff(&mut self, path: &Path) -> Self::DiffFuture {
        self.create_diff_future(PathBuf::from(path)).boxed::<'static>() as Self::DiffFuture
    }
}

pub struct BoxedRepoDispatcherIter<'a, C>(dispatch::Iter<'a, BoxedRepository>, &'a FilesystemOverlay<C>)
    where C: CacheLayer+Debug+Send+Sync+'static;

impl<'a, C> BoxedRepoDispatcherIter<'a, C> where C: CacheLayer+Debug+Send+Sync+'static {
    fn new(overlay: &'a FilesystemOverlay<C>) -> Result<Self> {
        let iter = overlay.submodules.iter();
        Ok(BoxedRepoDispatcherIter(iter, overlay))
    }
}

impl<'a, C> Iterator for BoxedRepoDispatcherIter<'a, C>
    where C: CacheLayer+Debug+Send+Sync+'static, <C as CacheLayer>::File: Send {
    type Item = Result<WorkspaceFileStatus>;

    fn next(&mut self) -> Option<Self::Item> {
        let BoxedRepoDispatcherIter(iter, fs) = self;

        let head_commit = match fs.head.cache_ref {
            Some(commit) => commit,
            None => return None,
        };

        iter.by_ref().filter_map(|(overlay, submodule_path)| {
            let cache = fs.get_cache();
            let gitlink_ref = match cache::get_gitlink(&*cache, &head_commit, submodule_path) {
                Ok(gitlink_ref)  => gitlink_ref,
                Err(e) => return Some(Err(Error::Generic(e)))
            };

            let head_ref = match overlay.get_current_head_ref()
                .and_then(|r|
                    r.ok_or_else(|| Error::MissingHeadRef(format!("No head ref for submodule {}",
                                                              submodule_path.display())))) {
                Ok(head) => head,
                Err(e) => return Some(Err(e))
            };

            if gitlink_ref != head_ref {
                Some(Ok(WorkspaceFileStatus(submodule_path.to_path_buf(),
                                            FileState::SubmoduleUpdated)))
            } else {
                None
            }
        }).next()
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

impl<'a, C> Iterator for FSStatusIter<'a, C> where C: CacheLayer+Debug+Send+Sync+'static,
                                                   <C as CacheLayer>::File: Send {
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
                                            Err(err) => break Some(Err(err))
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

type AddResultFn<T> = fn(T) -> Result<T>;
type MapAddResultToHashSetIter<T> = iter::Map<hash_set::IntoIter<T>, AddResultFn<T>>;
type HashOrBoxedDirIter = Either<MapAddResultToHashSetIter<OverlayDirEntry>, BoxedDirIter>;

impl<C> Overlay for FilesystemOverlay<C> where C: CacheLayer+Debug+Send+Sync+'static,
                                               <C as CacheLayer>::File: Send {
    type File = FSOverlayFile<C>;
    type DirIter = HashOrBoxedDirIter;

    fn open_file(&mut self, path: &Path, writable: bool)
        -> Result<FSOverlayFile<C>> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay_mut(path)? {
            return overlay.open_file(subpath, writable).map(FSOverlayFile::BoxedFile);
        }

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
                    let whiteout_exists = whiteout_path.exists();

                    if whiteout_exists && !writable {
                        info!("File was deleted in the workspace");
                        Err(Error::FileNotFound)
                    } else {
                        if whiteout_exists {
                            fs::remove_file(whiteout_path)?;
                        }

                        self.open_cache_file(writable, abs_fs_path, cache_path)
                            .map_err(Into::into)
                    }
                } else {
                    Err(ioerr.into())
                }
            }
        }
    }

    fn list_directory(&self, path: &Path) -> Result<Self::DirIter> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay(path)? {
            return overlay.list_directory(subpath).map(Either::Right);
        }

        let entries = OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)
            .and_then(|overlay_path|
                self.iter_directory(
                    &overlay_path,
                    |dir_entry| {
                        self.directory_entry_to_overlay_dir_entry(dir_entry)
                    },
                    Self::dir_entry_to_overlay_dir_entry))?;
        let dir_iter = entries.into_iter()
            .map(Ok as AddResultFn<OverlayDirEntry>);

        Ok(Either::Left(dir_iter))
    }

    fn ensure_directory(&self, path: &Path) -> Result<()> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay(path)? {
            return overlay.ensure_directory(subpath);
        }

        let overlay_path = OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        fs::create_dir_all(&*overlay_path.abs_fs_path())
            .map_err(|e| e.into())
    }

    fn metadata(&self, path: &Path) -> Result<Metadata> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay(path)? {
            return overlay.metadata(subpath);
        }

        let overlay_path =
            OverlayPath::with_overlay_path(Self::file_path(&self.base_path), path)?;
        let abs_fs_path = overlay_path.abs_fs_path();

        if abs_fs_path.exists() {
            fs::symlink_metadata(&*abs_fs_path)
                .map_err(|e| e.into())
                .and_then(|meta| Metadata::from_fs_metadata(&meta)
                    .map_err(Into::into))
        } else {
            let path = overlay_path.overlay_path();
            let file_name = path.file_name()
                .ok_or_else(|| Error::Generic(format_err!("Path doesn't contain a name")))?;
            let parent_dir = path.parent().unwrap_or_else(|| Path::new(""));
            debug!("Find metadata in {} for {}", parent_dir.display(), file_name.to_string_lossy());
            let dir_entry = self.resolve_object_ref(parent_dir)
                .and_then(|cache_ref|
                    cache_ref.ok_or_else(|| format_err!("Parent directory not found!").into()))
                .and_then(|cache_ref| self.cache.get(&cache_ref)
                    .and_then(|obj| obj.into_directory())
                    .map_err(Error::from))
                .and_then(|dir|
                    dir.find_entry(&file_name).cloned()
                        .ok_or_else(|| Error::Generic(
                            format_err!(r#"File "{}" not found in directory"#,
                            file_name.to_string_lossy()))))?;

            let head_commit = self.get_head_commit()
                .and_then(|commit|
                    commit.ok_or_else(|| Error::Generic(format_err!("Inexistent HEAD commit"))))?;

            let metadata = Metadata {
                object_type: ObjectType::from_cache_object_type(dir_entry.object_type)?,
                size: dir_entry.size,
                modified_date: head_commit.committed_date.with_timezone(&Utc)
            };

            Ok(metadata)
        }
    }

    fn delete_file(&self, path: &Path) -> Result<()> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay(path)? {
            return overlay.delete_file(subpath);
        }

        let obj_type = self.metadata(path)?.object_type;
        if obj_type == ObjectType::Directory &&
            self.list_directory(path)?.next().is_some() {
                return Err(Error::NonemptyDirectory)
        }

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
                    fs::remove_dir(source_path)
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

    fn revert_file(&mut self, path: &Path) -> Result<()> {
        if let Some((overlay, subpath)) = self.submodules.get_overlay_mut(path)? {
            if subpath.as_os_str() == "" {
                let head_ref = self.head.cache_ref
                    .ok_or_else(|| Error::MissingHeadRef(format!("Cannot revert submodule at {}", path.display())))?;
                let gitref = cache::get_gitlink(&*self.cache, &head_ref, path)?;
                debug!("Resetting submodule at {} to {}", path.display(), &gitref);
                return overlay.switch(RepoRef::CacheRef(gitref)).map(|_| ());
            } else {
                return overlay.revert_file(subpath);
            }
        }

        use std::path::Component::Normal;

        if !path.is_relative() {
            return Err("Can only operate on relative paths".into());
        }

        let mut cur_path = Self::file_path(&self.base_path);
        let mut num_components = 0;
        for component in path.components() {
            if let Normal(part) = component {
                num_components+=1;
                let mut part = part.to_owned();
                part.push(".d");
                cur_path.push(part);
                debug!("Check existence of {}", cur_path.display());
                if cur_path.exists() {
                    let mut restored_path = cur_path.clone();
                    restored_path.set_extension("f");
                    debug!("Restoring {} to {}", cur_path.display(), restored_path.display());
                    fs::rename(&cur_path, restored_path).map_err(Error::IoError)?;
                }
                cur_path.set_extension("f");
            } else {
                return Err("Unable to iterate through the overlay path".into());
            }
        }

        if cur_path.exists() {
            debug!("Remove overlay file {}", cur_path.display());
            if cur_path.is_dir() {
                fs::remove_dir_all(&cur_path)
            } else {
                fs::remove_file(&cur_path)
            }.map_err(Error::IoError)?;
        } else {
            debug!("File {} unchanged", path.display());
        }

        // Now check for all parent directories in the overlay whether they still contain files.
        // If not, we can safely remove them.
        for _ in 1..num_components {
            cur_path.pop();
            let contains_files = Self::contains_files(&cur_path)?;
            debug!("{} contains {}files",
                   cur_path.display(),
                   if contains_files { "" } else { "no " });
            if !contains_files {
                debug!("Removing overlay directory {}", cur_path.display());
                fs::remove_dir(&cur_path).map_err(Error::IoError)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod testutil {
    use std::{
        io::{Read, Seek, SeekFrom},
        path::{Path, PathBuf},
        collections::HashMap,
        borrow::Cow
    };

    use crate::hashfilecache::HashFileCache;
    use crate::overlay::*;
    use crate::nullcache::NullCache;
    use crate::types::RepoRef;

    pub fn open_working_copy<P: AsRef<Path>>(path: P) -> FilesystemOverlay<HashFileCache<NullCache>> {
        let cache_dir = path.as_ref().join("cache");
        let overlay_dir = path.as_ref().join("overlay");

        let cache = HashFileCache::new(NullCache::default(), &cache_dir)
            .expect("Unable to create cache");
        FilesystemOverlay::new(cache,
                               Cow::Borrowed(&overlay_dir),
                               RepoRef::Branch("master"))
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
        iter::FromIterator,
        borrow::Cow
    };
    use tempfile::tempdir;
    use chrono::{Utc, Offset};
    use log::debug;
    use crate::overlay::{Overlay, OverlayFile, OverlayDirEntry, FileState, WorkspaceController};
    use crate::types::RepoRef;
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

        let test_path = Path::new("test.txt");

        let mut test_file = overlay.open_file(test_path, true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let mut staged_file = overlay.open_file(test_path, false)
            .expect("Unable to open staged file!");
        let mut contents = String::new();
        staged_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(staged_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut committed_file = overlay.open_file(test_path, false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(committed_file);

        let metadata = overlay.metadata(test_path)
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);

        let mut editable_file = overlay.open_file(test_path, true)
            .expect("Unable to open file for writing!");
        editable_file.seek(SeekFrom::End(0)).expect("Unable to seek to end!");
        write!(editable_file, "Yay!")
            .expect("Unable to append to file!");
        drop(editable_file);

        let mut changed_file = overlay.open_file(test_path, false)
            .expect("Unable to open changed file!");
        let mut contents = String::new();
        changed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!Yay!");
        drop(changed_file);

        let metadata = overlay.metadata(test_path)
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!Yay!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);
    }

    #[test]
    fn two_commits() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let test_path = Path::new("test.txt");
        let test_path2 = Path::new("test2.txt");

        let mut test_file = overlay.open_file(test_path, true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut additional_file = overlay.open_file(test_path2, true)
            .expect("Unable to create file");
        write!(additional_file, "Another file").expect("Couldn't write to test file");
        drop(additional_file);

        overlay.commit("A test commit with parent").expect("Unable to commit");

        let mut first_file = overlay.open_file(test_path, false)
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

        let mut test_file = overlay.open_file(Path::new("test.txt").into(), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        drop(overlay);

        let mut overlay = open_working_copy(tempdir.path());

        let mut committed_file = overlay.open_file(Path::new("test.txt"), false)
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
        overlay.ensure_directory(Path::new("a/nested/dir"))
            .expect("Failed to create a nested directory");

        let mut test_file = overlay.open_file(Path::new("a/nested/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let status =
            workspace_status_from_controller(&overlay).unwrap();
        assert_eq!(Some(&FileState::New), status.get(Path::new("a/nested/dir/test.txt")));

        overlay.commit("A test commit").expect("Unable to commit");

        for writable in &[true, false] {
            let mut committed_file = overlay.open_file(Path::new("a/nested/dir/test.txt"), *writable)
                .expect(format!("Unable to open committed file as {}",
                                if *writable { "writable" } else { "readonly" }).as_str());
            let mut contents = String::new();
            committed_file.read_to_string(&mut contents)
                .expect("Unable to read from committed file");
            drop(committed_file);
            assert_eq!("What a test!", contents.as_str());
        }

        let dir_entries = overlay.list_directory(Path::new("a/nested/dir"))
            .expect("Unable to list directory a/nested/dir")
            .collect::<super::Result<HashSet<OverlayDirEntry>>>()
            .expect("Unable to get directory contents of a/nested/dir");
        assert_eq!(HashSet::<String>::from_iter(["test.txt"].iter().map(|s| s.to_string())),
                   HashSet::<String>::from_iter(dir_entries.iter().map(|e| e.name.clone())));
    }

    #[test]
    fn commit_empty_directory() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("test"))
            .expect("Unable to create directory");
        overlay.commit("Test message")
            .expect("Unable to commit empty directory");

        let dir = overlay.list_directory(Path::new(""))
            .expect("Unable to list root directory");
        let dir_names = dir.map(|e|
            e.map(|e| e.name)).collect::<super::Result<HashSet<String>>>()
            .expect("Error during directory iteration");
        assert_eq!(dir_names, HashSet::from_iter(["test"].iter().map(|s| s.to_string())));

        let mut subdir = overlay.list_directory(Path::new("test"))
            .expect("Unable to list root directory");
        assert!(subdir.next().is_none());
    }

    #[test]
    fn modify_file_after_commit() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file(Path::new("test.txt"), true)
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

        let mut test_file = overlay.open_file(Path::new("test.txt"), false)
            .expect("Unable to create file");
        check_file_content(&mut test_file, "What a test!Incredible!");
    }

    #[test]
    fn delete_file_after_commit() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");

        overlay.delete_file(Path::new("test.txt")).expect("File deletion failed");

        assert!(overlay.get_status().unwrap()
            .any(|status|
                status.ok() == Some(("test.txt", FileState::Deleted).into())));

        overlay.open_file(Path::new("test.txt"), false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_file_from_workspace() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.delete_file(Path::new("test.txt")).expect("File deletion failed");
        overlay.open_file(Path::new("test.txt"), false).expect_err("File can still be opened");
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.open_file(Path::new("test.txt"), false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_file_from_repository() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.delete_file(Path::new("test.txt")).expect("File deletion failed");
        overlay.commit("Committing deleted file").expect("Unable to create second commit");
        overlay.open_file(Path::new("test.txt"), false).expect_err("File can still be opened");
    }

    #[test]
    fn delete_dir_from_workspace() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.delete_file(Path::new("a/dir/test.txt")).expect("File deletion failed");
        overlay.delete_file(Path::new("a/dir")).expect("Dir deletion failed");
        overlay.open_file(Path::new("a/dr/test.txt"), false)
            .expect_err("File can still be opened");
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.open_file(Path::new("a/dir/test.txt"), false)
            .expect_err("File can still be opened");
    }

    #[test]
    fn delete_directory_in_overlay() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);
        overlay.commit("A test commit").expect("Unable to create first commit");
        overlay.delete_file(Path::new("a/dir/test.txt")).expect("File deletion failed");
        overlay.delete_file(Path::new("a/dir")).expect("Dir deletion failed");
        overlay.commit("Committing deleted dir").expect("Unable to create second commit");
        overlay.open_file(Path::new("a/dir/test.txt"), false)
            .expect_err("File can still be opened");
    }

    trait DirComparator {
        fn compare(&self, contents: &[&str]) -> bool;
    }

    trait DirLister {
        type Dir;
        fn dir<P>(self, path: P) -> Self::Dir where P: AsRef<Path>;
    }

    struct OverlayDir(Vec<OverlayDirEntry>);

    impl<'a, F, O> DirLister for &'a O where F: OverlayFile+'static, O: Overlay<File=F> {
        type Dir = OverlayDir;

        fn dir<P>(self, path: P) -> <Self as DirLister>::Dir where P: AsRef<Path> {
            let dir = self.list_directory(path.as_ref()).unwrap()
                .collect::<super::Result<Vec<OverlayDirEntry>>>().unwrap();
            OverlayDir(dir)
        }
    }

    impl DirComparator for OverlayDir {
        fn compare(&self, contents: &[&str]) -> bool {
            let mut dir: Vec<&str> = self.0.iter()
                .map(|e| e.name.as_str()).collect();
            dir.sort_unstable();
            let mut contents: Vec<&str> = contents.into_iter().map(|e| *e).collect();
            contents.sort_unstable();
            contents.dedup();
            dir == contents
        }
    }

    impl PartialEq<&[&str]> for OverlayDir {
        fn eq(&self, other: &&[&str]) -> bool {
            self.compare(*other)
        }
    }

    #[test]
    fn revert_deleted_directory() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("Create file in subdir").unwrap();

        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);

        overlay.delete_file(Path::new("a/dir/test.txt")).unwrap();
        overlay.delete_file(Path::new("a/dir")).unwrap();
        assert!(overlay.dir("a") == &[]);

        overlay.revert_file(Path::new("a/dir")).unwrap();
        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);
    }

    #[test]
    fn rewrite_deleted_file() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("Create file in subdir").unwrap();

        overlay.delete_file(Path::new("a/dir/test.txt"))
            .expect("Unable to delete file");

        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "Totally different content!")
            .expect("Couldn't write to test file after delete");
        drop(test_file);
    }

    #[test]
    fn revert_single_file() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("Create file in subdir").unwrap();

        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);

        overlay.delete_file(Path::new("a/dir/test.txt")).unwrap();
        assert!(overlay.dir("a/dir") == &[]);

        overlay.revert_file(Path::new("a/dir/test.txt")).unwrap();
        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);
    }

    #[test]
    fn revert_single_file_in_deleted_directory() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let mut test_file = overlay.open_file(Path::new("a/dir/test2.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What another test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("Create file in subdir").unwrap();

        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt", "test2.txt"]);

        overlay.delete_file(Path::new("a/dir/test.txt")).unwrap();
        assert!(overlay.dir("a/dir") == &["test2.txt"]);

        overlay.revert_file(Path::new("a/dir/test.txt")).unwrap();
        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt", "test2.txt"]);
    }

    #[test]
    fn revert_changed_file() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        overlay.ensure_directory(Path::new("a/dir")).expect("Unable to create dir");
        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("Create file in subdir").unwrap();

        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);

        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), true)
            .expect("Unable to open committed file");
        write!(test_file, "Now that's totally different!").expect("Couldn't write to test file");
        drop(test_file);

        assert!(overlay.dir("a/dir") == &["test.txt"]);

        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), false)
            .expect("Unable to open test file");
        check_file_content(&mut test_file, "Now that's totally different!");
        drop(test_file);

        overlay.revert_file(Path::new("a/dir/test.txt")).unwrap();

        let mut test_file = overlay.open_file(Path::new("a/dir/test.txt"), false)
            .expect("Unable to open test file");
        check_file_content(&mut test_file, "What a test!");

        assert!(overlay.dir("a") == &["dir"]);
        assert!(overlay.dir("a/dir") == &["test.txt"]);
    }

    #[test]
    fn refuse_update_if_workspace_unclean() {
        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        match overlay.update_head() {
            Err(super::Error::UncleanWorkspace) => (),
            Err(err) => panic!("Unexpected error during update of unclean workspace: {}", err),
            Ok(_) => panic!("Update worked despite unclean workspace!")
        }
    }

    #[tokio::test]
    async fn check_thread_safety() {
        use crate::nullcache::NullCache;
        use super::{RepositoryWrapper, BoxedRepository};
        use crate::control::ControlDir;
        use std::fmt::Debug;

        fn bounds_test<O>(mut overlay: O) where O: Overlay+Debug+Send+Sync+'static,
                                                <O as Overlay>::File: Debug+Send+Sync+'static {
            assert!(overlay.open_file(Path::new("test.txt"), true).is_ok());
            dbg!(overlay);
        }

        crate::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let fs_overlay = open_working_copy(tempdir.path());
        let control_overlay =
            ControlDir::new(fs_overlay, tempdir);
        let boxed_overlay = Box::new(RepositoryWrapper::new(control_overlay))
            as BoxedRepository;
        bounds_test(boxed_overlay);
    }

    #[cfg(feature = "gitcache")]
    mod withgitcache {
        use std::{
            borrow::Cow,
            path::Path,
            io::{self, Read, Write, Seek, SeekFrom}
        };

        use tempfile::tempdir;
        use chrono::{Utc, Offset};

        use crate::{
            init_logging,
            cache::{CacheObject, CacheLayer, DirectoryEntry, Commit},
            overlay::{Overlay, WorkspaceController, FilesystemOverlay, OverlayFile},
            gitcache::GitCache,
            hashfilecache::HashFileCache,
            types::RepoRef
        };

        use super::check_file_content;

        #[test]
        fn commit_with_open_file_and_external_change() {
            // This test shall verify that it is ok to commit when a file is open and was changed in
            // the cache by another action.
            init_logging();

            let tempdir = tempdir().expect("Unable to create temporary dir!");

            let cache_dir = tempdir.path().join("cache");
            let overlay_dir = tempdir.path().join("overlay");
            let git_dir = tempdir.path().join("git");

            let storage = crate::gitcache::GitCache::new(git_dir)
                .expect("Unable to initialize Git storage");
            let cache =
                crate::hashfilecache::HashFileCache::new(storage, &cache_dir)
                    .expect("Unable to create cache");
            let mut overlay =
                crate::overlay::FilesystemOverlay::new(cache,
                                                       Cow::Borrowed(&overlay_dir),
                                                       RepoRef::Branch("master"))
                    .expect("Unable to create overlay");

            // 1. Create two test files and commit
            let mut file1 =
                overlay.open_file(Path::new("test1.txt"), true)
                    .expect("Unable to create first test file");
            write!(file1, "A test file")
                .expect("Unable to write to first test file");
            drop(file1);
            let mut file2 =
                overlay.open_file(Path::new("test2.txt"), true)
                    .expect("Unable to create second test file");
            write!(file2, "Another test file")
                .expect("Unable to write to second test file");
            drop(file2);

            let commit_ref = overlay.commit("Create two test files")
                .expect("Unable to create first commit");

            // 2. Reopen the files, change one
            let mut file1 =
                overlay.open_file(Path::new("test1.txt"), true)
                    .expect("Unable to reopen first test file");
            write!(file1, "Alter one file in the overlay")
                .expect("Unable to change the first file");
            drop(file1);
            let mut file2 =
                overlay.open_file(Path::new("test2.txt"), true)
                    .expect("Unable to reopen second test file");

            // 3. Alter the other file directly in the cache
            let cache = overlay.get_cache();
            let commit = cache.get(&commit_ref)
                .and_then(CacheObject::into_commit)
                .expect("Unable to retrieve first commit object");
            let root_tree =
                cache.get(&commit.tree)
                    .and_then(CacheObject::into_directory)
                    .expect("Unable to get root commit");

            let mut newfile = tempfile::NamedTempFile::new().unwrap();
            write!(newfile, "Altered content").unwrap();
            let newfile_path = newfile.into_temp_path();
            let newfile_ref =
                cache.add_file_by_path(&newfile_path)
                    .expect("Unable to add file with altered path");
            let directory = root_tree.into_iter()
                .map(|mut entry: DirectoryEntry| {
                    if entry.name == "test2.txt" {
                        entry.cache_ref = newfile_ref;
                    }
                    entry
                })
                .collect::<Vec<_>>();
            let newtree_ref =
                cache.add_directory(&mut directory.into_iter())
                    .expect("Unable to create new root tree");
            let new_commit = Commit {
                tree: newtree_ref,
                parents: vec![commit_ref],
                message: String::from("Altered second test file"),
                committed_date: Utc::now().with_timezone(&Utc.fix())
            };
            let newcommit_ref =
                cache.add_commit(new_commit)
                    .expect("Unable to add commit that alters a file");
            cache.inner().set_reference("master", &newcommit_ref).unwrap();
            drop(cache);

            // 4. Commit the other file
            overlay.commit("Altered first test file")
                .expect("Unable to create the second commit via the overlay");

            // 5. Check if the locally unchanged file contains the changes from the cache
            check_file_content(&mut file2, "Altered content");
        }

        #[test]
        fn check_file_after_revert() {
            init_logging();

            let tempdir = tempdir().expect("Unable to create temporary dir!");
            let tempdir_path = tempdir.path();

            let overlay_dir = tempdir_path.join("overlay");
            let git_dir = tempdir_path.join("git");
            let cache_dir = tempdir_path.join("cache");

            let storage = GitCache::new(git_dir)
                .expect("Unable to initialize Git storage");
            let hashcache = HashFileCache::new(storage, cache_dir).unwrap();
            let mut overlay =
                FilesystemOverlay::new(hashcache,
                                       Cow::Borrowed(&overlay_dir),
                                       RepoRef::Branch("master"))
                    .expect("Unable to create overlay");

            let file_path = Path::new("test.txt");
            let mut file = overlay.open_file(&file_path, true).unwrap();
            io::copy(&mut io::repeat(b'a').take(1024), &mut file).unwrap();
            drop(file);

            overlay.commit("Test commit").unwrap();

            let metadata = overlay.metadata(&file_path).unwrap();
            assert_eq!(metadata.size, 1024);

            let mut file = overlay.open_file(&file_path, true).unwrap();
            file.truncate(512).unwrap();
            drop(file);

            let metadata = overlay.metadata(&file_path).unwrap();
            assert_eq!(metadata.size, 512);

            overlay.revert_file(&file_path).unwrap();

            let metadata = overlay.metadata(&file_path).unwrap();
            assert_eq!(metadata.size, 1024);

            let mut file = overlay.open_file(&file_path, true).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut contents = Vec::with_capacity(1024);
            let bytes_read = file.read_to_end(&mut contents).unwrap();
            assert_eq!(contents.len(), 1024);
            assert_eq!(bytes_read, 1024);
        }
    }
}
