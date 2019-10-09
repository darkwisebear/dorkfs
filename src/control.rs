use std::path::Path;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::iter::{self, FromIterator};
use std::fmt::{self, Debug, Formatter};
use std::collections::{hash_map, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::str;
use std::borrow::Cow;
use std::vec;

use chrono;
use owning_ref::{RwLockReadGuardRef, OwningHandle};

use crate::{
    types::{Metadata, ObjectType, RepoRef},
    overlay::{self, Overlay, OverlayFile, DebuggableOverlayFile, WorkspaceController,
              WorkspaceFileStatus, OverlayDirEntry, WorkspaceLog, Repository},
    cache::{CacheRef, ReferencedCommit},
    tempfile::TempDir,
    commandstream::FinishCommandSocket
};

static DORK_DIR_ENTRY: &'static str = ".dork";

lazy_static! {
    static ref ADDITIONAL_ROOT_DIRECTORIES: [OverlayDirEntry; 1] = [
        OverlayDirEntry {
            name: String::from(DORK_DIR_ENTRY),
            size: 0,
            object_type: ObjectType::Directory
        }
    ];
}

/// Implemented for all control files.
///
/// Due to https://github.com/rust-lang/rust/issues/27863 we cannot implement Drop for BufferedFile
/// without introducing a lifetime parameter for it. Therefore we introduce a trait that will then
/// be used in the Drop implementation of DynamicOverlayFileWrapper to execute the control file's
/// action on close.
pub trait ControlOverlayFile: OverlayFile {
    /// Called if the control file is closed
    ///
    /// # Panics
    /// This method shall not panic as it will be called inside a Drop implementation.
    fn close(&mut self) -> overlay::Result<()>;
}

pub struct DynamicOverlayFileWrapper(Box<dyn ControlOverlayFile+Send+Sync>);

impl Debug for DynamicOverlayFileWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "<Dynamic overlay file object>")
    }
}

impl Drop for DynamicOverlayFileWrapper {
    fn drop(&mut self) {
        if let Err(e) = self.0.close() {
            warn!("Unable to close file on drop: {}", &e);
            // Enable this in debug builds to make tests fail if there is an error during closing
            // the control file. Not sure if that's smart as the panic will most likely abort()...
            debug_assert!(false, "Closing a control file failed: {}", &e);
        }
    }
}

#[derive(Debug)]
pub enum ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    DynamicOverlayFile(DynamicOverlayFileWrapper),
    InnerOverlayFile(O::File)
}

impl<O> DebuggableOverlayFile for ControlFile<O>
    where for<'a> O: Overlay+WorkspaceController<'a>,
          <O as Overlay>::File: Debug {}

impl<O> OverlayFile for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn truncate(&mut self, size: u64) -> overlay::Result<()> {
        match self {
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(special_file)) =>
                special_file.truncate(size),
            ControlFile::InnerOverlayFile(ref mut file) => file.truncate(size)
        }
    }
}

impl<O> Read for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(ref mut special_file)) =>
                special_file.read(buf),
            ControlFile::InnerOverlayFile(ref mut file) => file.read(buf)
        }
    }
}

impl<O> Write for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(ref mut special_file)) =>
                special_file.write(buf),
            ControlFile::InnerOverlayFile(ref mut file) => file.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(ref mut special_file)) =>
                special_file.flush(),
            ControlFile::InnerOverlayFile(ref mut file) => file.flush()
        }
    }
}

impl<O> Seek for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match *self {
            ControlFile::InnerOverlayFile(ref mut file) => file.seek(pos),
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(ref mut special_file)) =>
                special_file.seek(pos)
        }
    }
}

pub trait SpecialFileOps: Clone+Debug {
    const CLEAR_ON_WRITE: bool = false;

    fn init<O>(&self, _control_dir: &O) -> overlay::Result<Vec<u8>>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        Ok(Vec::new())
    }

    fn close<O>(&self, _control_dir: &mut O, _buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        Ok(())
    }
}

pub trait SpecialFile<O>: Debug+Send+Sync where for<'a> O: Overlay+WorkspaceController<'a> {
    fn metadata(&self, control_dir: &ControlDir<O>) -> overlay::Result<Metadata>;
    fn get_file<'a, 'b>(&'a self, _control_dir: &'b ControlDir<O>)
        -> overlay::Result<Box<dyn ControlOverlayFile+Send+Sync>> {
        Err(format_err!("This special file type cannot be accessed like a normal file").into())
    }
}

struct ConstantSpecialFile {
    data: Cursor<Arc<[u8]>>
}

impl Read for ConstantSpecialFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}

impl Write for ConstantSpecialFile {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::PermissionDenied.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for ConstantSpecialFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

impl OverlayFile for ConstantSpecialFile {
    fn truncate(&mut self, _sze: u64) -> overlay::Result<()> {
        Err(overlay::Error::Generic(failure::err_msg("Cannot truncate constant file")))
    }
}

impl ControlOverlayFile for ConstantSpecialFile {
    fn close(&mut self) -> overlay::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct ConstantFileFactory {
    data: Arc<[u8]>,
    is_link: bool
}

impl ConstantFileFactory {
    fn new<B: Into<Box<[u8]>>>(data: B, is_link: bool) -> Self {
        Self {
            data: Arc::from(data.into()),
            is_link
        }
    }
}

impl<O> SpecialFile<O> for ConstantFileFactory where for<'a> O: Overlay+WorkspaceController<'a> {
    fn metadata(&self, _control_dir: &ControlDir<O>) -> overlay::Result<Metadata> {
        let object_type = if self.is_link {
            ObjectType::Symlink
        } else {
            ObjectType:: File
        };

        Ok(Metadata {
            object_type,
            modified_date: chrono::Utc::now(),
            size: self.data.len() as u64
        })
    }

    fn get_file<'a, 'b>(&'a self, _control_dir: &'b ControlDir<O>)
        -> overlay::Result<Box<dyn ControlOverlayFile+Send+Sync>> {
        let file = ConstantSpecialFile {
            data: Cursor::new(Arc::clone(&self.data))
        };

        Ok(Box::new(file) as Box<dyn ControlOverlayFile+Send+Sync>)
    }
}

#[derive(Debug)]
struct SpecialFileRegistry<O>(HashMap<&'static str, Box<dyn SpecialFile<O>>>)
    where for<'a> O: Overlay+WorkspaceController<'a>;

impl<O> SpecialFileRegistry<O> where for<'a> O: Overlay+WorkspaceController<'a>+Send+Sync+'static {
    fn new() -> Self {
        SpecialFileRegistry(HashMap::new())
    }

    fn insert<S>(&mut self, name: &'static str, file: S) where S: SpecialFile<O>+'static {
        self.0.insert(name, Box::new(file) as _);
    }

    fn get(&self, name: &str) -> Option<&dyn SpecialFile<O>> {
        self.0.get(name).map(Box::as_ref)
    }

    fn iter<'a>(&'a self) -> hash_map::Iter<'a, &'static str, Box<dyn SpecialFile<O>>> {
        self.0.iter()
    }
}

#[derive(Debug)]
pub struct ControlDir<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    overlay: Arc<RwLock<O>>,
    special_files: SpecialFileRegistry<O>,
    tempdir: TempDir,
    command_socket_finisher: Option<FinishCommandSocket>
}

impl<O> ControlDir<O> where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    pub fn new(overlay: O, tempdir: TempDir) -> Self {
        let mut special_files = SpecialFileRegistry::new();

        let overlay = Arc::new(RwLock::new(overlay));

        #[cfg(target_os = "linux")]
        let command_socket_finisher = {
            use std::os::unix::ffi::OsStrExt;
            use crate::commandstream::{create_command_socket, CommandExecutor};

            let dorkcmd_path = tempdir.path().join("dorkcmd");
            let link_file_factory = ConstantFileFactory::new(
                dorkcmd_path.as_os_str().as_bytes(), true);
            special_files.insert("cmd", link_file_factory);

            let command_executor = CommandExecutor::new(Arc::clone(&overlay));
            let (command_socket_future, command_socket_finisher) =
                create_command_socket(dorkcmd_path, command_executor);
            tokio::spawn(command_socket_future);

            Some(command_socket_finisher)
        };

        #[cfg(not(target_os = "linux"))] let command_socket_finisher = None;

        ControlDir {
            overlay,
            special_files,
            tempdir,
            command_socket_finisher
        }
    }
}

pub enum ControlDirIter<O: Overlay> {
    OverlayDirIter(O::DirIter),
    RootDirIter(iter::Chain<O::DirIter, vec::IntoIter<overlay::Result<OverlayDirEntry>>>),
    DorkfsDirIter(vec::IntoIter<overlay::Result<OverlayDirEntry>>)
}

impl<O: Overlay> Iterator for ControlDirIter<O> {
    type Item = overlay::Result<OverlayDirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            ControlDirIter::OverlayDirIter(ref mut dir_iter) => dir_iter.next(),
            ControlDirIter::RootDirIter(ref mut dir_iter) => dir_iter.next(),
            ControlDirIter::DorkfsDirIter(ref mut dir_iter) => dir_iter.next()
        }
    }
}

impl<O> Overlay for ControlDir<O>
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    type File = ControlFile<O>;
    type DirIter = ControlDirIter<O>;

    fn open_file(&mut self, path: &Path, writable: bool) -> overlay::Result<Self::File> {
        if let Ok(ref dorkfile) = path.strip_prefix(DORK_DIR_ENTRY) {
            match dorkfile.to_str() {
                Some(filename) => {
                    if let Some(special_file) = self.special_files.get(filename) {
                        special_file.get_file(self)
                            .map(DynamicOverlayFileWrapper)
                            .map(ControlFile::DynamicOverlayFile)
                    } else {
                        return Err(overlay::Error::from(format_err!("Unable to open special file {}",
                                                                    dorkfile.to_string_lossy())));
                    }
                }

                None => return Err("No filename given!".into())
            }
        } else {
            self.overlay.write().unwrap().open_file(&path, writable)
                .map(ControlFile::InnerOverlayFile)
        }
    }

    fn list_directory(&self, path: &Path) -> overlay::Result<Self::DirIter> {
        let result = match path.to_str() {
            Some("") => {
                let root_dir = self.overlay.read().unwrap().list_directory(&path)?;
                let additional_entries = ADDITIONAL_ROOT_DIRECTORIES.iter()
                    .map(|x| Ok(x.clone()));
                let result = root_dir.chain(Vec::from_iter(additional_entries).into_iter());
                ControlDirIter::RootDirIter(result)
            }

            Some(prefix) if prefix == DORK_DIR_ENTRY => {
                let dork_entries = self.special_files.iter()
                    .map(|(name, special_file)| {
                        special_file.metadata(self)
                            .map(|metadata|
                                OverlayDirEntry::from((*name, &metadata)))
                }).collect::<Vec<_>>();
                ControlDirIter::DorkfsDirIter(dork_entries.into_iter())
            }

            _ => ControlDirIter::OverlayDirIter(self.overlay.read().unwrap().list_directory(path)?)
        };

        Ok(result)
    }

    fn ensure_directory(&self, path: &Path) -> overlay::Result<()> {
        if path.starts_with(DORK_DIR_ENTRY) {
            Err(overlay::Error::Generic(
                failure::err_msg("Cannot create directory within .dork")))
        } else {
            self.overlay.read().unwrap().ensure_directory(path)
        }
    }

    fn metadata(&self, path: &Path) -> overlay::Result<Metadata> {
        if path == Path::new(DORK_DIR_ENTRY) {
            Ok(Metadata {
                size: 0,
                object_type: ObjectType::Directory,
                modified_date: chrono::Utc::now()
            })
        } else if path.starts_with(DORK_DIR_ENTRY) {
            let file_name = path.strip_prefix(DORK_DIR_ENTRY).unwrap();
            file_name.to_str().ok_or_else(|| "Unable to decode to UTF-8".into())
                .and_then(|s| {
                    self.special_files.get(s)
                        .ok_or_else(|| "File entry not found!".into())
                        .and_then(|special_file| special_file.metadata(self))
                })
        } else {
            self.overlay.read().unwrap().metadata(path)
        }
    }

    fn delete_file(&self, path: &Path) -> overlay::Result<()> {
        if path.starts_with(DORK_DIR_ENTRY) {
            Err("Cannot delete .dork special files".into())
        } else {
            self.overlay.read().unwrap().delete_file(path)
        }
    }

    fn revert_file(&self, path: &Path) -> overlay::Result<()> {
        if path.starts_with(DORK_DIR_ENTRY) {
            Err("Cannot revert .dork special files".into())
        } else {
            self.overlay.read().unwrap().revert_file(path)
        }
    }
}

struct DerefWrapper<T>(T);

impl<T> Deref for DerefWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for DerefWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct ControlDirHandle<'a, T, U>(OwningHandle<RwLockReadGuardRef<'a, T>,
    DerefWrapper<U>>) where for<'b> T: WorkspaceController<'b>+'a;

pub struct ControlDirLog<'a, T>(ControlDirHandle<'a, T, <T as WorkspaceController<'a>>::Log>)
    where for<'b> T: WorkspaceController<'b>;

impl<'a, T> ControlDirLog<'a, T> where for<'b> T: WorkspaceController<'b> {
    fn new(overlay: RwLockReadGuardRef<'a, T>, start_commit: &CacheRef) -> overlay::Result<Self> {
        OwningHandle::try_new(overlay, |o|
            unsafe {
                (*o).get_log(start_commit).map(DerefWrapper)
            })
            .map(|h| ControlDirLog(ControlDirHandle(h)))
    }
}

impl<'a, T> Iterator for ControlDirLog<'a, T>
    where for<'b> T: WorkspaceController<'b>+'a {
    type Item = overlay::Result<ReferencedCommit>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.0).0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0).0.size_hint()
    }
}

impl<'a, T> WorkspaceLog for ControlDirLog<'a, T>
    where for<'b> T: WorkspaceController<'b>+'a {}

pub struct ControlDirStatusIter<'a, T>(
    ControlDirHandle<'a, T, <T as WorkspaceController<'a>>::StatusIter>)
    where for<'b> T: WorkspaceController<'b>;

impl<'a, T> ControlDirStatusIter<'a, T> where for<'b> T: WorkspaceController<'b>+'a {
    fn new(overlay: RwLockReadGuardRef<'a, T>) -> overlay::Result<Self> {
        OwningHandle::try_new(overlay, |o|
            unsafe {
                (*o).get_status().map(DerefWrapper)
            })
            .map(|h|
                ControlDirStatusIter(ControlDirHandle(h)))
    }
}

impl<'a, T> Iterator for ControlDirStatusIter<'a, T>
    where for<'b> T: WorkspaceController<'b>+'a {
    type Item = overlay::Result<WorkspaceFileStatus>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.0).0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0).0.size_hint()
    }
}

impl<'a, T> WorkspaceController<'a> for ControlDir<T>
    where for<'b> T: Overlay+WorkspaceController<'b>+'a {
    type Log = ControlDirLog<'a, T>;
    type LogStream = <T as WorkspaceController<'a>>::LogStream;
    type StatusIter = ControlDirStatusIter<'a, T>;

    fn commit(&mut self, message: &str) -> overlay::Result<CacheRef> {
        self.overlay.write().unwrap().commit(message)
    }

    fn get_current_head_ref(&self) -> overlay::Result<Option<CacheRef>> {
        self.overlay.read().unwrap().get_current_head_ref()
    }

    fn get_current_branch(&self) -> overlay::Result<Option<Cow<str>>> {
        self.overlay.read().unwrap().get_current_branch()
            .map(|r| r.map(|o| Cow::Owned(o.into_owned())))
    }

    fn switch_branch(&mut self, branch: Option<&str>) -> overlay::Result<()> {
        self.overlay.write().unwrap().switch_branch(branch)
    }

    fn create_branch(&mut self, new_branch: &str, repo_ref: Option<RepoRef>) -> overlay::Result<()> {
        self.overlay.write().unwrap().create_branch(new_branch, repo_ref)
    }

    fn get_log(&'a self, start_commit: &CacheRef) -> overlay::Result<Self::Log> {
        let read_guard = self.overlay.read().unwrap();
        ControlDirLog::new(RwLockReadGuardRef::new(read_guard), start_commit)
    }

    fn get_log_stream(&self, start_commit: CacheRef) -> Self::LogStream {
        self.overlay.read().unwrap().get_log_stream(start_commit)
    }

    fn get_status(&'a self) -> overlay::Result<Self::StatusIter> {
        let read_guard = self.overlay.read().unwrap();
        ControlDirStatusIter::new(RwLockReadGuardRef::new(read_guard))
    }

    fn update_head(&mut self) -> overlay::Result<CacheRef> {
        self.overlay.write().unwrap().update_head()
    }
}

impl<'b, O> Repository<'b> for ControlDir<O>
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {}

#[cfg(test)]
mod test {
    use std::{
        io::Write,
        path::Path
    };

    use tempfile::tempdir;

    use crate::{
        overlay::{
            testutil::open_working_copy,
            Overlay
        }
    };

    #[test]
    fn dont_delete_control_files() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy, dir);
        control_overlay.delete_file(Path::new(".dork/dorkcmd")).unwrap_err();

        let mut file = control_overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        control_overlay.delete_file(Path::new("test.txt")).unwrap();
    }
}

