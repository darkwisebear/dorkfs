use std::path::Path;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::iter::FromIterator;
use std::fmt::{self, Debug, Formatter};
use std::collections::{hash_map, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::str;
use std::borrow::Cow;

use chrono;
use owning_ref::{RwLockReadGuardRef, OwningHandle};

use crate::{
    types::*,
    overlay::{self, *},
    cache::CacheRef
};
use crate::cache::ReferencedCommit;

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
    fn close(&mut self) -> Result<()>;
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

#[derive(Debug)]
struct BufferedFileFactory<F: SpecialFileOps> {
    ops: F
}

impl<O, F> SpecialFile<O> for BufferedFileFactory<F>
    where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static,
          F: SpecialFileOps+Send+Sync+'static {
    fn metadata(&self, control_dir: &ControlDir<O>) -> overlay::Result<Metadata> {
        let metadata = Metadata {
            size: self.ops.init(control_dir.get_overlay().deref())?.len() as u64,
            object_type: ObjectType::File,
            modified_date: chrono::Utc::now()
        };

        Ok(metadata)
    }

    fn get_file<'a, 'b>(&'a self, control_dir: &'b ControlDir<O>)
        -> overlay::Result<Box<dyn ControlOverlayFile+Send+Sync>> {
        let file = BufferedFile {
            buf: Cursor::new(self.ops.init(control_dir.get_overlay().deref())?),
            ops: self.ops.clone(),
            overlay: Arc::clone(&control_dir.overlay)
        };
        Ok(Box::new(file) as Box<dyn ControlOverlayFile+Send+Sync>)
    }
}

impl<F: SpecialFileOps+Send+Sync> BufferedFileFactory<F> {
    fn new(ops: F) -> Self {
        BufferedFileFactory {
            ops
        }
    }
}

#[derive(Debug)]
struct BufferedFile<F, O> where F: SpecialFileOps, for <'a> O: Overlay+WorkspaceController<'a> {
    buf: Cursor<Vec<u8>>,
    ops: F,
    overlay: Arc<RwLock<O>>
}

impl<F, O> Read for BufferedFile<F, O> where F: SpecialFileOps,
                                             for<'a> O: Overlay+WorkspaceController<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.buf.read(buf)
    }
}

impl<F, O> Write for BufferedFile<F, O> where F: SpecialFileOps,
                                              for<'a> O: Overlay+WorkspaceController<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buf.flush()
    }
}

impl<F, O> Seek for BufferedFile<F, O> where F: SpecialFileOps,
                                             for<'a> O: Overlay+WorkspaceController<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.buf.seek(pos)
    }
}

impl<F, O> OverlayFile for BufferedFile<F, O>
    where F: SpecialFileOps,
          for<'a> O: Overlay+WorkspaceController<'a>+Send+Sync+'static {
    fn truncate(&mut self, size: u64) -> overlay::Result<()> {
        if self.buf.position() as u64 > size {
            self.buf.set_position(size);
        }
        self.buf.get_mut().truncate(size as usize);
        Ok(())
    }
}

impl<F, O> ControlOverlayFile for BufferedFile<F, O>
    where F: SpecialFileOps,
    for<'a> O: Overlay+WorkspaceController<'a>+Send+Sync+'static {
    fn close(&mut self) -> overlay::Result<()> {
        self.ops.close(&mut *self.overlay.write().unwrap(),
                       self.buf.get_ref().as_slice())
    }
}

#[derive(Debug, Clone)]
struct LogFileOps;

impl LogFileOps {
    fn generate_log_string<O>(workspace_controller: &O) -> overlay::Result<String>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        if let Some(head_ref) = workspace_controller.get_current_head_ref()? {
            let log = workspace_controller.get_log(&head_ref)?;

            let mut result_string = String::from("On branch (HEAD)\n");
            for commit_result in log {
                result_string.push_str(&commit_result?.to_string());
                result_string.push('\n');
            }

            Ok(result_string)
        } else {
            Ok(String::from("(No commit yet)"))
        }
    }
}

impl SpecialFileOps for LogFileOps {
    fn init<O>(&self, control_dir: &O) -> overlay::Result<Vec<u8>>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        Self::generate_log_string(control_dir).map(String::into_bytes)
    }
}

#[derive(Debug, Clone)]
struct WorkspaceStatusFileOps;

impl WorkspaceStatusFileOps {
    fn generate_status_string<O>(overlay: &O) -> overlay::Result<String>
        where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
        let mut status = String::new();

        for file_status in overlay.get_status()? {
            let WorkspaceFileStatus(path, state) = file_status?;
            let state_string = match state {
                FileState::New => "N ",
                FileState::Modified => "M ",
                FileState::Deleted => "D "
            };
            status.push_str(state_string);
            status.push_str(path.display().to_string().as_str());
            status.push('\n');
        }

        Ok(status)
    }
}

impl SpecialFileOps for WorkspaceStatusFileOps {
    fn init<O>(&self, control_dir: &O) -> overlay::Result<Vec<u8>>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        Self::generate_status_string(control_dir).map(String::into_bytes)
    }
}

#[derive(Debug, Clone)]
struct CommitFileOps;

impl SpecialFileOps for CommitFileOps {
    fn close<O>(&self, control_dir: &mut O, buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay+WorkspaceController<'o>+Send+Sync+'static {
        str::from_utf8(buffer)
            .map_err(overlay::Error::from_fail)
            .and_then(|message| {
            control_dir.commit(message)
                .map(|cache_ref|
                    info!("Successfully committed {} through commit file", cache_ref))
                .map_err(Into::into)
        })
    }
}

#[derive(Debug, Clone)]
struct BranchFileOps;

impl SpecialFileOps for BranchFileOps {
    fn init<O>(&self, control_dir: &O) -> overlay::Result<Vec<u8>>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        control_dir.get_current_branch()
            .map(|branch|
                branch.unwrap_or(Cow::Borrowed("(detached)")).as_bytes().to_vec())
    }

    fn close<O>(&self, control_dir: &mut O, buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        let target_branch = str::from_utf8(buffer)
            .map_err(overlay::Error::from_fail)?;
        if Some(Cow::Borrowed(target_branch)) != control_dir.get_current_branch()? {
            control_dir.switch_branch(Some(target_branch.trim_end_matches('\n')))
                .map(|_|
                    info!("Successfully switched to branch {}", target_branch))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
struct CreateBranchFileOps;

impl SpecialFileOps for CreateBranchFileOps {
    fn close<O>(&self, control_dir: &mut O, buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        str::from_utf8(buffer)
            .map_err(overlay::Error::from_fail)
            .and_then(|target_branch|
                control_dir.create_branch(target_branch.trim_end_matches('\n'), None))
    }
}

#[derive(Debug, Clone, Copy)]
struct RevertFileOps;

impl SpecialFileOps for RevertFileOps {
    fn close<O>(&self, control_dir: &mut O, buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        str::from_utf8(buffer)
            .map(|p| Path::new(p.trim_end_matches('\n')))
            .map_err(overlay::Error::from_fail)
            .and_then(|overlay_path| control_dir.revert_file(overlay_path))
    }
}

#[derive(Debug, Clone, Copy)]
struct HeadFileOps;

impl SpecialFileOps for HeadFileOps {
    fn init<O>(&self, control_dir: &O) -> Result<Vec<u8>>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        control_dir.get_current_head_ref()
            .map(|cache_ref|
                cache_ref
                    .map(|cache_ref| cache_ref.to_string().into_bytes())
                    .unwrap_or_else(|| b"(no HEAD)".to_vec()))
    }

    fn close<O>(&self, control_dir: &mut O, _buffer: &[u8]) -> Result<()>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        control_dir.update_head()
            .map(|cache_ref| info!("Updating workspace to {}", cache_ref))
    }
}

#[derive(Debug)]
struct SpecialFileRegistry<O>(HashMap<&'static str, Box<SpecialFile<O>>>)
    where for<'a> O: Overlay+WorkspaceController<'a>;

impl<O> SpecialFileRegistry<O> where for<'a> O: Overlay+WorkspaceController<'a>+Send+Sync+'static {
    fn new() -> Self {
        SpecialFileRegistry(HashMap::new())
    }

    fn insert_buffered<T>(&mut self, name: &'static str, ops: T)
        where T: SpecialFileOps+Send+Sync+'static {
        self.0.insert(name, Box::new(BufferedFileFactory::new(ops)) as _);
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
    special_files: SpecialFileRegistry<O>
}

impl<O> ControlDir<O> where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    pub fn new(overlay: O) -> Self {
        let mut special_files = SpecialFileRegistry::new();
        special_files.insert_buffered("log", LogFileOps);
        special_files.insert_buffered("commit", CommitFileOps);
        special_files.insert_buffered("status", WorkspaceStatusFileOps);
        special_files.insert_buffered("current_branch", BranchFileOps);
        special_files.insert_buffered("create_branch", CreateBranchFileOps);
        special_files.insert_buffered("revert", RevertFileOps);
        special_files.insert_buffered("HEAD", HeadFileOps);
        ControlDir {
            overlay: Arc::new(RwLock::new(overlay)),
            special_files
        }
    }

    pub fn get_overlay<'a>(&'a self) -> impl Deref<Target=O>+'a {
        self.overlay.read().unwrap()
    }

    #[cfg(test)]
    pub fn get_overlay_mut<'a>(&'a self) -> impl ::std::ops::DerefMut<Target=O>+'a {
        self.overlay.write().unwrap()
    }
}

pub enum ControlDirIter<O: Overlay> {
    OverlayDirIter(O::DirIter),
    RootDirIter(::std::iter::Chain<O::DirIter, ::std::vec::IntoIter<Result<OverlayDirEntry>>>),
    DorkfsDirIter(::std::vec::IntoIter<Result<OverlayDirEntry>>)
}

impl<O: Overlay> Iterator for ControlDirIter<O> {
    type Item = Result<OverlayDirEntry>;

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

    fn list_directory(&self, path: &Path) -> Result<Self::DirIter> {
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

    fn ensure_directory(&self, path: &Path) -> Result<()> {
        if path.starts_with(DORK_DIR_ENTRY) {
            Err(Error::Generic(failure::err_msg("Cannot create directory within .dork")))
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
    fn new(overlay: RwLockReadGuardRef<'a, T>, start_commit: &CacheRef) -> Result<Self> {
        OwningHandle::try_new(overlay, |o|
            unsafe {
                (*o).get_log(start_commit).map(DerefWrapper)
            })
            .map(|h| ControlDirLog(ControlDirHandle(h)))
    }
}

impl<'a, T> Iterator for ControlDirLog<'a, T>
    where for<'b> T: WorkspaceController<'b>+'a {
    type Item = Result<ReferencedCommit>;

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
    fn new(overlay: RwLockReadGuardRef<'a, T>) -> Result<Self> {
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
    type Item = Result<WorkspaceFileStatus>;

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
        io::{Read, Write},
        iter::Iterator,
        path::Path
    };

    use tempfile::tempdir;

    use crate::{
        cache::ReferencedCommit,
        overlay::{
            testutil::{
                open_working_copy,
                check_file_content
            },
            Overlay, OverlayFile, WorkspaceController
        }
    };

    #[test]
    fn commit_via_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut file = control_overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        let mut commit_file = control_overlay.open_file(Path::new(".dork/commit"), true)
            .expect("Unable to open commit file");
        commit_file.write("Test commit message".as_bytes())
            .expect("Unable to write commit message");
        drop(commit_file);

        let workspace_controller = control_overlay.get_overlay();
        let head_ref = workspace_controller.get_current_head_ref()
            .expect("Unable to get new head revision")
            .expect("No head revision existing");
        let mut log = workspace_controller.get_log(&head_ref)
            .expect("Unable to get log for workspace");
        let ReferencedCommit(_, head_commit) = log.next()
            .expect("No head commit available")
            .expect("Error while retrieving head commit");
        assert_eq!("Test commit message", head_commit.message.as_str());
    }

    #[test]
    fn switch_between_branches() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        // Create first test commit
        let mut file1 =
            control_overlay.open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);

        let first_commit = control_overlay.get_overlay_mut()
            .commit("Commit to first branch")
            .expect("Unable to create first commit");

        // Create branch "feature"
        let mut create_branch =
            control_overlay.open_file(Path::new(".dork/create_branch"), true)
                .expect("Unable to open create_branch special file");
        create_branch.write_all(b"feature")
            .expect("Create branch \"feature\" failed");
        drop(create_branch);

        // Check if we still track "master"
        let mut switch_branch =
            control_overlay.open_file(Path::new(".dork/current_branch"), false)
                .expect("Unable to open current_branch for reading");
        let mut content = Vec::new();
        switch_branch.read_to_end(&mut content).unwrap();
        assert_eq!(b"master", content.as_slice());
        drop(switch_branch);

        // Switch to branch "feature"
        let mut switch_branch =
            control_overlay.open_file(Path::new(".dork/current_branch"), true)
                .expect("Unable to open current_branch for reading");
        switch_branch.write_all(b"feature").unwrap();
        drop(switch_branch);

        // Check if switch is reflected
        let mut switch_branch =
            control_overlay.open_file(Path::new(".dork/current_branch"), false)
                .expect("Unable to open current_branch for reading");
        let mut content = Vec::new();
        switch_branch.read_to_end(&mut content).unwrap();
        assert_eq!(b"feature", content.as_slice());
        drop(switch_branch);

        // Add a commit with a second file to the branch "feature"
        let mut file2 =
            control_overlay.open_file(Path::new("file2.txt"), true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        drop(file2);

        let second_commit = control_overlay.get_overlay_mut().commit("Commit to second branch")
            .expect("Unable to create second commit");

        // Switch back to master
        control_overlay.get_overlay_mut().switch_branch(Some("master")).unwrap();

        // Check if checked-out workspace is unchanged
        let mut file2 =
            control_overlay.open_file(Path::new("file2.txt"), false)
                .expect("Unable to open first test file");
        let mut content = Vec::new();
        file2.read_to_end(&mut content)
            .expect("Unable to read from first file");
        assert_eq!(b"Test 2", content.as_slice());
        drop(file2);

        // Now update the workspace
        let mut head =
            control_overlay.open_file(Path::new(".dork/HEAD"), true)
                .expect("Unable to open HEAD");
        write!(&mut head, "latest").expect("Unable to update workspace");
        drop(head);

        // ...and check if the second file is gone
        assert!(control_overlay.open_file(Path::new("file2.txt"), false).is_err());

        // Check if we're pointing to the right commit
        check_file_content(
            &mut control_overlay.open_file(Path::new(".dork/HEAD"), false).unwrap(),
            first_commit.to_string().as_str());

        // Switch back to feature branch
        control_overlay.get_overlay_mut().switch_branch(Some("feature")).unwrap();

        // ...and check if the second file is still gone
        assert!(control_overlay.open_file(Path::new("file2.txt"), false).is_err());

        // Update the workspace
        let mut head =
            control_overlay.open_file(Path::new(".dork/HEAD"), true)
                .expect("Unable to open HEAD");
        write!(&mut head, "latest").expect("Unable to update workspace");
        drop(head);

        // Check if we're pointing to the right commit
        check_file_content(
            &mut control_overlay.open_file(Path::new(".dork/HEAD"), false).unwrap(),
            second_commit.to_string().as_str());

        // Check if all committed files are present and contain the correct contents
        let mut file1 =
            control_overlay.open_file(Path::new("file1.txt"), false)
                .expect("Unable to open first test file");
        let mut content = Vec::new();
        file1.read_to_end(&mut content)
            .expect("Unable to read from first file");
        assert_eq!(b"Test 1", content.as_slice());

        let mut file2 =
            control_overlay.open_file(Path::new("file2.txt"), false)
                .expect("Unable to open second test file");
        let mut content = Vec::new();
        file2.read_to_end(&mut content)
            .expect("Unable to read from second file");
        assert_eq!(b"Test 2", content.as_slice());
    }

    #[test]
    fn fetch_empty_log() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut log_file = control_overlay.open_file(Path::new(".dork/log"), false)
            .expect("Unable to open log file");
        let mut log_contents = String::new();
        log_file.read_to_string(&mut log_contents)
            .expect("Unable to read from log");
    }

    #[test]
    fn dont_delete_control_files() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);
        control_overlay.delete_file(Path::new(".dork/log")).unwrap_err();

        let mut file = control_overlay.open_file(Path::new("test.txt"), true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        control_overlay.delete_file(Path::new("test.txt")).unwrap();
    }

    #[test]
    fn revert_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut file1 =
            control_overlay.open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(Path::new(".dork/revert"), true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"file1.txt")
            .expect("unable to write to revert file");
        drop(revert_file);
        assert_eq!(0, control_overlay.get_overlay().get_status().unwrap().count());
    }

    #[test]
    fn revert_nonexistent_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut file1 =
            control_overlay.open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(".dork/revert", true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"file2.txt")
            .expect("unable to write to revert file");
        drop(revert_file);
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());
    }

    #[test]
    fn revert_directory() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut file1 =
            control_overlay.open_file("file1.txt", true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut file2 =
            control_overlay.open_file(Path::new("dir/file2.txt"), true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        drop(file2);
        assert_eq!(2, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(Path::new(".dork/revert"), true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"file1.txt")
            .expect("unable to write to revert file");
        drop(revert_file);
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(Path::new(".dork/revert"), true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"dir")
            .expect("unable to write to revert dir");
        drop(revert_file);
        assert_eq!(0, control_overlay.get_overlay().get_status().unwrap().count());
    }

    #[test]
    fn update_via_control_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut test_file =
            control_overlay.open_file(Path::new("test.txt"), true).unwrap();
        write!(test_file, "A test text").unwrap();
        drop(test_file);

        let first_commit = control_overlay.get_overlay_mut().commit("A test commit").unwrap();

        let mut test_file =
            control_overlay.open_file(Path::new("test2.txt"), true).unwrap();
        write!(&mut test_file, "A second test").unwrap();
        drop(test_file);

        let second_commit = control_overlay.get_overlay_mut().commit("A test commit").unwrap();

        control_overlay.get_overlay_mut().set_head(first_commit).unwrap();
        assert_eq!(Some(first_commit),
                   control_overlay.get_overlay_mut().get_current_head_ref().unwrap());

        let mut head_file =
            control_overlay.open_file(Path::new(".dork/HEAD"), true).unwrap();
        write!(&mut head_file, "latest").unwrap();
        drop(head_file);

        assert_eq!(Some(second_commit),
                   control_overlay.get_overlay_mut().get_current_head_ref().unwrap());
    }
}
