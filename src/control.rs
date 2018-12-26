use std::path::Path;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::iter::FromIterator;
use std::fmt::{self, Debug, Formatter};
use std::collections::{hash_map, HashMap};
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::str;

use crate::{
    types::*,
    overlay::{self, *}
};

static DORK_DIR_ENTRY: &'static str = ".dork";

lazy_static! {
    static ref ADDITIONAL_ROOT_DIRECTORIES: [OverlayDirEntry; 1] = [
        OverlayDirEntry {
            name: String::from(DORK_DIR_ENTRY),
            metadata: Metadata {
                size: 0,
                object_type: ObjectType::Directory
            }
        }
    ];
}

pub struct DynamicOverlayFileWrapper(Box<dyn OverlayFile+Send+Sync>);

impl Debug for DynamicOverlayFileWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "<Dynamic overlay file object>")
    }
}

#[derive(Debug)]
pub enum ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    DynamicOverlayFile(DynamicOverlayFileWrapper),
    InnerOverlayFile(O::File)
}

// Due to https://github.com/rust-lang/rust/issues/27863 we cannot implement Drop for ControlFile
// without introducing a lifetime parameter for it. Do not auto-execute a started commit on Drop
// for now. Instead, close() has to be called explicitly.
//
// impl<O> Drop for ControlFile<O> where for <'a> O: Overlay+WorkspaceController<'a> {
//     fn drop(&mut self) {
//         if let Err(e) = self.execute_commit() {
//             error!("Unable to complete commit: {}", e);
//         }
//     }
// }

impl<O> OverlayFile for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn close(&mut self) -> overlay::Result<()> {
        match self {
            ControlFile::DynamicOverlayFile(DynamicOverlayFileWrapper(ref mut overlay_file)) =>
                overlay_file.close(),
            ControlFile::InnerOverlayFile(ref mut overlay_file) => overlay_file.close()
        }
    }

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
        -> overlay::Result<Box<dyn OverlayFile+Send+Sync>> {
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
            object_type: ObjectType::File
        };

        Ok(metadata)
    }

    fn get_file<'a, 'b>(&'a self, control_dir: &'b ControlDir<O>)
        -> overlay::Result<Box<dyn OverlayFile+Send+Sync>> {
        let file = BufferedFile {
            buf: Cursor::new(self.ops.init(control_dir.get_overlay().deref())?),
            ops: self.ops.clone(),
            overlay: Arc::clone(&control_dir.overlay)
        };
        Ok(Box::new(file) as Box<dyn OverlayFile+Send+Sync>)
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
    fn close(&mut self) -> overlay::Result<()> {
        self.ops.close(&mut *self.overlay.write().unwrap(),
                       self.buf.get_ref().as_slice())
    }

    fn truncate(&mut self, size: u64) -> overlay::Result<()> {
        if self.buf.position() as u64 > size {
            self.buf.set_position(size);
        }
        self.buf.get_mut().truncate(size as usize);
        Ok(())
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
            .map(|branch| branch.unwrap_or("(detached)").as_bytes().to_vec())
    }

    fn close<O>(&self, control_dir: &mut O, buffer: &[u8]) -> overlay::Result<()>
        where for<'o> O: Overlay + WorkspaceController<'o> + Send + Sync + 'static {
        let target_branch = str::from_utf8(buffer)
            .map_err(overlay::Error::from_fail)?;
        if Some(target_branch) != control_dir.get_current_branch()? {
            control_dir.switch_branch(target_branch.trim_end_matches('\n'))
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
                    .unwrap_or(b"(no HEAD)".to_vec()))
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

impl<O> Overlay for ControlDir<O>
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    type File = ControlFile<O>;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool) -> overlay::Result<Self::File> {
        if let Ok(ref dorkfile) = path.as_ref().strip_prefix(DORK_DIR_ENTRY) {
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

    fn list_directory<I, P>(&self, path: P) -> overlay::Result<I>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path> {
        match path.as_ref().to_str() {
            Some("") => {
                let root_dir: Vec<OverlayDirEntry> =
                    self.overlay.read().unwrap().list_directory(&path)?;
                let result = root_dir.into_iter()
                    .chain(ADDITIONAL_ROOT_DIRECTORIES.iter().cloned()).collect();
                Ok(result)
            }

            Some(prefix) if prefix == DORK_DIR_ENTRY => {
                self.special_files.iter().map(|(name, special_file)| {
                    special_file.metadata(self)
                        .map(|metadata| OverlayDirEntry::from((*name, &metadata)))
                }).collect()
            }

            _ => self.overlay.read().unwrap().list_directory(path)
        }
    }

    fn metadata<P: AsRef<Path>>(&self, path: P) -> overlay::Result<Metadata> {
        if path.as_ref() == Path::new(DORK_DIR_ENTRY) {
            Ok(Metadata {
                size: 0,
                object_type: ObjectType::Directory
            })
        } else if path.as_ref().starts_with(DORK_DIR_ENTRY) {
            let file_name = path.as_ref().strip_prefix(DORK_DIR_ENTRY).unwrap();
            file_name.to_str().ok_or("Unable to decode to UTF-8".into())
                .and_then(|s| {
                    self.special_files.get(s)
                        .ok_or("File entry not found!".into())
                        .and_then(|special_file| special_file.metadata(self))
                })
        } else {
            self.overlay.read().unwrap().metadata(path)
        }
    }

    fn delete_file<P: AsRef<Path>>(&self, path: P) -> overlay::Result<()> {
        if path.as_ref().starts_with(DORK_DIR_ENTRY) {
            Err("Cannot delete .dork special files".into())
        } else {
            self.overlay.read().unwrap().delete_file(path)
        }
    }

    fn revert_file<P: AsRef<Path>>(&self, path: P) -> overlay::Result<()> {
        if path.as_ref().starts_with(DORK_DIR_ENTRY) {
            Err("Cannot revert .dork special files".into())
        } else {
            self.overlay.read().unwrap().revert_file(path)
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Write},
        iter::Iterator
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

        let mut file = control_overlay.open_file("test.txt", true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        let mut commit_file = control_overlay.open_file(".dork/commit", true)
            .expect("Unable to open commit file");
        commit_file.write("Test commit message".as_bytes())
            .expect("Unable to write commit message");
        commit_file.close()
            .expect("Closing commit message failed");

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
            control_overlay.open_file("file1.txt", true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        file1.close().expect("Couldn't close test file");

        let first_commit = control_overlay.get_overlay_mut()
            .commit("Commit to first branch")
            .expect("Unable to create first commit");

        // Create branch "feature"
        let mut create_branch =
            control_overlay.open_file(".dork/create_branch", true)
                .expect("Unable to open create_branch special file");
        create_branch.write_all(b"feature")
            .expect("Create branch \"feature\" failed");
        create_branch.close().unwrap();

        // Check if we still track "master"
        let mut switch_branch =
            control_overlay.open_file(".dork/current_branch", false)
                .expect("Unable to open current_branch for reading");
        let mut content = Vec::new();
        switch_branch.read_to_end(&mut content).unwrap();
        assert_eq!(b"master", content.as_slice());
        switch_branch.close().unwrap();

        // Switch to branch "feature"
        let mut switch_branch =
            control_overlay.open_file(".dork/current_branch", true)
                .expect("Unable to open current_branch for reading");
        switch_branch.write_all(b"feature").unwrap();
        switch_branch.close().unwrap();

        // Check if switch is reflected
        let mut switch_branch =
            control_overlay.open_file(".dork/current_branch", false)
                .expect("Unable to open current_branch for reading");
        let mut content = Vec::new();
        switch_branch.read_to_end(&mut content).unwrap();
        assert_eq!(b"feature", content.as_slice());
        switch_branch.close().unwrap();

        // Add a commit with a second file to the branch "feature"
        let mut file2 =
            control_overlay.open_file("file2.txt", true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        file2.close().expect("Couldn't close test file");

        let second_commit = control_overlay.get_overlay_mut().commit("Commit to second branch")
            .expect("Unable to create second commit");

        // Switch back to master
        control_overlay.get_overlay_mut().switch_branch("master").unwrap();

        // Check if checked-out workspace is unchanged
        let mut file2 =
            control_overlay.open_file("file2.txt", false)
                .expect("Unable to open first test file");
        let mut content = Vec::new();
        file2.read_to_end(&mut content)
            .expect("Unable to read from first file");
        assert_eq!(b"Test 2", content.as_slice());
        file2.close().unwrap();
        drop(file2);

        // Now update the workspace
        let mut head =
            control_overlay.open_file(".dork/HEAD", true)
                .expect("Unable to open HEAD");
        write!(&mut head, "latest").expect("Unable to update workspace");
        head.close().unwrap();

        // ...and check if the second file is gone
        assert!(control_overlay.open_file("file2.txt", false).is_err());

        // Check if we're pointing to the right commit
        check_file_content(
            &mut control_overlay.open_file(".dork/HEAD", false).unwrap(),
            first_commit.to_string().as_str());

        // Switch back to feature branch
        control_overlay.get_overlay_mut().switch_branch("feature").unwrap();

        // ...and check if the second file is still gone
        assert!(control_overlay.open_file("file2.txt", false).is_err());

        // Update the workspace
        let mut head =
            control_overlay.open_file(".dork/HEAD", true)
                .expect("Unable to open HEAD");
        write!(&mut head, "latest").expect("Unable to update workspace");
        head.close().unwrap();

        // Check if we're pointing to the right commit
        check_file_content(
            &mut control_overlay.open_file(".dork/HEAD", false).unwrap(),
            second_commit.to_string().as_str());

        // Check if all committed files are present and contain the correct contents
        let mut file1 =
            control_overlay.open_file("file1.txt", false)
                .expect("Unable to open first test file");
        let mut content = Vec::new();
        file1.read_to_end(&mut content)
            .expect("Unable to read from first file");
        assert_eq!(b"Test 1", content.as_slice());

        let mut file2 =
            control_overlay.open_file("file2.txt", false)
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

        let mut log_file = control_overlay.open_file(".dork/log", false)
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
        control_overlay.delete_file(".dork/log").unwrap_err();

        let mut file = control_overlay.open_file("test.txt", true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        control_overlay.delete_file("test.txt").unwrap();
    }

    #[test]
    fn revert_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut file1 =
            control_overlay.open_file("file1.txt", true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        file1.close().expect("Couldn't close test file");
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(".dork/revert", true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"file1.txt")
            .expect("unable to write to revert file");
        revert_file.close().unwrap();
        assert_eq!(0, control_overlay.get_overlay().get_status().unwrap().count());
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
        file1.close().expect("Couldn't close test file");
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut file2 =
            control_overlay.open_file("dir/file2.txt", true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        file2.close().expect("Couldn't close test file");
        assert_eq!(2, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(".dork/revert", true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"file1.txt")
            .expect("unable to write to revert file");
        revert_file.close().unwrap();
        assert_eq!(1, control_overlay.get_overlay().get_status().unwrap().count());

        let mut revert_file =
            control_overlay.open_file(".dork/revert", true)
                .expect("Unable to open revert file");
        revert_file.write_all(b"dir")
            .expect("unable to write to revert dir");
        revert_file.close().unwrap();
        assert_eq!(0, control_overlay.get_overlay().get_status().unwrap().count());
    }

    #[test]
    fn update_via_control_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = open_working_copy(&dir);
        let mut control_overlay = super::ControlDir::new(working_copy);

        let mut test_file =
            control_overlay.open_file("test.txt", true).unwrap();
        write!(test_file, "A test text").unwrap();
        test_file.close().unwrap();
        drop(test_file);

        let first_commit = control_overlay.get_overlay_mut().commit("A test commit").unwrap();

        let mut test_file =
            control_overlay.open_file("test2.txt", true).unwrap();
        write!(&mut test_file, "A second test").unwrap();
        test_file.close().unwrap();
        drop(test_file);

        let second_commit = control_overlay.get_overlay_mut().commit("A test commit").unwrap();

        control_overlay.get_overlay_mut().set_head(first_commit).unwrap();
        assert_eq!(Some(first_commit),
                   control_overlay.get_overlay_mut().get_current_head_ref().unwrap());

        let mut head_file =
            control_overlay.open_file(".dork/HEAD", true).unwrap();
        write!(&mut head_file, "latest").unwrap();
        head_file.close().unwrap();
        drop(head_file);

        assert_eq!(Some(second_commit),
                   control_overlay.get_overlay_mut().get_current_head_ref().unwrap());
    }
}
