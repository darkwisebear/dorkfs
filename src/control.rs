use std::path::Path;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::iter::FromIterator;
use std::fmt::{self, Debug, Formatter};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock, Mutex};
use std::str;
use std::marker::PhantomData;

use failure::Error;

use types::*;

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

pub struct OverlayFileWrapper(Box<dyn OverlayFile+Send+Sync>);

impl Debug for OverlayFileWrapper {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "<Dynamic overlay file object>")
    }
}

#[derive(Debug)]
pub enum ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    Commit(Option<Cursor<Vec<u8>>>, Arc<RwLock<O>>),
    Log(Cursor<Vec<u8>>),
    SpecialFile(OverlayFileWrapper),
    OverlayFile(O::File)
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
    fn close(mut self) -> Result<(), Error> {
        self.execute_commit()
    }

    fn truncate(&mut self, size: u64) -> Result<(), Error> {
        match self {
            ControlFile::Commit(Some(ref mut buf), ..) => {
                let max_pos = u64::min(buf.position(), size);
                buf.set_position(max_pos);

                buf.get_mut().truncate(size as usize);

                Ok(())
            }
            ControlFile::Commit(None, ..) => Err(format_err!("Commit already done!")),
            ControlFile::Log(..) => Ok(()),
            ControlFile::SpecialFile(OverlayFileWrapper(special_file)) => special_file.truncate(size),
            ControlFile::OverlayFile(ref mut file) => file.truncate(size)
        }
    }
}

impl<O> ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn execute_commit(&mut self) -> Result<(), Error> {
        match self {
            ControlFile::Commit(ref mut buf, ref control_dir) => {
                if let Some(buf) = buf.take() {
                    info!("Executing commit");
                    let contents = buf.get_ref().as_slice();
                    str::from_utf8(contents)
                        .map_err(Error::from)
                        .and_then(|commit_msg| {
                            if !commit_msg.is_empty() {
                                info!("Commit message: {}", commit_msg);
                                let mut controller = control_dir.write().unwrap();
                                controller.commit(commit_msg).map(|_| ())
                            } else {
                                info!("Aborting commit due to empty commit message.");
                                Ok(())
                            }
                        })
                } else {
                    debug!("Commit already executed");
                    Ok(())
                }
            }
            _ => Ok(())
        }
    }

    fn get_writer(&mut self) -> Result<&mut dyn Write, io::Error> {
        match *self {
            ControlFile::Commit(Some(ref mut data), ..) => Ok(data as &mut dyn Write),
            ControlFile::Commit(None, ..) => Err(io::Error::from(io::ErrorKind::NotConnected)),

            ControlFile::Log(_) => {
                error!("Log is read-only!");
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            }

            ControlFile::SpecialFile(OverlayFileWrapper(ref mut special_file)) =>
                Ok(special_file as &mut dyn Write),

            ControlFile::OverlayFile(ref mut file) => Ok(file as &mut dyn Write)
        }
    }
}

impl<O> Read for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match *self {
            ControlFile::Commit(..) => {
                error!("Unable to read from commit");
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            }
            ControlFile::Log(ref mut data) => data.read(buf),
            ControlFile::SpecialFile(OverlayFileWrapper(ref mut special_file)) => special_file.read(buf),
            ControlFile::OverlayFile(ref mut file) => file.read(buf)
        }
    }
}

impl<O> Write for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.get_writer().and_then(|w| w.write(buf))
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        if let Ok(writer) = self.get_writer() {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl<O> Seek for ControlFile<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match *self {
            ControlFile::OverlayFile(ref mut file) => file.seek(pos),
            ControlFile::Commit(Some(ref mut buf), ..) => buf.seek(pos),
            ControlFile::Commit(None, ..) => Err(io::Error::from(io::ErrorKind::NotConnected)),
            ControlFile::SpecialFile(OverlayFileWrapper(ref mut special_file)) =>
                special_file.seek(pos),
            ControlFile::Log(ref mut buf) => buf.seek(pos)
        }
    }
}

pub trait SpecialFile<O>: Debug+Send+Sync where for<'a> O: Overlay+WorkspaceController<'a> {
    fn metadata(&self, control_dir: &ControlDir<O>) -> Result<Metadata, Error>;
    fn get_file<'a, 'b>(&'a self, _control_dir: &'b ControlDir<O>)
        -> Result<Box<dyn OverlayFile+Send+Sync>, Error> {
        bail!("This special file type cannot be accessed like a normal file")
    }
}

#[derive(Debug)]
struct ConstantSpecialFile;

impl<O> SpecialFile<O> for ConstantSpecialFile where for<'a> O: Overlay+WorkspaceController<'a> {
    fn metadata(&self, _control_dir: &ControlDir<O>) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: 0,
            object_type: ObjectType::File
        };

        Ok(metadata)
    }
}

struct BufferedFile<O, F> where for<'a> O: Overlay+WorkspaceController<'a>,
                                F: Fn(&ControlDir<O>) -> Result<Vec<u8>, Error> {
    update: F,
    _marker: PhantomData<O>
}

impl<O, F> Debug for BufferedFile<O, F> where for<'a> O: Overlay+WorkspaceController<'a>,
                                F: Fn(&ControlDir<O>) -> Result<Vec<u8>, Error> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Mem-buffered special file")
    }
}

impl<O, F> SpecialFile<O> for BufferedFile<O, F>
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>,
          F: Send+Sync+Fn(&ControlDir<O>) -> Result<Vec<u8>, Error> {
    fn metadata(&self, control_dir: &ControlDir<O>) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: (self.update)(control_dir)?.len() as u64,
            object_type: ObjectType::File
        };

        Ok(metadata)
    }

    fn get_file<'a, 'b>(&'a self, control_dir: &'b ControlDir<O>)
        -> Result<Box<dyn OverlayFile+Send+Sync>, Error> {
        Ok(Box::new(Cursor::new((self.update)(control_dir)?)) as Box<dyn OverlayFile+Send+Sync>)
    }
}

impl<O, F> BufferedFile<O, F> where for<'a> O: Overlay+WorkspaceController<'a>,
                                    F: Fn(&ControlDir<O>) -> Result<Vec<u8>, Error> {
    fn new(update: F) -> Self {
        BufferedFile {
            update,
            _marker: PhantomData
        }
    }
}

#[derive(Debug)]
struct LogFile;

impl<O> SpecialFile<O> for LogFile
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    fn metadata(&self, control_dir: &ControlDir<O>) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: control_dir.generate_log_string()?.as_bytes().len() as u64,
            object_type: ObjectType::File
        };

        Ok(metadata)
    }
}

#[derive(Debug)]
struct StatusFile;

impl<O> SpecialFile<O> for StatusFile
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    fn metadata(&self, control_dir: &ControlDir<O>) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: control_dir.generate_status_string()?.as_bytes().len() as u64,
            object_type: ObjectType::File
        };

        Ok(metadata)
    }
}

#[derive(Debug)]
pub struct ControlDir<O> where for<'a> O: Overlay+WorkspaceController<'a> {
    overlay: Arc<RwLock<O>>,
    special_files: HashMap<&'static str, Box<SpecialFile<O>>>
}

impl<O> ControlDir<O> where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    pub fn new(overlay: O) -> Self {

        let mut special_files = HashMap::new();
        special_files.insert("log",
                             Box::new(LogFile) as Box<dyn SpecialFile<O>>);
        special_files.insert("commit",
                             Box::new(ConstantSpecialFile) as Box<dyn SpecialFile<O>>);
        special_files.insert("status",
                             Box::new(BufferedFile::new(|control|
                                 control.generate_status_string().map(String::into_bytes))) as Box<dyn SpecialFile<O>>);
        ControlDir {
            overlay: Arc::new(RwLock::new(overlay)),
            special_files
        }
    }

    pub fn get_overlay<'a>(&'a self) -> impl Deref<Target=O>+'a {
        self.overlay.read().unwrap()
    }

    fn generate_log_string(&self) -> Result<String, Error> {
        let workspace_controller = self.get_overlay();
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

    fn generate_status_string(&self) -> Result<String, Error> {
        let overlay = self.get_overlay();
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

impl<O> Overlay for ControlDir<O>
    where for<'a> O: Send+Sync+Overlay+WorkspaceController<'a>+'static {
    type File = ControlFile<O>;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool) -> Result<Self::File, Error> {
        if let Ok(ref dorkfile) = path.as_ref().strip_prefix(DORK_DIR_ENTRY) {
            match dorkfile.to_str() {
                Some("commit") => {
                    info!("Open commit special file");
                    Ok(ControlFile::Commit(Some(Cursor::new(Vec::new())),
                                           Arc::clone(&self.overlay)))
                }

                Some("log") => {
                    info!("Open log special file");
                    self.generate_log_string()
                        .map(|full_log_string| {
                            let byte_vec = Vec::from(full_log_string);
                            ControlFile::Log(Cursor::new(byte_vec))
                        })
                }

                Some(filename) => {
                    if let Some(special_file) = self.special_files.get(filename) {
                        special_file.get_file(self)
                            .map(OverlayFileWrapper)
                            .map(ControlFile::SpecialFile)
                    } else {
                        bail!("Unable to open special file {}", dorkfile.to_string_lossy())
                    }
                }

                None => bail!("No filename given!")
            }
        } else {
            self.overlay.write().unwrap().open_file(&path, writable)
                .map(ControlFile::OverlayFile)
        }
    }

    fn list_directory<I, P>(&self, path: P) -> Result<I, Error> where I: FromIterator<OverlayDirEntry>,
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

            _ => self.overlay.read().unwrap().list_directory(&path)
        }
    }

    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata, Error> {
        if path.as_ref() == Path::new(DORK_DIR_ENTRY) {
            Ok(Metadata {
                size: 0,
                object_type: ObjectType::Directory
            })
        } else if path.as_ref().starts_with(DORK_DIR_ENTRY) {
            let file_name = path.as_ref().strip_prefix(DORK_DIR_ENTRY).unwrap();
            file_name.to_str().ok_or(format_err!("Unable to decode to UTF-8"))
                .and_then(|s| {
                    self.special_files.get(s)
                        .ok_or(format_err!("File entry not found!"))
                        .and_then(|special_file| special_file.metadata(self))
                })
        } else {
            self.overlay.read().unwrap().metadata(path)
        }
    }

    fn delete_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        if path.as_ref().starts_with(DORK_DIR_ENTRY) {
            bail!("Cannot delete .dork special files")
        } else {
            self.overlay.read().unwrap().delete_file(path)
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use overlay::testutil::open_working_copy;
    use types::{Overlay, OverlayFile, WorkspaceController};
    use std::io::{Read, Write};
    use std::iter::Iterator;
    use cache::ReferencedCommit;

    #[test]
    fn commit_via_file() {
        ::init_logging();

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
    fn fetch_empty_log() {
        ::init_logging();

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
        ::init_logging();

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
}
