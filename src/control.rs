use std::path::{PathBuf, Path};
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::iter::FromIterator;
use std::fmt::Debug;
use std::collections::{HashSet, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::str;
use std::mem::replace;

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

    static ref DORKFS_FILES: HashMap<String, Metadata> = [
        (String::from("log"),
            Metadata {
                size: 0,
                object_type: ObjectType::File
            }
        ),
        (String::from("commit"),
            Metadata {
                size: 0,
                object_type: ObjectType::File
            }
        )
    ].iter().cloned().collect();
}

#[derive(Debug)]
pub enum ControlFile<O: Overlay+WorkspaceController> {
    Commit(Cursor<Vec<u8>>, Arc<RwLock<O>>),
    Log(Cursor<Vec<u8>>),
    OverlayFile(O::File)
}

impl<O: Overlay+WorkspaceController> Drop for ControlFile<O> {
    fn drop(&mut self) {
        self.execute_commit()
            .expect("Unable to execute commit")
    }
}

impl<O: Overlay+WorkspaceController> OverlayFile for ControlFile<O> {
    fn close(mut self) -> Result<(), Error> {
        self.execute_commit()
    }
}

impl<O: Overlay+WorkspaceController> ControlFile<O> {
    fn execute_commit(&mut self) -> Result<(), Error> {
        match self {
            ControlFile::Commit(ref mut buf, ref control_dir) => {
                info!("Executing commit");
                let buf = replace(buf, Cursor::new(Vec::default()));
                let contents = buf.get_ref().as_slice();
                if contents.len() > 0 {
                    str::from_utf8(contents)
                        .map_err(Error::from)
                        .and_then(|commit_msg| {
                            info!("Commit message: {}", commit_msg);
                            let mut controller = control_dir.write().unwrap();
                            controller.commit(commit_msg).map(|_| ())
                        })
                } else {
                    info!("No commit message set");
                    Ok(())
                }
            }
            _ => Ok(())
        }
    }

    fn get_writer(&mut self) -> Result<&mut Write, io::Error> {
        match *self {
            ControlFile::Commit(ref mut data, ..) => Ok(data as &mut Write),
            ControlFile::Log(ref data) => {
                error!("Log is read-only!");
                Err(io::ErrorKind::PermissionDenied.into())
            }
            ControlFile::OverlayFile(ref mut file) => Ok(file as &mut Write)
        }
    }
}

impl<O: Overlay+WorkspaceController> Read for ControlFile<O> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match *self {
            ControlFile::Commit(..) => {
                error!("Unable to read from commit");
                Err(io::ErrorKind::PermissionDenied.into())
            }
            ControlFile::Log(ref mut data) => data.read(buf),
            ControlFile::OverlayFile(ref mut file) => file.read(buf)
        }
    }
}

impl<O: Overlay+WorkspaceController> Write for ControlFile<O> {
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

impl<O: Overlay+WorkspaceController> Seek for ControlFile<O> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        if let ControlFile::OverlayFile(ref mut file) = *self {
            file.seek(pos)
        } else {
            Err(io::ErrorKind::AddrNotAvailable.into())
        }
    }
}

#[derive(Debug)]
pub struct ControlDir<O: Overlay+WorkspaceController> {
    overlay: Arc<RwLock<O>>
}

impl<O: Overlay+WorkspaceController> ControlDir<O> {
    pub fn new(overlay: O) -> Self {
        ControlDir {
            overlay: Arc::new(RwLock::new(overlay))
        }
    }

    pub fn as_ref<'a>(&'a self) -> impl Deref<Target=O>+'a {
        self.overlay.read().unwrap()
    }

    pub fn as_mut<'a>(&'a mut self) -> impl DerefMut<Target=O>+'a {
        self.overlay.write().unwrap()
    }
}

impl<O: Overlay+WorkspaceController> Overlay for ControlDir<O> {
    type File = ControlFile<O>;

    fn open_file<P: AsRef<Path>>(&self, path: P, writable: bool) -> Result<Self::File, Error> {
        if let Ok(ref dorkfile) = path.as_ref().strip_prefix(DORK_DIR_ENTRY) {
            match dorkfile.to_str() {
                Some("commit") => {
                    info!("Open commit special file");
                    Ok(ControlFile::Commit(Cursor::new(Vec::new()),
                                                         Arc::clone(&self.overlay)))
                },
                _ => bail!("Unable to open special file {}", dorkfile.to_string_lossy())
            }
        } else {
            self.overlay.read().unwrap().open_file(&path, writable)
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
                Ok(DORKFS_FILES.iter().map(OverlayDirEntry::from).collect())
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
                    DORKFS_FILES.get(s).cloned().ok_or(format_err!("File entry not found!"))
                })
        } else {
            self.overlay.read().unwrap().metadata(path)
        }
    }
}
