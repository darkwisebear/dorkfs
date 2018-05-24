use std::path::Path;
use std::io::{Read, Write, Seek, SeekFrom};
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::sync::RwLock;
use std::result::Result;
use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;
use std::borrow::Cow;

use failure::Error;
use time::{get_time, Timespec};
use fuse_mt::*;
use libc;

use cache::*;
use control::*;

use overlay::*;
use types;
use types::*;
use utility::OpenHandleSet;

lazy_static! {
    static ref STANDARD_DIR_ENTRIES: [::fuse_mt::DirectoryEntry; 2] = [
        ::fuse_mt::DirectoryEntry {
            name: OsString::from("."),
            kind: FileType::Directory
        },

        ::fuse_mt::DirectoryEntry {
            name: OsString::from(".."),
            kind: FileType::Directory
        }
    ];
}

fn system_time_to_timespec(systime: SystemTime) -> Timespec {
    use std::time::UNIX_EPOCH;

    let duration = systime.duration_since(UNIX_EPOCH)
        .expect("Time before epoch!");

    Timespec {
        sec: duration.as_secs() as i64,
        nsec: duration.subsec_nanos() as i32
    }
}

fn file_type_to_kind(file_type: fs::FileType) -> Result<FileType, Error> {
    if file_type.is_dir() {
        Ok(FileType::Directory)
    } else if file_type.is_file() {
        Ok(FileType::RegularFile)
    } else if file_type.is_symlink() {
        Ok(FileType::Symlink)
    } else {
        bail!("Unknown file type!");
    }
}

type Cache = HashFileCache;

#[derive(Debug)]
enum OpenObject {
    File(ControlFile<FilesystemOverlay<HashFileCache>>),
    Directory(Vec<OverlayDirEntry>),
}

pub struct DorkFS {
    overlay: ControlDir<FilesystemOverlay<Cache>>,
    open_handles: RwLock<OpenHandleSet<OpenObject>>,
    uid: u32,
    gid: u32,
    umask: u32
}

impl FilesystemMT for DorkFS {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let path = path.strip_prefix("/").expect("Expect absolute path");

        let metadata = match self.overlay.metadata(path) {
            Ok(metadata) => metadata,
            Err(err) => {
                info!("Unable to get attributes for {}: {}", path.to_string_lossy(), err);
                return Err(libc::ENOENT);
            }
        };

        let (kind, perm) = match metadata.object_type {
            types::ObjectType::File => {
                (FileType::RegularFile, self.calculate_permission(6, 6, 6))
            }

            types::ObjectType::Directory => {
                (FileType::Directory, self.calculate_permission(7, 7, 7))
            }

            _ => {
                warn!("Unsupported object type in directory");
                return Err(libc::EINVAL);
            }
        };

        let attr = FileAttr {
            size: metadata.size,
            blocks: (metadata.size+4095) / 4096,
            atime: get_time(),
            mtime: get_time(),
            ctime: get_time(),
            crtime: get_time(),
            kind,
            perm,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0
        };

        Ok((get_time(), attr))
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let path = path.strip_prefix("/").expect("Expect absolute path");

        match self.overlay.list_directory(path) {
            Ok(dir) => {
                let mut open_handles =
                    self.open_handles.write().unwrap();
                let dir_handle = open_handles.push(OpenObject::Directory(dir),
                                                   Cow::Borrowed(path.as_os_str()));
                Ok((dir_handle, 0))
            }

            Err(err) => {
                error!("Unable to open directory {}: {}", path.to_string_lossy(), err);
                Err(libc::ENOENT)
            }
        }
    }

    fn readdir(&self, _req: RequestInfo, _path: &Path, fh: u64) -> ResultReaddir {
        let open_handles =
            self.open_handles.read().unwrap();
        if let Some(open_obj) = open_handles.get(fh) {
            if let OpenObject::Directory(ref dir) = *open_obj {
                Ok(dir.iter()
                    .map(Self::overlay_dir_entry_to_fuse_dir_entry)
                    .chain(STANDARD_DIR_ENTRIES.iter().cloned())
                    .collect())
            } else {
                Err(libc::EBADF)
            }
        } else {
            Err(libc::EBADF)
        }
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        let mut open_handles =
            self.open_handles.write().unwrap();
        open_handles.remove(fh);
        Ok(())
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        info!("Opening file {}", path.to_string_lossy());

        let path = path.strip_prefix("/").expect("Expect absolute path");

        let is_writable = Self::is_writable(flags);
        let file = self.overlay.open_file(path, is_writable);

        match file {
            Ok(file) => {
                let handle = self.open_handles.write().unwrap()
                    .push(OpenObject::File(file), Cow::Borrowed(path.as_os_str()));
                Ok((handle, 0))
            },

            Err(err) => {
                error!("Unable to open file {}: {}", path.to_string_lossy(), err);
                Err(libc::EIO)
            }
        }
    }

    fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
        let mut open_handles =
            self.open_handles.write().unwrap();
        let file_obj =
            open_handles.get_mut(fh).ok_or(libc::EBADF)?;
        if let OpenObject::File(ref mut file) = *file_obj {
            let mut result = Vec::with_capacity(size as usize);
            unsafe { result.set_len(size as usize); }
            let count = file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.read(result.as_mut_slice()))
                .map_err(|e| {
                    error!("Couldn't read from file: {}", e);
                    libc::EIO
                })?;
            result.truncate(count);
            Ok(result)
        } else {
            Err(libc::EBADF)
        }
    }

    fn release(&self,
               _req: RequestInfo,
               path: &Path,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool) -> ResultEmpty {
        self.open_handles.write().unwrap().remove(fh)
            .ok_or(libc::EBADF)
            .and_then(|handle| {
                if let OpenObject::File(file) = handle {
                    file.close().
                        map_err(|e| {
                            error!("Unable to successfully close file {}: {}",
                                   path.to_string_lossy(),
                                   e);
                            libc::EIO
                        })
                } else {
                    Ok(())
                }
            })
    }

    fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, _mode: u32, flags: u32 )
        -> ResultCreate {
        let path = parent.strip_prefix("/").expect("Expect absolute path").join(name);

        if self.overlay.exists(&path) {
            error!("File already exists: {}", path.to_string_lossy());
            return Err(libc::EEXIST)
        }

        self.overlay.open_file(&path, Self::is_writable(flags))
            .map(|file| {
                let mut open_handles =
                    self.open_handles.write().unwrap();
                let fh = open_handles.push(OpenObject::File(file),
                                           Cow::Owned(path.into_os_string()));

                let current_time = get_time();
                CreatedEntry {
                    ttl: Timespec::new(0, 0),
                    attr: FileAttr {
                        size: 0,
                        blocks: 1,
                        atime: current_time,
                        mtime: current_time,
                        ctime: current_time,
                        crtime: current_time,
                        kind: FileType::RegularFile,
                        perm: self.calculate_permission(6, 6, 6),
                        nlink: 1,
                        uid: self.uid,
                        gid: self.gid,
                        rdev: 0,
                        flags: 0
                    },
                    fh,
                    flags: 0,
                }
            })
            .map_err(|e| {
                error!("Unable to create file: {}", e);
                libc::EIO
            })
    }

    fn write(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32)
        -> ResultWrite {
        self.open_handles.write().unwrap().get_mut(fh).ok_or(libc::EINVAL)
            .and_then(|handle| {
                if let OpenObject::File(ref mut file) = *handle {
                    file.seek(SeekFrom::Start(offset)).and_then(|_| {
                        file.write_all(data.as_slice())
                            .map(|_| data.len() as u32)
                    })
                    .map_err(|e| {
                        error!("Unable to write to file: {}", e);
                        libc::EIO
                    })
                } else {
                    Err(libc::EINVAL)
                }
            })
    }

    fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, _mode: u32) -> ResultEntry {
        let path = parent.strip_prefix("/").expect("Expect absolute path").join(name);

        info!("Creating overlay directory {}", path.to_string_lossy());
        self.overlay.as_ref().ensure_directory(&path)
            .map(|_| {
                let current_time = get_time();

                (Timespec::new(0, 0),
                 FileAttr {
                     size: 0,
                     blocks: 1,
                     atime: current_time,
                     mtime: current_time,
                     ctime: current_time,
                     crtime: current_time,
                     kind: FileType::Directory,
                     perm: self.calculate_permission(7, 7, 7),
                     nlink: 1,
                     uid: self.uid,
                     gid: self.gid,
                     rdev: 0,
                     flags: 0
                 })
            })
            .map_err(|e| {
                error!("Unable to create directory {}: {}", path.to_string_lossy(), e);
                libc::EEXIST
            })
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64)
        -> ResultEmpty {
        let path = path.strip_prefix("/").expect("Expect absolute path");

        info!("Truncating file fh: {:?} path: {:?} to {}", fh, path, size);

        let mut open_handles = self.open_handles.write().unwrap();
        let file = {
            let open_object = if let Some(fh) = fh {
                open_handles.get_mut(fh)
            } else {
                open_handles.get_named_mut(path)
            };

            match open_object {
                Some(OpenObject::File(ref mut file)) => Ok(file),
                Some(_) => {
                    warn!("Trying to truncate non-file object!");
                    Err(libc::EINVAL)
                }
                None => {
                    warn!("Object not found!");
                    Err(libc::ENOENT)
                }
            }
        };

        file.and_then(|file| {
            file.truncate(size).map_err(|e| {
                warn!("Truncating file failed: {}", e);
                libc::EIO
            })
        })
    }
}


impl DorkFS {
    fn is_writable(flags: u32) -> bool {
        ((flags as libc::c_int & libc::O_RDWR) != 0) ||
            ((flags as libc::c_int & libc::O_WRONLY) != 0)
    }

    fn calculate_permission(&self, u: u32, g: u32, o: u32) -> u16 {
        (((((u << 3) + g) << 3) + o) & (!self.umask)) as u16
    }

    pub fn with_overlay(overlay: FilesystemOverlay<HashFileCache>,
                        uid: u32,
                        gid: u32,
                        umask: u32) -> Result<Self, Error> {
        let fs = DorkFS {
            overlay: ControlDir::new(overlay),
            open_handles: RwLock::new(OpenHandleSet::new()),
            uid,
            gid,
            umask
        };
        Ok(fs)
    }

    pub fn mount<P: AsRef<Path>>(self, mountpoint: P) -> Result<(), Error> {
        mount(FuseMT::new(self, 1), &mountpoint, &[])
            .map_err(|e| Error::from(e))
    }

    fn object_type_to_file_type(obj_type: types::ObjectType) -> Result<FileType, Error> {
        match obj_type {
            types::ObjectType::File => Ok(FileType::RegularFile),
            types::ObjectType::Directory => Ok(FileType::Directory),
            _ => bail!("Unmappable object type")
        }
    }

    fn overlay_dir_entry_to_fuse_dir_entry(dir_entry: &OverlayDirEntry) -> ::fuse_mt::DirectoryEntry {
        let kind = match Self::object_type_to_file_type(dir_entry.metadata.object_type) {
            Ok(ft) => ft,
            Err(err) => {
                warn!("Unable to convert file type in dir entry: {}", err);
                FileType::RegularFile
            }
        };

        ::fuse_mt::DirectoryEntry {
            name: OsString::from(dir_entry.name.clone()),
            kind
        }
    }

    fn attr_from_metadata(&self, metadata: &fs::Metadata) -> Result<FileAttr, Error> {
        let size = metadata.len();

        let atime = system_time_to_timespec(metadata.accessed()?);
        let mtime = system_time_to_timespec(metadata.modified()?);
        let crtime = system_time_to_timespec(metadata.created()?);

        let kind = file_type_to_kind(metadata.file_type())?;
        let perm = if let FileType::Directory = kind {
            self.calculate_permission(7, 7, 7)
        } else {
            self.calculate_permission(6, 6, 6)
        };

        Ok(FileAttr {
            size,
            blocks: (size + 4095) / 4096,
            atime,
            mtime,
            ctime: mtime,
            crtime,
            kind,
            perm,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        })
    }
}
