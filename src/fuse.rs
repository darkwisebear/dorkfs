use std::path::Path;
use std::io::{Read, Write, Seek, SeekFrom};
use std::ffi::{OsStr, OsString};
use std::sync::RwLock;
use std::result::Result;
use std::borrow::Cow;
use std::fmt::Debug;

use failure::Error;
use time::{get_time, Timespec};
use chrono::{DateTime, TimeZone, Timelike};
use fuse_mt::*;
use libc;

use crate::overlay::{self, *};
use crate::types;
use crate::utility::OpenHandleSet;

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

fn to_timespec<Tz: TimeZone>(time: &DateTime<Tz>) -> Timespec {
    let sec = time.timestamp();
    let nsec = time.nanosecond() as i32;
    Timespec::new(sec, nsec)
}

#[derive(Debug)]
enum OpenObject<O> where O: Overlay+'static, <O as Overlay>::File: Debug {
    File(<O as Overlay>::File),
    Directory(Vec<OverlayDirEntry>),
}

#[derive(Debug)]
struct FsState<O> where O: Overlay+Debug+'static, <O as Overlay>::File: Debug {
    overlay: O,
    open_handles: OpenHandleSet<OpenObject<O>>
}

#[derive(Debug)]
pub struct DorkFS<O> where O: Overlay+Debug+'static, <O as Overlay>::File: Debug {
    state: RwLock<FsState<O>>,
    uid: u32,
    gid: u32,
    umask: u16
}

impl<O> FilesystemMT for DorkFS<O> where
    O: Overlay+Debug+Send+Sync,
    <O as Overlay>::File: Debug+Send+Sync+'static {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let path = path.strip_prefix("/").expect("Expect absolute path");
        let state = self.state.read().unwrap();

        let metadata = match state.overlay.metadata(path) {
            Ok(metadata) => metadata,
            Err(err) => {
                info!("Unable to get attributes for {}: {}", path.to_string_lossy(), err);
                return Err(libc::ENOENT);
            }
        };

        let perm = match metadata.object_type {
            types::ObjectType::File => self.calculate_permission(6, 6, 6),
            types::ObjectType::Pipe => self.calculate_permission(4, 4, 4),
            types::ObjectType::Directory => self.calculate_permission(7, 7, 7),
            types::ObjectType::Symlink => Self::octal_to_val(7, 7, 7)
        };

        let kind = Self::object_type_to_file_type(metadata.object_type);

        let time = to_timespec(&metadata.modified_date);
        let attr = FileAttr {
            size: metadata.size,
            blocks: (metadata.size+4095) / 4096,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
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
        let mut state = self.state.write().unwrap();

        match state.overlay.list_directory(path)
            .and_then(|dir_iter|
                dir_iter.collect::<Result<Vec<_>, _>>()) {
            Ok(dir) => {
                let dir_handle = state.open_handles.push(OpenObject::Directory(dir),
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
        let state = self.state.read().unwrap();
        if let Some(open_obj) = state.open_handles.get(fh) {
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
        let mut state = self.state.write().unwrap();
        state.open_handles.remove(fh);
        Ok(())
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        info!("Opening file {}", path.to_string_lossy());

        let path = path.strip_prefix("/").expect("Expect absolute path");
        let mut state = self.state.write().unwrap();

        let is_writable = Self::is_writable(flags);
        let file = state.overlay.open_file(path, is_writable);

        match file {
            Ok(file) => {
                let handle = state.open_handles.push(OpenObject::File(file),
                                                     Cow::Borrowed(path.as_os_str()));
                Ok((handle, 0))
            },

            Err(err) => {
                error!("Unable to open file {}: {}", path.to_string_lossy(), err);
                Err(libc::EIO)
            }
        }
    }

    fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
        let mut state = self.state.write().unwrap();
        let file_obj =
            state.open_handles.get_mut(fh).ok_or(libc::EBADF)?;
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
               _path: &Path,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool) -> ResultEmpty {
        self.state.write().unwrap().open_handles.remove(fh)
            .ok_or(libc::EBADF)
            .map(drop)
    }

    fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, _mode: u32, flags: u32 )
        -> ResultCreate {
        let path = parent.strip_prefix("/").expect("Expect absolute path").join(name);
        let mut state = self.state.write().unwrap();

        if state.overlay.exists(&path) {
            error!("File already exists: {}", path.to_string_lossy());
            return Err(libc::EEXIST)
        }

        state.overlay.open_file(&path, Self::is_writable(flags))
            .map(|file| {
                let fh = state.open_handles.push(OpenObject::File(file),
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
        self.state.write().unwrap().open_handles.get_mut(fh).ok_or(libc::EINVAL)
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
        let state = self.state.read().unwrap();

        info!("Creating overlay directory {}", path.to_string_lossy());

        let ensure_result = state.overlay.ensure_directory(&path);
        match ensure_result {
            Ok(()) => {
                let current_time = get_time();

                Ok((Timespec::new(0, 0),
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
                 }))
            }

            Err(e) => {
                error!("Unable to create directory {}: {}", path.to_string_lossy(), e);
                Err(libc::EEXIST)
            }
        }
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64)
        -> ResultEmpty {
        let path = path.strip_prefix("/").expect("Expect absolute path");

        info!("Truncating file fh: {:?} path: {:?} to {}", fh, path, size);

        let mut state = self.state.write().unwrap();
        let file = {
            let open_object = if let Some(fh) = fh {
                state.open_handles.get_mut(fh)
            } else {
                state.open_handles.get_named_mut(path)
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

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        self.delete_file(parent, name)
            .map_err(|e| {
                warn!("Unable to delete file: {}", e);
                libc::ENOENT
            })
    }

    fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> Result<(), i32> {
        self.delete_file(parent, name)
            .map_err(|e| match e {
                overlay::Error::FileNotFound => libc::ENOENT,
                overlay::Error::NonemptyDirectory => libc::ENOTEMPTY,
                e => {
                    warn!("Unable to remove directory: {}", e);
                    libc::ENOENT
                }
            })
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> Result<Vec<u8>, i32> {
        let path = path.strip_prefix("/").expect("Expect absolute path");
        let mut state = self.state.write().unwrap();
        state.overlay.open_file(path, false)
            .map_err(|e| {
                warn!("Unable to open symlink: {}", &e);
                match e {
                    overlay::Error::FileNotFound => libc::ENOENT,
                    _ => libc::EIO
                }
            })
            .and_then(|mut file| {
                let mut result = Vec::new();
                file.read_to_end(&mut result)
                    .map(move |_| result)
                    .map_err(|e| {
                        warn!("Unable to read link from overlay: {}", e);
                        libc::EIO
                    })
            })
    }
}

impl<O> DorkFS<O> where
    O: Overlay+Debug+Send+Sync+'static,
    <O as Overlay>::File: Debug+Send+Sync+'static {
    fn is_writable(flags: u32) -> bool {
        ((flags as libc::c_int & libc::O_RDWR) != 0) ||
            ((flags as libc::c_int & libc::O_WRONLY) != 0)
    }

    fn octal_to_val(u: u16, g: u16, o: u16) -> u16 {
        (((u << 3) +g) << 3) + o
    }

    fn calculate_permission(&self, u: u16, g: u16, o: u16) -> u16 {
        Self::octal_to_val(u, g, o) & (!self.umask)
    }

    pub fn with_overlay(overlay: O, uid: u32, gid: u32, umask: u16) -> Result<Self, Error> {
        Ok(DorkFS {
            state: RwLock::new(FsState {
                overlay,
                open_handles: OpenHandleSet::new()
            }),
            uid, gid, umask
        })
    }

    pub fn mount<P: AsRef<Path>>(self, mountpoint: P) -> Result<(), Error> {
        let fuse = FuseMT::new(self, 1);
        mount(fuse, &mountpoint, &[OsStr::new("default_permissions")])
            .map_err(Error::from)
    }

    fn object_type_to_file_type(obj_type: types::ObjectType) -> FileType {
        match obj_type {
            types::ObjectType::File => FileType::RegularFile,
            types::ObjectType::Directory => FileType::Directory,
            types::ObjectType::Pipe => FileType::NamedPipe,
            types::ObjectType::Symlink => FileType::Symlink
        }
    }

    fn overlay_dir_entry_to_fuse_dir_entry(dir_entry: &OverlayDirEntry) -> ::fuse_mt::DirectoryEntry {
        let kind = Self::object_type_to_file_type(dir_entry.object_type);

        ::fuse_mt::DirectoryEntry {
            name: OsString::from(dir_entry.name.clone()),
            kind
        }
    }

    fn delete_file(&self, parent: &Path, name: &OsStr) -> overlay::Result<()> {
        let path = parent.strip_prefix("/")
            .expect("Expect absolute path")
            .join(name);
        let state = self.state.read().unwrap();
        state.overlay.delete_file(&path)
    }
}
