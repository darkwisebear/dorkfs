use std::path::Path;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::sync::RwLock;
use std::result::Result;
use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;
use failure::Error;
use time::{get_time, Timespec};
use fuse_mt::*;
use libc;

use cache;
use cache::*;

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

#[derive(Debug)]
struct OpenHandleSet<T: Debug> {
    next_handle: u64,
    open_objects: HashMap<u64, T>
}

impl<T: Debug> OpenHandleSet<T> {
    fn new() -> Self {
        OpenHandleSet {
            next_handle: 0,
            open_objects: HashMap::new()
        }
    }

    fn push(&mut self, value: T) -> u64 {
        let handle = self.next_handle;
        self.next_handle += 1;

        self.open_objects.insert(handle, value);

        handle
    }

    fn get(&self, handle: u64) -> Option<&T> {
        self.open_objects.get(&handle)
    }

    fn get_mut(&mut self, handle: u64) -> Option<&mut T> {
        self.open_objects.get_mut(&handle)
    }

    fn remove(&mut self, handle: u64) -> Option<T> {
        self.open_objects.remove(&handle)
    }
}

type Cache = HashFileCache;
type CacheFile = <Cache as CacheLayer>::File;
type CacheDirectory = <Cache as CacheLayer>::Directory;
type CacheObject = cache::CacheObject<CacheFile, CacheDirectory>;

#[derive(Debug)]
enum OpenObject {
    File(CacheFile),
    Directory(CacheDirectory)
}

pub struct DorkFS {
    cache: Cache,
    head_commit: CacheRef,
    open_handles: RwLock<OpenHandleSet<OpenObject>>
}

impl FilesystemMT for DorkFS {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let cache_ref = match self.resolve_object_ref(path) {
            Ok(entry) => entry,
            Err(_) => return Err(libc::ENOENT)
        };

        let metadata = self.cache.metadata(&cache_ref)
            .map_err(|err| {
                warn!("Error querying cache object metadata {}", err);
                libc::EIO
            })?;

        let (kind, perm) = match metadata.object_type {
            ObjectType::File => {
                (FileType::RegularFile, (6 << 6) + (4 << 3) + 0)
            }

            ObjectType::Directory => {
                (FileType::Directory, (7 << 6) + (5 << 3) + 0)
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
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0
        };

        Ok((get_time(), attr))
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        match self.resolve_object(path) {
            Ok(cache_obj) => {
                match cache_obj {
                    cache::CacheObject::Directory(dir) => {
                        let mut open_handles =
                            self.open_handles.write().unwrap();
                        let dir_handle = open_handles.push(OpenObject::Directory(dir));
                        Ok((dir_handle, 0))
                    }

                    _ => {
                        warn!("Referenced object not a directory!");
                        Err(libc::ENOENT)
                    }
                }
            }

            Err(err) => {
                warn!("{}", err);
                Err(libc::ENOENT)
            }
        }
    }

    fn readdir(&self, _req: RequestInfo, _path: &Path, fh: u64) -> ResultReaddir {
        use std::iter::IntoIterator;

        let open_handles =
            self.open_handles.read().unwrap();
        if let Some(open_obj) = open_handles.get(fh) {
            if let OpenObject::Directory(ref dir) = *open_obj {
                Ok(dir.clone().into_iter()
                    .map(Self::cache_dir_entry_to_fuse_dir_entry)
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

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        self.resolve_object_ref(path)
            .and_then(|cache_ref|
                self.cache.metadata(&cache_ref)
                    .map(|metadata| (metadata, cache_ref))
                    .map_err(Error::from)
            )
            .and_then(|(metadata, cache_ref)| {
                if let ObjectType::File = metadata.object_type {
                    self.cache.get(&cache_ref)
                        .map(|cache_obj| cache_obj.into_file().unwrap())
                        .map(|file|
                            (self.open_handles.write().unwrap().push(OpenObject::File(file)), 0)
                        )
                        .map_err(Error::from)
                } else {
                    Err(format_err!("Cache object not a file"))
                }
            })
            .map_err(|err| {
                warn!("Error opening file: {}", err);
                libc::EIO
            })
    }

    fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
        use std::io::{Read, Seek, SeekFrom};

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
               _path: &Path,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool) -> ResultEmpty {
        self.open_handles.write().unwrap().remove(fh).ok_or(libc::EBADF)?;
        Ok(())
    }

    /*fn create(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32, _flags: u32 )
        -> ResultCreate {
        self.cache.create_file()
            .map(|file| {
                let file_attr = file.metadata;
                let mut open_handles =
                    self.open_handles.write().unwrap();
                let handle = open_handles.push(OpenObject::WritableFile(file));
                CreatedEntry {
                    ttl: Timespec::new(0, 0),
                    attr: FileAttr {
                        size: 0,
                        blocks: 1,
                        atime: get_time(),
                        mtime: get_time(),
                        ctime: get_time(),
                        crtime: get_time(),
                        kind: FileType::RegularFile,
                        perm: (6 << 6) + (4 << 3) + 0,
                        nlink: 1,
                        uid: 1000,
                        gid: 1000,
                        rdev: 0,
                        flags: 0
                    },
                    fh: handle,
                    flags: 0,
                }
            })
            .map_err(|err| {
                warn!("Error creating file: {}", err);
                Err(libc::EIO)
            })
    }*/
}

impl DorkFS {
    pub fn with_path<P: AsRef<Path>>(cachedir: P, head_commit: CacheRef) -> Result<Self, Error> {
        let cache = Cache::new(&cachedir)?;
        Self::with_cache(cache, head_commit)
    }

    pub fn with_cache(cache: Cache, head_commit: CacheRef) -> Result<Self, Error> {
        let fs = DorkFS {
            cache,
            head_commit,
            open_handles: RwLock::new(OpenHandleSet::new())
        };
        Ok(fs)
    }

    pub fn mount<P: AsRef<Path>>(self, mountpoint: P) -> Result<(), Error> {
        mount(FuseMT::new(self, 1), &mountpoint, &[])
            .map_err(|e| Error::from(e))
    }

    fn resolve_object(&self, path: &Path) -> Result<CacheObject, Error> {
        self.resolve_object_ref(path)
            .and_then(|cache_ref| self.cache.get(&cache_ref).map_err(Error::from))
    }

    fn resolve_object_ref(&self, path: &Path) -> Result<CacheRef, Error> {
        use std::path::Component;

        let commit = self.cache.get(&self.head_commit)?.into_commit()?;
        let mut objs = Vec::new();

        fn find_entry<D: Directory>(dir: D, component: &OsStr) -> Option<cache::DirectoryEntry> {
            dir.into_iter().find(|entry| Some(entry.name.as_str()) == component.to_str())
        }

        for component in path.components() {
            match component {
                Component::Prefix(..) => panic!("A prefix is not allowed to appear in a cache path"),
                Component::RootDir => {
                    objs.clear();
                    objs.push(commit.tree.clone());
                }
                Component::CurDir => (),
                Component::ParentDir => {
                    objs.pop().ok_or(format_err!("Outside path due to too many parent dirs!"))?;
                }
                Component::Normal(entry) => {
                    let cache_ref = {
                        let cur_dir_ref = objs.last()
                            .expect("Non-directory ref in directory stack");
                        let cur_dir =
                            self.cache.get(cur_dir_ref)?.into_directory()?;
                        let dir_entry = find_entry(cur_dir, entry)
                            .ok_or(format_err!("Couldn't resolve path {}", path.display()))?;
                        dir_entry.cache_ref
                    };
                    objs.push(cache_ref);
                }
            }
        }

        objs.last().cloned().ok_or(format_err!("No object not found in {}", path.display()))
    }

    fn object_type_to_file_type(obj_type: ObjectType) -> Result<FileType, Error> {
        match obj_type {
            ObjectType::File => Ok(FileType::RegularFile),
            ObjectType::Directory => Ok(FileType::Directory),
            _ => bail!("Unmappable object type")
        }
    }

    fn cache_dir_entry_to_fuse_dir_entry(dir_entry: cache::DirectoryEntry)
        -> ::fuse_mt::DirectoryEntry {
        let kind = match Self::object_type_to_file_type(dir_entry.object_type) {
            Ok(ft) => ft,
            Err(err) => {
                warn!("Unable to convert file type in dir entry: {}", err);
                FileType::RegularFile
            }
        };

        ::fuse_mt::DirectoryEntry {
            name: OsString::from(dir_entry.name),
            kind
        }
    }

    fn attr_from_metadata(metadata: &fs::Metadata) -> Result<FileAttr, Error> {
        let size = metadata.len();

        let atime = system_time_to_timespec(metadata.accessed()?);
        let mtime = system_time_to_timespec(metadata.modified()?);
        let crtime = system_time_to_timespec(metadata.created()?);

        let kind = file_type_to_kind(metadata.file_type())?;

        Ok(FileAttr {
            size,
            blocks: (size + 4095) / 4096,
            atime,
            mtime,
            ctime: mtime,
            crtime,
            kind,
            perm: 0,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
        })
    }
}
