use std::path::Path;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::sync::RwLock;
use std::result::Result;
use std::collections::HashMap;
use failure::Error;
use time::get_time;
use fuse_mt::*;
use libc;

use cache::*;

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

    fn remove(&mut self, handle: u64) -> Option<T> {
        self.open_objects.remove(&handle)
    }
}

enum StandardDirEntries {
    CurDir,
    ParentDir,
    Done
}

impl StandardDirEntries {
    fn new() -> Self {
        StandardDirEntries::CurDir
    }
}

impl Iterator for StandardDirEntries {
    type Item = ::fuse_mt::DirectoryEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = match *self {
            StandardDirEntries::CurDir => {
                *self = StandardDirEntries::ParentDir;
                Some(".")
            }

            StandardDirEntries::ParentDir => {
                *self = StandardDirEntries::Done;
                Some("..")
            }

            StandardDirEntries::Done => None
        };

        entry.map(|name| ::fuse_mt::DirectoryEntry {
            name: OsString::from(name),
            kind: FileType::Directory
        })
    }
}

pub struct DorkFS {
    cache: HashFileCache,
    head_commit: CacheRef,
    open_handles: RwLock<OpenHandleSet<HashCacheObject>>
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
                    dir @ CacheObject::Directory(..) => {
                        let mut open_handles =
                            self.open_handles.write().unwrap();
                        let dir_handle = open_handles.push(dir);
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
        let open_handles =
            self.open_handles.read().unwrap();
        if let Some(cache_obj) = open_handles.get(fh) {
            if let CacheObject::Directory(ref dir) = *cache_obj {
                Ok(dir.iter()
                    .map(Self::cache_dir_entry_to_fuse_dir_entry)
                    .chain(StandardDirEntries::new())
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
}

impl DorkFS {
    pub fn with_path<P: AsRef<Path>>(cachedir: P, head_commit: CacheRef) -> Result<Self, Error> {
        let cache = HashFileCache::new(&cachedir)?;
        Self::with_cache(cache, head_commit)
    }

    pub fn with_cache(cache: HashFileCache, head_commit: CacheRef) -> Result<Self, Error> {
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

    fn resolve_object(&self, path: &Path) -> Result<HashCacheObject, Error> {
        self.resolve_object_ref(path)
            .and_then(|cache_ref| self.cache.get(&cache_ref).map_err(Error::from))
    }

    fn resolve_object_ref(&self, path: &Path) -> Result<CacheRef, Error> {
        use std::path::Component;

        let commit = self.cache.get(&self.head_commit)?.into_commit()?;
        let mut objs = Vec::new();

        fn find_entry<'a, 'b, D: Directory>(dir: &'a D, component: &'b OsStr)
                                            -> Option<::cache::DirectoryEntry> {
            dir.iter().find(|entry| Some(entry.name.as_str()) == component.to_str())
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
                        let dir_entry = find_entry(&cur_dir, entry)
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

    fn cache_dir_entry_to_fuse_dir_entry(dir_entry: ::cache::DirectoryEntry)
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
}
