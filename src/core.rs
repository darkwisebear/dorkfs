use std::path::Path;
use std::ffi::OsString;
use failure::Error;
use std::result::Result;
use fuse_mt::*;
use libc;
use time::get_time;

pub struct DorkFs;

impl FilesystemMT for DorkFs {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let p = path.as_os_str();
        if p == "/" {
            let attr = FileAttr {
                size: 4096,
                blocks: 1,
                atime: get_time(),
                mtime: get_time(),
                ctime: get_time(),
                crtime: get_time(),
                kind: FileType::Directory,
                perm: (7 << 6) + (5 << 3) + 0,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0
            };

            Ok((get_time(), attr))
        } else if p == "/test.txt" {
            let attr = FileAttr {
                size: 13,
                blocks: 1,
                atime: get_time(),
                mtime: get_time(),
                ctime: get_time(),
                crtime: get_time(),
                kind: FileType::RegularFile,
                perm: (6 << 6) + (4 << 3) + 4,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0
            };

            Ok((get_time(), attr))
        } else {
            Err(libc::ENOENT)
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        if path.as_os_str() == "/" {
            Ok((17, flags))
        } else {
            Err(libc::ENOENT)
        }
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        if path.as_os_str() == "/" {
            Ok(vec![
                DirectoryEntry {
                    name: OsString::from("."),
                    kind: FileType::Directory
                },
                DirectoryEntry {
                    name: OsString::from(".."),
                    kind: FileType::Directory
                },
                DirectoryEntry {
                    name: OsString::from("test.txt"),
                    kind: FileType::RegularFile
                }
            ])
        } else {
            Err(libc::ENOENT)
        }
    }
}

impl DorkFs {
    pub fn new<P: AsRef<Path>>(_cachedir: P) -> Result<Self, Error> {
        Ok(DorkFs)
    }

    pub fn mount<P: AsRef<Path>>(self, mountpoint: P) -> Result<(), Error> {
        mount(FuseMT::new(self, 1), &mountpoint, &[])
            .map_err(|e| Error::from(e))
    }
}
