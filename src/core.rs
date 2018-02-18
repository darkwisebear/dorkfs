use std::path::Path;
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
        if path.as_os_str() == "/" {
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
        } else {
            Err(libc::ENOENT)
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        info!("opendir on path {:?}", path);
        Err(libc::ENOSYS)
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
