use std::path::Path;
use failure::Error;
use std::result::Result;
use fuse_mt::*;
use libc;

pub struct DorkFs;

impl FilesystemMT for DorkFs {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>) -> ResultEntry {
        Err(libc::ENOSYS)
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
