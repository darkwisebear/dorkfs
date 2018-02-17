use std::path::Path;
use failure::Error;
use std::result::Result;
use fuse_mt::{FilesystemMT, mount, FuseMT};

pub struct DorkFs;

impl FilesystemMT for DorkFs {

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