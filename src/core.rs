use std::path::Path;
use failure::{Error, err_msg};
use std::result::Result;

pub struct DorkFs;

impl DorkFs {
    pub fn new<P: AsRef<Path>>(_cachedir: P) -> Result<Self, Error> {
        Ok(DorkFs)
    }

    pub fn mount<P: AsRef<Path>>(self, _mountpoint: P) -> Result<(), Error> {
        Err(err_msg("Not implemented yet"))
    }
}