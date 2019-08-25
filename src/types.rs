use std::fs;

use failure::Error;
use chrono::{DateTime, Utc};

use crate::cache::{self, CacheRef};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ObjectType {
    File,
    Directory,
    Symlink,
    Pipe
}

// This is basically a placeholder until creating a branch from a ref other than the
// current HEAD is supported
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum RepoRef<'a> {
    CacheRef(CacheRef),
    Branch(&'a str)
}

impl ObjectType {
    pub fn from_file_type(file_type: fs::FileType) -> Result<ObjectType, Error> {
        if file_type.is_file() {
            Ok(ObjectType::File)
        } else if file_type.is_dir() {
            Ok(ObjectType::Directory)
        } else if file_type.is_symlink() {
            Ok(ObjectType::Symlink)
        } else {
            Err(format_err!("Unknown fs::FileType"))
        }
    }

    pub fn from_cache_object_type(obj_type: cache::ObjectType) -> Result<ObjectType, Error> {
        match obj_type {
            cache::ObjectType::File => Ok(ObjectType::File),
            cache::ObjectType::Directory |
            cache::ObjectType::Commit => Ok(ObjectType::Directory),
            cache::ObjectType::Symlink => Ok(ObjectType::Symlink),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    pub size: u64,
    pub object_type: ObjectType,
    pub modified_date: DateTime<Utc>
}

impl Metadata {
    pub fn from_fs_metadata(fs_metadata: &fs::Metadata) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: fs_metadata.len(),
            object_type: ObjectType::from_file_type(fs_metadata.file_type())?,
            modified_date: fs_metadata.modified()?.into()
        };

        Ok(metadata)
    }
}
