use std::hash::{Hash, Hasher};
use std::fs;
use std::io::{Read, Write, Seek};
use std::path::Path;
use std::iter::FromIterator;
use std::fmt::Debug;

use failure::Error;

use cache::{self, CacheRef, DirectoryEntry};
use utility::os_string_to_string;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ObjectType {
    File,
    Directory,
    Symlink
}

impl ObjectType {
    fn from_file_type(file_type: fs::FileType) -> Result<ObjectType, Error> {
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

    fn from_cache_object_type(obj_type: cache::ObjectType) -> Result<ObjectType, Error> {
        match obj_type {
            cache::ObjectType::File => Ok(ObjectType::File),
            cache::ObjectType::Directory => Ok(ObjectType::Directory),
            _ => Err(format_err!("Unmappable object type from cache!"))
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Metadata {
    pub size: u64,
    pub object_type: ObjectType
}

impl Metadata {
    pub fn from_fs_metadata(fs_metadata: fs::Metadata) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: fs_metadata.len(),
            object_type: ObjectType::from_file_type(fs_metadata.file_type())?
        };

        Ok(metadata)
    }

    pub fn from_cache_metadata(cache_metadata: cache::CacheObjectMetadata) -> Result<Metadata, Error> {
        let metadata = Metadata {
            size: cache_metadata.size,
            object_type: ObjectType::from_cache_object_type(cache_metadata.object_type)?
        };

        Ok(metadata)
    }
}


#[derive(Debug, Clone)]
pub struct OverlayDirEntry {
    pub name: String,
    pub metadata: Metadata
}

impl PartialEq for OverlayDirEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for OverlayDirEntry {}

impl Hash for OverlayDirEntry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.name.hash(hasher);
    }
}

impl<'a> From<(&'a String, &'a Metadata)> for OverlayDirEntry {
    fn from(val: (&'a String, &'a Metadata)) -> OverlayDirEntry {
        OverlayDirEntry {
            name: val.0.clone(),
            metadata: val.1.clone()
        }
    }
}

impl OverlayDirEntry {
    pub fn from_directory_entry<F>(dir_entry: DirectoryEntry, get_metadata: F)
                               -> Result<OverlayDirEntry, Error>
        where F: Fn(&DirectoryEntry) -> Result<cache::CacheObjectMetadata, Error> {
        let metadata = Metadata::from_cache_metadata(get_metadata(&dir_entry)?)?;
        let entry = OverlayDirEntry {
            name: dir_entry.name,
            metadata
        };

        Ok(entry)
    }

    pub fn from_dir_entry(dir_entry: fs::DirEntry) -> Result<OverlayDirEntry, Error> {
        let entry = OverlayDirEntry {
            name: os_string_to_string(dir_entry.file_name())?,
            metadata: Metadata::from_fs_metadata(dir_entry.metadata()?)?
        };

        Ok(entry)
    }
}

pub trait OverlayFile: Read+Write+Seek {
    fn close(self) -> Result<(), Error>;
    fn truncate(&mut self, size: u64) -> Result<(), Error>;
}

pub trait Overlay: Debug {
    type File: OverlayFile;

    fn open_file<P: AsRef<Path>>(&self, path: P, writable: bool) -> Result<Self::File, Error>;
    fn list_directory<I,P>(&self, path: P) -> Result<I, Error>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata, Error>;
    fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        self.metadata(path).is_ok()
    }
}

pub trait WorkspaceController: Debug {
    fn commit(&mut self, message: &str) -> Result<CacheRef, Error>;
}