use std::hash::{Hash, Hasher};
use std::fs;
use std::io::{Read, Write, Seek, Cursor};
use std::path::{Path, PathBuf};
use std::iter::FromIterator;
use std::fmt::{self, Debug, Display, Formatter};

use failure::Error;

use cache::{self, ReferencedCommit, CacheRef};
use utility::os_string_to_string;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ObjectType {
    File,
    Directory,
    Symlink,
    Pipe
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

impl<'a> From<(&'a str, &'a Metadata)> for OverlayDirEntry {
    fn from(val: (&'a str, &'a Metadata)) -> OverlayDirEntry {
        OverlayDirEntry {
            name: val.0.to_owned(),
            metadata: val.1.clone()
        }
    }
}

pub trait OverlayFile: Read+Write+Seek {
    fn close(self) -> Result<(), Error>;
    fn truncate(&mut self, size: u64) -> Result<(), Error>;
}

impl OverlayFile for Cursor<Vec<u8>> {
    fn close(self) -> Result<(), Error> {
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), Error> {
        if self.position() as u64 > size {
            self.set_position(size);
        }
        self.get_mut().truncate(size as usize);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileState {
    New,
    Modified,
    Deleted
}

impl Display for FileState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let literal = match self {
            FileState::New => "new",
            FileState::Modified => "modified",
            FileState::Deleted => "deleted"
        };
        f.write_str(literal)
    }
}

pub trait Overlay: Debug {
    type File: OverlayFile;

    fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool) -> Result<Self::File, Error>;
    fn list_directory<I,P>(&self, path: P) -> Result<I, Error>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata, Error>;
    fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        self.metadata(path).is_ok()
    }
    fn delete_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Error>;
}

pub trait WorkspaceLog<'a>: Iterator<Item=Result<ReferencedCommit, Error>> { }

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceFileStatus(pub PathBuf, pub FileState);

impl<P: AsRef<Path>> From<(P, FileState)> for WorkspaceFileStatus {
    fn from(val: (P, FileState)) -> Self {
        WorkspaceFileStatus(val.0.as_ref().to_path_buf(), val.1)
    }
}

pub trait WorkspaceController<'a>: Debug {
    type Log: WorkspaceLog<'a>;
    type StatusIter: Iterator<Item=Result<WorkspaceFileStatus, Error>>;

    fn commit(&mut self, message: &str) -> Result<CacheRef, Error>;
    fn get_current_head_ref(&self) -> Result<Option<CacheRef>, Error>;
    fn get_log<'b: 'a>(&'b self, start_commit: &CacheRef) -> Result<Self::Log, Error>;
    fn get_status<'b: 'a>(&'b self) -> Result<Self::StatusIter, Error>;
}
