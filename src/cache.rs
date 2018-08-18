use std::path::{Component, Path};
use std::ffi::OsStr;
use std::io;
use std::fmt::{Debug, Display, Formatter, self};
use std::fs;
use std::result;
use std::string::ToString;
use std::str::FromStr;
use std::hash::{Hash, Hasher};

use serde_json;
use failure::{Fail, Error};
use serde::{Serialize, Deserialize, Serializer, Deserializer, self};
use serde::de::Visitor;

pub trait LayerError: Fail {}

#[derive(Fail, Debug)]
pub enum CacheError {
    #[fail(display = "Cache object {} not found", _0)]
    ObjectNotFound(CacheRef),

    #[fail(display = "Unknown object type {}", _0)]
    UnknownObjectType(u8),

    #[fail(display = "Unable to initialize object storage")]
    InitializationFailed,

    #[fail(display = "Unexpected object type {}", _0)]
    UnexpectedObjectType(ObjectType),

    #[fail(display = "IO error: {}", _0)]
    IoError(io::Error),

    #[fail(display = "JSON error: {}", _0)]
    JsonError(serde_json::Error),

    #[fail(display = "Unparsable reference {}: {}", _0, _1)]
    UnparsableCacheRef(String, Error),

    #[fail(display = "Runtime error {}: {}", _0, _1)]
    Custom(&'static str, Error),

    #[fail(display = "Layer error: {}", _0)]
    LayerError(Error)
}

pub type Result<T> = result::Result<T, CacheError>;

impl From<io::Error> for CacheError {
    fn from(ioerr: io::Error) -> Self {
        CacheError::IoError(ioerr)
    }
}

impl From<serde_json::Error> for CacheError {
    fn from(json_err: serde_json::Error) -> Self {
        CacheError::JsonError(json_err)
    }
}

impl<E: LayerError> From<E> for CacheError {
    fn from(err: E) -> Self {
        CacheError::LayerError(err.into())
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct CacheRef(pub [u8; 32]);

impl Display for CacheRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl FromStr for CacheRef {
    type Err = Error;

    fn from_str(v: &str) -> result::Result<Self, Self::Err> {
        let mut last_index = 0;
        let mut result: [u8; 32] = unsafe { ::std::mem::uninitialized() };

        for (index, byte_result ) in HexCharIterator::new(v).enumerate() {
            ensure!(index < 32, "Given hex string {} too large", v.to_string());
            result[index] = byte_result?;
            last_index = index;
        }

        ensure!(last_index == 31, "Given hex string {} too small", v.to_string());
        Ok(CacheRef(result))
    }
}

impl Serialize for CacheRef {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

struct HexCharIterator<'a> {
    hex: &'a str
}

impl<'a> HexCharIterator<'a> {
    fn new(hex: &'a str) -> HexCharIterator<'a> {
        HexCharIterator {
            hex
        }
    }
}

impl<'a> Iterator for HexCharIterator<'a> {
    type Item = result::Result<u8, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let (byte_str, rest) =
            self.hex.split_at(usize::min(self.hex.len(), 2));

        if byte_str.len() > 0 {
            self.hex = rest;

            let byte = u8::from_str_radix(byte_str, 16)
                .map_err(|e| {
                    format_err!("Unparsable cacheref {}: {}", self.hex.to_string(), e)
                });
            Some(byte)
        } else {
            None
        }
    }
}

struct StringVisitor;

impl<'de> Visitor<'de> for StringVisitor {
    type Value = CacheRef;

    fn expecting(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.write_str("Expecting a string of 64 hex characters")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> result::Result<Self::Value, E> {
        v.parse().map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for CacheRef {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
        where D: Deserializer<'de> {
        deserializer.deserialize_str(StringVisitor)
    }
}

pub trait ReadonlyFile: io::Read+io::Seek {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectType {
    File,
    Directory,
    Commit
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            ObjectType::File => write!(f, "file"),
            ObjectType::Directory => write!(f, "directory"),
            ObjectType::Commit => write!(f, "commit"),
        }.map_err(|_| fmt::Error)
    }
}

impl<'a, F: ReadonlyFile, D: Directory> From<&'a CacheObject<F, D>> for ObjectType {
    fn from(obj: &'a CacheObject<F, D>) -> Self {
        match obj {
            &CacheObject::File(..) => ObjectType::File,
            &CacheObject::Directory(..) => ObjectType::Directory,
            &CacheObject::Commit(..) => ObjectType::Commit
        }
    }
}

impl ObjectType {
    pub fn as_identifier(&self) -> &'static [u8; 1] {
        match *self {
            ObjectType::File => &[b'F'],
            ObjectType::Directory => &[b'D'],
            ObjectType::Commit => &[b'C']
        }
    }

    pub fn from_identifier(id: &[u8; 1]) -> Result<ObjectType> {
        match id[0] {
            b'F' => Ok(ObjectType::File),
            b'D' => Ok(ObjectType::Directory),
            b'C' => Ok(ObjectType::Commit),
            obj_type => Err(CacheError::UnknownObjectType(obj_type))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub cache_ref: CacheRef,
    pub object_type: ObjectType
}

impl PartialEq for DirectoryEntry {
    fn eq(&self, other: &DirectoryEntry) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for DirectoryEntry {}

impl Hash for DirectoryEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state)
    }
}

pub trait Directory: Sized+IntoIterator<Item=DirectoryEntry>+Clone { }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Commit {
    pub tree: CacheRef,
    pub parents: Vec<CacheRef>,
    pub message: String
}

impl Display for Commit {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        f.write_str(self.message.as_str())?;
        write!(f, "\n\nTree:   {}\n", self.tree)?;
        for parent_commit in &self.parents {
            write!(f, "Parent: {}", parent_commit)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReferencedCommit(pub CacheRef, pub Commit);

impl Display for ReferencedCommit {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Commit: {}\n\n{}", self.0, self.1)
    }
}

#[derive(Debug)]
pub enum CacheObject<F: ReadonlyFile, D: Directory> {
    File(F),
    Directory(D),
    Commit(Commit),
}

#[derive(Debug)]
pub struct CacheObjectMetadata {
    pub size: u64,
    pub object_type: ObjectType
}

impl<F: ReadonlyFile, D: Directory> CacheObject<F, D> {
    pub fn into_commit(self) -> Result<Commit> {
        if let CacheObject::Commit(commit) = self {
            Ok(commit)
        } else {
            Err(CacheError::UnexpectedObjectType(ObjectType::from(&self)))
        }
    }

    pub fn into_directory(self) -> Result<D> {
        if let CacheObject::Directory(dir) = self {
            Ok(dir)
        } else {
            Err(CacheError::UnexpectedObjectType(ObjectType::from(&self)))
        }
    }

    pub fn into_file(self) -> Result<F> {
        if let CacheObject::File(file) = self {
            Ok(file)
        } else {
            Err(CacheError::UnexpectedObjectType(ObjectType::from(&self)))
        }
    }
}

pub trait CacheLayer {
    type File: ReadonlyFile+Debug;
    type Directory: Directory+Debug;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>>;
    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata>;
    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef>;
    fn add_directory<I: Iterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef>;
    fn add_commit(&self, commit: Commit) -> Result<CacheRef>;
    fn get_head_commit(&self) -> Result<Option<CacheRef>> { Ok(None) }
}

pub fn resolve_object_ref<C, P>(cache: &C, commit: &Commit, path: P)
    -> result::Result<Option<CacheRef>, Error> where C: CacheLayer,
                                                              P: AsRef<Path> {
    let mut objs = vec![commit.tree.clone()];
    let path = path.as_ref();

    fn find_entry<D: Directory>(dir: D, component: &OsStr) -> Option<DirectoryEntry> {
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
                        cache.get(cur_dir_ref)?.into_directory()?;
                    match find_entry(cur_dir, entry) {
                        Some(dir_entry) => dir_entry.cache_ref,
                        None => return Ok(None)
                    }
                };
                objs.push(cache_ref);
            }
        }
    }

    Ok(objs.last().cloned())
}

#[cfg(test)]
mod test {
    #[test]
    fn hex_char_iterator() {
        use super::HexCharIterator;
        use failure::Error;

        let iter = HexCharIterator::new("123456789abcdef");
        let bytes: Result<Vec<u8>, Error> = iter.collect();
        assert_eq!(vec![0x12u8, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf], bytes.unwrap());
    }
}
