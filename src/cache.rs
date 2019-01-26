use std::path::{Component, Path};
use std::ffi::OsStr;
use std::io;
use std::fmt::{Debug, Display, Formatter, self};
use std::result;
use std::string::ToString;
use std::str::FromStr;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::iter::FromIterator;
use std::vec;
use std::fs;

use serde_json;
use failure::{Fail, Error};
use serde::{Serialize, Deserialize, Serializer, Deserializer, self};
use serde::de::Visitor;
use chrono::{DateTime, FixedOffset};

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

#[derive(PartialEq, Eq, Clone, Copy, Hash)]
pub struct CacheRef(pub [u8; 32]);

impl Debug for CacheRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl Display for CacheRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        <Self as Debug>::fmt(self, f)
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

impl CacheRef {
    pub fn null() -> Self {
        CacheRef([0u8; 32])
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

pub trait ReadonlyFile: io::Read+io::Seek+Debug {}

impl ReadonlyFile for fs::File {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectType {
    File,
    Directory,
    Commit,
    Symlink
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            ObjectType::File => write!(f, "file"),
            ObjectType::Directory => write!(f, "directory"),
            ObjectType::Commit => write!(f, "commit"),
            ObjectType::Symlink => write!(f, "symlink"),
        }.map_err(|_| fmt::Error)
    }
}

impl<'a, F: ReadonlyFile, D: Directory> From<&'a CacheObject<F, D>> for ObjectType {
    fn from(obj: &'a CacheObject<F, D>) -> Self {
        match obj {
            &CacheObject::File(..) => ObjectType::File,
            &CacheObject::Directory(..) => ObjectType::Directory,
            &CacheObject::Commit(..) => ObjectType::Commit,
            &CacheObject::Symlink(..) => ObjectType::Symlink
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub cache_ref: CacheRef,
    pub object_type: ObjectType,
    pub size: u64
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

pub trait Directory: Sized+IntoIterator<Item=DirectoryEntry>+Clone {
    fn find_entry<S: Borrow<OsStr>>(&self, name: &S) -> Option<&DirectoryEntry>;
}

impl Directory for Vec<DirectoryEntry> {
    fn find_entry<S: Borrow<OsStr>>(&self, name: &S) -> Option<&DirectoryEntry> {
        self.iter().find(|entry|
            <String as AsRef<OsStr>>::as_ref(&entry.name) == name.borrow())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Commit {
    pub tree: CacheRef,
    pub parents: Vec<CacheRef>,
    pub message: String,
    pub committed_date: DateTime<FixedOffset>
}

impl Display for Commit {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        f.write_str(self.message.as_str())?;
        write!(f, "\n\nCommitted: {}", self.committed_date)?;
        write!(f, "\nTree:      {}\n", self.tree)?;

        match self.parents.len() {
            0 => (),
            1 => f.write_str("Parent:    ")?,
            _ => f.write_str("Parents:   ")?
        }

        let mut parents = self.parents.iter();
        if let Some(parent_commit) = parents.next() {
            write!(f, "{}", parent_commit)?;
        }

        for parent_commit in parents {
            write!(f, "\n           {}", parent_commit)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReferencedCommit(pub CacheRef, pub Commit);

impl Display for ReferencedCommit {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "\nCommit:    {}\n\n{}", self.0, self.1)
    }
}

#[derive(Debug)]
pub enum CacheObject<F: ReadonlyFile, D: Directory> {
    File(F),
    Directory(D),
    Commit(Commit),
    Symlink(String)
}

impl<F, D> Clone for CacheObject<F, D> where F: ReadonlyFile+Clone, D: Directory {
    fn clone(&self) -> Self {
        match self {
            CacheObject::File(f) => CacheObject::File(f.clone()),
            CacheObject::Directory(d) => CacheObject::Directory(d.clone()),
            CacheObject::Commit(c) => CacheObject::Commit(c.clone()),
            CacheObject::Symlink(s) => CacheObject::Symlink(s.clone())
        }
    }
}

#[derive(Debug, Clone)]
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

pub trait CacheLayer: Debug {
    type File: ReadonlyFile+Debug;
    type Directory: Directory+Debug;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>>;
    fn add_file_by_path(&self, source_path: &Path) -> Result<CacheRef>;
    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> Result<CacheRef>;
    fn add_commit(&self, commit: Commit) -> Result<CacheRef>;

    fn get_head_commit(&self, branch: &str) -> Result<Option<CacheRef>>;
    fn merge_commit(&mut self, branch: &str, cache_ref: CacheRef) -> Result<CacheRef>;
    fn create_branch(&mut self, branch: &str, cache_ref: CacheRef) -> Result<()>;
}

pub fn resolve_object_ref<C, P>(cache: &C, commit: &Commit, path: P)
    -> result::Result<Option<CacheRef>, Error> where C: CacheLayer,
                                                              P: AsRef<Path> {
    let mut objs = vec![commit.tree.clone()];
    let path = path.as_ref();

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
                    match cur_dir.find_entry(&entry) {
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

impl ReadonlyFile for Box<ReadonlyFile+Send+Sync> {}

pub type BoxedCacheLayer =
    Box<CacheLayer<File=Box<ReadonlyFile+Send+Sync+'static>,
        Directory=Vec<DirectoryEntry>>+Send+Sync>;

#[derive(Debug)]
struct CacheLayerWrapper<C: CacheLayer+Send+Sync> {
    inner: C
}

impl<C: CacheLayer+Send+Sync> CacheLayerWrapper<C> {
    fn new(inner: C) -> Self {
        Self { inner }
    }
}

pub fn boxed<C>(inner: C) -> BoxedCacheLayer where C: CacheLayer+Send+Sync+'static,
                                                   C::File: Send+Sync {
    Box::new(CacheLayerWrapper::new(inner)) as BoxedCacheLayer
}

impl<C> CacheLayer for CacheLayerWrapper<C> where C: CacheLayer+Send+Sync,
                                                  C::File: Send+Sync+'static {
    type File = Box<ReadonlyFile+Send+Sync>;
    type Directory = Vec<DirectoryEntry>;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let obj = match self.inner.get(cache_ref)? {
            CacheObject::File(file) =>
                CacheObject::File(Box::new(file) as Box<ReadonlyFile+Send+Sync>),
            CacheObject::Directory(dir) => CacheObject::Directory(Vec::from_iter(dir)),
            CacheObject::Commit(commit) => CacheObject::Commit(commit),
            CacheObject::Symlink(symlink) => CacheObject::Symlink(symlink)
        };
        Ok(obj)
    }

    fn add_file_by_path(&self, source_path: &Path) -> Result<CacheRef> {
        self.inner.add_file_by_path(source_path)
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> Result<CacheRef> {
        self.inner.add_directory(items)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        self.inner.add_commit(commit)
    }

    fn get_head_commit(&self, branch: &str) -> Result<Option<CacheRef>> {
        self.inner.get_head_commit(branch)
    }

    fn merge_commit(&mut self, branch: &str, cache_ref: CacheRef) -> Result<CacheRef> {
        self.inner.merge_commit(branch, cache_ref)
    }

    fn create_branch(&mut self, branch: &str, cache_ref: CacheRef) -> Result<()> {
        self.inner.create_branch(branch, cache_ref)
    }
}

impl<C, F, D> CacheLayer for Box<C> where C: CacheLayer<File=F, Directory=D>+?Sized,
                                          F: ReadonlyFile+Send+Sync,
                                          D: Directory+Debug {
    type File = F;
    type Directory = D;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        (**self).get(cache_ref)
    }

    fn add_file_by_path(&self, source_path: &Path) -> Result<CacheRef> {
        (**self).add_file_by_path(source_path)
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> Result<CacheRef> {
        (**self).add_directory(items)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        (**self).add_commit(commit)
    }

    fn get_head_commit(&self, branch: &str) -> Result<Option<CacheRef>> {
        (**self).get_head_commit(branch)
    }

    fn merge_commit(&mut self, branch: &str, cache_ref: CacheRef) -> Result<CacheRef> {
        (**self).merge_commit(branch, cache_ref)
    }

    fn create_branch(&mut self, branch: &str, cache_ref: CacheRef) -> Result<()> {
        (**self).create_branch(branch, cache_ref)
    }
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

    #[test]
    fn check_thread_safety() {
        use crate::nullcache::NullCache;
        use super::{boxed, CacheLayer};

        fn bounds_test<F: FnOnce()+Send+Sync+'static>(f: F) {
            f()
        }

        let test_cache_layer = boxed(NullCache::default());
        bounds_test(move || assert!(test_cache_layer.get_head_commit("(unused)").is_ok()));
    }
}
