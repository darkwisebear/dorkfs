use std::path::{Component, Path};
use std::ffi::{OsStr, OsString};
use std::io;
use std::fmt::{Debug, Display, Formatter, self};
use std::result;
use std::string::ToString;
use std::str::FromStr;
use std::hash::{Hash, Hasher};
use std::collections::{HashMap, hash_map};
use std::borrow::Borrow;
use std::iter::{self, FromIterator};
use std::vec;
use std::fs;
use std::mem;

use serde_json;
use failure::{Fail, Error, Fallible, format_err, ensure, bail};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use chrono::{DateTime, FixedOffset};
use futures::{future, Future};
use either::Either;

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
        let mut result = mem::MaybeUninit::<[u8; 32]>::uninit();

        let ref_bytes = result.as_mut_ptr() as *mut u8;
        for (index, byte_result) in HexCharIterator::new(v).enumerate() {
            ensure!(index < 32, "Given hex string {} too large", v.to_string());
            unsafe {
                *ref_bytes.add(index) = byte_result?;
            }
            last_index = index;
        }

        ensure!(last_index == 31, "Given hex string {} too small", v.to_string());
        Ok(CacheRef(unsafe { result.assume_init() }))
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

pub struct CommitRange {
    pub start: Option<CacheRef>,
    pub end: Option<CacheRef>
}

impl FromStr for CommitRange {
    type Err = Error;

    fn from_str(range: &str) -> Fallible<Self> {
        let mut split = range.split("..");
        let start: &str = split.next()
            .ok_or_else(|| format_err!("No range start in range expression\"{}\"", range))?;
        let end: &str = split.next()
            .ok_or_else(|| format_err!("No range start in range expression\"{}\"", range))?;

        if split.next().is_some() {
            bail!("Too many range separators in expression");
        }

        Ok(Self {
            start: if start.is_empty() { None } else { Some(CacheRef::from_str(start)?) },
            end: if start.is_empty() { None } else { Some(CacheRef::from_str(end)?) }
        })
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

        if !byte_str.is_empty() {
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

impl<'de> serde::de::Visitor<'de> for StringVisitor {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
        match *obj {
            CacheObject::File(..) => ObjectType::File,
            CacheObject::Directory(..) => ObjectType::Directory,
            CacheObject::Commit(..) => ObjectType::Commit,
            CacheObject::Symlink(..) => ObjectType::Symlink
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

impl Into<(OsString, DirectoryEntry)> for DirectoryEntry {
    fn into(self) -> (OsString, DirectoryEntry) {
        let name = OsString::from(&self.name);
        (name, self)
    }
}

pub trait Directory: Sized+IntoIterator<Item=DirectoryEntry>+Clone {
    fn find_entry<S: Borrow<OsStr>+?Sized>(&self, name: &S) -> Option<&DirectoryEntry>;
}

impl Directory for Vec<DirectoryEntry> {
    fn find_entry<S: Borrow<OsStr>+?Sized>(&self, name: &S) -> Option<&DirectoryEntry> {
        self.iter().find(|entry|
            <String as AsRef<OsStr>>::as_ref(&entry.name) == name.borrow())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryImpl {
    #[serde(with = "serde_dir_impl")]
    entries: HashMap<OsString, DirectoryEntry>
}

type HashMapDirEntryIter = hash_map::IntoIter<OsString, DirectoryEntry>;
type HashMapEntryToDirectoryEntry = fn((OsString, DirectoryEntry)) -> DirectoryEntry;
type IntoDirEntryIter = iter::Map<HashMapDirEntryIter, HashMapEntryToDirectoryEntry>;

impl IntoIterator for DirectoryImpl {
    type Item = DirectoryEntry;
    type IntoIter = IntoDirEntryIter;

    fn into_iter(self) -> Self::IntoIter {
        #[inline]
        fn map_entry(e: (OsString, DirectoryEntry)) -> DirectoryEntry {
            e.1
        }

        self.entries.into_iter().map(map_entry)
    }
}

impl FromIterator<DirectoryEntry> for DirectoryImpl {
    fn from_iter<T: IntoIterator<Item=DirectoryEntry>>(iter: T) -> Self {
        let entries = iter.into_iter().map(Into::into).collect();
        DirectoryImpl { entries }
    }
}

impl Directory for DirectoryImpl {
    fn find_entry<S: Borrow<OsStr>+?Sized>(&self, name: &S) -> Option<&DirectoryEntry> {
        self.entries.get(name.borrow())
    }
}

mod serde_dir_impl {
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::fmt::{self, Formatter};

    use serde::{Serializer, Deserializer, ser::SerializeSeq, de::{Visitor, SeqAccess}};

    use crate::cache::DirectoryEntry;

    pub fn serialize<S>(value: &HashMap<OsString, DirectoryEntry>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        let mut seq_serializer = serializer.serialize_seq(Some(value.len()))?;
        for entry in value.values() {
            seq_serializer.serialize_element(entry)?;
        }
        seq_serializer.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<OsString, DirectoryEntry>, D::Error>
        where D: Deserializer<'de> {

        struct DirVisitor;

        impl<'de> Visitor<'de> for DirVisitor {
            type Value = HashMap<OsString, DirectoryEntry>;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                formatter.write_str("a sequence of type \"DirectoryEntry\"")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where A: SeqAccess<'de> {
                let mut dir = seq.size_hint()
                    .map(HashMap::with_capacity)
                    .unwrap_or_default();
                loop {
                    match seq.next_element() {
                        Ok(Some(entry)) => {
                            let (name, entry) = DirectoryEntry::into(entry);
                            dir.insert(name, entry);
                        }
                        Ok(None) => break Ok(dir),
                        Err(e) => break Err(e)
                    }
                }
            }
        }

        deserializer.deserialize_seq(DirVisitor)
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
        write!(f, "Commit:    {}\n\n{}\n", self.0, self.1)
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

impl<F, D> CacheObject<F, D> where F: ReadonlyFile, D: Directory {
    pub fn map_file<G: ReadonlyFile, M: FnOnce(F) -> G>(self, map_fn: M) -> CacheObject<G, D> {
        match self {
            CacheObject::File(f) => CacheObject::File(map_fn(f)),
            CacheObject::Directory(d) => CacheObject::Directory(d),
            CacheObject::Commit(c) => CacheObject::Commit(c),
            CacheObject::Symlink(s) => CacheObject::Symlink(s),
        }
    }

    pub fn map_dir<E: Directory, M: FnOnce(D) -> E>(self, map_fn: M) -> CacheObject<F, E> {
        match self {
            CacheObject::File(f) => CacheObject::File(f),
            CacheObject::Directory(d) => CacheObject::Directory(map_fn(d)),
            CacheObject::Commit(c) => CacheObject::Commit(c),
            CacheObject::Symlink(s) => CacheObject::Symlink(s),
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

pub trait CacheLayer {
    type File: ReadonlyFile;
    type Directory: Directory;
    type GetFuture: Future<Item=CacheObject<Self::File, Self::Directory>, Error=CacheError>+Send;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>>;
    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef>;
    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef>;
    fn add_commit(&self, commit: Commit) -> Result<CacheRef>;

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>>;
    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef>;
    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<()>;

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture;
}

pub fn resolve_object_ref<C, P, T>(cache: T, commit: &Commit, path: P)
    -> result::Result<Option<CacheRef>, Error>
    where C: CacheLayer+?Sized,
          T: ::std::ops::Deref<Target=C>,
          P: AsRef<Path> {
    let cache = cache.deref();
    let mut objs = vec![commit.tree];
    let path = path.as_ref();

    for component in path.components() {
        match component {
            Component::Prefix(..) => panic!("A prefix is not allowed to appear in a cache path"),
            Component::RootDir => {
                objs.clear();
                objs.push(commit.tree);
            }
            Component::CurDir => (),
            Component::ParentDir => {
                objs.pop().ok_or_else(|| format_err!("Outside path due to too many parent dirs!"))?;
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

#[derive(Debug)]
pub struct ReadonlyFileWrapper<L: ReadonlyFile+Debug, R: ReadonlyFile+Debug>(Either<L, R>);

impl<L: ReadonlyFile+Debug, R: ReadonlyFile+Debug> io::Read for ReadonlyFileWrapper<L, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<L: ReadonlyFile+Debug, R: ReadonlyFile+Debug> io::Seek for ReadonlyFileWrapper<L, R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match self.0 {
            Either::Left(ref mut l) => l.seek(pos),
            Either::Right(ref mut r) => r.seek(pos),
        }
    }
}

impl<L: ReadonlyFile+Debug, R: ReadonlyFile+Debug> ReadonlyFile for ReadonlyFileWrapper<L, R> {}

impl<L: ReadonlyFile+Debug, R: ReadonlyFile+Debug> ReadonlyFileWrapper<L, R> {
    pub fn left(val: L) -> ReadonlyFileWrapper<L, R> {
        ReadonlyFileWrapper(Either::Left(val))
    }

    pub fn right(val: R) -> ReadonlyFileWrapper<L, R> {
        ReadonlyFileWrapper(Either::Right(val))
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryWrapper<L: Directory+Debug, R: Directory+Debug>(Either<L, R>);

impl<L: Directory+Debug, R: Directory+Debug> IntoIterator for DirectoryWrapper<L, R> {
    type Item = DirectoryEntry;
    type IntoIter = Either<<L as IntoIterator>::IntoIter, <R as IntoIterator>::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl <L: Directory+Debug, R: Directory+Debug> Directory for DirectoryWrapper<L, R> {
    fn find_entry<S: Borrow<OsStr> + ?Sized>(&self, name: &S) -> Option<&DirectoryEntry> {
        match &self.0 {
            Either::Left(l) => l.find_entry(name),
            Either::Right(r) => r.find_entry(name),
        }
    }
}

impl <L: Directory+Debug, R: Directory+Debug> DirectoryWrapper<L, R> {
    pub fn left(val: L) -> DirectoryWrapper<L, R> {
        DirectoryWrapper(Either::Left(val))
    }

    pub fn right(val: R) -> DirectoryWrapper<L, R> {
        DirectoryWrapper(Either::Right(val))
    }
}

impl<L, R> CacheLayer for Either<L, R>
    where L: CacheLayer,
          R: CacheLayer,
          L::Directory: Debug+Send,
          R::Directory: Debug+Send,
          L::File: Send,
          R::File: Send {
    type File = ReadonlyFileWrapper<L::File, R::File>;
    type Directory = DirectoryWrapper<L::Directory, R::Directory>;
    type GetFuture = future::Either<
        future::Map<
            L::GetFuture,
            fn(<L::GetFuture as Future>::Item) -> CacheObject<Self::File, Self::Directory>>,
        future::Map<
            R::GetFuture,
            fn(<R::GetFuture as Future>::Item) -> CacheObject<Self::File, Self::Directory>>>;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let result = match self {
            Either::Left(l) =>
                l.get(cache_ref)?
                    .map_file(ReadonlyFileWrapper::left)
                    .map_dir(DirectoryWrapper::left),
            Either::Right(r) =>
                r.get(cache_ref)?
                    .map_file(ReadonlyFileWrapper::right)
                    .map_dir(DirectoryWrapper::right),
        };
        Ok(result)
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef> {
        match self {
            Either::Left(l) => l.add_file_by_path(source_path),
            Either::Right(r) => r.add_file_by_path(source_path),
        }
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef> {
        match self {
            Either::Left(l) => l.add_directory(items),
            Either::Right(r) => r.add_directory(items),
        }
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        match self {
            Either::Left(l) => l.add_commit(commit),
            Either::Right(r) => r.add_commit(commit),
        }
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>> {
        match self {
            Either::Left(l) => l.get_head_commit(branch),
            Either::Right(r) => r.get_head_commit(branch),
        }
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef> {
        match self {
            Either::Left(l) => l.merge_commit(branch, cache_ref),
            Either::Right(r) => r.merge_commit(branch, cache_ref),
        }
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<()> {
        match self {
            Either::Left(l) => l.create_branch(branch, cache_ref),
            Either::Right(r) => r.create_branch(branch, cache_ref),
        }
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        match self {
            Either::Left(l) => future::Either::A(l.get_poll(cache_ref)
                .map(|f| f.map_file(ReadonlyFileWrapper::left).map_dir(DirectoryWrapper::left))),
            Either::Right(r) => future::Either::B(r.get_poll(cache_ref)
                .map(|f| f.map_file(ReadonlyFileWrapper::right).map_dir(DirectoryWrapper::right)))
        }
    }
}

#[derive(Debug)]
pub struct NullReadonlyFile;

impl io::Read for NullReadonlyFile {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        unimplemented!()
    }
}

impl io::Seek for NullReadonlyFile {
    fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
        unimplemented!()
    }
}

impl ReadonlyFile for NullReadonlyFile {}

impl CacheLayer for () {
    type File = NullReadonlyFile;
    type Directory = DirectoryImpl;
    type GetFuture = future::FutureResult<CacheObject<Self::File, Self::Directory>, CacheError>;

    fn get(&self, _cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        unimplemented!()
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, _source_path: P) -> Result<CacheRef> {
        unimplemented!()
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, _items: I) -> Result<CacheRef> {
        unimplemented!()
    }

    fn add_commit(&self, _commit: Commit) -> Result<CacheRef> {
        unimplemented!()
    }

    fn get_head_commit<S: AsRef<str>>(&self, _branch: S) -> Result<Option<CacheRef>> {
        unimplemented!()
    }

    fn merge_commit<S: AsRef<str>>(&self, _branch: S, _cache_ref: &CacheRef) -> Result<CacheRef> {
        unimplemented!()
    }

    fn create_branch<S: AsRef<str>>(&self, _branch: S, _cache_ref: &CacheRef) -> Result<()> {
        unimplemented!()
    }

    fn get_poll(&self, _cache_ref: &CacheRef) -> Self::GetFuture {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::ffi::OsString;
    use std::iter::FromIterator;
    use std::collections::HashMap;

    use crate::cache::{DirectoryImpl, Directory, ObjectType, CacheRef, DirectoryEntry};

    use serde_json;

    #[test]
    fn hex_char_iterator() {
        use super::HexCharIterator;
        use failure::Error;

        let iter = HexCharIterator::new("123456789abcdef");
        let bytes: Result<Vec<u8>, Error> = iter.collect();
        assert_eq!(vec![0x12u8, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf], bytes.unwrap());
    }

    #[test]
    fn deserialize_directory() {
        let source = r#"{"entries": [
            {"name": "test.txt",
             "size": 1234,
             "cache_ref": "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
             "object_type": "File"
            }
        ]}"#;

        let entries = serde_json::from_str::<DirectoryImpl>(source).unwrap();
        let entry = entries.find_entry(&OsString::from("test.txt")).unwrap();
        assert_eq!(entry.name, "test.txt");
        assert_eq!(entry.size, 1234);
        assert_eq!(entry.object_type, ObjectType::File);
        assert_eq!(entry.cache_ref, CacheRef::from_str("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF").unwrap());
    }

    #[test]
    fn serialize_directory() {
        let source = DirectoryImpl {
            entries: HashMap::from_iter(vec![(OsString::from("test.txt"), DirectoryEntry {
                name: String::from("test.txt"),
                size: 1234,
                object_type: ObjectType::File,
                cache_ref: CacheRef::from_str("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF").unwrap()
            })].into_iter())
        };

        let value = serde_json::to_value(source).unwrap();
        let first_entry = &value["entries"][0];
        assert_eq!(first_entry["name"], "test.txt");
        assert_eq!(first_entry["size"].as_u64().unwrap(), 1234);
        assert_eq!(first_entry["object_type"], "File");
        assert_eq!(first_entry["cache_ref"], "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    }
}
