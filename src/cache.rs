use std::path::{Component, Path, PathBuf};
use std::ffi::OsStr;
use std::io::{Read, Write, Seek, SeekFrom, self};
use std::fmt::{Display, Formatter, self};
use std::fs;
use std::sync::Arc;
use std::result;
use std::error::Error;
use std::iter::FromIterator;
use std::vec;
use std::string::ToString;
use std::str::FromStr;
use std::hash::{Hash, Hasher};
use serde_json;
use failure;
use serde::{Serialize, Deserialize, Serializer, Deserializer, self};
use serde::de::Visitor;
use tiny_keccak::Keccak;

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
    UnparsableCacheRef(String, failure::Error)
}

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

pub type Result<T> = result::Result<T, CacheError>;

#[derive(PartialEq, Eq, Debug, Clone, Copy, Default)]
pub struct CacheRef([u8; 32]);

impl CacheRef {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn is_root(&self) -> bool {
        self.eq(&Self::root())
    }
}

impl Display for CacheRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl FromStr for CacheRef {
    type Err = CacheError;

    fn from_str(v: &str) -> Result<Self> {
        let mut last_index = 0;
        let mut result: [u8; 32] = unsafe { ::std::mem::uninitialized() };

        for (index, byte_result ) in HexCharIterator::new(v).enumerate() {
            if index < 32 {
                result[index] = byte_result?;
                last_index = index;
            } else {
                return Err(CacheError::UnparsableCacheRef(
                    v.to_string(),
                    format_err!("Given hex string {} too large", v.to_string())));
            }
        }

        if last_index == 31 {
            Ok(CacheRef(result))
        } else {
            return Err(CacheError::UnparsableCacheRef(
                v.to_string(),
                format_err!("Given hex string {} too small", v.to_string())));
        }
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
    type Item = Result<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let (byte_str, rest) =
            self.hex.split_at(usize::min(self.hex.len(), 2));

        if byte_str.len() > 0 {
            self.hex = rest;

            let byte = u8::from_str_radix(byte_str, 16)
                .map_err(|e| {
                    CacheError::UnparsableCacheRef(self.hex.to_string(), e.into())
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

pub trait ReadonlyFile: io::Read+io::Seek {
    fn metadata(&self) -> Result<fs::Metadata>;
}

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
    fn as_identifier(&self) -> &'static [u8; 1] {
        match *self {
            ObjectType::File => &[b'F'],
            ObjectType::Directory => &[b'D'],
            ObjectType::Commit => &[b'C']
        }
    }

    fn from_identifier(id: &[u8; 1]) -> Result<ObjectType> {
        match id[0] {
            b'F' => Ok(ObjectType::File),
            b'D' => Ok(ObjectType::Directory),
            b'C' => Ok(ObjectType::Commit),
            obj_type => return Err(CacheError::UnknownObjectType(obj_type))
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
    type File: ReadonlyFile;
    type Directory: Directory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>>;
    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata>;
    fn add(&mut self, object: CacheObject<Self::File, Self::Directory>) -> Result<CacheRef>;
    fn create_file(&self, source_file: fs::File) -> Result<Self::File>;
    fn create_directory<I: Iterator<Item=DirectoryEntry>>(&self, items: I)
        -> Result<Self::Directory>;
}

#[derive(Debug)]
pub struct HashFile {
    file: fs::File
}

impl ReadonlyFile for HashFile {
    fn metadata(&self) -> Result<fs::Metadata> {
        self.file.metadata().map_err(|err| CacheError::from(err))
    }
}

impl Read for HashFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for HashFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let io::SeekFrom::Start(p) = pos {
            self.file.seek(SeekFrom::Start(p + 1))
        } else {
            self.file.seek(pos)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HashDirectory {
    entries: Vec<DirectoryEntry>
}

impl IntoIterator for HashDirectory {
    type Item = DirectoryEntry;
    type IntoIter = vec::IntoIter<DirectoryEntry>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        self.entries.into_iter()
    }
}

impl Directory for HashDirectory {}

#[derive(Debug)]
pub struct HashFileCache {
    base_path: Arc<PathBuf>
}

impl CacheLayer for HashFileCache {
    type File = HashFile;
    type Directory = HashDirectory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let mut file = self.open_object_file(cache_ref)?;
        let objtype = Self::identify_object_type(&mut file)?;

        let cache_object = match objtype {
            ObjectType::File => {
                let hash_file = HashFile {
                    file
                };

                CacheObject::File(hash_file)
            }

            ObjectType::Directory => {
                let hash_dir = serde_json::from_reader(file)?;
                CacheObject::Directory(hash_dir)
            }

            ObjectType::Commit => {
                let commit = serde_json::from_reader(file)?;
                CacheObject::Commit(commit)
            }
        };

        Ok(cache_object)
    }

    fn add(&mut self, object: CacheObject<Self::File, Self::Directory>) -> Result<CacheRef> {
        let obj_type = ObjectType::from(&object);
        let (mut target_file, rel_path) = self.create_staging_file(obj_type)?;

        match object {
            CacheObject::File(mut hash_file) => {
                hash_file.file.seek(SeekFrom::Start(0))?;
                // TODO: Use CoW if possible
                let copied = io::copy(&mut hash_file.file, &mut target_file)?;
                debug!("Bytes copied to staging file: {}", copied);
            }

            CacheObject::Directory(hash_dir, ..) => {
                serde_json::to_writer(&mut target_file, &hash_dir)?;
            }

            CacheObject::Commit(commit) => {
                serde_json::to_writer(&mut target_file, &commit)?;
            }
        };

        target_file.seek(SeekFrom::Start(0))?;

        let cache_ref = Self::build_cache_ref(target_file)?;
        let rel_target_path = Self::rel_path_from_ref(&cache_ref);

        let parent_path = rel_target_path.parent()
            .ok_or(CacheError::ObjectNotFound(cache_ref))?.to_owned();
        self.ensure_path(parent_path)?;

        let source_path = self.base_path.join(rel_path);
        let target_path = self.base_path.join(rel_target_path);
        info!("Adding object {} to {}", cache_ref, target_path.display());
        if !target_path.exists() {
            fs::hard_link(&source_path, target_path)?;
        }

        debug!("Removing staging file {}", source_path.display());
        if let Err(err) = fs::remove_file(&source_path) {
            warn!("Unable to remove staging file {}: {}", source_path.display(), err.description());
        }

        Ok(cache_ref)
    }

    fn create_file(&self, source_file: fs::File) -> Result<Self::File> {
        let hash_file = HashFile {
            file: source_file
        };

        Ok(hash_file)
    }

    fn create_directory<I: Iterator<Item=DirectoryEntry>>(&self, entries: I)
        -> Result<Self::Directory> {
        let dir = HashDirectory {
            entries: Vec::from_iter(entries)
        };

        Ok(dir)
    }

    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata> {
        let mut file = self.open_object_file(cache_ref)?;
        let size = file.metadata()?.len() - 1;
        let object_type = Self::identify_object_type(&mut file)?;

        let result = CacheObjectMetadata {
            size,
            object_type
        };

        Ok(result)
    }
}


impl HashFileCache {
    pub fn new<P: AsRef<Path>>(cache_dir: &P) -> Result<Self> {
        let cache = HashFileCache {
            base_path: Arc::new(cache_dir.as_ref().to_owned())
        };

        // Actually this could be omitted, but it will tell us early if the target location is
        // accessible
        cache.ensure_path("objects")?;
        cache.ensure_path("staging")?;

        Ok(cache)
    }

    fn ensure_path<P: AsRef<Path>>(&self, rel_path: P) -> Result<()> {
        let full_path = self.base_path.join(rel_path);
        debug!("Ensuring path {}", full_path.display());
        if !full_path.exists() {
            debug!("Creating path {}", full_path.display());
            ::std::fs::create_dir_all(&full_path)?;
        }
        if !full_path.is_dir() {
            Err(CacheError::InitializationFailed)
        } else {
            Ok(())
        }
    }

    fn build_cache_ref<R: Read>(mut reader: R) -> Result<CacheRef> {
        let mut hash_writer = HashWriter::new();
        io::copy(&mut reader, &mut hash_writer)?;
        Ok(CacheRef(hash_writer.finish()))
    }

    fn rel_path_from_ref(cache_ref: &CacheRef) -> PathBuf {
        let hexstr = cache_ref.to_string();
        Path::new("objects")
            .join(&hexstr[0..2])
            .join(&hexstr[2..4])
            .join(&hexstr[4..])
    }

    fn generate_staging_path() -> PathBuf {
        use rand::{weak_rng, Rng};

        let rand_name: String = weak_rng().gen_ascii_chars().take(8).collect();
        Path::new("staging").join(rand_name)
    }

    fn create_staging_file(&self, object_type: ObjectType) -> Result<(fs::File, PathBuf)> {
        self.ensure_path("staging")?;
        let rel_path = HashFileCache::generate_staging_path();
        let full_path = self.base_path.join(&rel_path);

        info!("Creating staging file {}", full_path.display());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(full_path)?;

        file.write(object_type.as_identifier())?;

        Ok((file, rel_path))
    }

    fn open_object_file(&self, cache_ref: &CacheRef) -> Result<fs::File> {
        let rel_path = Self::rel_path_from_ref(cache_ref);
        let path = self.base_path.join(&rel_path);

        if path.exists() {
            let file = fs::File::open(path)?;
            Ok(file)
        } else {
            Err(CacheError::ObjectNotFound(cache_ref.clone()))
        }
    }

    fn identify_object_type<R: Read>(object_file: &mut R) -> Result<ObjectType> {
        let mut objtype_identifier = [0u8; 1];
        object_file.read_exact(&mut objtype_identifier)?;
        ObjectType::from_identifier(&objtype_identifier)
    }
}

struct HashWriter {
    hasher: Keccak
}

impl HashWriter {
    fn new() -> Self {
        HashWriter {
            hasher: Keccak::new_shake256()
        }
    }

    fn finish(self) -> [u8; 32] {
        let mut result = unsafe {
            ::std::mem::uninitialized::<[u8; 32]>()
        };
        self.hasher.finalize(&mut result);
        result
    }
}

impl Write for HashWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.hasher.update(buf);
        Ok(())
    }
}

pub fn resolve_object_ref<C, P>(cache: &C, commit: &Commit, path: P)
    -> result::Result<Option<CacheRef>, failure::Error> where C: CacheLayer,
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
        use super::Result;

        let iter = HexCharIterator::new("123456789abcdef");
        let bytes: Result<Vec<u8>> = iter.collect();
        assert_eq!(vec![0x12u8, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf], bytes.unwrap());
    }
}
