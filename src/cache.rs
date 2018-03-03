use std::path::{Path, PathBuf};
use std::io::{Read, Write, Seek, SeekFrom, self};
use std::fmt::{Display, Formatter, self};
use std::fs;
use std::sync::Arc;
use std::result;
use std::error::Error;
use serde_json;
use std::string::ToString;
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy, Default)]
pub struct CacheRef([u8; 32]);

impl CacheRef {
    pub fn root() -> Self {
        Self::default()
    }
}

impl Display for CacheRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use std::fmt::LowerHex;
        for b in self.0.iter() {
            <u8 as LowerHex>::fmt(b, f)?;
        }
        Ok(())
    }
}

pub trait File: Read+Seek+Sized {
    type Writable: WritableFile<ReadOnly=Self>;

    fn to_writable(&self) -> Result<Self::Writable>;
}

pub trait WritableFile: Read+Write+Seek+Sized {
    type ReadOnly: File<Writable=Self>;

    fn finish(self) -> Result<Self::ReadOnly>;
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

impl<'a, F: File, D: Directory> From<&'a CacheObject<F, D>> for ObjectType {
    fn from(cache_object: &'a CacheObject<F, D>) -> ObjectType {
        match cache_object {
            &CacheObject::File(_) => ObjectType::File,
            &CacheObject::Directory(_) => ObjectType::Directory,
            &CacheObject::Commit(_) => ObjectType::Commit
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub cache_ref: CacheRef,
    pub object_type: ObjectType
}

pub trait Directory: Sized {
    type Writable: WritableDirectory<ReadOnly=Self>;
    type Iter: Iterator<Item=DirectoryEntry>;

    fn to_writable(&self) -> Result<Self::Writable>;
    fn iter(&self) -> Self::Iter;
}

pub trait WritableDirectory: Sized {
    type ReadOnly: Directory<Writable=Self>;
    type Iter: Iterator<Item=<<Self::ReadOnly as Directory>::Iter as Iterator>::Item>;

    fn add_entry(&mut self, entry: DirectoryEntry) -> Result<()>;
    fn finish(self) -> Result<Self::ReadOnly>;
    fn iter(&self) -> Self::Iter;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Commit {
    pub tree: CacheRef,
    pub parents: Vec<CacheRef>,
    pub message: String
}

#[derive(Debug)]
pub enum CacheObject<F: File, D: Directory> {
    File(F),
    Directory(D),
    Commit(Commit),
}

impl<F: File, D: Directory> CacheObject<F, D> {
    pub fn directory(dir: D) -> Self {
        CacheObject::Directory(dir)
    }
}

#[derive(Debug)]
pub struct CacheObjectMetadata {
    pub size: u64,
    pub object_type: ObjectType
}

impl<F: File, D: Directory> CacheObject<F, D> {
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
}

pub trait CacheLayer {
    type File: File;
    type Directory: Directory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>>;
    fn metadata(&self, cache_ref: &CacheRef) -> Result<CacheObjectMetadata>;
    fn add(&mut self, object: CacheObject<Self::File, Self::Directory>) -> Result<CacheRef>;
    fn delete(&mut self, cache_ref: &CacheRef) -> Result<()>;
    fn create_file(&self) -> Result<<Self::File as File>::Writable>;
    fn create_directory(&self) -> Result<<Self::Directory as Directory>::Writable>;
}

#[derive(Debug)]
pub struct HashFile {
    file: fs::File,
    base_path: Arc<PathBuf>,
    rel_path: PathBuf
}

impl File for HashFile {
    type Writable = WritableHashFile;

    fn to_writable(&self) -> Result<Self::Writable> {
        let rel_path = HashFileCache::generate_staging_path();
        let full_path = self.base_path.join(&rel_path);

        fs::copy(&self.base_path.join(&self.rel_path), &full_path)?;

        let file = fs::OpenOptions::new()
            .truncate(false)
            .read(true)
            .write(true)
            .open(full_path)?;

        let hash_file = HashFile {
            file,
            base_path: Arc::clone(&self.base_path),
            rel_path
        };

        Ok(WritableHashFile(hash_file))
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

#[derive(Debug)]
pub struct WritableHashFile(HashFile);

impl WritableFile for WritableHashFile {
    type ReadOnly = HashFile;

    fn finish(self) -> Result<Self::ReadOnly> {
        Ok(self.0)
    }
}

impl Read for WritableHashFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.file.read(buf)
    }
}

impl Seek for WritableHashFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if let SeekFrom::Start(p) = pos {
            self.0.file.seek(SeekFrom::Start(p + 1))
        } else {
            self.0.file.seek(pos)
        }
    }
}
impl Write for WritableHashFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.file.flush()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HashDirectory {
    entries: Vec<DirectoryEntry>
}

#[derive(Debug)]
pub struct WritableHashDirectory(HashDirectory);

impl Directory for HashDirectory {
    type Writable = WritableHashDirectory;
    type Iter = ::std::vec::IntoIter<DirectoryEntry>;

    fn to_writable(&self) -> Result<Self::Writable> {
        Ok(WritableHashDirectory(self.clone()))
    }

    fn iter(&self) -> Self::Iter {
        self.entries.clone().into_iter()
    }
}

impl WritableDirectory for WritableHashDirectory {
    type ReadOnly = HashDirectory;
    type Iter = ::std::vec::IntoIter<DirectoryEntry>;

    fn add_entry(&mut self, entry: DirectoryEntry) -> Result<()> {
        self.0.entries.push(entry);
        Ok(())
    }

    fn finish(self) -> Result<Self::ReadOnly> {
        Ok(self.0)
    }

    fn iter(&self) -> Self::Iter {
        self.0.entries.clone().into_iter()
    }
}

pub struct HashFileCache {
    base_path: Arc<PathBuf>
}

impl CacheLayer for HashFileCache {
    type File = HashFile;
    type Directory = HashDirectory;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let mut file = self.open_object_file(cache_ref)?;
        let rel_path = Self::rel_path_from_ref(cache_ref);
        let objtype = Self::identify_object_type(&mut file)?;

        let cache_object = match objtype {
            ObjectType::File => {
                let hash_file = HashFile {
                    file,
                    base_path: Arc::clone(&self.base_path),
                    rel_path
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
        let (file, staging_path) = match object {
            CacheObject::File(hash_file) => {
                (hash_file.file, hash_file.base_path.join(hash_file.rel_path))
            }

            CacheObject::Directory(hash_dir, ..) => {
                let (mut dir_file, staging_path) = self.create_staging_file(ObjectType::Directory)?;
                serde_json::to_writer(&mut dir_file, &hash_dir)?;
                dir_file.seek(SeekFrom::Start(0))?;
                (dir_file, self.base_path.join(staging_path))
            }

            CacheObject::Commit(commit) => {
                let (mut commit_file, staging_path) = self.create_staging_file(ObjectType::Commit)?;
                serde_json::to_writer(&mut commit_file, &commit)?;
                commit_file.seek(SeekFrom::Start(0))?;
                (commit_file, self.base_path.join(staging_path))
            }
        };

        let cache_ref = Self::build_cache_ref(file)?;
        let rel_target_path = Self::rel_path_from_ref(&cache_ref);

        let parent_path = rel_target_path.parent()
            .ok_or(CacheError::ObjectNotFound(cache_ref))?.to_owned();
        self.ensure_path(parent_path)?;

        let target_path = self.base_path.join(rel_target_path);
        info!("Adding object {} to {}", cache_ref, target_path.display());
        fs::hard_link(&staging_path, target_path)?;

        debug!("Removing staging file {}", staging_path.display());
        if let Err(err) = fs::remove_file(&staging_path) {
            warn!("Unable to remove staging file {}: {}", staging_path.display(), err.description());
        }

        Ok(cache_ref)
    }

    fn delete(&mut self, cache_ref: &CacheRef) -> Result<()> {
        let path = self.base_path.join(Self::rel_path_from_ref(cache_ref));
        if path.exists() {
            Ok(fs::remove_file(path)?)
        } else {
            Err(CacheError::ObjectNotFound(cache_ref.clone()))
        }
    }

    fn create_file(&self) -> Result<<Self::File as File>::Writable> {
        let (file, rel_path) = self.create_staging_file(ObjectType::File)?;
        let hash_file = HashFile {
            file,
            base_path: Arc::clone(&self.base_path),
            rel_path
        };

        Ok(WritableHashFile(hash_file))
    }

    fn create_directory(&self) -> Result<<Self::Directory as Directory>::Writable> {
        let dir = HashDirectory {
            entries: Vec::new()
        };

        Ok(WritableHashDirectory(dir))
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

        info!("Creating staging file {:?}", full_path);
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

pub type HashCacheObject =
    CacheObject<<HashFileCache as CacheLayer>::File, <HashFileCache as CacheLayer>::Directory>;

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
