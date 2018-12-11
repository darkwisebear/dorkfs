use std::{
    path::Path,
    iter::IntoIterator,
    vec::IntoIter,
    io::{self, Read, Write, Seek, SeekFrom},
    fs::File,
    collections::HashMap
};

use tiny_keccak::Keccak;

use cache::{self, CacheLayer, CacheError, CacheObject, CacheObjectMetadata, CacheRef,
            DirectoryEntry, ReadonlyFile, Directory, Commit};

pub struct HashWriter {
    hasher: Keccak
}

impl HashWriter {
    pub fn new() -> Self {
        HashWriter {
            hasher: Keccak::new_shake256()
        }
    }

    pub fn finish(self) -> [u8; 32] {
        let mut result = unsafe {
            ::std::mem::uninitialized::<[u8; 32]>()
        };
        self.hasher.finalize(&mut result);
        result
    }
}

impl Into<[u8; 32]> for HashWriter {
    fn into(self) -> [u8; 32] {
        self.finish()
    }
}

impl From<Keccak> for HashWriter {
    fn from(hasher: Keccak) -> Self {
        Self {
            hasher
        }
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

#[derive(Debug)]
pub struct NullFile;

impl Read for NullFile {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        unreachable!()
    }
}

impl Seek for NullFile {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        unreachable!()
    }
}

impl ReadonlyFile for NullFile {}

#[derive(Debug, Clone)]
pub struct NullDirectory;

impl IntoIterator for NullDirectory {
    type Item = DirectoryEntry;
    type IntoIter = IntoIter<DirectoryEntry>;

    fn into_iter(self) -> Self::IntoIter {
        unreachable!()
    }
}

impl Directory for NullDirectory {}

#[derive(Debug, Default)]
pub struct NullCache {
    branches: HashMap<String, CacheRef>
}

impl CacheLayer for NullCache {
    type File = NullFile;
    type Directory = NullDirectory;

    fn get(&self, cache_ref: &CacheRef) -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        Err(CacheError::ObjectNotFound(cache_ref.clone()))
    }

    fn metadata(&self, cache_ref: &CacheRef) -> cache::Result<CacheObjectMetadata> {
        Err(CacheError::ObjectNotFound(cache_ref.clone()))
    }

    fn add_file_by_path(&self, source_path: &Path) -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        let mut file = File::open(source_path)?;
        file.seek(SeekFrom::Start(0))?;
        io::copy(&mut file, &mut keccak)?;

        Ok(CacheRef(keccak.into()))
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>)
        -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        items.fold(Ok(0), |result, entry|
            // TODO: Add object type
            result.and_then(|_| keccak.write(entry.name.as_bytes()))
                .and_then(|_| keccak.write(entry.cache_ref.0.as_ref())))?;

        Ok(CacheRef(keccak.into()))
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        keccak.write(commit.tree.0.as_ref())?;
        commit.parents.iter().fold(Ok(0), |result, parent|
            result.and_then(|_| keccak.write(parent.0.as_ref())))?;
        keccak.write(commit.message.as_bytes())?;

        Ok(CacheRef(keccak.into()))
    }

    fn get_head_commit(&self, branch: &str) -> Result<Option<CacheRef>, CacheError> {
        Ok(self.branches.get(branch).cloned())
    }

    fn merge_commit(&mut self, branch: &str, cache_ref: CacheRef) -> Result<CacheRef, CacheError> {
        self.branches.insert(branch.to_string(), cache_ref);
        Ok(cache_ref)
    }

    fn create_branch(&mut self, branch: &str, cache_ref: CacheRef) -> Result<(), CacheError> {
        self.branches.insert(branch.to_string(), cache_ref);
        Ok(())
    }
}
