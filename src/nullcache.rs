use std::path::Path;
use std::iter::IntoIterator;
use std::vec::IntoIter;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fs::File;

use cache::{self, CacheLayer, LayerError, CacheError, CacheObject, CacheObjectMetadata, CacheRef,
            DirectoryEntry, ReadonlyFile, Directory, Commit};
use utility::HashWriter;

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

#[derive(Debug)]
pub struct NullCache;

impl CacheLayer for NullCache {
    type File = NullFile;
    type Directory = NullDirectory;

    fn get(&self, cache_ref: &CacheRef) -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        Err(CacheError::ObjectNotFound(cache_ref.clone()))
    }

    fn metadata(&self, cache_ref: &CacheRef) -> cache::Result<CacheObjectMetadata> {
        Err(CacheError::ObjectNotFound(cache_ref.clone()))
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        let mut file = File::open(source_path)?;
        file.seek(SeekFrom::Start(0))?;
        io::copy(&mut file, &mut keccak)?;

        Ok(CacheRef(keccak.into()))
    }

    fn add_directory<I: Iterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef, CacheError> {
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
}
