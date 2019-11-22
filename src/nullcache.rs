use std::{
    path::Path,
    iter::IntoIterator,
    vec::IntoIter,
    io::{self, Read, Write, Seek, SeekFrom},
    fs::File,
    collections::HashMap,
    borrow::Borrow,
    ffi::OsStr,
    sync::Mutex,
    mem::MaybeUninit
};

use tiny_keccak::Keccak;

use crate::cache::{self, CacheLayer, CacheError, CacheObject, CacheObjectMetadata, CacheRef,
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
        let mut result = MaybeUninit::<[u8; 32]>::uninit();
        unsafe {
            self.hasher.finalize(result.as_mut_ptr().as_mut().unwrap());
            result.assume_init()
        }
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

pub type NullDirectory = Vec<DirectoryEntry>;

#[derive(Debug, Default)]
pub struct NullCache {
    branches: Mutex<HashMap<String, CacheRef>>
}

impl CacheLayer for NullCache {
    type File = NullFile;
    type Directory = NullDirectory;
    type GetFuture = ::futures::future::Ready<Result<CacheObject<NullFile, NullDirectory>, cache::CacheError>>;

    fn get(&self, cache_ref: &CacheRef) -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        Err(CacheError::ObjectNotFound(cache_ref.clone()))
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        let mut file = File::open(source_path)?;
        file.seek(SeekFrom::Start(0))?;
        io::copy(&mut file, &mut keccak)?;

        Ok(CacheRef(keccak.into()))
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I)
        -> Result<CacheRef, CacheError> {
        let mut keccak = HashWriter::new();

        items.into_iter().fold(Ok(0), |result, entry|
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

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>, CacheError> {
        Ok(self.branches.lock().unwrap().get(branch.as_ref()).cloned())
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef, CacheError> {
        self.branches.lock().unwrap().insert(branch.as_ref().to_string(), *cache_ref);
        Ok(*cache_ref)
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<(), CacheError> {
        self.merge_commit(branch, cache_ref).map(|_| ())
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        futures::future::ready(self.get(cache_ref))
    }
}
