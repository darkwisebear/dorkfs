use std::fs::{self, DirEntry, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::hash::{Hash, Hasher};
use std::ffi::OsString;
use std::fmt::Debug;

use failure::Error;

use types::*;
use utility::*;
use cache::{self, DirectoryEntry, CacheLayer, CacheRef, Commit, CacheObject};

#[derive(Debug)]
pub enum OverlayFile<C: CacheLayer+Debug> {
    FsFile(File),
    CacheFile(C::File)
}

impl<C: CacheLayer+Debug> Read for OverlayFile<C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match *self {
            OverlayFile::FsFile(ref mut read) => read.read(buf),
            OverlayFile::CacheFile(ref mut read) => read.read(buf),
        }
    }
}

impl<C: CacheLayer+Debug> Write for OverlayFile<C> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        match *self {
            OverlayFile::FsFile(ref mut write) => write.write(buf),
            OverlayFile::CacheFile(..) => Err(io::Error::from(io::ErrorKind::PermissionDenied))
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        if let OverlayFile::FsFile(ref mut write) = *self {
            write.flush()
        } else {
            Ok(())
        }
    }
}

impl<C: CacheLayer+Debug> Seek for OverlayFile<C> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match *self {
            OverlayFile::FsFile(ref mut seek) => seek.seek(pos),
            OverlayFile::CacheFile(ref mut seek) => seek.seek(pos),
        }
    }
}

pub struct Overlay<C: CacheLayer> {
    cache: C,
    head: CacheRef,
    base_path: PathBuf
}

impl<C: CacheLayer+Debug> Overlay<C> {
    fn file_path<P: AsRef<Path>>(base_path: P) -> PathBuf {
        base_path.as_ref().join("files")
    }

    fn meta_path<P: AsRef<Path>>(base_path: P) -> PathBuf {
        base_path.as_ref().join("meta")
    }

    pub fn new<P: AsRef<Path>>(cache: C, base_path: P) -> Result<Self, Error> {
        fs::create_dir_all(Self::file_path(base_path.as_ref()))?;
        let meta_path = Self::meta_path(base_path.as_ref());
        fs::create_dir_all(&meta_path)?;

        let new_head_file_path = meta_path.join("NEW_HEAD");
        if new_head_file_path.exists() {
            unimplemented!("TODO: Recover from a previous crash during commit");
        }

        let head_path = meta_path.join("HEAD");
        let head = match File::open(&head_path) {
            Ok(mut file) => {
                let mut head = String::new();
                file.read_to_string(&mut head)?;
                head.parse()?
            }

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    let head = CacheRef::default();
                    let mut new_head_file = File::create(&head_path)?;
                    new_head_file.write(head.to_string().as_bytes())?;
                    head
                } else {
                    return Err(ioerr.into());
                }
            }
        };

        Ok(Overlay {
            cache,
            head,
            base_path: base_path.as_ref().to_owned()
        })
    }

    fn resolve_object_ref<P: AsRef<Path>>(&self, path: P) -> Result<Option<CacheRef>, Error> {
        if !self.head.is_root() {
            let commit = self.cache.get(&self.head)?.into_commit()
                .expect("Head ref is not a commit");
            cache::resolve_object_ref(&self.cache, &commit, path)
        } else {
            Ok(None)
        }
    }

    pub fn open_file<P: AsRef<Path>>(&self, path: P, writable: bool)
        -> Result<OverlayFile<C>, Error> {
        let overlay_path = Self::file_path(&self.base_path).join(path.as_ref());
        debug!("Trying to open {} in the overlay", overlay_path.to_string_lossy());

        // Check if the file is already part of the overlay
        let overlay_file = fs::OpenOptions::new()
            .read(true)
            .write(writable)
            .create(false)
            .open(&overlay_path);
        match  overlay_file {
            Ok(file) => Ok(OverlayFile::FsFile(file)),

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    let cache_ref = self.resolve_object_ref(path)?;
                    let mut cache_file = if let Some(existing_ref) = cache_ref {
                        let cache_object = self.cache.get(&existing_ref)?;
                        Some(cache_object.into_file()?)
                    } else {
                        None
                    };

                    if writable {
                        if let Some(parent_path) = overlay_path.parent() {
                            fs::create_dir_all(parent_path)?;
                        }

                        let mut new_file = fs::OpenOptions::new()
                            .create(true)
                            .read(true)
                            .write(true)
                            .open(&overlay_path)?;
                        if let Some(mut cache_file) = cache_file {
                            io::copy(&mut cache_file, &mut new_file)?;
                            new_file.seek(SeekFrom::Start(0))?;
                        }
                        Ok(OverlayFile::FsFile(new_file))
                    } else {
                        if let Some(cache_file) = cache_file {
                            Ok(OverlayFile::CacheFile(cache_file))
                        } else {
                            bail!("File not found!");
                        }
                    }
                } else {
                    Err(ioerr.into())
                }
            }
        }
    }

    fn dir_entry_to_directory_entry(&self, dir_entry: DirEntry, file_path: &Path)
        -> Result<cache::DirectoryEntry, Error> {
        let file_type = dir_entry.file_type()?;

        let (cache_ref, object_type) = if file_type.is_dir() {
            let cache_ref  = self.generate_tree(dir_entry.path(), file_path)?;
            (cache_ref, cache::ObjectType::Directory)
        } else if file_type.is_file() {
            let file = File::open(dir_entry.path())?;
            let cache_file  = self.cache.create_file(file)?;
            let cache_ref = self.cache.add(CacheObject::File(cache_file))?;
            (cache_ref, cache::ObjectType::File)
        } else {
            unimplemented!("TODO: Implement tree generation for further file types, e.g. links!");
        };

        let name = os_string_to_string(dir_entry.file_name())?;
        Ok(cache::DirectoryEntry {
            cache_ref,
            object_type,
            name
        })
    }

    fn iter_directory<I,T,P,F,G>(&self, path: P, mut from_directory_entry: F, mut from_dir_entry: G)
        -> Result<I, Error>
        where I: FromIterator<T>,
              T: Eq+Hash,
              P: AsRef<Path>,
              F: FnMut(DirectoryEntry) -> Result<T, Error>,
              G: FnMut(DirEntry) -> Result<T, Error> {
        // Get the directory contents from the cache if it's already there
        let dir_entries_result: Result<HashSet<T>, Error> =
            if let Some(cache_dir_ref) = self.resolve_object_ref(path.as_ref())? {
                let dir = self.cache.get(&cache_dir_ref)?.into_directory()?;
                dir.into_iter().map(|e| from_directory_entry(e)).collect()
            } else {
                Ok(HashSet::new())
            };
        let mut dir_entries = dir_entries_result?;

        // Now merge the staged content into the directory
        let abs_path = Self::file_path(&self.base_path).join(path.as_ref());
        info!("Reading overlay path {}", abs_path.to_string_lossy());
        for dir_entry in fs::read_dir(abs_path)? {
            let entry = from_dir_entry(dir_entry?)?;
            dir_entries.insert(entry);
        }

        Ok(I::from_iter(dir_entries.into_iter()))
    }

    fn directory_entry_to_overlay_dir_entry(&self, dir_entry: DirectoryEntry)
        -> Result<OverlayDirEntry, Error> {
        OverlayDirEntry::from_directory_entry(
            dir_entry,
            |dir_entry| {
                self.cache.metadata(&dir_entry.cache_ref).map_err(|e| e.into())
            })
    }

    pub fn list_directory<I,P>(&self, path: P) -> Result<I, Error>
        where I: FromIterator<OverlayDirEntry>,
              P: AsRef<Path> {
        self.iter_directory(
            path,
            |dir_entry| {
                self.directory_entry_to_overlay_dir_entry(dir_entry)
            },
            OverlayDirEntry::from_dir_entry)
    }

    fn generate_tree<P, Q>(&self, abs_path: P, file_path: Q) -> Result<CacheRef, Error>
        where P: AsRef<Path>,
              Q: AsRef<Path> {
        let rel_path = abs_path.as_ref().strip_prefix(file_path.as_ref())
            .expect("Path outside base path");

        let dir_entries: HashSet<DirectoryEntry> = self.iter_directory(
            rel_path,
            |e| Ok(e),
            |e| {
                self.dir_entry_to_directory_entry(e, file_path.as_ref())
            })?;
        let directory = self.cache.create_directory(dir_entries.into_iter())?;

        Ok(self.cache.add(CacheObject::Directory(directory))?)
    }

    fn create_new_head_commit(&mut self, commit_ref: &CacheRef) -> Result<PathBuf, Error> {
        self.head = commit_ref.clone();

        let meta_path = Self::meta_path(&self.base_path);
        let new_head_file_path = meta_path.join("NEW_HEAD");

        let mut new_head_file = File::create(&new_head_file_path)?;
        new_head_file.write(commit_ref.to_string().as_bytes())?;
        new_head_file.flush()?;
        new_head_file.sync_all()?;

        Ok(new_head_file_path)
    }

    pub fn commit(&mut self, message: &str) -> Result<CacheRef, Error> {
        let file_path = Self::file_path(&self.base_path);
        let meta_path = Self::meta_path(&self.base_path);

        let tree = self.generate_tree(&file_path, &file_path)?;
        let commit = Commit {
            parents: vec![self.head],
            tree,
            message: message.to_owned()
        };

        let new_commit_ref = self.cache.add(CacheObject::Commit(commit))?;

        // Stage 1: create file "NEW_HEAD"
        let new_head_path = self.create_new_head_commit(&new_commit_ref)?;

        let head_file_path = meta_path.join("HEAD");

        // Stage 2: Cleanup overlay content
        fs::remove_dir_all(&file_path)?;
        fs::create_dir(&file_path)?;
        fs::remove_file(&head_file_path)?;

        // Stage 3: Replace old HEAD with NEW_HEAD
        fs::rename(new_head_path, head_file_path)?;

        Ok(new_commit_ref)
    }

    pub fn ensure_directory<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        fs::create_dir_all(Self::file_path(&self.base_path).join(path))
            .map_err(|e| e.into())
    }

    pub fn metadata<P: AsRef<Path>>(&self, path: P) -> Result<Metadata, Error> {
        let file_path = Self::file_path(&self.base_path).join(path.as_ref());

        if file_path.exists() {
            fs::symlink_metadata(file_path)
                .map_err(|e| e.into())
                .and_then(Metadata::from_fs_metadata)
        } else {
            self.resolve_object_ref(path)
                .and_then(|cache_ref| cache_ref.ok_or(format_err!("File not found!")))
                .and_then(|cache_ref| self.cache.metadata(&cache_ref)
                    .map_err(|e| e.into()))
                .and_then(Metadata::from_cache_metadata)
        }
    }

    pub fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        self.metadata(path).is_ok()
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use cache::*;
    use overlay::*;
    use std::path::Path;
    use std::io::{Read, Write, Seek, SeekFrom};
    use std::collections::HashSet;

    fn open_working_copy<P: AsRef<Path>>(path: P) -> Overlay<HashFileCache> {
        let cache_dir = path.as_ref().join("cache");
        let overlay_dir = path.as_ref().join("overlay");

        let cache = HashFileCache::new(&cache_dir).expect("Unable to create cache");
        Overlay::new(cache, &overlay_dir)
            .expect("Unable to create overlay")
    }

    #[test]
    fn create_root_commit() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        let mut staged_file = overlay.open_file("test.txt", false)
            .expect("Unable to open staged file!");
        let mut contents = String::new();
        staged_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(staged_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut committed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(committed_file);

        let metadata = overlay.metadata("test.txt")
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);

        let mut editable_file = overlay.open_file("test.txt", true)
            .expect("Unable to open file for writing!");
        editable_file.seek(SeekFrom::End(0)).expect("Unable to seek to end!");
        write!(editable_file, "Yay!")
            .expect("Unable to append to file!");
        drop(editable_file);

        let mut changed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open changed file!");
        let mut contents = String::new();
        changed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!Yay!");
        drop(changed_file);

        let metadata = overlay.metadata("test.txt")
            .expect("Unable to fetch metadata");
        assert_eq!("What a test!Yay!".len() as u64, metadata.size);
        assert_eq!(super::ObjectType::File, metadata.object_type);
    }

    #[test]
    fn two_commits() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("A test commit").expect("Unable to commit");

        let mut additional_file = overlay.open_file("test2.txt", true)
            .expect("Unable to create file");
        write!(additional_file, "Another file").expect("Couldn't write to test file");
        drop(additional_file);

        overlay.commit("A test commit with parent").expect("Unable to commit");

        let mut first_file = overlay.open_file("test.txt", false)
            .expect("Unable to open first file");
        let mut content = String::new();
        first_file.read_to_string(&mut content)
            .expect("Unable to read from first file");
        assert_eq!("What a test!", content.as_str());
    }

    #[test]
    fn reopen_workspace() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let overlay = open_working_copy(tempdir.path());

        let mut test_file = overlay.open_file("test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        drop(overlay);

        let overlay = open_working_copy(tempdir.path());

        let mut committed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
    }

    #[test]
    fn commit_directories() {
        ::init_logging();

        let tempdir = tempdir().expect("Unable to create temporary dir!");
        let mut overlay = open_working_copy(tempdir.path());
        overlay.ensure_directory("a/nested/dir")
            .expect("Failed to create a nested directory");

        let mut test_file = overlay.open_file("a/nested/dir/test.txt", true)
            .expect("Unable to create file");
        write!(test_file, "What a test!").expect("Couldn't write to test file");
        drop(test_file);

        overlay.commit("A test commit").expect("Unable to commit");

        for writable in &[true, false] {
            let mut committed_file = overlay.open_file("a/nested/dir/test.txt", *writable)
                .expect(format!("Unable to open committed file as {}",
                        if *writable { "writable" } else { "readonly" }).as_str());
            let mut contents = String::new();
            committed_file.read_to_string(&mut contents)
                .expect("Unable to read from committed file");
            drop(committed_file);
            assert_eq!("What a test!", contents.as_str());
        }

        let dir_entries: HashSet<OverlayDirEntry> = overlay.list_directory("a/nested/dir")
            .expect("Unable to get directory contents of a/nested/dir");
        assert_eq!(HashSet::<String>::from_iter(["test.txt"].iter().map(|s| s.to_string())),
                   HashSet::<String>::from_iter(dir_entries.iter().map(|e| e.name.clone())));
    }
}
