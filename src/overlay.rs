use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::collections::HashSet;

use failure::Error;

use cache::{self, DirectoryEntry, CacheLayer, CacheRef, Commit, ReadonlyFile, CacheObject,
            CacheError};

pub enum OverlayFile<C: CacheLayer> {
    FsFile(fs::File),
    CacheFile(C::File)
}

impl<C: CacheLayer> io::Read for OverlayFile<C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match *self {
            OverlayFile::FsFile(ref mut read) => read.read(buf),
            OverlayFile::CacheFile(ref mut read) => read.read(buf),
        }
    }
}

impl<C: CacheLayer> io::Write for OverlayFile<C> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        match *self {
            OverlayFile::FsFile(ref mut write) => write.write(buf),
            OverlayFile::CacheFile(ref mut read) => Err(io::Error::from(io::ErrorKind::PermissionDenied))
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

impl<C: CacheLayer> io::Seek for OverlayFile<C> {
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

impl<C: CacheLayer> Overlay<C> {
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

        let head = match fs::File::open(meta_path.join("HEAD")) {
            Ok(mut file) => {
                let mut head = String::new();
                file.read_to_string(&mut head)?;
                head.parse()?
            }

            Err(ioerr) => {
                if ioerr.kind() == io::ErrorKind::NotFound {
                    let head = CacheRef::default();
                    let mut new_head_file = fs::File::create(meta_path.join("HEAD"))?;
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

    fn resolve_object_ref<P: AsRef<Path>>(&mut self, path: P) -> Result<Option<CacheRef>, Error> {
        if !self.head.is_root() {
            let commit = self.cache.get(&self.head)?.into_commit()
                .expect("Head ref is not a commit");
            cache::resolve_object_ref(&self.cache, &commit, path)
        } else {
            Ok(None)
        }
    }

    pub fn get_cache_layer_mut(&mut self) -> &mut C {
        &mut self.cache
    }

    pub fn open_file<P: AsRef<Path>>(&mut self, path: P, writable: bool)
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

    fn dir_entry_to_directory_entry(&mut self,
                                    dir_entry: fs::DirEntry,
                                    file_path: &Path) -> Result<cache::DirectoryEntry, Error> {
        let file_type = dir_entry.file_type()?;

        let (cache_ref, object_type) = if file_type.is_dir() {
            let cache_ref  = self.generate_tree(dir_entry.path(), file_path)?;
            (cache_ref, cache::ObjectType::Directory)
        } else if file_type.is_file() {
            let file = fs::File::open(dir_entry.path())?;
            let cache_file  = self.cache.create_file(file)?;
            let cache_ref = self.cache.add(CacheObject::File(cache_file))?;
            (cache_ref, cache::ObjectType::File)
        } else {
            unimplemented!()
        };

        let name = dir_entry.file_name().into_string()
            .map_err(|entry| format_err!("Unable to convert {} to UTF-8", entry.to_string_lossy()))?;
        Ok(cache::DirectoryEntry {
            cache_ref,
            object_type,
            name
        })
    }

    fn generate_tree<P, Q>(&mut self, abs_path: P, file_path: Q) -> Result<CacheRef, Error>
        where P: AsRef<Path>,
              Q: AsRef<Path> {
        let rel_path = abs_path.as_ref().strip_prefix(file_path.as_ref())
            .expect("Path outside base path");

        // Get the directory contents from the cache if it's already there
        let mut dir_entries: HashSet<DirectoryEntry> =
            if let Some(cache_dir_ref) = self.resolve_object_ref(rel_path)? {
                let dir = self.cache.get(&cache_dir_ref)?.into_directory()?;
                dir.into_iter().collect()
            } else {
                HashSet::new()
            };

        // Now merge the staged content into the directory
        for dir_entry in fs::read_dir(abs_path.as_ref())? {
            let entry = self.dir_entry_to_directory_entry(dir_entry?, file_path.as_ref())?;
            dir_entries.insert(entry);
        }

        let directory = self.cache.create_directory(dir_entries.into_iter())?;

        Ok(self.cache.add(CacheObject::Directory(directory))?)
    }

    pub fn commit(&mut self, message: &str) -> Result<CacheRef, Error> {
        let file_path = Self::file_path(&self.base_path);

        let tree = self.generate_tree(&file_path, &file_path)?;
        let commit = Commit {
            parents: vec![self.head],
            tree,
            message: message.to_owned()
        };

        let new_commit_ref = self.cache.add(CacheObject::Commit(commit))?;
        self.head = new_commit_ref;

        fs::remove_dir_all(&file_path)?;
        fs::create_dir(&file_path)?;

        Ok(new_commit_ref)
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use cache::*;
    use overlay::*;
    use std::path::Path;
    use std::io::{Read, Write, Seek, SeekFrom};

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

        let head_ref = overlay.commit("A test commit").expect("Unable to commit");

        let mut committed_file = overlay.open_file("test.txt", false)
            .expect("Unable to open committed file!");
        let mut contents = String::new();
        committed_file.read_to_string(&mut contents).expect("Unable to read from file");
        assert_eq!(contents.as_str(), "What a test!");
        drop(committed_file);

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
}
