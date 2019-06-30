pub mod gitmodules;
mod gitconfig;

use std::ffi::{OsStr, OsString};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex, MutexGuard, atomic::AtomicUsize};
use std::borrow::{Cow, Borrow};
use std::io::{self, Read, Seek, Write, BufRead, SeekFrom, Cursor};
use std::error;

use futures::prelude::*;
use failure::Fallible;

pub fn os_string_to_string(s: OsString) -> Fallible<String> {
    s.into_string()
        .map_err(|s| {
            format_err!("Unable to convert {} into UTF-8", s.to_string_lossy())
        })
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ObjectName(Arc<OsString>);

impl Borrow<OsStr> for ObjectName {
    fn borrow(&self) -> &OsStr {
        self.0.as_os_str()
    }
}

#[derive(Debug)]
pub struct OpenHandleSet<T: Debug> {
    next_handle: u64,
    open_objects: HashMap<u64, (ObjectName, T)>,
    object_names: HashMap<ObjectName, u64>
}

impl<T: Debug> OpenHandleSet<T> {
    pub fn new() -> Self {
        OpenHandleSet {
            next_handle: 0,
            open_objects: HashMap::new(),
            object_names: HashMap::new()
        }
    }

    pub fn push(&mut self, value: T, name: Cow<OsStr>) -> u64 {

        let handle = self.next_handle;
        self.next_handle += 1;

        info!("Adding {} as #{} to open objects storage", name.to_string_lossy(), handle);

        let identifier = ObjectName(Arc::new(name.into_owned()));
        self.open_objects.insert(handle, (identifier.clone(), value));
        self.object_names.insert(identifier, handle);

        handle
    }

    pub fn get(&self, handle: u64) -> Option<&T> {
        self.open_objects.get(&handle)
            .map(|(_, ref obj) | obj)
    }

    pub fn get_mut(&mut self, handle: u64) -> Option<&mut T> {
        self.open_objects.get_mut(&handle)
            .map(|(_, ref mut obj)| obj)
    }

    #[allow(dead_code)]
    pub fn get_named<S: AsRef<OsStr>>(&self, name: S) -> Option<&T> {
        self.object_names.get(name.as_ref())
            .and_then(|handle| self.get(*handle))
    }

    pub fn get_named_mut<S: AsRef<OsStr>>(&mut self, name: S) -> Option<&mut T> {
        match self.object_names.get(name.as_ref()).cloned() {
            Some(handle) => self.get_mut(handle),
            None => None
        }
    }

    pub fn remove(&mut self, handle: u64) -> Option<T> {
        self.open_objects.remove(&handle)
            .map(|(name, obj)| {
                self.object_names.remove(name.0.as_os_str());
                obj
            })
    }
}

pub enum RepoUrl<'a> {
    GithubHttps {
        apiurl: Cow<'a, str>,
        org: Cow<'a, str>,
        repo: Cow<'a, str>
    }
}

impl<'a> RepoUrl<'a> {
    pub fn from_str(repo: &'a str) -> Fallible<Self> {
        let (scheme, remainder) = Self::split_scheme(repo)?;
        match scheme {
            "github+https" => {
                let mut splitter = remainder.rsplitn(3, '/');
                let repo = splitter.next()
                    .ok_or_else(|| format_err!("Repo missing in repo URL"))?;
                let org = splitter.next()
                    .ok_or_else(|| format_err!("Org/user missing in repo URL"))?;
                let apiurl = splitter.next()
                    .ok_or_else(|| format_err!("Api URL missing in repo URL"))?;
                Ok(RepoUrl::GithubHttps {
                    apiurl: Cow::Borrowed(apiurl),
                    org: Cow::Borrowed(org),
                    repo: Cow::Borrowed(repo)
                })
            }

            unknown_scheme => bail!("Unknown repo URL scheme {}", unknown_scheme)
        }
    }

    fn split_scheme(repo: &str) -> Fallible<(&str, &str)> {
        repo.find(':').ok_or_else(|| format_err!("Missing scheme in repo URL"))
            .and_then(|pos| repo.get(pos+3..)
                .ok_or_else(|| format_err!("Incomplete repo URL"))
                .map(|path| (&repo[..pos], path)))
    }
}

pub type SyncedBuffer = Mutex<Vec<u8>>;

#[derive(Default, Debug)]
pub struct SharedBuffer {
    buffer: Arc<SyncedBuffer>
}

impl SharedBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_content(content: Box<[u8]>) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::from(content)))
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    pub fn build_reader(&self) -> SharedBufferReader {
        SharedBufferReader { buffer: Arc::clone(&self.buffer), pos: 0 }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf).map(|_| buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SharedBufferReader {
    buffer: Arc<SyncedBuffer>,
    pos: usize
}

impl SharedBufferReader {
    pub fn len(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }
}

impl Read for SharedBufferReader {
    fn read(&mut self, dest_buf: &mut [u8]) -> io::Result<usize> {
        let buffer = self.buffer.lock().unwrap();

        let old_pos = self.pos;
        let src_buf = &buffer[old_pos..];
        let count = usize::min(dest_buf.len(), src_buf.len());

        let src_chunk = &src_buf[..count];
        let dest_chunk = &mut dest_buf[..count];

        dest_chunk.copy_from_slice(src_chunk);
        let new_pos = old_pos+count;

        self.pos = new_pos;

        Ok(count)
    }
}

impl Seek for SharedBufferReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => {
                self.pos = pos as usize;
                Ok(pos)
            }

            SeekFrom::Current(offset) =>
                if offset >= 0 {
                    self.pos.checked_add(offset as usize)
                } else {
                    self.pos.checked_sub((-offset) as usize)
                }.map(|val| val as u64).ok_or(io::ErrorKind::InvalidInput.into()),

            SeekFrom::End(offset) =>
                self.pos.checked_sub(offset as usize)
                    .map(|val| val as u64)
                    .ok_or(io::ErrorKind::InvalidInput.into())
        }
    }
}

pub fn collect_strings<T, E, S>(stream: S) -> (SharedBufferReader, impl Future<Item=(), Error=()>)
    where T: ToString,
          E: From<io::Error>+Display+Send+'static,
          S: Stream<Item=T, Error=E>+Send+'static {
    let mut writer = SharedBuffer::default();
    let reader = writer.build_reader();

    let folding = stream.for_each(move |item|
        writer.write_all(item.to_string().as_bytes())
            .map_err(E::from))
        .map_err(|e| warn!("Unable to completely render log file to buffer: {}", e));

    (reader, folding)
}

#[cfg(test)]
mod test {
    use super::RepoUrl;

    #[test]
    fn parse_github_url() {
        let repo_parts =
            RepoUrl::from_str("github+https://api.github.com/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps { apiurl, org, repo } => {
                assert_eq!("api.github.com", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
            }
        }
    }

    #[test]
    fn parse_on_premises_url() {
        let repo_parts =
            RepoUrl::from_str("github+https://github.example.com/api/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps { apiurl, org, repo } => {
                assert_eq!("github.example.com/api", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
            }
        }
    }
}
