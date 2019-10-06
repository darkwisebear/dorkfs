pub mod gitmodules;
mod gitconfig;

use std::ffi::{OsStr, OsString};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, Condvar};
use std::borrow::{Cow, Borrow};
use std::io::{self, Read, Seek, Write, SeekFrom};
use std::str::FromStr;
use std::path::Path;

use futures::{task::{self, Task}, prelude::*};
use failure::Fallible;
use tokio::io::AsyncWrite;

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

#[derive(Debug)]
pub enum RepoUrl<'a> {
    GithubHttps {
        apiurl: Cow<'a, str>,
        org: Cow<'a, str>,
        repo: Cow<'a, str>
    },

    GitFile {
        path: Cow<'a, Path>
    }
}

impl FromStr for RepoUrl<'static> {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let borrowed_repo_url = RepoUrl::from_borrowed_str(s)?;
        match borrowed_repo_url {
            RepoUrl::GithubHttps {
                apiurl, org, repo
            } => {
                let apiurl = apiurl.into_owned();
                let org = org.into_owned();
                let repo = repo.into_owned();
                Ok(RepoUrl::GithubHttps {
                    apiurl: Cow::Owned(apiurl),
                    org: Cow::Owned(org),
                    repo: Cow::Owned(repo)
                })
            }

            RepoUrl::GitFile { path } => Ok(RepoUrl::GitFile {
                path: Cow::Owned(path.into_owned())
            })
        }
    }
}

impl<'a> RepoUrl<'a> {
    pub fn from_borrowed_str(repo: &'a str) -> Fallible<Self> {
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

            "git+file" => Ok(RepoUrl::GitFile {
                path: Cow::Borrowed(Path::new(remainder))
            }),

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

const BUF_PREFILL: usize = 2048;

#[derive(Debug)]
struct SyncedBufferContent {
    buf: Vec<u8>,
    task: Option<Task>,
    finished: bool,
    max_pos: usize
}

impl Default for SyncedBufferContent {
    fn default() -> Self {
        Self {
            buf: Vec::default(),
            task: None,
            finished: false,
            max_pos: 0
        }
    }
}

impl SyncedBufferContent {
    fn set_max_read_pos(&mut self, read_pos: usize) {
        self.max_pos = self.max_pos.max(read_pos);
    }
}

#[derive(Debug, Default)]
pub struct SyncedBuffer(Mutex<SyncedBufferContent>, Condvar);

#[derive(Default, Debug)]
pub struct SharedBuffer {
    buffer: Arc<SyncedBuffer>
}

impl SharedBuffer {
    fn finish(&mut self) {
        self.buffer.0.lock().unwrap().finished = true;
        self.buffer.1.notify_all();
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.buffer.0.lock().unwrap();
        if inner.buf.len() - inner.max_pos >= BUF_PREFILL && task::is_in_task() {
            inner.task = Some(task::current());
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            inner.buf.extend_from_slice(buf);
            self.buffer.1.notify_all();
            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for SharedBuffer {
    fn shutdown(&mut self) -> Result<Async<()>, io::Error> {
        self.finish();
        Ok(Async::Ready(()))
    }
}

#[derive(Debug, Clone)]
pub struct SharedBufferReader {
    buffer: Arc<SyncedBuffer>,
    pos: usize
}

impl Read for SharedBufferReader {
    fn read(&mut self, dest_buf: &mut [u8]) -> io::Result<usize> {
        let mut inner = self.buffer.0.lock().unwrap();
        loop {
            let buffer = inner.buf.as_slice();

            let old_pos = self.pos;
            let src_buf = &buffer[old_pos..];
            let count = usize::min(dest_buf.len(), src_buf.len());

            if count > 0 {
                let src_chunk = &src_buf[..count];
                let dest_chunk = &mut dest_buf[..count];

                dest_chunk.copy_from_slice(src_chunk);
                let new_pos = old_pos + count;

                self.pos = new_pos;
                inner.set_max_read_pos(new_pos);

                break Ok(count);
            } else if inner.finished {
                break Ok(0);
            } else {
                if let Some(task) = inner.task.take() {
                    task.notify();
                }
                inner = self.buffer.1.wait(inner).unwrap();
            }
        }
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
                }.map(|val| val as u64).ok_or_else(|| io::ErrorKind::InvalidInput.into()),

            SeekFrom::End(offset) =>
                self.pos.checked_sub(offset as usize)
                    .map(|val| val as u64)
                    .ok_or_else(|| io::ErrorKind::InvalidInput.into())
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::RepoUrl;

    #[test]
    fn parse_github_url() {
        let repo_parts =
            RepoUrl::from_borrowed_str("github+https://api.github.com/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps { apiurl, org, repo } => {
                assert_eq!("api.github.com", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
            }

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }

    #[test]
    fn parse_on_premises_url() {
        let repo_parts =
            RepoUrl::from_borrowed_str("github+https://github.example.com/api/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps { apiurl, org, repo } => {
                assert_eq!("github.example.com/api", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
            }

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }

    #[test]
    fn parse_local_git_repo() {
        let repo_parts =
            RepoUrl::from_borrowed_str("git+file:///tmp/path/to/repo.git")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GitFile { path } =>
                assert_eq!(Path::new("/tmp/path/to/repo.git"), path),

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }
}
