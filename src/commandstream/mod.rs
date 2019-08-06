use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    io,
    net::Shutdown,
    borrow::Cow
};

use crate::{
    cache::CacheRef,
    overlay::{self, Overlay, WorkspaceController}
};

use futures::{failed, Future, Stream, IntoFuture, future::Either, stream};
use tokio::{
    self,
    prelude::{AsyncRead, AsyncWrite}
};
use failure::{self, Fallible};
use serde_json;
use glob::Pattern as GlobPattern;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Commit {
        message: String
    },

    Log {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_commit: Option<CacheRef>
    },

    Status,

    CurrentBranch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_branch: Option<String>
    },

    CreateBranch {
        name: String
    },

    Revert {
        path_glob: String,
        #[serde(default = "Command::dry_run_default")]
        dry_run: bool
    },
}

impl Command {
    fn dry_run_default() -> bool {
        false
    }
}

pub struct CommandExecutor<R: Overlay+for<'a> WorkspaceController<'a>+'static> {
    repo: Arc<RwLock<R>>
}

// We need to manually implement this as derive(Clone) will only work
// if W: Clone.
impl<R: Overlay+for<'a> WorkspaceController<'a>+'static> Clone for CommandExecutor<R> {
    fn clone(&self) -> Self {
        Self {
            repo: Arc::clone(&self.repo)
        }
    }
}

impl<R> CommandExecutor<R> where R: Overlay+for<'a> WorkspaceController<'a>+'static {
    pub fn new(repo: Arc<RwLock<R>>) -> Self {
        Self {
            repo
        }
    }

    pub fn command<T: AsyncWrite+Send+Sync+'static>(&self, command: Command, target: T)
        -> impl Future<Item=T, Error=failure::Error>+Send {
        match command {
            Command::Commit { message } =>
                Box::new(self.commit_handler(message, target))
                    as Box<dyn Future<Item=T, Error=failure::Error>+Send>,

            Command::Log { start_commit } =>
                Box::new(self.handle_log_stream(start_commit, target)) as Box<_>,

            Command::Status => Box::new(self.handle_status(target)) as Box<_>,

            Command::CurrentBranch { target_branch } =>
                Box::new(self.handle_switch(target_branch, target)) as Box<_>,

            Command::CreateBranch {
                name
            } => {
                let create_result =
                    self.repo.write().unwrap().create_branch(name.as_str(), None)
                        .map_err(failure::Error::from)
                        .into_future()
                        .map(|_| target);
                Box::new(create_result) as Box<_>
            }

            Command::Revert {
                path_glob,
                dry_run
            } => Box::new(self.handle_revert(target, path_glob, dry_run)) as Box<_>
        }
    }

    fn handle_switch<T>(&self, target_branch: Option<String>, target: T)
        -> impl Future<Item=T, Error=failure::Error> where T: AsyncWrite+Send+Sync+'static {
        let mut repo = self.repo.write().unwrap();
        let branch_status = match target_branch {
            Some(target_branch_name) =>
                repo
                    .switch_branch(Some(&target_branch_name))
                    .and_then(|_| repo.update_head())
                    .map(move |cache_ref|
                        (Cow::Owned(target_branch_name), cache_ref))
                    .map_err(failure::Error::from),

            None => {
                let current_branch = repo.get_current_branch()
                    .map(|option| option.map(Cow::into_owned));
                match current_branch {
                    Ok(Some(name)) =>
                        repo.update_head()
                            .map(|cache_ref|
                                (Cow::Owned(name), cache_ref))
                            .map_err(failure::Error::from),
                    Ok(None) =>
                        repo.get_current_head_ref()
                            .map(|cache_ref| {
                                let cache_ref = cache_ref
                                    .unwrap_or_else(|| CacheRef::null());
                                (Cow::Borrowed(&"(detached)"[..]), cache_ref)
                            })
                            .map_err(failure::Error::from),
                    Err(e) => Err(format_err!("Error retrieving current branch: {}", e))
                }
            }
        };

        branch_status
            .into_future()
            .and_then(|(branch_name, cache_ref)| {
                let status_string = format!("{} checked out at {}", branch_name, cache_ref);
                Self::send_string(target, status_string)
            })
    }

    fn handle_revert<T>(&self, target: T, path_glob: String, dry_run: bool)
        -> impl Future<Item=T, Error=failure::Error> where T: AsyncWrite+Send+Sync+'static {
        let repo = self.repo.read().unwrap();

        let matching_paths = repo.get_status()
            .map_err(failure::Error::from)
            .and_then(|status_iter| {
                let glob = GlobPattern::new(path_glob.as_str())
                    .map_err(failure::Error::from)?;

                status_iter
                    .filter_map(move |file_path| match file_path {
                        Ok(path) => if glob.matches_path(path.as_path()) {
                            Some(Ok(path.as_path().to_owned()))
                        } else {
                            None
                        },

                        Err(e) => Some(Err(failure::Error::from(e)))
                    })
                    .collect::<Fallible<Vec<PathBuf>>>()
            });

        let revert_result = matching_paths
            .and_then(|matching_paths| {
                if !dry_run {
                    for matching_path in &matching_paths {
                        repo.revert_file(matching_path.as_path())
                            .map_err(failure::Error::from)?
                    }
                }

                Ok(matching_paths)
            });

        revert_result.into_future()
            .and_then(|matching_paths|
                stream::iter_ok::<_, failure::Error>(matching_paths)
                    .fold(target, |target, matching_path| {
                        let message = format!("Reverting {}\n", matching_path.display());
                        tokio::io::write_all(target, message)
                            .map(|(target, _)| target)
                    })
                    .map_err(failure::Error::from))
    }

    fn send_string<T, E>(target: T, string: String)
        -> impl Future<Item=T, Error=E> where T: AsyncWrite+Send+Sync+'static, E: From<io::Error> {
        tokio::io::write_all(target, string.into_bytes())
            .map(|(target, _)| target)
            .map_err(E::from)
    }

    fn handle_status<T: AsyncWrite+Send+Sync+'static>(&self, target: T)
        -> impl Future<Item=T, Error=failure::Error> {
        let status_string: Result<String, _> = self.repo.read().unwrap().get_status()
            .and_then(|status_iter|
                status_iter.fold(Ok(String::new()), |s: overlay::Result<String>, t| {
                    s.and_then(move |mut s| {
                        s.push_str(&t?.to_string());
                        s.push('\n');
                        Ok(s)
                    })
                }))
            .map_err(failure::Error::from);

        status_string
            .into_future()
            .and_then(move |status_string|
                Self::send_string(target, status_string))
    }

    fn handle_log_stream<T>(&self, start_commit: Option<CacheRef>, target: T)
        -> impl Future<Item=T, Error=failure::Error>+Send
        where T: AsyncWrite+Send+Sync+'static {
        static NO_LOG_STRING: &[u8] = b"(empty log)";

        let repo = self.repo.read().unwrap();

        let start_commit = match start_commit {
            Some(start_commit) => Ok(Some(start_commit)),
            None => repo.get_current_head_ref()
                .map_err(failure::Error::from)
        };

        match start_commit {
            Ok(start_commit) => {
                let ok_future = match start_commit {
                    Some(start_commit) => {
                        let log_stream = repo.get_log_stream(start_commit)
                                            .map_err(failure::Error::from);
                        Either::A(log_stream
                            .fold(target, |target, commit| {
                                let mut commit_string = commit.to_string();
                                commit_string.push('\n');
                                tokio::io::write_all(target, commit_string)
                                    .map(|(target, _)| target)
                                    .map_err(failure::Error::from)
                            }))
                    }

                    None => Either::B(tokio::io::write_all(target, NO_LOG_STRING)
                                .map(|(target, _)| target)
                                .map_err(failure::Error::from)),
                };

                Either::A(ok_future)
            }

            Err(e) => Either::B(failed(
                format_err!("Unable to get start commit for log listing: {}", e)))
        }
    }

    fn commit_handler<T>(&self, message: String, target: T)
        -> impl Future<Item=T, Error=failure::Error>
        where T: AsyncWrite+Send+Sync+'static {
        let message = match self.repo.write().unwrap().commit(message.as_str()) {
            Ok(cache_ref) => format!("Successfully committed, HEAD at {}", cache_ref),
            Err(e) => format!("Failed to commit: {}", e)
        };

        tokio::io::write_all(target, message)
            .map(|(target, _)| target)
            .map_err(failure::Error::from)
    }
}

pub fn execute_commands<R, C>(channel: C, executor: CommandExecutor<R>)
    -> impl Future<Item=(), Error=failure::Error> where R: Overlay+for<'a> WorkspaceController<'a>+'static,
                                                        C: AsyncRead+AsyncWrite+Send+'static {
    let (input, output) = channel.split();
    tokio::io::read_to_end(input, Vec::new())
        .map_err(failure::Error::from)
        .and_then(|(_, command_string)|
                  serde_json::from_slice(&command_string[..])
                    .map_err(failure::Error::from))
        .and_then(move |command| {
            debug!("Received command {:?}", command);
            executor.command(command, output)
        })
        .and_then(|target|
            tokio::io::shutdown(target)
                .map(|_| ())
                .map_err(failure::Error::from))
        .then(|result| match result {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Error during command processing: {}", e);
                Ok(())
            }
        })
}

#[cfg(target_os = "linux")]
pub fn create_command_socket<P, R>(path: P, command_executor: CommandExecutor<R>)
    -> impl Future<Item=(), Error=failure::Error> where P: AsRef<Path>,
                                                        R: Overlay+for<'a> WorkspaceController<'a> {
    debug!("Creating command socket at {}", path.as_ref().display());

    let listener = tokio_uds::UnixListener::bind(&path)
        .expect(format!("Unable to bind UDS listener {}", path.as_ref().display()).as_str());

    listener.incoming()
        .map_err(failure::Error::from)
        .for_each(move |connection|
            execute_commands(connection, command_executor.clone()))
}

#[cfg(target_os = "linux")]
fn find_command_stream() -> impl Future<Item=tokio_uds::UnixStream, Error=io::Error> {
    fn find_stream_path() -> io::Result<PathBuf> {
        let cwd = std::env::current_dir()?;

        let mut dork_root = cwd.as_path();
        if dork_root.ends_with(".dork") {
            dork_root = dork_root.parent().unwrap();
        }

        loop {
            debug!("Looking for command socket in {}", dork_root.display());

            let command_socket_path = dork_root.join(".dork/cmd");
            if command_socket_path.exists() {
                info!("Using {} as command socket path", command_socket_path.display());
                break Ok(command_socket_path);
            }

            match dork_root.parent() {
                Some(parent) => dork_root = parent,
                None => break Err(io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        format!("Unable to find a mounted dork file system at {}",
                                cwd.display())))
            }
        }
    }

    find_stream_path().into_future()
        .and_then(|command_socket_path|
            tokio_uds::UnixStream::connect(command_socket_path))
}

#[cfg(not(target_os = "linux"))]
fn find_command_stream() -> impl Future<Item=tokio::fs::File, Error=io::Error> {
    futures::failed(io::Error::new(
        io::ErrorKind::Other,
        "dorkfs only supports Linux. Bummer."))
}

pub fn send_command(command: Command) {
    let command_string = serde_json::to_string(&command).unwrap();
    let task = find_command_stream()
        .and_then(move |stream| tokio::io::write_all(stream, command_string))
        .and_then(|(stream, _)| {
            #[cfg(target_os = "linux")] {
                let result = ::tokio_uds::UnixStream::shutdown(&stream, Shutdown::Write);
                result.map(move |_| stream)
            }
            #[cfg(not(target_os = "linux"))] {
                Ok(stream)
            }
        })
        .and_then(|stream| tokio::io::copy(stream, tokio::io::stdout()))
        .and_then(|(_, stream, _)| tokio::io::shutdown(stream))
        .map(|_| ())
        .map_err(|e| {
            error!("Unable to communicate with daemon: {}", e);
            ()
        });

    tokio::runtime::run(task);
}

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Write, Cursor, self},
        path::Path, fmt::Debug
    };

    use crate::{
        overlay::testutil::{check_file_content, open_working_copy},
        cache::ReferencedCommit
    };

    use tempfile::tempdir;
    use futures;
    use tokio::runtime::current_thread::run as run_current_thread;

    use super::*;

    fn execute_command_future<T, E, F>(command_future: F) -> T
        where T: AsyncWrite+Send+Sync+'static,
              E: Debug,
              F: Future<Item=T, Error=E>+'static {
        command_future.wait().expect("Unable to execute command")
    }

    #[test]
    fn commit_via_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let output = io::sink();
        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        let mut file = working_copy.write().unwrap().open_file(Path::new("test.txt"), true)
            .expect("Unable to create test file");
        file.write("Teststring".as_bytes())
            .expect("Unable to write to test file");
        drop(file);

        let commit_command = Command::Commit { message: String::from("Test commit message") };
        let command_future = commander.command(commit_command, output);
        execute_command_future(command_future);

        let head_ref = working_copy.read().unwrap().get_current_head_ref()
            .expect("Unable to get new head revision")
            .expect("No head revision existing");

        let working_copy = working_copy.read().unwrap();
        let mut log = working_copy.get_log(&head_ref)
            .expect("Unable to get log for workspace");
        let ReferencedCommit(_, head_commit) = log.next()
            .expect("No head commit available")
            .expect("Error while retrieving head commit");
        assert_eq!("Test commit message", head_commit.message.as_str());
    }

    #[test]
    fn switch_between_branches() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));

        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        // Create first test commit
        let mut file1 =
            working_copy.write().unwrap().open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);

        let first_commit = working_copy.write().unwrap()
            .commit("Commit to first branch")
            .expect("Unable to create first commit");

        // Create branch "feature"
        let create_branch_command = Command::CreateBranch { name: String::from("feature") };
        let create_branch_future = commander.command(create_branch_command, io::sink());
        execute_command_future(create_branch_future);

        // Check if we still track "master"
        let current_branch_command = Command::CurrentBranch { target_branch: None };
        let current_branch_future =
            commander.command(current_branch_command,Cursor::new(Vec::<u8>::new()));
        let output = execute_command_future(current_branch_future).into_inner();
        assert_eq!(format!("master checked out at {}", first_commit).as_bytes(), output.as_slice());

        // Switch to branch "feature"
        let current_branch_command = Command::CurrentBranch { target_branch: Some(String::from("feature")) };
        let current_branch_future =
            commander.command(current_branch_command,Cursor::new(Vec::<u8>::new()));
        let output = execute_command_future(current_branch_future).into_inner();
        assert_eq!(format!("feature checked out at {}", first_commit).as_bytes(), output.as_slice());

        // Check if switch is reflected
        let current_branch_command = Command::CurrentBranch { target_branch: None };
        let current_branch_future =
            commander.command(current_branch_command,Cursor::new(Vec::<u8>::new()));
        let output = execute_command_future(current_branch_future).into_inner();
        assert_eq!(format!("feature checked out at {}", first_commit).as_bytes(), output.as_slice());

        // Add a commit with a second file to the branch "feature"
        let mut file2 =
            working_copy.write().unwrap().open_file(Path::new("file2.txt"), true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        drop(file2);

        let second_commit = working_copy.write().unwrap().commit("Commit to second branch")
            .expect("Unable to create second commit");

        // Switch back to master
        working_copy.write().unwrap().switch_branch(Some("master")).unwrap();
        working_copy.write().unwrap().update_head().unwrap();

        // ...and check if the second file is gone
        assert!(working_copy.write().unwrap().open_file(Path::new("file2.txt"), false).is_err());

        // Switch back to feature branch
        working_copy.write().unwrap().switch_branch(Some("feature")).unwrap();
        working_copy.write().unwrap().update_head().unwrap();

        // Check if all committed files are present and contain the correct contents
        let mut file1 =
            working_copy.write().unwrap().open_file(Path::new("file1.txt"), false)
                .expect("Unable to open first test file");
        let mut content = Vec::new();
        file1.read_to_end(&mut content)
            .expect("Unable to read from first file");
        assert_eq!(b"Test 1", content.as_slice());

        let mut file2 =
            working_copy.write().unwrap().open_file(Path::new("file2.txt"), false)
                .expect("Unable to open second test file");
        let mut content = Vec::new();
        file2.read_to_end(&mut content)
            .expect("Unable to read from second file");
        assert_eq!(b"Test 2", content.as_slice());
    }

    #[test]
    fn fetch_empty_log() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        let log_command = Command::Log { start_commit: None };
        let log_future = commander.command(log_command, Cursor::<Vec<u8>>::default());
        execute_command_future(log_future);
    }

    #[test]
    fn revert_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        let mut file1 =
            working_copy.write().unwrap().open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());

        let revert_command = Command::Revert { path_glob: String::from("file1.txt"), dry_run: false };
        let revert_future = commander.command(revert_command, io::sink());
        execute_command_future(revert_future);

        assert_eq!(0, working_copy.read().unwrap().get_status().unwrap().count());
    }

    #[test]
    fn revert_nonexistent_file() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        let mut file1 =
            working_copy.write().unwrap().open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());

        let revert_command = Command::Revert { path_glob: String::from("file2.txt"), dry_run: false };
        let revert_future = commander.command(revert_command, io::sink());
        execute_command_future(revert_future);

        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());
    }

    #[test]
    fn revert_directory() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let commander = CommandExecutor::new(Arc::clone(&working_copy));

        let mut file1 =
            working_copy.write().unwrap().open_file(Path::new("file1.txt"), true)
                .expect("Unable to open test file");
        file1.write(b"Test 1")
            .expect("Unable to write to test file");
        drop(file1);
        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());

        let mut file2 =
            working_copy.write().unwrap().open_file(Path::new("dir/file2.txt"), true)
                .expect("Unable to open test file");
        file2.write(b"Test 2")
            .expect("Unable to write to test file");
        drop(file2);
        assert_eq!(2, working_copy.read().unwrap().get_status().unwrap().count());

        let revert_command = Command::Revert { path_glob: String::from("file1.txt"), dry_run: false };
        let revert_future = commander.command(revert_command, io::sink());
        execute_command_future(revert_future);
        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());

        let revert_command = Command::Revert { path_glob: String::from("dir/*"), dry_run: false };
        let revert_future = commander.command(revert_command, io::sink());
        execute_command_future(revert_future);
        assert_eq!(0, working_copy.read().unwrap().get_status().unwrap().count());
    }
}
