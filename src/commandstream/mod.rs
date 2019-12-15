use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    io,
    net::Shutdown,
    borrow::Cow,
    fmt::Display,
    pin::Pin
};

use crate::{
    cache::{CacheRef, ReferencedCommit},
    overlay::{self, Overlay, WorkspaceController},
    types::RepoRef
};

use futures::{
    future::{self, AbortHandle},
    Future, FutureExt, Stream, StreamExt, TryStreamExt,
    task::Poll,
    task::Context,
    stream
};
use failure::{self, Fallible, format_err};
use serde::{Serialize, Deserialize};
use serde_json;
use glob::Pattern as GlobPattern;
use log::{debug, info, warn, error};
use tokio::{
    self,
    io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt}
};

type InOutResult<T, E, U = T> = Result<U, (T, E)>;
type InOutFallible<T, U = T> = Result<U, (T, failure::Error)>;

struct LogStreamEmitter<S> {
    stream: S,
    error_encountered: bool
}

impl<S> LogStreamEmitter<S> {
    fn new(stream:S) -> Self {
        Self {
            stream,
            error_encountered: false
        }
    }
}

impl<S> Stream for LogStreamEmitter<S> where S: Stream<Item=Result<ReferencedCommit, overlay::Error>> {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if !self.error_encountered {
            let stream = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) =>
                    Poll::Ready(Some(commit.to_string())),
                Poll::Ready(Some(Err(err))) => {
                    unsafe {
                        self.get_unchecked_mut().error_encountered = true;
                    }
                    warn!("Error during log streaming: {}", err);
                    Poll::Ready(Some(format!("ERROR: Unable to retrieve log entry: {}", err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending
            }
        } else {
            Poll::Ready(None)
        }
    }
}

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

    Diff,
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

    pub async fn command<T: AsyncWrite+Send+Sync+'static+Unpin>(&self, command: Command, target: T)
        -> InOutFallible<T> {
        match command {
            Command::Commit { message } => self.commit_handler(message, target).await,

            Command::Log { start_commit } =>
                self.handle_log_stream(start_commit, target).await,

            Command::Status => self.handle_status(target).await,

            Command::CurrentBranch { target_branch } =>
                self.handle_switch(target_branch, target).await,

            Command::CreateBranch {
                name
            } => {
                let create_result =
                    self.repo.write().unwrap().create_branch(name.as_str(), None)
                        .map(|_| format!("Successfully created branch {}", name.as_str()));
                Self::send_result_string(
                    target, create_result, "Failed to create branch").await
            }

            Command::Revert {
                path_glob,
                dry_run
            } => self.handle_revert(target, path_glob, dry_run).await,

            Command::Diff => {
                let iter = match crate::utility::diff::WorkspaceDiffIter::new(Arc::clone(&self.repo)) {
                    Ok(iter) => iter,
                    Err(e) => return Err((target, e))
                };
                let target = iter.fold(target, move |target, diff|
                    Self::send_result_string::<failure::Error, _, _, _>(
                        target, diff, "Unable to display file diff")
                        .map(|result|
                            match result {
                                Ok(target) => target,
                                Err((target, e)) => {
                                    warn!("Unable to send diff to client: {}", e);
                                    target
                                }
                            }))
                    .await;
                Ok(target)
            }
        }
    }

    async fn handle_switch<T>(&self, target_branch: Option<String>, target: T)
        -> InOutFallible<T> where T: AsyncWrite+Send+Sync+'static+Unpin {
        let branch_status = match target_branch {
            Some(target_branch_name) =>
                self.repo.write().unwrap()
                    .switch(RepoRef::Branch(&target_branch_name))
                    .map(move |cache_ref|
                        (Cow::Owned(target_branch_name), cache_ref))
                    .map_err(failure::Error::from),

            None => {
                let current_branch = self.repo.read().unwrap().get_current_branch()
                    .map(|option| option.map(Cow::into_owned));
                match current_branch {
                    Ok(Some(name)) =>
                        self.repo.write().unwrap().update_head()
                            .map(|cache_ref|
                                (Cow::Owned(name), cache_ref))
                            .map_err(failure::Error::from),
                    Ok(None) =>
                        self.repo.read().unwrap().get_current_head_ref()
                            .map(|cache_ref| {
                                let cache_ref = cache_ref
                                    .unwrap_or_else(CacheRef::null);
                                (Cow::Borrowed(&"(detached)"[..]), cache_ref)
                            })
                            .map_err(failure::Error::from),
                    Err(e) => Err(format_err!("Error retrieving current branch: {}", e))
                }
            }
        };

        let string = branch_status.map(|(branch_name, cache_ref)|
            format!("{} checked out at {}", branch_name, cache_ref));
        Self::send_result_string(target, string, "Unable to retrieve workspace status")
            .await
    }

    async fn handle_revert<T>(&self, target: T, path_glob: String, dry_run: bool)
        -> InOutFallible<T> where T: AsyncWrite+Send+Sync+'static+Unpin {
        let matching_paths = self.repo.read().unwrap().get_status()
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
                        self.repo.write().unwrap().revert_file(matching_path.as_path())
                            .map_err(failure::Error::from)?
                    }
                }

                Ok(matching_paths)
            });

            match revert_result {
                Ok(matching_paths) =>
                    stream::iter(matching_paths.into_iter().map(Ok))
                        .try_fold(target, |target: T, matching_path: PathBuf| {
                            async move {
                                let message = format!("Reverting {}\n", matching_path.display());
                                Self::send_string::<failure::Error, _, _>(target, &message).await
                            }
                        })
                        .map(|result| match result {
                            Ok(target) => Ok(target),
                            Err((target, e)) => Err((target, e))
                        }).await,
                Err(e) => {
                    warn!("Revert failed: {}", e);
                    let string = format!("ERROR: Unable to execute revert operation: {}", e);
                    Self::send_string(target, string).await
                }
            }
    }

    async fn send_string<E, T, S>(mut target: T, string: S) -> InOutResult<T, E>
        where T: AsyncWrite+Send+Sync+'static+Unpin,
              S: AsRef<str>,
              E: From<io::Error> {
        match target.write_all(string.as_ref().as_bytes()).await {
            Ok(_) => Ok(target),
            Err(e) => Err((target, E::from(e)))
        }
    }

    async fn send_result_string<E, S, T, F>(target: T, string: Result<S, F>, error_prefix: &str)
        -> InOutResult<T, E> where T: AsyncWrite+Send+Sync+'static+Unpin,
                                   S: Display,
                                   E: From<io::Error>,
                                   F: Display {
        let message = match string {
            Ok(string) => string.to_string(),
            Err(err) => {
                warn!("Command operation failed: {}: {}", error_prefix, err);
                format!("ERROR: {}: {}", error_prefix, err)
            }
        };
        Self::send_string(target, message).await
    }

    async fn handle_status<T: AsyncWrite+Send+Sync+'static+Unpin>(&self, target: T)
        -> InOutFallible<T> {
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

        Self::send_result_string(
            target,
            status_string,
            "Failed to retrieve status").await
    }

    async fn handle_log_stream<T>(&self, start_commit: Option<CacheRef>, target: T)
        -> InOutFallible<T> where T: AsyncWrite+Send+Sync+'static+Unpin {
        static NO_LOG_STRING: &str = "(empty log)";

        let start_commit = match start_commit {
            Some(start_commit) => Ok(Some(start_commit)),
            None => self.repo.read().unwrap().get_current_head_ref()
                .map_err(failure::Error::from)
        };

        match start_commit {
            Ok(start_commit) => {
                match start_commit {
                    Some(start_commit) => {
                        let log_stream = LogStreamEmitter::new(
                            self.repo.read().unwrap().get_log_stream(start_commit));
                        log_stream
                            .map(Result::<_, (_, failure::Error)>::Ok)
                            .try_fold(target, |target, mut message| {
                                async move {
                                    message.push('\n');
                                    Self::send_string(target, &message).await
                                }
                            })
                            .await
                    }

                    None => Self::send_string(target, NO_LOG_STRING).await,
                }
            }

            Err(e) => {
                warn!("Unable to get start commit for log listing: {}", e);
                let string = format!("ERROR: Unable to get start commit for log listing: {}", e);
                Self::send_string(target, string).await
            }
        }
    }

    async fn commit_handler<T>(&self, message: String, target: T) -> InOutFallible<T>
        where T: AsyncWrite+Send+Sync+'static+Unpin {
        let commit_result = self.repo.write().unwrap()
            .commit(message.as_str());
        let string = commit_result.map(|cache_ref|
            format!("Successfully committed, HEAD at {}", cache_ref));
        Self::send_result_string(target, string, "Failed to commit").await
    }
}

async fn execute_single_command<R, C>(channel: C, executor: CommandExecutor<R>) -> Fallible<()>
    where R: Overlay+for<'a> WorkspaceController<'a>+'static,
          C: AsyncRead+AsyncWrite+Send+Sync+'static {
    let (mut input, output) = tokio::io::split(channel);
    let mut command_string = Vec::new();
    input.read_to_end(&mut command_string).await?;
    let command = serde_json::from_slice(&command_string[..])?;
    debug!("Received command {:?}", command);
    executor.command(command, output).await
        .map(|_| ())
        .map_err(|(_, e)| e)
}

pub async fn execute_commands<R, C>(channel: C, executor: CommandExecutor<R>)
    where R: Overlay+for<'a> WorkspaceController<'a>+'static+Send+Sync,
          C: AsyncRead+AsyncWrite+Send+Sync+'static {
    if let Err(e) = execute_single_command(channel, executor).await {
        warn!("Error processing command: {}", e);
    }
}

#[cfg(target_os = "linux")]
pub fn create_command_socket<P, R>(path: P, command_executor: CommandExecutor<R>)
    -> (impl Future<Output=()>, AbortHandle)
    where P: AsRef<Path>,
          R: Overlay+for<'a> WorkspaceController<'a>+Send+Sync {
    debug!("Creating command socket at {}", path.as_ref().display());

    let mut listener = tokio::net::UnixListener::bind(&path)
        .unwrap_or_else(|_| panic!("Unable to bind UDS listener {}", path.as_ref().display()));

    let stream_future = async move {
        let mut incoming = listener.incoming();
        loop {
            match incoming.next().await.unwrap() {
                Ok(connection) => {
                    tokio::spawn(execute_commands(connection, command_executor.clone()));
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::BrokenPipe {
                        error!("Error during command socket processing: {}", e);
                    } else {
                        debug!("Broken pipe during command socket processing: {}", e);
                    }
                }
            }
        }
    };

    let (abortable, abort_handle) = future::abortable(stream_future);
    let abortable = abortable.map(|_| ());
    
    (abortable, abort_handle)
}

#[cfg(target_os = "linux")]
async fn find_command_stream() -> Result<tokio::net::UnixStream, io::Error> {
    let cwd = std::env::current_dir()?;

    let mut dork_root = cwd.as_path();
    if dork_root.ends_with(".dork") {
        dork_root = dork_root.parent().unwrap();
    }

    let path = loop {
        debug!("Looking for command socket in {}", dork_root.display());

        let command_socket_path = dork_root.join(".dork/cmd");
        if command_socket_path.exists() {
            info!("Using {} as command socket path", command_socket_path.display());
            break command_socket_path;
        }

        match dork_root.parent() {
            Some(parent) => dork_root = parent,
            None => return Err(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("Unable to find a mounted dork file system at {}",
                            cwd.display())))
        }
    };

    tokio::net::UnixStream::connect(path).await
}

#[cfg(not(target_os = "linux"))]
async fn find_command_stream() -> Result<tokio::fs::File, io::Error> {
    Err(io::Error::new(
        io::ErrorKind::Other,
        "dorkfs only supports Linux. Bummer."))
}

async fn send_command_impl(command: Command) -> io::Result<()> {
    let command_string = serde_json::to_string(&command)?;
    let mut stream = find_command_stream().await?;
    if let Err(e) = stream.write_all(command_string.as_bytes()).await {
        error!("Unable to send command t daemon: {}", e);
        return Ok(())
    }
    #[cfg(target_os = "linux")] {
        tokio::net::UnixStream::shutdown(&stream, Shutdown::Write)?;
    }
    tokio::io::copy(&mut stream, &mut tokio::io::stdout()).await?;
    Ok(())
}

pub async fn send_command(command: Command) {
    if let Err(e) = send_command_impl(command).await {
        if e.kind() != io::ErrorKind::BrokenPipe {
            error!("Unable to communicate with daemon: {}", e);
        } else {
            debug!("Broken pipe during result reception");
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Write, Cursor, self},
        path::Path, fmt::Debug,
        sync::{Arc, RwLock}
    };

    use crate::{
        overlay::{Overlay, WorkspaceController},
        overlay::testutil::{check_file_content, open_working_copy},
        cache::ReferencedCommit,
        types::RepoRef
    };

    use tempfile::tempdir;
    use futures::{
        AsyncWrite, Future, FutureExt, TryFutureExt,
    };
    use tokio;

    use super::{Command, CommandExecutor};

    fn execute_command_future<T, E, F>(command_future: F) -> T
        where T: AsyncWrite+Send+Sync+'static,
              E: Debug,
              F: Future<Output=Result<T, E>>+Send {
        futures::executor::block_on(command_future)
            .expect("Unable to finish future in test")
    }

    #[test]
    fn commit() {
        crate::init_logging();

        let dir = tempdir().expect("Unable to create temp test directory");
        let working_copy = Arc::new(RwLock::new(open_working_copy(&dir)));
        let output = Vec::<u8>::new();
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
        let create_branch_future = commander.command(create_branch_command, Vec::<u8>::new());
        execute_command_future(create_branch_future);

        // Check if we still track "master"
        let current_branch_command = Command::CurrentBranch { target_branch: None };
        let current_branch_future =
            commander.command(current_branch_command,Vec::<u8>::new());
        let output = execute_command_future(current_branch_future);
        assert_eq!(format!("master checked out at {}", first_commit).as_bytes(), output.as_slice());

        // Switch to branch "feature"
        let current_branch_command = Command::CurrentBranch { target_branch: Some(String::from("feature")) };
        let current_branch_future =
            commander.command(current_branch_command,Vec::<u8>::new());
        let output = execute_command_future(current_branch_future);
        assert_eq!(format!("feature checked out at {}", first_commit).as_bytes(), output.as_slice());

        // Check if switch is reflected
        let current_branch_command = Command::CurrentBranch { target_branch: None };
        let current_branch_future =
            commander.command(current_branch_command,Vec::<u8>::new());
        let output = execute_command_future(current_branch_future);
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
        working_copy.write().unwrap().switch(RepoRef::Branch("master")).unwrap();

        // ...and check if the second file is gone
        assert!(working_copy.write().unwrap().open_file(Path::new("file2.txt"), false).is_err());

        // Switch back to feature branch
        working_copy.write().unwrap().switch(RepoRef::Branch("feature")).unwrap();

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
        let log_future = commander.command(log_command, Vec::<u8>::new());
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
        let revert_future = commander.command(revert_command, Vec::<u8>::new());
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
        let revert_future = commander.command(revert_command, Vec::<u8>::new());
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
        let revert_future = commander.command(revert_command, Vec::<u8>::new());
        execute_command_future(revert_future);
        assert_eq!(1, working_copy.read().unwrap().get_status().unwrap().count());

        let revert_command = Command::Revert { path_glob: String::from("dir/*"), dry_run: false };
        let revert_future = commander.command(revert_command, Vec::<u8>::new());
        execute_command_future(revert_future);
        assert_eq!(0, working_copy.read().unwrap().get_status().unwrap().count());
    }
}
